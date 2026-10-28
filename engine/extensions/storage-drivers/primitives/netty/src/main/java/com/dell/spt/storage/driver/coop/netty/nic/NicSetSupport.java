package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.EndpointResolver;
import com.dell.spt.base.nic.Ipv4;
import com.dell.spt.base.nic.NicInventory;
import com.dell.spt.base.nic.NicPlanner;
import com.dell.spt.base.nic.RdmaDeviceProbe;
import com.dell.spt.base.nic.SysfsInventoryReader;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool.OpenNic;
import io.netty.channel.EventLoopGroup;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;

/**
 * The start of a driver with a NIC set, shared by its drivers: it reads the host's NICs, plans the
 * NIC set and its Paths for the driver's endpoints, creates one event-loop group per chosen NIC,
 * opens the {@link NicSetConnectionPool} on them and probes every Path before the first operation
 * (items 7a, 9 and 9a). A plan failure fails the step as a configuration error and a probe
 * failure as an execution error, both naming the NIC and the endpoint; the groups made so far are
 * shut down first.
 */
public final class NicSetSupport {

	/** Reads the host's NIC inventory. */
	@FunctionalInterface
	public interface InventorySource {
		NicInventory read() throws IOException;
	}

	/** Creates one NIC's event-loop group; the driver base's {@code createIoGroup}. */
	@FunctionalInterface
	public interface IoGroupFactory {
		EventLoopGroup create(int threads, String name);
	}

	/**
	 * Where a driver's NIC set comes from. The product reads sysfs, resolves with the system
	 * resolver and binds to the device; tests substitute a fixture inventory and the source-address
	 * binding on loopback.
	 */
	public record Environment(InventorySource inventory, EndpointResolver resolver, NicBinding binding) {

		/** The host's sysfs, the system resolver and the device binding. RDMA attributes are not read. */
		public static Environment system() {
			return new Environment(SysfsInventoryReader.system(RdmaDeviceProbe.NONE)::read, EndpointResolver.SYSTEM,
							NicBinding.DEVICE);
		}
	}

	private final String driver;
	private final NicSetSettings settings;
	private final Environment environment;
	private final AtomicBoolean setupFailureLogged = new AtomicBoolean();
	private volatile NicSetLayout layout;

	/** @param driver the driver's name in log lines */
	public NicSetSupport(final String driver, final NicSetSettings settings, final Environment environment) {
		this.driver = driver;
		this.settings = settings;
		this.environment = environment;
	}

	/**
	 * Plans the NIC set for {@code endpoints}, the driver's {@code host:port} strings, creates its
	 * groups, opens {@code pool} on them and probes every Path.
	 *
	 * @param shutdownGroups shuts down every group {@code groups} made, on a failure
	 * @throws RunTerminalException when the host has no plan for the setting or a Path does not
	 *     connect
	 */
	public void open(final NicSetConnectionPool pool, final List<String> endpoints, final int concurrencyLimit,
					final IoGroupFactory groups, final Runnable shutdownGroups) throws InterruptedException {
		final NicInventory inventory;
		try {
			inventory = environment.inventory().read();
		} catch (final IOException e) {
			throw new RunTerminalException(IntegrityTerminalException.Category.EXECUTION,
							"Cannot read this host's network interfaces for storage.net.interfaces: " + e.getMessage(), e);
		}
		final var plan = new NicPlanner(environment.resolver()).plan(inventory, settings.interfaces(), endpoints, false);
		plan.warnings().forEach(warning -> Loggers.MSG.warn("{}: {}", driver, warning.fix() == null
						? warning.text()
						: warning.text() + " (" + warning.fix() + ")"));
		if (!plan.ok()) {
			throw new RunTerminalException(IntegrityTerminalException.Category.CONFIGURATION,
							"storage.net.interfaces=" + String.join(",", settings.interfaces()) + " has no plan on this host: "
											+ NicSetLayout.failureMessage(plan),
							null);
		}
		final var planned = NicSetLayout.from(plan);
		try {
			final var nics = new ArrayList<OpenNic>(planned.nics().size());
			for (final var nic : planned.nics()) {
				nics.add(new OpenNic(nic, groups.create(settings.threadsPerNic(), nic.name())));
			}
			pool.open(nics, environment.binding(), settings.connectTimeoutMillis(), concurrencyLimit);
			final var failures = pool.probe(settings.probeTimeoutMillis(), (nic, destination) -> {
				final int address = Ipv4.parse(destination.getHostAddress());
				return inventory.routes().stream().anyMatch(route -> route.iface().equals(nic) && route.covers(address));
			});
			if (!failures.isEmpty()) {
				throw new RunTerminalException(IntegrityTerminalException.Category.EXECUTION,
								"storage.net.interfaces: " + String.join("; ", failures), null);
			}
		} catch (final RuntimeException | InterruptedException | Error e) {
			pool.close();
			shutdownGroups.run();
			throw e;
		}
		this.layout = planned;
	}

	/**
	 * Pre-opens connections when the concurrency limit is unlimited, one per Path, which the base
	 * start does not (it pre-opens only a positive limit), and logs each NIC.
	 */
	public void started(final NicSetConnectionPool pool, final int concurrencyLimit) throws InterruptedException {
		if (concurrencyLimit <= 0) {
			pool.preOpen(concurrencyLimit, settings.probeTimeoutMillis());
		}
		for (final var state : pool.state()) {
			final var nic = layout.nics().stream().filter(n -> n.name().equals(state.nic())).findFirst().orElseThrow();
			Loggers.MSG.info("{}: NIC {} ({}) with {} I/O workers, open limit {}, Paths to {}", driver, nic.name(),
							nic.address().getHostAddress(), settings.threadsPerNic(),
							state.openLimit() > 0 ? state.openLimit() : "unlimited",
							nic.paths().stream().map(NicSetLayout.PathSpec::endpoint).collect(Collectors.joining(", ")));
		}
	}

	/** The layout the driver started with, or {@code null} before a successful open. */
	public NicSetLayout layout() {
		return layout;
	}

	/**
	 * Logs a request attempt's setup failure: the first at WARN, the rest and those after stop at
	 * DEBUG, as a back-off can fail many attempts in a row.
	 */
	public void logSetupFailure(final Throwable cause, final boolean stopping) {
		if (!stopping && setupFailureLogged.compareAndSet(false, true)) {
			LogUtil.exception(Level.WARN, cause,
							"{}: a NIC-set Path failed to set up a request; further failures are logged at DEBUG", driver);
		} else {
			LogUtil.exception(Level.DEBUG, cause, "{}: a NIC-set Path failed to set up a request", driver);
		}
	}
}
