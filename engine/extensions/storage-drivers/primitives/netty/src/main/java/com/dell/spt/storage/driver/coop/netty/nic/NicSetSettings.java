package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.base.config.EndpointSelectionConfig;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.Transport;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriverBase;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import com.github.akurilov.confuse.Config;
import io.netty.channel.epoll.Epoll;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Validated NIC-set settings of one driver, parsed before its construction. Without a NIC set
 * there are none and the driver keeps the 5.16 route.
 *
 * @param interfaces the requested entries, as stored
 * @param connectTimeoutMillis the Path Pools' connect timeout: {@code storage.net.timeoutMilliSec}
 *     when positive, otherwise {@link #DEFAULT_CONNECT_TIMEOUT_MILLIS}
 * @param probeTimeoutMillis the connect timeout of the start-time probe
 * @param threadsPerNic event loops in each NIC's group
 */
public record NicSetSettings(List<String> interfaces, int connectTimeoutMillis, int probeTimeoutMillis, int threadsPerNic) {

	/** The connect timeout when {@code storage.net.timeoutMilliSec} is not positive. */
	public static final int DEFAULT_CONNECT_TIMEOUT_MILLIS = 30_000;

	/** The probe never waits longer than this per Path. */
	public static final int MAX_PROBE_TIMEOUT_MILLIS = 10_000;

	/**
	 * Without a concurrency limit, the attempts per NIC that may connect or wait for a connection at
	 * once. It bounds the connects to one failing Path at P5's storm bound, while in-flight requests
	 * stay unbounded.
	 */
	public static final int UNLIMITED_WAITING_LIMIT = 8;

	public NicSetSettings {
		interfaces = List.copyOf(interfaces);
	}

	/**
	 * Parses the settings, or returns empty without a NIC set.
	 *
	 * @throws IllegalConfigurationException for a transport that cannot bind to a device (D31), a
	 *     non-default endpoint mode (D14) or {@code storage.net.node.connAttemptsLimit} above 0
	 */
	public static Optional<NicSetSettings> fromStorage(final Config storageConfig) {
		final var interfaces = NicSetConfig.interfaces(storageConfig);
		if (interfaces.isEmpty()) {
			return Optional.empty();
		}
		final var selection = EndpointSelectionConfig.selection(storageConfig);
		if (!EndpointSelectionConfig.DEFAULT_SELECTION.equals(selection)) {
			throw new IllegalConfigurationException("storage.net.interfaces cannot be combined with "
							+ "storage.net.endpoint.selection=" + selection + " (--endpoint-selection); use the default selection");
		}
		final var netConfig = storageConfig.configVal("net");
		final var transport = NettyStorageDriverBase.resolveTransport(netConfig);
		if (transport != Transport.EPOLL) {
			throw new IllegalConfigurationException("storage.net.interfaces needs the epoll transport to bind connections "
							+ "to a NIC, but storage.net.transport is " + transport.name().toLowerCase(Locale.ROOT));
		}
		if (!Epoll.isAvailable()) {
			throw new IllegalConfigurationException("storage.net.interfaces needs the epoll transport to bind connections "
							+ "to a NIC, but its native library did not load on this host: " + Epoll.unavailabilityCause());
		}
		final var connAttemptsLimit = netConfig.intVal("node-connAttemptsLimit");
		if (connAttemptsLimit > 0) {
			throw new IllegalConfigurationException("storage.net.node.connAttemptsLimit=" + connAttemptsLimit
							+ " cannot be combined with storage.net.interfaces, whose Paths back off instead; set it to 0");
		}
		final var timeout = netConfig.intVal("timeoutMilliSec");
		final var connectTimeout = timeout > 0 ? timeout : DEFAULT_CONNECT_TIMEOUT_MILLIS;
		final var threads = storageConfig.intVal("driver-threads");
		return Optional.of(new NicSetSettings(
						interfaces,
						connectTimeout,
						Math.min(connectTimeout, MAX_PROBE_TIMEOUT_MILLIS),
						threads >= 1 ? threads : defaultThreadsPerNic()));
	}

	/** Today's default group size, {@code max(4, hardware threads / 8)}, for each NIC (D49). */
	static int defaultThreadsPerNic() {
		return Math.max(4, ThreadUtil.getHardwareThreadCount() / 8);
	}

	/**
	 * The open-connection limit of NIC {@code index} of {@code nics} for the driver's concurrency
	 * limit: its share {@code floor(P/N)}, plus one for the first {@code P mod N} NICs, when
	 * {@code P >= N}; one connection when {@code 0 < P < N}; none (0) when unlimited.
	 */
	public static int openLimit(final int concurrencyLimit, final int nics, final int index) {
		if (concurrencyLimit <= 0) {
			return 0;
		}
		if (concurrencyLimit < nics) {
			return 1;
		}
		return concurrencyLimit / nics + (index < concurrencyLimit % nics ? 1 : 0);
	}

	/**
	 * The attempts NIC {@code index} of {@code nics} admits at once: its open limit when the
	 * concurrency limit is positive, counted until each attempt ends, and otherwise
	 * {@link #UNLIMITED_WAITING_LIMIT}, counted until each attempt's wait for a connection ends.
	 */
	public static int admissionLimit(final int concurrencyLimit, final int nics, final int index) {
		return concurrencyLimit <= 0 ? UNLIMITED_WAITING_LIMIT : openLimit(concurrencyLimit, nics, index);
	}

	/**
	 * Whether every one of {@code nics} NICs has its own share of the concurrency limit,
	 * {@code P >= N}. Only then does a completed attempt hand its connection and permit straight to
	 * the next queued operation (D37): in the shared mode every attempt goes through the rotation,
	 * and without a limit there are no shares.
	 */
	public static boolean hasShares(final int concurrencyLimit, final int nics) {
		return concurrencyLimit > 0 && concurrencyLimit >= nics;
	}

	/**
	 * The connections NIC {@code index} pre-opens: its share when {@code P >= N}, one when
	 * {@code 0 < P < N}, and one per Path when unlimited.
	 */
	public static int preOpenCount(final int concurrencyLimit, final int nics, final int index, final int paths) {
		return concurrencyLimit <= 0 ? paths : openLimit(concurrencyLimit, nics, index);
	}
}
