package com.dell.spt.base.control;

import com.dell.spt.base.control.run.RunServlet;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.svc.Service;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns the ordered node-shutdown transition: stop and join the active API run,
 * then close node services so {@code Main.runNode()} can begin terminal API
 * linger. The extension classloader remains open until the coordinator and
 * linger have both completed.
 */
public final class NodeShutdownCoordinator {

	static final long ACTIVE_RUN_STOP_TIMEOUT_SECONDS = 10;

	private final RunServlet runServlet;
	private final List<Service> services;
	private final AtomicBoolean shutdownStarted = new AtomicBoolean();

	public NodeShutdownCoordinator(final RunServlet runServlet, final List<Service> services) {
		this.runServlet = runServlet;
		this.services = List.copyOf(Objects.requireNonNull(services, "services"));
	}

	/**
	 * Compatibility constructor for callers which only own services and have no
	 * API-run executor.
	 */
	public NodeShutdownCoordinator(final List<Service> services) {
		this(null, services);
	}

	/** Starts the ordered shutdown at most once. */
	public void shutdown() {
		if (!shutdownStarted.compareAndSet(false, true)) {
			return;
		}

		var interrupted = false;
		try {
			if (runServlet != null
							&& !runServlet.stopActiveRunAndAwait(
											ACTIVE_RUN_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				Loggers.ERR.warn(
								"Active API run did not stop within {} s; closing node services",
								ACTIVE_RUN_STOP_TIMEOUT_SECONDS);
			}
		} catch (final InterruptedException e) {
			interrupted = true;
			Loggers.ERR.warn("Interrupted while waiting for the active API run to stop");
		} finally {
			closeServices();
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private void closeServices() {
		for (final Service service : services) {
			try {
				service.close();
			} catch (final Throwable cause) {
				Loggers.ERR.warn("Service close failed", cause);
			}
		}
	}
}
