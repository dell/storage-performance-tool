package com.dell.spt.base.concurrent;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.logging.log4j.Level;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

/**
* Modifies the async runnable to make sure that all instances are closed even if a user hits ^C
*/
public abstract class DaemonBase extends AsyncRunnableBase implements Daemon {

	private static final Queue<WeakReference<Daemon>> REGISTRY = new ConcurrentLinkedQueue<>();

	private final WeakReference<Daemon> daemonRef;

	protected DaemonBase() {
		daemonRef = new WeakReference<>(this);
		REGISTRY.add(daemonRef);
	}

	@Override
	public final void close() throws IOException {
		super.close();
		REGISTRY.remove(daemonRef);
	}

	public static void closeAll() {
		InterruptedException ex = null;
		synchronized (REGISTRY) {
			for (final var daemonRef : REGISTRY) {
				final var daemon = daemonRef.get();
				try {
					if (daemon != null && !daemon.isClosed()) {
						daemon.close();
					}
				} catch (final Throwable cause) {
					if (cause instanceof InterruptedException) {
						ex = (InterruptedException) cause;
					}
					// Log4j, not java.util.logging: at JVM exit java.util.logging's own shutdown handling races
					// this hook and loses the line, while Log4j is still up and only queues the event. The class
					// name, not the daemon: its toString() may run on a half-built object.
					LogUtil.trace(Loggers.ERR, Level.WARN, cause, "Failed to close the daemon instance {}",
									daemon.getClass().getName());
				}
			}
			REGISTRY.clear();
		}
		if (ex != null) {
			throwUnchecked(ex);
		}
	}
}
