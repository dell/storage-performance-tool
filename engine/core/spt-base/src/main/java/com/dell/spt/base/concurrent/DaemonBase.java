package com.dell.spt.base.concurrent;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

/**
* Modifies the async runnable to make sure that all instances are closed even if a user hits ^C
*/
public abstract class DaemonBase extends AsyncRunnableBase implements Daemon {

	private static final Logger LOG = Logger.getLogger(DaemonBase.class.getSimpleName());
	private static final Queue<WeakReference<Daemon>> REGISTRY = new ConcurrentLinkedQueue<>();

	private final WeakReference<Daemon> daemonRef;

	protected DaemonBase() {
		daemonRef = new WeakReference<>(this);
		REGISTRY.add(daemonRef);
	}

	/**
	 * Takes this daemon out of the registry, so {@link #closeAll()} does not reach it. For a constructor
	 * that fails: what it leaves behind is only partly built, and closing it would read unset fields.
	 */
	protected final void abandonRegistration() {
		REGISTRY.remove(daemonRef);
	}

	@Override
	public final void close() throws IOException {
		super.close();
		REGISTRY.remove(daemonRef);
	}

	/** Whether a registered daemon, still reachable, matches; for tests. */
	static boolean isRegistered(final Predicate<? super Daemon> matcher) {
		for (final var daemonRef : REGISTRY) {
			final var daemon = daemonRef.get();
			if (daemon != null && matcher.test(daemon)) {
				return true;
			}
		}
		return false;
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
					LOG.log(Level.WARNING, "Failed to close the daemon instance", cause);
				}
			}
			REGISTRY.clear();
		}
		if (ex != null) {
			throwUnchecked(ex);
		}
	}
}
