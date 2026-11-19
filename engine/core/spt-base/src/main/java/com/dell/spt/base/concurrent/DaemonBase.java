package com.dell.spt.base.concurrent;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
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
	private static final ThreadLocal<ConstructionScope> OPEN_SCOPE = new ThreadLocal<>();

	private final WeakReference<Daemon> daemonRef;

	protected DaemonBase() {
		daemonRef = new WeakReference<>(this);
		REGISTRY.add(daemonRef);
		final var scope = OPEN_SCOPE.get();
		if (scope != null) {
			scope.daemons.add(this);
		}
	}

	/**
	 * Records the daemons the calling thread builds until the scope closes. When a constructor throws,
	 * nothing else holds the partly built object, so this is how its resources are still released.
	 * Scopes nest; close them on the opening thread, the latest first.
	 */
	public static ConstructionScope openConstructionScope() {
		final var scope = new ConstructionScope(OPEN_SCOPE.get());
		OPEN_SCOPE.set(scope);
		return scope;
	}

	/** See {@link #openConstructionScope()}. */
	public static final class ConstructionScope implements AutoCloseable {

		private final ConstructionScope parent;
		private final List<DaemonBase> daemons = new ArrayList<>();
		private boolean succeeded;

		private ConstructionScope(final ConstructionScope parent) {
			this.parent = parent;
		}

		/** What this scope recorded stays as it is; an enclosing scope that fails still releases it. */
		public void succeeded() {
			succeeded = true;
		}

		/** Unless {@link #succeeded()} was called, releases the recorded daemons, the latest first. */
		@Override
		public void close() {
			if (parent == null) {
				OPEN_SCOPE.remove();
			} else {
				OPEN_SCOPE.set(parent);
			}
			if (succeeded) {
				if (parent != null) {
					parent.daemons.addAll(daemons);
				}
			} else {
				for (int i = daemons.size() - 1; i >= 0; i--) {
					release(daemons.get(i));
				}
			}
			daemons.clear();
		}

		private static void release(final DaemonBase daemon) {
			try {
				if (!daemon.isClosed()) {
					daemon.releaseAfterFailedConstruction();
				}
			} catch (final Throwable cause) {
				LOG.log(Level.WARNING, "Failed to release a daemon whose construction failed", cause);
			}
			daemon.abandonRegistration();
		}
	}

	/**
	 * Releases what this object acquired when its construction, or the construction around it, failed.
	 * It may run on a partly built object: touch only this class's own fields, null-check each, and
	 * never throw. An override calls {@code super} too. No lifecycle method has run or will run.
	 */
	protected void releaseAfterFailedConstruction() {}

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
