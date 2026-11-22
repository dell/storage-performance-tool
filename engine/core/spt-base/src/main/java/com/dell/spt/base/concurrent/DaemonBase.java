package com.dell.spt.base.concurrent;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.logging.Logger;
import org.apache.logging.log4j.Level;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

/**
* Modifies the async runnable to make sure that all instances are closed even if a user hits ^C
*/
public abstract class DaemonBase extends AsyncRunnableBase implements Daemon {

	private static final Logger LOG = Logger.getLogger(DaemonBase.class.getSimpleName());
	private static final Queue<WeakReference<DaemonBase>> REGISTRY = new ConcurrentLinkedQueue<>();
	private static final ThreadLocal<ConstructionScope> OPEN_SCOPE = new ThreadLocal<>();

	private final WeakReference<DaemonBase> daemonRef;
	private final AtomicBoolean registered = new AtomicBoolean();
	/** Its construction failed: {@link #closeAll()} never closes it. */
	private volatile boolean abandoned;
	/**
	 * Built inside a construction scope that has not yet closed as succeeded, or one that failed:
	 * {@link #closeAll()} skips it.
	 */
	private volatile boolean constructing;

	/** Registers this daemon for {@link #closeAll()} at once, before a subclass's fields are set. */
	protected DaemonBase() {
		this(false);
	}

	/**
	 * With {@code deferRegistration}, the subclass calls {@link #registerForCloseAll()} once its own
	 * fields are set, so the exit hook's {@link #closeAll()} cannot run a lifecycle method on a partly
	 * built object; until then it does not reach this daemon.
	 */
	protected DaemonBase(final boolean deferRegistration) {
		daemonRef = new WeakReference<>(this);
		final var scope = OPEN_SCOPE.get();
		if (scope != null) {
			constructing = true;
			scope.daemons.add(this);
		}
		if (!deferRegistration) {
			registerForCloseAll();
		}
	}

	/**
	 * Lets {@link #closeAll()} close this daemon. Call it as the last statement of the constructor that
	 * deferred registration; a second call does nothing, and so does a call once the registration was
	 * abandoned or the daemon closed.
	 */
	protected final void registerForCloseAll() {
		if (!abandoned && !isClosed() && registered.compareAndSet(false, true)) {
			REGISTRY.add(daemonRef);
			if (abandoned || isClosed()) {
				REGISTRY.remove(daemonRef);
			}
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
				} else {
					for (final var daemon : daemons) {
						if (!daemon.abandoned) {
							daemon.constructing = false;
						}
					}
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
				warnReleaseFailed(cause);
			} finally {
				daemon.abandonRegistration();
			}
		}

		/** A log handler that throws must not stop the release of the other daemons. */
		private static void warnReleaseFailed(final Throwable cause) {
			try {
				LOG.log(java.util.logging.Level.WARNING, "Failed to release a daemon whose construction failed", cause);
			} catch (final Throwable logFailure) {
				if (logFailure != cause) {
					cause.addSuppressed(logFailure);
				}
			}
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
	 * A {@link #closeAll()} that already holds the entry skips it too.
	 */
	protected final void abandonRegistration() {
		// Set first, and constructing left as it is: a closeAll() that read the entry before the remove
		// must still see a daemon it may not close.
		abandoned = true;
		REGISTRY.remove(daemonRef);
	}

	@Override
	public final void close() throws IOException {
		try {
			super.close();
		} finally {
			if (isClosed()) {
				REGISTRY.remove(daemonRef);
			}
		}
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

	/** Neither still in its construction scope, which releases it if the construction fails, nor abandoned. */
	private boolean closeableByCloseAll() {
		return !abandoned && !constructing;
	}

	public static void closeAll() {
		InterruptedException ex = null;
		synchronized (REGISTRY) {
			// A close waits for a start in progress, so every start is told to end before the first close.
			for (final var daemonRef : REGISTRY) {
				final var daemon = daemonRef.get();
				if (daemon != null && daemon.closeableByCloseAll()) {
					daemon.interruptStarter();
				}
			}
			// Each entry is removed once it is handled. One still in its construction scope stays, so a later
			// closeAll() reaches it once it is built, and so does one registered after this loop passed.
			for (final var it = REGISTRY.iterator(); it.hasNext();) {
				final var daemonRef = it.next();
				final var daemon = daemonRef.get();
				if (daemon != null && daemon.constructing && !daemon.abandoned) {
					continue;
				}
				try {
					if (daemon != null && daemon.closeableByCloseAll() && !daemon.isClosed()) {
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
				} finally {
					it.remove();
				}
			}
		}
		if (ex != null) {
			throwUnchecked(ex);
		}
	}
}
