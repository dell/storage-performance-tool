package com.dell.spt.base.load.lifecycle;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Tracks outstanding operation ownership while leaving exactly-once terminal state on each
 * lifecycle-aware operation.
 *
 * <p>A single outstanding registry spans generator, queue, dispatch, and result-delivery state,
 * so steady-state memory and hot-path map churn are bounded by admitted work. Compatibility
 * operations use a weak identity sidecar state map: duplicate callbacks remain exactly-once
 * without aliasing value-equal third-party operations or retaining them for the duration of a run.
 */
public final class OperationLifecycleTracker<O extends Operation<? extends Item>> {
	private static final class IdentityKey<T> {
		private final T value;
		private final int hashCode;

		private IdentityKey(final T value) {
			this.value = value;
			this.hashCode = System.identityHashCode(value);
		}

		@Override
		public boolean equals(final Object obj) {
			return obj instanceof IdentityKey<?> other && value == other.value;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	private static final class WeakIdentityKey<T> extends WeakReference<T> {
		private final int hashCode;

		private WeakIdentityKey(final T value) {
			super(value);
			this.hashCode = System.identityHashCode(value);
		}

		private WeakIdentityKey(final T value, final ReferenceQueue<T> referenceQueue) {
			super(value, referenceQueue);
			this.hashCode = System.identityHashCode(value);
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (!(obj instanceof WeakIdentityKey<?> other)) {
				return false;
			}
			final T value = get();
			return value != null && value == other.get();
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	private static final class CompatibilityState {
		private OperationLifecycleState state = OperationLifecycleState.NEW;
		private long circulation;
		private long explicitDispatchCirculation = -1;
	}

	/** Opaque proof that one exact queued circulation was eligible for dispatch. */
	public static final class DispatchToken<O> {
		private final O operation;
		private final OperationLifecycle lifecycle;
		private final long compatibilityCirculation;

		private DispatchToken(
						final O operation,
						final OperationLifecycle lifecycle,
						final long compatibilityCirculation) {
			this.operation = operation;
			this.lifecycle = lifecycle;
			this.compatibilityCirculation = compatibilityCirculation;
		}
	}

	private static final OperationLifecycleTracker<?> DISABLED = new OperationLifecycleTracker<>(false);

	private final boolean enabled;
	private final Consumer<O> dispatchPublicationObserver;
	private volatile Consumer<O> terminalObserver;
	private final Set<IdentityKey<O>> outstanding = ConcurrentHashMap.newKeySet();
	private final Set<IdentityKey<O>> inFlightOperations = ConcurrentHashMap.newKeySet();
	private final ReferenceQueue<O> compatibilityReferences = new ReferenceQueue<>();
	private final Map<WeakIdentityKey<O>, CompatibilityState> compatibilityStates = new HashMap<>();
	private final LongAdder dispatched = new LongAdder();
	private final AtomicLong inFlight = new AtomicLong();
	private final LongAdder terminal = new LongAdder();
	private final LongAdder unattempted = new LongAdder();
	private final LongAdder unresolved = new LongAdder();
	private final ArrayList<O> unattemptedOperations = new ArrayList<>();
	private final ArrayList<O> unresolvedOperations = new ArrayList<>();

	/** Creates an enabled tracker for one load-step run. */
	public OperationLifecycleTracker() {
		this(true, null);
	}

	private OperationLifecycleTracker(final boolean enabled) {
		this(enabled, null);
	}

	OperationLifecycleTracker(final Consumer<O> dispatchPublicationObserver) {
		this(true, dispatchPublicationObserver);
	}

	private OperationLifecycleTracker(
					final boolean enabled, final Consumer<O> dispatchPublicationObserver) {
		this.enabled = enabled;
		this.dispatchPublicationObserver = dispatchPublicationObserver;
	}

	/** Returns the allocation-free compatibility tracker used by lifecycle-unaware extensions. */
	@SuppressWarnings("unchecked")
	public static <O extends Operation<? extends Item>> OperationLifecycleTracker<O> disabled() {
		return (OperationLifecycleTracker<O>) DISABLED;
	}

	/** Returns whether this tracker records lifecycle transitions. */
	public boolean isEnabled() {
		return enabled;
	}

	/** Returns whether an operation carries per-circulation lifecycle state itself. */
	public boolean isOperationLifecycleTracked(final O op) {
		return !enabled || lifecycle(op).isTracked();
	}

	/**
	 * Installs a bounded, non-blocking callback which commits derived accounting only when the
	 * terminal transition wins against drain-time unresolved recovery. The callback runs inside
	 * that ownership decision and therefore must not block or throw.
	 */
	public void terminalObserver(final Consumer<O> observer) {
		terminalObserver = observer;
	}

	/** Clears completed-run counters before a representative lifecycle restart. */
	public void reset() {
		if (!enabled) {
			return;
		}
		outstanding.clear();
		inFlightOperations.clear();
		// Keep weak compatibility states across restarts. Otherwise a late callback for an
		// untracked prior-run operation becomes NEW and contaminates the next run's counters.
		dispatched.reset();
		inFlight.set(0);
		terminal.reset();
		unattempted.reset();
		unresolved.reset();
		synchronized (unattemptedOperations) {
			unattemptedOperations.clear();
		}
		synchronized (unresolvedOperations) {
			unresolvedOperations.clear();
		}
	}

	/** Records generator ownership before driver queue admission. */
	public boolean generatorBuffered(final O op) {
		if (!enabled) {
			return true;
		}
		final var lifecycle = op.startNextLifecycle();
		if (!transition(op, lifecycle, OperationLifecycleState.GENERATOR_BUFFERED)) {
			return false;
		}
		outstanding.add(identityKey(op));
		return true;
	}

	/** Records driver queue ownership without classifying the operation as attempted. */
	public boolean driverQueued(final O op) {
		if (!enabled) {
			return true;
		}
		if (!transition(op, op.startNextLifecycle(), OperationLifecycleState.DRIVER_QUEUED)) {
			return false;
		}
		outstanding.add(identityKey(op));
		return true;
	}

	/** Records direct or compatibility evidence that an actual request attempt began. */
	public boolean dispatched(final O op) {
		if (!enabled) {
			return true;
		}
		final var lifecycle = lifecycle(op);
		if (lifecycle.isTracked()) {
			synchronized (lifecycle) {
				return publishDispatch(op, lifecycle, false);
			}
		}
		synchronized (compatibilityStates) {
			return publishDispatch(op, lifecycle, false);
		}
	}

	/** Records the exact transport handoff made through the driver's explicit dispatch hook. */
	public boolean explicitlyDispatched(final O op) {
		if (!enabled) {
			return true;
		}
		final var lifecycle = lifecycle(op);
		if (lifecycle.isTracked()) {
			synchronized (lifecycle) {
				return publishDispatch(op, lifecycle, true);
			}
		}
		synchronized (compatibilityStates) {
			return publishDispatch(op, lifecycle, true);
		}
	}

	/** Returns whether this circulation crossed the explicit transport handoff hook. */
	public boolean hasExplicitDispatchBoundary(final O op) {
		if (!enabled) {
			return false;
		}
		final var lifecycle = lifecycle(op);
		if (lifecycle.isTracked()) {
			synchronized (lifecycle) {
				return lifecycle.hasExplicitDispatchBoundary();
			}
		}
		synchronized (compatibilityStates) {
			final var compatibilityState = compatibilityState(op);
			return compatibilityState != null
							&& compatibilityState.explicitDispatchCirculation == compatibilityState.circulation;
		}
	}

	/** Returns whether the exact circulation named by the token crossed the explicit hook. */
	public boolean hasExplicitDispatchBoundary(final DispatchToken<O> token) {
		if (!enabled || token == null) {
			return false;
		}
		if (token.lifecycle.isTracked()) {
			synchronized (token.lifecycle) {
				return token.lifecycle.hasExplicitDispatchBoundary();
			}
		}
		synchronized (compatibilityStates) {
			final var compatibilityState = compatibilityState(token.operation);
			return compatibilityState != null
							&& compatibilityState.explicitDispatchCirculation == token.compatibilityCirculation;
		}
	}

	/** Captures the exact queued circulation before invoking a compatibility submit method. */
	public DispatchToken<O> queuedDispatchToken(final O op) {
		if (!enabled) {
			return new DispatchToken<>(op, lifecycle(op), 0);
		}
		final var lifecycle = lifecycle(op);
		if (lifecycle.isTracked()) {
			return lifecycle.state() == OperationLifecycleState.DRIVER_QUEUED
							? new DispatchToken<>(op, lifecycle, 0)
							: null;
		}
		synchronized (compatibilityStates) {
			final var compatibilityState = compatibilityState(op);
			return compatibilityState != null
							&& compatibilityState.state == OperationLifecycleState.DRIVER_QUEUED
											? new DispatchToken<>(
															op, lifecycle, compatibilityState.circulation)
											: null;
		}
	}

	/** Dispatches only if the token still names the same queued circulation. */
	public boolean dispatched(final DispatchToken<O> token) {
		if (!enabled) {
			return true;
		}
		if (token == null) {
			return false;
		}
		final O op = token.operation;
		if (token.lifecycle.isTracked()) {
			synchronized (token.lifecycle) {
				if (lifecycle(op) != token.lifecycle || !token.lifecycle.dispatched()) {
					return false;
				}
				recordDispatched(op);
				observeDispatchPublication(op);
				return true;
			}
		}
		synchronized (compatibilityStates) {
			final var compatibilityState = compatibilityState(op);
			if (compatibilityState == null
							|| compatibilityState.circulation != token.compatibilityCirculation
							|| compatibilityState.state != OperationLifecycleState.DRIVER_QUEUED) {
				return false;
			}
			compatibilityState.state = OperationLifecycleState.DISPATCHED;
			recordDispatched(op);
			observeDispatchPublication(op);
			return true;
		}
	}

	private boolean publishDispatch(
					final O op, final OperationLifecycle lifecycle, final boolean explicit) {
		final boolean transitioned;
		if (lifecycle.isTracked()) {
			transitioned = explicit
							? lifecycle.explicitlyDispatched()
							: lifecycle.dispatched();
		} else {
			transitioned = transition(op, lifecycle, OperationLifecycleState.DISPATCHED);
			if (transitioned && explicit) {
				final var compatibilityState = compatibilityState(op);
				compatibilityState.explicitDispatchCirculation = compatibilityState.circulation;
			}
		}
		if (!transitioned) {
			return false;
		}
		recordDispatched(op);
		observeDispatchPublication(op);
		return true;
	}

	private void recordDispatched(final O op) {
		outstanding.add(identityKey(op));
		inFlightOperations.add(identityKey(op));
		dispatched.increment();
		inFlight.incrementAndGet();
	}

	private void observeDispatchPublication(final O op) {
		if (dispatchPublicationObserver != null) {
			dispatchPublicationObserver.accept(op);
		}
	}

	/** Claims the one completion callback allowed to construct and publish a terminal result. */
	public boolean completionStarted(final O op) {
		return !enabled || transition(op, lifecycle(op), OperationLifecycleState.COMPLETING);
	}

	/** Records one successfully published terminal result and releases outstanding ownership. */
	public boolean terminal(final O op) {
		if (!enabled) {
			return true;
		}
		if (!commitTerminal(op)) {
			return false;
		}
		outstanding.remove(identityKey(op));
		inFlightOperations.remove(identityKey(op));
		inFlight.decrementAndGet();
		terminal.increment();
		return true;
	}

	private boolean commitTerminal(final O op) {
		final var lifecycle = lifecycle(op);
		if (lifecycle.isTracked()) {
			synchronized (lifecycle) {
				if (lifecycle.state() != OperationLifecycleState.COMPLETING
								|| !lifecycle.terminal()) {
					return false;
				}
				observeTerminal(op);
				return true;
			}
		}
		synchronized (compatibilityStates) {
			final var compatibilityState = compatibilityState(op);
			if (compatibilityState == null
							|| compatibilityState.state != OperationLifecycleState.COMPLETING) {
				return false;
			}
			compatibilityState.state = OperationLifecycleState.TERMINAL;
			observeTerminal(op);
			return true;
		}
	}

	private void observeTerminal(final O op) {
		final var observer = terminalObserver;
		if (observer != null) {
			observer.accept(op);
		}
	}

	/** Recovers an operation which never reached actual dispatch. */
	public boolean unattempted(final O op) {
		if (!enabled) {
			return true;
		}
		if (!transition(op, lifecycle(op), OperationLifecycleState.UNATTEMPTED)) {
			return false;
		}
		outstanding.remove(identityKey(op));
		unattempted.increment();
		synchronized (unattemptedOperations) {
			unattemptedOperations.add(op);
		}
		return true;
	}

	/** Records a dispatched or completing operation which outlived the bounded drain. */
	public boolean unresolved(final O op) {
		if (!enabled) {
			return true;
		}
		final var state = stateOf(op);
		if (state != OperationLifecycleState.DISPATCHED
						&& state != OperationLifecycleState.COMPLETING) {
			return false;
		}
		if (!transition(op, lifecycle(op), OperationLifecycleState.UNRESOLVED)) {
			return false;
		}
		outstanding.remove(identityKey(op));
		inFlightOperations.remove(identityKey(op));
		inFlight.decrementAndGet();
		unresolved.increment();
		synchronized (unresolvedOperations) {
			unresolvedOperations.add(op);
		}
		return true;
	}

	/**
	 * Conservatively resolves a queued compatibility submission which did not return within the
	 * dispatcher's bounded stop wait. It may have started transport, so it is not unattempted;
	 * it was never counted in flight, so this transition does not decrement that counter.
	 */
	public boolean unresolvedSubmission(final O op) {
		if (!enabled) {
			return true;
		}
		if (stateOf(op) != OperationLifecycleState.DRIVER_QUEUED
						|| !transition(op, lifecycle(op), OperationLifecycleState.UNRESOLVED)) {
			return false;
		}
		recordUnresolvedSubmission(op);
		return true;
	}

	/**
	 * Conservatively resolves only the queued circulation captured before compatibility submit.
	 * A synchronously recycled later circulation of the same identity is left untouched.
	 */
	public boolean unresolvedSubmission(final DispatchToken<O> token) {
		if (!enabled) {
			return true;
		}
		if (token == null) {
			return false;
		}
		final O op = token.operation;
		if (token.lifecycle.isTracked()) {
			synchronized (token.lifecycle) {
				if (lifecycle(op) != token.lifecycle
								|| token.lifecycle.state() != OperationLifecycleState.DRIVER_QUEUED
								|| !token.lifecycle.unresolved()) {
					return false;
				}
			}
		} else {
			synchronized (compatibilityStates) {
				final var compatibilityState = compatibilityState(op);
				if (compatibilityState == null
								|| compatibilityState.circulation != token.compatibilityCirculation
								|| compatibilityState.state != OperationLifecycleState.DRIVER_QUEUED) {
					return false;
				}
				compatibilityState.state = OperationLifecycleState.UNRESOLVED;
			}
		}
		recordUnresolvedSubmission(op);
		return true;
	}

	private void recordUnresolvedSubmission(final O op) {
		outstanding.remove(identityKey(op));
		unresolved.increment();
		synchronized (unresolvedOperations) {
			unresolvedOperations.add(op);
		}
	}

	/** Marks every dispatched or completing operation still lacking a result as unresolved. */
	public int resolveOutstandingAsUnresolved() {
		if (!enabled) {
			return 0;
		}
		var count = 0;
		for (final var operationKey : Set.copyOf(inFlightOperations)) {
			final O op = operationKey.value;
			final var state = stateOf(op);
			if ((state == OperationLifecycleState.DISPATCHED
							|| state == OperationLifecycleState.COMPLETING) && unresolved(op)) {
				count++;
			}
		}
		return count;
	}

	/** Returns queued identities without touching buffers owned by a live dispatch task. */
	public List<O> driverQueuedOperations() {
		if (!enabled) {
			return List.of();
		}
		return outstanding.stream()
						.map(operationKey -> operationKey.value)
						.filter(op -> stateOf(op) == OperationLifecycleState.DRIVER_QUEUED)
						.toList();
	}

	/** Returns an immutable snapshot of counters, outstanding work, and recoverable identities. */
	public OperationLifecycleSnapshot<O> snapshot() {
		long generatorBufferedCount = 0;
		long driverQueuedCount = 0;
		for (final var operationKey : outstanding) {
			final O op = operationKey.value;
			switch (stateOf(op)) {
			case GENERATOR_BUFFERED -> generatorBufferedCount++;
			case DRIVER_QUEUED -> driverQueuedCount++;
			default -> {
				// Concurrent terminal removal may leave a transient registry observation.
			}
			}
		}
		final List<O> unattemptedCopy;
		final List<O> unresolvedCopy;
		synchronized (unattemptedOperations) {
			unattemptedCopy = List.copyOf(unattemptedOperations);
		}
		synchronized (unresolvedOperations) {
			unresolvedCopy = List.copyOf(unresolvedOperations);
		}
		return new OperationLifecycleSnapshot<>(
						generatorBufferedCount,
						driverQueuedCount,
						dispatched.sum(),
						inFlight.get(),
						terminal.sum(),
						unattempted.sum(),
						unresolved.sum(),
						unattemptedCopy,
						unresolvedCopy);
	}

	/**
	 * Returns the number of dispatched operations still awaiting a retained terminal result.
	 * This counter-only query is suitable for bounded drain polling and does not materialize
	 * recovered-operation identity lists.
	 */
	public long inFlightCount() {
		return enabled ? inFlight.get() : 0;
	}

	int outstandingOperationCount() {
		return enabled ? outstanding.size() : 0;
	}

	int inFlightOperationCount() {
		return enabled ? inFlightOperations.size() : 0;
	}

	private boolean transition(
					final O op,
					final OperationLifecycle lifecycle,
					final OperationLifecycleState next) {
		if (lifecycle.isTracked()) {
			synchronized (lifecycle) {
				return switch (next) {
				case GENERATOR_BUFFERED -> lifecycle.generatorBuffered();
				case DRIVER_QUEUED -> lifecycle.driverQueued();
				case DISPATCHED -> lifecycle.dispatched();
				case COMPLETING -> lifecycle.completionStarted();
				case TERMINAL -> lifecycle.terminal();
				case UNATTEMPTED -> lifecycle.unattempted();
				case UNRESOLVED -> lifecycle.unresolved();
				case NEW -> false;
				};
			}
		}
		synchronized (compatibilityStates) {
			var compatibilityState = compatibilityState(op);
			final var current = compatibilityState == null
							? OperationLifecycleState.NEW
							: compatibilityState.state;
			if (next == OperationLifecycleState.GENERATOR_BUFFERED
							&& current == OperationLifecycleState.TERMINAL) {
				// Legacy result() implementations may recycle the same operation instance and
				// cannot attach a fresh lifecycle. Preserve that circulation behavior while
				// lifecycle-aware implementations retain per-attempt stale-callback protection.
				compatibilityState.state = next;
				compatibilityState.circulation++;
				return true;
			}
			if (!current.canTransitionTo(next)) {
				return false;
			}
			if (compatibilityState == null) {
				compatibilityState = new CompatibilityState();
				compatibilityStates.put(
								new WeakIdentityKey<>(op, compatibilityReferences), compatibilityState);
			}
			compatibilityState.state = next;
			return true;
		}
	}

	/** Returns the tracked or compatibility-sidecar state for one operation identity. */
	public OperationLifecycleState stateOf(final O op) {
		final var lifecycle = lifecycle(op);
		if (lifecycle.isTracked()) {
			return lifecycle.state();
		}
		synchronized (compatibilityStates) {
			final var compatibilityState = compatibilityState(op);
			return compatibilityState == null
							? OperationLifecycleState.NEW
							: compatibilityState.state;
		}
	}

	private CompatibilityState compatibilityState(final O op) {
		WeakIdentityKey<?> staleKey;
		while ((staleKey = (WeakIdentityKey<?>) compatibilityReferences.poll()) != null) {
			compatibilityStates.remove(staleKey);
		}
		return compatibilityStates.get(new WeakIdentityKey<>(op));
	}

	private OperationLifecycle lifecycle(final O op) {
		final var lifecycle = op.lifecycle();
		return lifecycle == null ? OperationLifecycle.untracked() : lifecycle;
	}

	private static <T> IdentityKey<T> identityKey(final T value) {
		return new IdentityKey<>(value);
	}
}
