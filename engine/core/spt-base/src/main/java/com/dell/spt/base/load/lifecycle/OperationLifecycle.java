package com.dell.spt.base.load.lifecycle;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-operation lifecycle state shared by an operation and its result snapshots.
 *
 * <p>The state is deliberately attached to the operation rather than retained forever by a
 * global ledger. This makes completion races exactly-once without making completed-work memory
 * grow with the length of a run.
 */
public final class OperationLifecycle {

	private static final OperationLifecycle UNTRACKED = new OperationLifecycle(false);

	private final boolean tracked;
	private final AtomicReference<OperationLifecycleState> state = new AtomicReference<>(OperationLifecycleState.NEW);
	private boolean explicitDispatch;

	/** Creates an independently tracked lifecycle in the {@link OperationLifecycleState#NEW} state. */
	public OperationLifecycle() {
		this(true);
	}

	private OperationLifecycle(final boolean tracked) {
		this.tracked = tracked;
	}

	/** Compatibility lifecycle for operation implementations which do not opt in to tracking. */
	public static OperationLifecycle untracked() {
		return UNTRACKED;
	}

	/** Returns the operation's current lifecycle state. */
	public OperationLifecycleState state() {
		return state.get();
	}

	/** Returns whether this instance records state instead of acting as a compatibility marker. */
	public boolean isTracked() {
		return tracked;
	}

	boolean generatorBuffered() {
		return transitionTo(OperationLifecycleState.GENERATOR_BUFFERED);
	}

	boolean driverQueued() {
		return transitionTo(OperationLifecycleState.DRIVER_QUEUED);
	}

	boolean dispatched() {
		return transitionTo(OperationLifecycleState.DISPATCHED);
	}

	boolean explicitlyDispatched() {
		if (!transitionTo(OperationLifecycleState.DISPATCHED)) {
			return false;
		}
		explicitDispatch = true;
		return true;
	}

	boolean hasExplicitDispatchBoundary() {
		return tracked && explicitDispatch;
	}

	boolean completionStarted() {
		return transitionTo(OperationLifecycleState.COMPLETING);
	}

	boolean terminal() {
		return transitionTo(OperationLifecycleState.TERMINAL);
	}

	boolean unattempted() {
		return transitionTo(OperationLifecycleState.UNATTEMPTED);
	}

	boolean unresolved() {
		return transitionTo(OperationLifecycleState.UNRESOLVED);
	}

	private boolean transitionTo(final OperationLifecycleState next) {
		if (!tracked) {
			return true;
		}
		while (true) {
			final var current = state.get();
			if (!current.canTransitionTo(next)) {
				return false;
			}
			if (state.compareAndSet(current, next)) {
				return true;
			}
		}
	}
}
