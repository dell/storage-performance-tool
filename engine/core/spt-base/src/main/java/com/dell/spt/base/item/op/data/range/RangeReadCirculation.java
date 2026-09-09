package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Shared by operation/result copies of one logical selection; bounded to the current attempt. */
public final class RangeReadCirculation {
	private final OperationLifecycle lifecycle;
	private final LongSupplier clock;
	private long firstDispatch;
	private final RangeReadPolicy.Selection selection;
	private RangeReadAttempt current;
	private RangeReadAttempt.Outcome lastFailure;
	private boolean outcomeClaimed;
	private boolean closed;
	private RangeReadAttempt.Outcome terminalOutcome;

	public RangeReadCirculation(final OperationLifecycle lifecycle, final RangeReadPolicy.Selection selection) {
		this(lifecycle, selection, RangeReadAttempt::clockMicros);
	}

	RangeReadCirculation(final OperationLifecycle lifecycle, final RangeReadPolicy.Selection selection,
					final LongSupplier clock) {
		this.clock = Objects.requireNonNull(clock);
		this.lifecycle = Objects.requireNonNull(lifecycle);
		this.selection = Objects.requireNonNull(selection);
	}

	public OperationLifecycle lifecycle() {
		return lifecycle;
	}

	public RangeReadPolicy.Selection selection() {
		return selection;
	}

	/**
	 * Compare-and-claim attempt creation. Null expected means the initial attempt. A retry caller
	 * must first claim/account the previous outcome and apply configured retry limits/backoff.
	 * This method does not perform transport handoff or change logical dispatch accounting.
	 */
	public RangeReadAttempt beginAttempt(final RangeReadAttempt expectedPrevious) {
		synchronized (lifecycle) {
			if (closed || selection.range() == null || current != expectedPrevious) {
				return null;
			}
			final var state = lifecycle.state();
			if (current == null) {
				if (state != OperationLifecycleState.DRIVER_QUEUED) {
					return null;
				}
			} else {
				if (state != OperationLifecycleState.DISPATCHED || !outcomeClaimed) {
					return null;
				}
				final var outcome = current.outcome();
				if (outcome == null || (outcome.category() != RangeReadAttempt.Category.HTTP
								&& outcome.category() != RangeReadAttempt.Category.TRANSPORT)) {
					return null;
				}
			}
			if (current != null) {
				lastFailure = current.outcome();
				if (firstDispatch == 0) {
					firstDispatch = lastFailure.timing().dispatch();
				}
			}
			current = new RangeReadAttempt(lifecycle, selection.range(), clock);
			outcomeClaimed = false;
			return current;
		}
	}

	/**
	 * Returns an immutable outcome once for accounting/retry decisions. No user callback runs
	 * under this lock. A stale token or recovered lifecycle cannot publish an outcome.
	 */
	public RangeReadAttempt.Outcome claimOutcome(final RangeReadAttempt expected) {
		synchronized (lifecycle) {
			final var state = lifecycle.state();
			if (closed || current == null || current != expected || outcomeClaimed
							|| (state != OperationLifecycleState.DRIVER_QUEUED
											&& state != OperationLifecycleState.DISPATCHED)) {
				return null;
			}
			final var outcome = current.outcome();
			if (outcome == null) {
				return null;
			}
			outcomeClaimed = true;
			return outcome;
		}
	}

	/**
	 * Retains a final attempt outcome and commits logical accounting before optional output.
	 * This also settles the last known failure when shutdown cancels a pending retry. The
	 * runtime coordinator must record attempt metrics once through claimOutcome separately.
	 */
	public <I extends DataItem> boolean complete(final RangeReadOperation<I> op,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker,
					final RangeReadAttempt expected) {
		synchronized (lifecycle) {
			final var state = lifecycle.state();
			if (closed || current == null || current != expected || op.circulation() != this
							|| op.lifecycle() != lifecycle || (state != OperationLifecycleState.DRIVER_QUEUED
											&& state != OperationLifecycleState.DISPATCHED)) {
				return false;
			}
			final var outcome = current.outcome();
			if (outcome == null || outcome.category() == RangeReadAttempt.Category.UNRESOLVED) {
				return false;
			}
			return retainAndCommit(op, tracker, outcome);
		}
	}

	private <I extends DataItem> boolean retainAndCommit(final RangeReadOperation<I> op,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker,
					final RangeReadAttempt.Outcome outcome) {
		final long previousBytes = op.countBytesDone();
		final var previousTiming = op.timing();
		terminalOutcome = outcome;
		if (outcome.category() == RangeReadAttempt.Category.SUCCESS) {
			if (firstDispatch == 0) {
				firstDispatch = outcome.timing().dispatch();
			}
			final var timing = outcome.timing();
			op.timing(new RangeReadAttempt.Timing(firstDispatch, timing.requestComplete(),
							timing.responseHeaders(), timing.firstBody(), timing.responseComplete()));
		} else {
			op.timing(RangeReadAttempt.Timing.EMPTY);
		}
		op.countBytesDone(outcome.category() == RangeReadAttempt.Category.SUCCESS ? selection.range().length() : 0);
		try {
			return tracker.retainedTerminal(op, lifecycle, outcome.status());
		} finally {
			if (lifecycle.state() == OperationLifecycleState.TERMINAL) {
				// A broken observer may throw after accounting committed; never erase that result.
				closed = true;
			} else {
				terminalOutcome = null;
				op.countBytesDone(previousBytes);
				op.timing(previousTiming);
			}
		}
	}

	/**
	 * Called by range-aware shutdown after admission is closed and the drain bound expires.
	 * A known current result or queued retry's previous failure wins over indeterminate recovery.
	 * The adapter still owns cancellation/resource release, using the captured attempt token.
	 */
	public <I extends DataItem> boolean settleAtDeadline(final RangeReadOperation<I> op,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker) {
		synchronized (lifecycle) {
			if (closed || op.circulation() != this || op.lifecycle() != lifecycle
							|| !tracker.hasOutstandingCustody(op, lifecycle)) {
				return false;
			}
			final var attempt = current;
			if (attempt != null) {
				synchronized (attempt) {
					final var outcome = current.outcome();
					if (outcome != null && outcome.category() != RangeReadAttempt.Category.UNRESOLVED) {
						return complete(op, tracker, current);
					}
					if (!current.wasHandedOff() && lastFailure != null) {
						current.cancelBeforeHandoff();
						return retainAndCommit(op, tracker, lastFailure);
					}
					if (current.wasHandedOff()) {
						current.unresolved();
						if (tracker.unresolved(op)) {
							terminalOutcome = current.outcome();
							closed = true;
							return true;
						}
						return false;
					}
					current.cancelBeforeHandoff();
				}
			}
			if (tracker.unattempted(op)) {
				closed = true;
				return true;
			}
			return false;
		}
	}

	public RangeReadAttempt.Outcome terminalOutcome() {
		synchronized (lifecycle) {
			return terminalOutcome;
		}
	}

	/** Seals retry creation before final logical publication or recovery. Idempotent by token. */
	public boolean close(final RangeReadAttempt expected) {
		synchronized (lifecycle) {
			if (closed || current != expected) {
				return false;
			}
			closed = true;
			return true;
		}
	}
}
