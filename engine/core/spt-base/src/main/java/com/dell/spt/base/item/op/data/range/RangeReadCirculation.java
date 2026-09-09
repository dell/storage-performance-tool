package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OperationRetryPolicy;
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
	private boolean retryQueueClaimed;
	private int retryCount;
	private boolean finalMetricsClaimed;
	private boolean resultOutputClaimed;
	private boolean closed;
	private RangeReadAttempt.Outcome terminalOutcome;
	private RangeReadAttempt terminalAttempt;

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
				if (!outcomeClaimed || retryCount == Integer.MAX_VALUE) {
					return null;
				}
				final var outcome = current.outcome();
				if (outcome == null || (outcome.category() != RangeReadAttempt.Category.HTTP
								&& outcome.category() != RangeReadAttempt.Category.TRANSPORT)) {
					return null;
				}
				if (!OperationRetryPolicy.isRetryableStatus(outcome.status())) {
					return null;
				}
				final boolean queuedSubmissionFailure = state == OperationLifecycleState.DRIVER_QUEUED
								&& outcome.category() == RangeReadAttempt.Category.TRANSPORT && !outcome.requestHandedOff();
				if (state != OperationLifecycleState.DISPATCHED && !queuedSubmissionFailure) {
					return null;
				}
			}
			if (current != null) {
				retryCount++;
				lastFailure = current.outcome();
				if (firstDispatch == 0) {
					firstDispatch = lastFailure.timing().dispatch();
				}
			}
			current = new RangeReadAttempt(lifecycle, selection.range(), clock);
			outcomeClaimed = false;
			retryQueueClaimed = false;
			return current;
		}
	}

	/** Additional attempts in this circulation, shared by every operation/result copy. */
	public int retryCount() {
		synchronized (lifecycle) {
			return retryCount;
		}
	}

	/**
	 * Captures the prepared token for driver queue ownership. This is not transport permission:
	 * the adapter must recheck isPendingAttempt(token) under its actual handoff admission gate.
	 */
	public RangeReadAttempt pendingAttempt() {
		synchronized (lifecycle) {
			return isPendingAttempt(current) ? current : null;
		}
	}

	/** True while this exact prepared attempt still awaits transport handoff. */
	public boolean isPendingAttempt(final RangeReadAttempt expected) {
		synchronized (lifecycle) {
			final var state = lifecycle.state();
			return !closed && current != null && current == expected
							&& (state == OperationLifecycleState.DRIVER_QUEUED || state == OperationLifecycleState.DISPATCHED)
							&& !current.wasHandedOff() && current.outcome() == null;
		}
	}

	/** Once-only generator retry queue ownership, after the retry coordinator creates the token. */
	public boolean claimRetryQueue(final RangeReadAttempt expected) {
		synchronized (lifecycle) {
			if (retryQueueClaimed || lastFailure == null || !isPendingAttempt(expected)) {
				return false;
			}
			retryQueueClaimed = true;
			return true;
		}
	}

	/**
	 * Returns an immutable outcome once for accounting/retry decisions. No user callback runs
	 * under this lock. A stale token cannot publish an outcome. Final accounting may claim the retained terminal attempt.
	 */
	public RangeReadAttempt.Outcome claimOutcome(final RangeReadAttempt expected) {
		synchronized (lifecycle) {
			final var state = lifecycle.state();
			final boolean live = !closed && (state == OperationLifecycleState.DRIVER_QUEUED
							|| state == OperationLifecycleState.DISPATCHED);
			final boolean retained = terminalOutcome != null && current != null
							&& current == terminalAttempt && (state == OperationLifecycleState.TERMINAL
											|| state == OperationLifecycleState.UNRESOLVED);
			if (current == null || current != expected || outcomeClaimed || (!live && !retained)) {
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
			return retainAndCommit(op, tracker, outcome, current);
		}
	}

	private <I extends DataItem> boolean retainAndCommit(final RangeReadOperation<I> op,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker,
					final RangeReadAttempt.Outcome outcome, final RangeReadAttempt owner) {
		final long previousBytes = op.countBytesDone();
		final var previousTiming = op.timing();
		terminalOutcome = outcome;
		terminalAttempt = owner;
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
				terminalAttempt = null;
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
						return retainAndCommit(op, tracker, lastFailure, null);
					}
					if (current.wasHandedOff()) {
						current.unresolved();
						if (tracker.unresolved(op)) {
							terminalOutcome = current.outcome();
							terminalAttempt = current;
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

	public record FinalOutcome(RangeReadPolicy.SelectionError localError,
					RangeReadAttempt.Outcome outcome, RangeReadAttempt attempt) {}

	/** Claims additive final accounting once, including local errors and indeterminate recovery. */
	public FinalOutcome claimFinalMetrics() {
		synchronized (lifecycle) {
			final var state = lifecycle.state();
			if (finalMetricsClaimed || (state != OperationLifecycleState.TERMINAL
							&& state != OperationLifecycleState.UNRESOLVED)) {
				return null;
			}
			if (terminalOutcome == null && selection.error() == null) {
				throw new IllegalStateException("Range final accounting requires a retained outcome");
			}
			finalMetricsClaimed = true;
			return new FinalOutcome(selection.error(), terminalOutcome, terminalAttempt);
		}
	}

	/** Immutable values for optional output; independent of mutable operation/result copies. */
	record OutputResult(com.dell.spt.base.item.op.Operation.Status status, long bytes,
					RangeReadAttempt.Timing timing) {}

	/** One publication attempt per determinate circulation, independent of metrics claims. */
	OutputResult claimResultOutput() {
		synchronized (lifecycle) {
			if (resultOutputClaimed || lifecycle.state() != OperationLifecycleState.TERMINAL) {
				return null;
			}
			if (selection.error() != null) {
				resultOutputClaimed = true;
				return new OutputResult(com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_CLIENT,
								0, RangeReadAttempt.Timing.EMPTY);
			}
			if (terminalOutcome == null) {
				return null;
			}
			resultOutputClaimed = true;
			final boolean success = terminalOutcome.category() == RangeReadAttempt.Category.SUCCESS;
			final var timing = terminalOutcome.timing();
			return new OutputResult(terminalOutcome.status(), success ? selection.range().length() : 0,
							success ? new RangeReadAttempt.Timing(firstDispatch, timing.requestComplete(),
											timing.responseHeaders(), timing.firstBody(), timing.responseComplete())
											: RangeReadAttempt.Timing.EMPTY);
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
