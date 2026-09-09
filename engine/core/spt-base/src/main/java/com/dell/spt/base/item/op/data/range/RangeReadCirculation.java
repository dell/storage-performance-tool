package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import java.util.Objects;

/** Shared by operation/result copies of one logical selection; bounded to the current attempt. */
public final class RangeReadCirculation {
	private final OperationLifecycle lifecycle;
	private final RangeReadPolicy.Selection selection;
	private RangeReadAttempt current;
	private boolean outcomeClaimed;
	private boolean closed;

	public RangeReadCirculation(final OperationLifecycle lifecycle, final RangeReadPolicy.Selection selection) {
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
			current = new RangeReadAttempt(lifecycle, selection.range());
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
