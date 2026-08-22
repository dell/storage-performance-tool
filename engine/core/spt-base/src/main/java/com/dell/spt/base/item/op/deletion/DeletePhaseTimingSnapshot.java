package com.dell.spt.base.item.op.deletion;

/**
 * Monotonic phase timing for one standalone DELETE worker.
 *
 * <p>The scheduled interval ends when request admission closes. The drain interval starts at that
 * same boundary and includes queued-work recovery plus the bounded wait for actually dispatched
 * requests. Values use nanoseconds so later result layers can aggregate without losing precision.
 */
public record DeletePhaseTimingSnapshot(long scheduledNanos, long drainNanos) {
	/** Returns the zero timing used by contexts without standalone DELETE lifecycle support. */
	public static DeletePhaseTimingSnapshot empty() {
		return new DeletePhaseTimingSnapshot(0, 0);
	}
}
