package com.dell.spt.base.nic.pool;

import java.util.function.LongSupplier;

/**
 * That the Entry Node records each slice's item sizes as it writes the slices, and until when
 * (D54-R2). The budget is smaller than the preparation deadline. Its expiry leaves the summaries
 * unknown, while the distribution goes on, within the deadline, to its end.
 *
 * @param deadlineNanos the end of the summary's budget, on {@code nanoTime}
 */
public record ItemSizeRecording(long deadlineNanos, LongSupplier nanoTime) {

	/** The summary's budget is this fraction of the preparation deadline. */
	public static final int BUDGET_DIVISOR = 4;

	public boolean expired() {
		return nanoTime.getAsLong() - deadlineNanos >= 0;
	}
}
