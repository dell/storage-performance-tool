package com.dell.spt.base.load.step;

import java.util.concurrent.TimeUnit;

/** Signed-safe monotonic time arithmetic for bounded duration phases. */
public final class DurationTime {
	private static final long MONOTONIC_EPOCH_OFFSET_NANOS = Math.subtractExact(
					TimeUnit.MILLISECONDS.toNanos(System.currentTimeMillis()), System.nanoTime());

	private DurationTime() {}

	/**
	 * Returns an epoch-aligned timestamp which remains monotonic for this runtime.
	 *
	 * <p>The fixed startup offset lets cooperating JVMs with synchronized system clocks exchange
	 * workflow boundaries without comparing their private {@link System#nanoTime()} origins.
	 *
	 * @return the current monotonic epoch-relative timestamp in nanoseconds
	 */
	public static long monotonicEpochNanos() {
		return Math.addExact(MONOTONIC_EPOCH_OFFSET_NANOS, System.nanoTime());
	}

	/** Returns a monotonic deadline using the signed-difference arithmetic required by nanoTime. */
	public static long deadlineAfter(final long boundaryNanos, final long budgetNanos) {
		return boundaryNanos + Math.max(0, budgetNanos);
	}

	/** Returns whether the ordered range has positive duration without assuming either value's sign. */
	public static boolean isPositiveRange(final long startNanos, final long deadlineNanos) {
		return deadlineNanos - startNanos > 0;
	}

	/** Returns whether an observation reached a finite deadline. */
	public static boolean deadlineReached(final long deadlineNanos, final long observedNanos) {
		return observedNanos - deadlineNanos >= 0;
	}

	/** Returns whether a timestamp precedes a finite deadline. */
	public static boolean timestampBeforeDeadline(
					final long timestampNanos, final long deadlineNanos) {
		return timestampNanos - deadlineNanos < 0;
	}

	/** Returns the remaining finite interval, or zero after expiry. */
	public static long remainingNanos(final long deadlineNanos, final long observedNanos) {
		final long remainingNanos = deadlineNanos - observedNanos;
		return remainingNanos > 0 ? remainingNanos : 0;
	}

	/** Selects the deadline with less time remaining without comparing absolute nanoTime values. */
	public static long earlierDeadline(
					final long firstDeadlineNanos,
					final long secondDeadlineNanos,
					final long observedNanos) {
		return remainingNanos(firstDeadlineNanos, observedNanos) <= remainingNanos(secondDeadlineNanos, observedNanos)
						? firstDeadlineNanos
						: secondDeadlineNanos;
	}

	/** Returns a non-negative elapsed interval without assuming either timestamp's sign. */
	public static long elapsedNanos(final long startedNanos, final long finishedNanos) {
		final long elapsedNanos = finishedNanos - startedNanos;
		return elapsedNanos >= 0 ? elapsedNanos : 0;
	}
}
