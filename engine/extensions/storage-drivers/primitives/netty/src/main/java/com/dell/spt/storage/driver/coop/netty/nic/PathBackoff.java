package com.dell.spt.storage.driver.coop.netty.nic;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Connect back-off of one Path. A failed connect starts it: 100 ms after the first failure,
 * doubling with each consecutive failure up to 5 s, without jitter. During the interval no connect
 * starts. After it, exactly one trial connect starts; until the trial completes the Path stays in
 * back-off. A successful connect resets the Path, a failed trial doubles the interval.
 *
 * <p>Each permitted connect carries the epoch it started in. Only a failure of the current epoch
 * counts, so connects that were already in flight when the first one failed do not double the
 * interval again.
 */
final class PathBackoff {

	static final long INITIAL_MILLIS = 100;
	static final long MAX_MILLIS = 5_000;

	/** Returned by {@link #tryConnect()} while the Path is in back-off. */
	static final long REJECTED = -1;

	private final LongSupplier nanoClock;
	private int failures;
	private long untilNanos;
	private boolean trialPending;
	private long epoch;

	PathBackoff(final LongSupplier nanoClock) {
		this.nanoClock = nanoClock;
	}

	PathBackoff() {
		this(System::nanoTime);
	}

	/**
	 * Returns the epoch a connect may start in, or {@link #REJECTED} while the interval runs or a
	 * trial is pending.
	 */
	synchronized long tryConnect() {
		if (failures == 0) {
			return epoch;
		}
		if (trialPending || nanoClock.getAsLong() - untilNanos < 0) {
			return REJECTED;
		}
		trialPending = true;
		return epoch;
	}

	/** A connect of any epoch succeeded: the route works, so the Path leaves back-off. */
	synchronized void connected() {
		if (failures != 0 || trialPending) {
			failures = 0;
			trialPending = false;
			epoch++;
		}
	}

	/** A connect started in {@code connectEpoch} failed; a stale epoch changes nothing. */
	synchronized void failed(final long connectEpoch) {
		if (connectEpoch != epoch) {
			return;
		}
		failures++;
		trialPending = false;
		epoch++;
		untilNanos = nanoClock.getAsLong() + TimeUnit.MILLISECONDS.toNanos(intervalMillis(failures));
	}

	synchronized boolean inBackoff() {
		return failures != 0;
	}

	synchronized int failures() {
		return failures;
	}

	/** The interval after {@code failures} consecutive failures. */
	static long intervalMillis(final int failures) {
		if (failures < 1) {
			return 0;
		}
		// 100 << 6 is already above the cap.
		final var shift = Math.min(failures - 1, 6);
		return Math.min(INITIAL_MILLIS << shift, MAX_MILLIS);
	}
}
