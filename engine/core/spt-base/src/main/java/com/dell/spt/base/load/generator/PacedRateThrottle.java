package com.dell.spt.base.load.generator;

import com.github.akurilov.commons.concurrent.throttle.Throttle;

import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * A {@link Throttle} that paces permits at a fixed rate from its first acquisition.
 *
 * <p>The clock starts at the first call, which may take one permit. One more permit becomes
 * available each period after that, and permits that are not taken stay available. A call
 * never grants more than it requests, more than is available, or a negative count.
 *
 * <p>This replaces {@link com.github.akurilov.commons.concurrent.throttle.RateThrottle}, which
 * grants its whole first request at once and then reports the resulting deficit as a negative
 * count. The generator's first request is a whole batch, so every rate-limited worker used to
 * start with one extra batch at full speed.
 */
public final class PacedRateThrottle implements Throttle {

	private final long periodNanos;
	private final LongSupplier nanoClock;
	private boolean started;
	private long startNanos;
	private long acquiredCount;

	public PacedRateThrottle(final double rate) {
		this(rate, System::nanoTime);
	}

	PacedRateThrottle(final double rate, final LongSupplier nanoClock) {
		if (!(rate > 0)) {
			throw new IllegalArgumentException("Rate should be more than 0, but got " + rate);
		}
		// Whole nanoseconds, as RateThrottle computes it, so paced runs keep today's rate.
		this.periodNanos = Math.max(1, (long) (TimeUnit.SECONDS.toNanos(1) / rate));
		this.nanoClock = nanoClock;
	}

	@Override
	public boolean tryAcquire() {
		return tryAcquire(1) == 1;
	}

	@Override
	public synchronized int tryAcquire(final int permits) {
		if (permits <= 0) {
			return 0;
		}
		final long now = nanoClock.getAsLong();
		if (!started) {
			started = true;
			startNanos = now;
		}
		final long available = (now - startNanos) / periodNanos + 1 - acquiredCount;
		final int granted = (int) Math.max(0, Math.min(permits, available));
		acquiredCount += granted;
		return granted;
	}
}
