package com.dell.spt.base.load.generator;

import com.github.akurilov.commons.concurrent.throttle.IndexThrottle;

import java.util.Arrays;

/**
 * An {@link IndexThrottle} sharing a cycle of weights between origins, where an origin that will
 * schedule nothing more can leave the cycle.
 *
 * <p>Each origin may take its weight once per cycle, and the next cycle starts when no origin has
 * any left. While no origin is retired, this grants exactly what
 * {@link com.github.akurilov.commons.concurrent.throttle.SequentialWeightsThrottle} grants. That
 * class cannot be told that an origin has finished, so the share a finished origin never takes
 * would hold back the next cycle for every other origin, for good.
 *
 * <p>A retired origin holds no share and gets none at a new cycle. It is still granted every permit
 * it asks for, outside the cycle: only a retry of an operation it already scheduled can ask.
 */
public final class RetiringWeightsThrottle implements IndexThrottle {

	private final int[] weights;
	private final int[] remaining;
	private final boolean[] retired;

	public RetiringWeightsThrottle(final int[] weights) {
		this.weights = Arrays.copyOf(weights, weights.length);
		this.remaining = Arrays.copyOf(weights, weights.length);
		this.retired = new boolean[weights.length];
	}

	/** Takes the origin out of the cycle. Calling it again has no further effect. */
	public void retire(final int origin) {
		synchronized (remaining) {
			retired[origin] = true;
			remaining[origin] = 0;
		}
	}

	/** Retired origins stay at zero, so the scan needs no check of its own. */
	private void ensureRemaining() {
		for (final int share : remaining) {
			if (share > 0) {
				return;
			}
		}
		for (int i = 0; i < remaining.length; i++) {
			remaining[i] = retired[i] ? 0 : weights[i];
		}
	}

	@Override
	public boolean tryAcquire(final int origin) {
		synchronized (remaining) {
			ensureRemaining();
			final int available = remaining[origin];
			if (available > 0) {
				remaining[origin] = available - 1;
				return true;
			}
			return retired[origin];
		}
	}

	@Override
	public int tryAcquire(final int origin, final int permits) {
		if (permits == 0) {
			return 0;
		}
		synchronized (remaining) {
			ensureRemaining();
			final int available = remaining[origin];
			if (permits > available) {
				remaining[origin] = 0;
				return available == 0 && retired[origin] ? permits : available;
			}
			remaining[origin] = available - permits;
			return permits;
		}
	}
}
