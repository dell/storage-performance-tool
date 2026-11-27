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
 *
 * <p>With a minimum cycle, a cycle whose positive weights sum above 0 and below it is lengthened,
 * keeping the ratio: each positive weight is multiplied by the smallest integer that brings the sum to
 * the minimum. The sum counts only the origins still in the cycle, and is taken again at each
 * retirement from the weights as written, so the origins left get the cycle of a throttle built from
 * their weights alone. A weight of 0 or below never gets a permit, so it stays as written and does
 * not count. Shares held when an origin retires are used at the old scale; the new one applies from
 * the next cycle.
 */
public final class RetiringWeightsThrottle implements IndexThrottle {

	private final int[] written;
	private final int minCycle;
	private final int[] weights;
	private final int[] remaining;
	private final boolean[] retired;

	/** A throttle without a minimum cycle: each origin's share is its weight as written. */
	public RetiringWeightsThrottle(final int[] weights) {
		this(weights, 0);
	}

	/**
	 * A throttle whose cycle, while any origin in it has a positive weight, gives out at least
	 * {@code minCycle} permits. A minimum of 0 or below lengthens no cycle.
	 */
	public RetiringWeightsThrottle(final int[] weights, final int minCycle) {
		this.written = Arrays.copyOf(weights, weights.length);
		this.minCycle = minCycle;
		this.weights = new int[weights.length];
		this.retired = new boolean[weights.length];
		scaleLiveWeights();
		this.remaining = Arrays.copyOf(this.weights, weights.length);
	}

	/** Takes the origin out of the cycle. Calling it again has no further effect. */
	public void retire(final int origin) {
		synchronized (remaining) {
			retired[origin] = true;
			remaining[origin] = 0;
			scaleLiveWeights();
		}
	}

	/** The share each origin gets at the next new cycle: 0 for a retired origin. */
	public int[] cycleWeights() {
		synchronized (remaining) {
			final int[] cycle = new int[weights.length];
			for (int i = 0; i < cycle.length; i++) {
				cycle[i] = retired[i] ? 0 : weights[i];
			}
			return cycle;
		}
	}

	/**
	 * Sets each origin's weight from its written one and the sum of the positive written weights of
	 * the origins still in the cycle. A retired origin keeps its written weight, which no cycle uses.
	 */
	private void scaleLiveWeights() {
		long liveSum = 0;
		for (int i = 0; i < written.length; i++) {
			if (!retired[i] && written[i] > 0) {
				liveSum += written[i];
			}
		}
		final long factor = liveSum > 0 && liveSum < minCycle ? (minCycle + liveSum - 1) / liveSum : 1;
		for (int i = 0; i < written.length; i++) {
			final int weight = written[i];
			weights[i] = retired[i] || weight <= 0 ? weight : (int) Math.min(weight * factor, Integer.MAX_VALUE);
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
