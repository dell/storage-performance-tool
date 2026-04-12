package com.dell.spt.load.step.mixed;

import com.github.akurilov.commons.concurrent.throttle.IndexThrottle;

import java.util.concurrent.ThreadLocalRandom;

/**
 * An {@link IndexThrottle} that grants permits probabilistically based on integer weights.
 *
 * <p>For each call to {@link #tryAcquire(int, int)}, the throttle flips a weighted coin:
 * the probability of granting permits to generator {@code i} is
 * {@code weights[i] / sum(weights)}. This produces a random interleaving of operations
 * that converges to the configured distribution over time — unlike
 * {@link com.github.akurilov.commons.concurrent.throttle.SequentialWeightsThrottle},
 * which uses a deterministic budget-depletion cycle that creates bursty patterns.
 */
public final class WeightedRandomThrottle implements IndexThrottle {

	private final int[] weights;
	private final int totalWeight;

	public WeightedRandomThrottle(final int[] weights) {
		if (weights == null || weights.length == 0) {
			throw new IllegalArgumentException("Weights array must be non-empty");
		}
		int sum = 0;
		for (final int w : weights) {
			if (w < 0) {
				throw new IllegalArgumentException("Negative weight: " + w);
			}
			sum += w;
		}
		if (sum == 0) {
			throw new IllegalArgumentException("Total weight must be > 0");
		}
		this.weights = weights.clone();
		this.totalWeight = sum;
	}

	/**
	 * Single-permit acquire. Returns {@code true} with probability
	 * {@code weights[originIndex] / totalWeight}.
	 */
	@Override
	public boolean tryAcquire(final int originIndex) {
		if (originIndex < 0 || originIndex >= weights.length || weights[originIndex] == 0) {
			return false;
		}
		return ThreadLocalRandom.current().nextInt(totalWeight) < weights[originIndex];
	}

	/**
	 * Batch-permit acquire. Grants all requested permits with probability
	 * {@code weights[originIndex] / totalWeight}, or zero permits otherwise.
	 *
	 * <p>This all-or-nothing approach keeps operations atomic (a generator either runs
	 * its full batch or skips entirely), which avoids partial-batch overhead in the
	 * engine's operation pipeline.
	 */
	@Override
	public int tryAcquire(final int originIndex, final int permits) {
		if (permits == 0) {
			return 0;
		}
		return tryAcquire(originIndex) ? permits : 0;
	}
}
