package com.dell.spt.load.step.mixed;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link WeightedRandomThrottle}.
 *
 * <p>Statistical tests use large trial counts so that observed frequencies converge to the
 * expected probabilities with comfortable margins.
 */
class WeightedRandomThrottleTest {

	@Test
	@DisplayName("Distribution converges within 1% margin for weights {70, 20, 10}")
	void distributionConvergesWithinMargin() {
		final int[] weights = {70, 20, 10
		};
		final WeightedRandomThrottle throttle = new WeightedRandomThrottle(weights);
		final int trials = 1_000_000;
		final int[] wins = new int[weights.length];

		for (int i = 0; i < weights.length; i++) {
			for (int t = 0; t < trials; t++) {
				if (throttle.tryAcquire(i)) {
					wins[i]++;
				}
			}
		}

		final double totalWeight = 70 + 20 + 10;
		final double[] expected = {70 / totalWeight, 20 / totalWeight, 10 / totalWeight
		};
		for (int i = 0; i < weights.length; i++) {
			final double observed = (double) wins[i] / trials;
			assertEquals(expected[i], observed, 0.01,
							"Index " + i + ": expected " + expected[i] + " but observed " + observed);
		}
	}

	@Test
	@DisplayName("Batch acquire returns all permits or zero — never a partial value")
	void batchAcquireReturnsAllOrNothing() {
		final int[] weights = {50, 50
		};
		final WeightedRandomThrottle throttle = new WeightedRandomThrottle(weights);
		final int permits = 17;

		for (int t = 0; t < 10_000; t++) {
			final int result = throttle.tryAcquire(0, permits);
			assertTrue(result == permits || result == 0,
							"Expected " + permits + " or 0 but got " + result);
		}
	}

	@Test
	@DisplayName("Zero-weight index is never granted")
	void zeroWeightNeverGranted() {
		final int[] weights = {50, 0, 50
		};
		final WeightedRandomThrottle throttle = new WeightedRandomThrottle(weights);

		for (int t = 0; t < 100_000; t++) {
			assertFalse(throttle.tryAcquire(1),
							"Zero-weight index should never be granted (trial " + t + ")");
		}
	}

	@Test
	@DisplayName("Out-of-bounds index returns false / 0")
	void outOfBoundsIndexReturnsFalse() {
		final int[] weights = {50, 50
		};
		final WeightedRandomThrottle throttle = new WeightedRandomThrottle(weights);

		assertFalse(throttle.tryAcquire(-1));
		assertFalse(throttle.tryAcquire(2));
		assertEquals(0, throttle.tryAcquire(-1, 10));
		assertEquals(0, throttle.tryAcquire(2, 10));
	}

	@Test
	@DisplayName("Constructor rejects invalid input")
	void constructorRejectsInvalidInput() {
		assertThrows(IllegalArgumentException.class, () -> new WeightedRandomThrottle(null),
						"null array should be rejected");
		assertThrows(IllegalArgumentException.class, () -> new WeightedRandomThrottle(new int[0]),
						"empty array should be rejected");
		assertThrows(IllegalArgumentException.class, () -> new WeightedRandomThrottle(new int[]{0, 0, 0
		}),
						"all-zero weights should be rejected");
		assertThrows(IllegalArgumentException.class, () -> new WeightedRandomThrottle(new int[]{10, -5, 20
		}),
						"negative weight should be rejected");
	}

	@Test
	@DisplayName("Single weight always grants")
	void singleWeightAlwaysGranted() {
		final WeightedRandomThrottle throttle = new WeightedRandomThrottle(new int[]{100
		});

		for (int t = 0; t < 10_000; t++) {
			assertTrue(throttle.tryAcquire(0),
							"Single-weight throttle must always grant (trial " + t + ")");
		}
	}
}
