package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.akurilov.commons.concurrent.throttle.SequentialWeightsThrottle;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class RetiringWeightsThrottleTest {

	private static final int ANY = 1_000;

	@Test
	void withNoOriginRetiredItGrantsWhatTheLibraryThrottleGrants() {
		final Random random = new Random(20261118L);
		for (int round = 0; round < 500; round++) {
			final int origins = 1 + random.nextInt(6);
			final int[] weights = new int[origins];
			for (int i = 0; i < origins; i++) {
				weights[i] = random.nextInt(8) == 0 ? 0 : 1 + random.nextInt(100);
			}
			final var library = new SequentialWeightsThrottle(weights);
			final var throttle = new RetiringWeightsThrottle(weights);
			for (int call = 0; call < 2_000; call++) {
				final int origin = random.nextInt(origins);
				if (random.nextInt(8) == 0) {
					assertEquals(library.tryAcquire(origin), throttle.tryAcquire(origin), "round " + round + " call " + call);
				} else {
					final int permits = switch (random.nextInt(4)) {
					case 0 -> 0;
					case 1 -> random.nextInt(4_096);
					default -> random.nextInt(8);
					};
					assertEquals(
									library.tryAcquire(origin, permits),
									throttle.tryAcquire(origin, permits),
									"round " + round + " call " + call);
				}
			}
		}
	}

	@Test
	void anUnusedShareHoldsBackTheCycleUntilItsOriginRetires() {
		final var throttle = new RetiringWeightsThrottle(new int[]{5, 5
		});
		assertEquals(5, throttle.tryAcquire(1, ANY));
		assertEquals(0, throttle.tryAcquire(1, ANY));
		throttle.retire(0);
		assertEquals(5, throttle.tryAcquire(1, ANY));
		assertEquals(5, throttle.tryAcquire(1, ANY));
	}

	@Test
	void theRemainingOriginsKeepTheirRatio() {
		final var throttle = new RetiringWeightsThrottle(new int[]{10, 20, 80
		});
		throttle.retire(0);
		for (int cycle = 0; cycle < 3; cycle++) {
			assertEquals(20, throttle.tryAcquire(1, ANY));
			assertEquals(0, throttle.tryAcquire(1, ANY), "origin 2 still holds its share");
			assertEquals(80, throttle.tryAcquire(2, ANY));
		}
	}

	@Test
	void aRetiredOriginIsGrantedWhatItAsksForWithoutTouchingTheCycle() {
		final var throttle = new RetiringWeightsThrottle(new int[]{5, 5
		});
		throttle.retire(0);
		assertEquals(3, throttle.tryAcquire(0, 3));
		assertTrue(throttle.tryAcquire(0));
		assertEquals(0, throttle.tryAcquire(0, 0));
		assertEquals(2, throttle.tryAcquire(1, 2));
		assertEquals(7, throttle.tryAcquire(0, 7));
		assertEquals(3, throttle.tryAcquire(1, ANY), "the retired origin took none of this cycle");
	}

	@Test
	void retiringEveryOriginIsSafe() {
		final var throttle = new RetiringWeightsThrottle(new int[]{5, 5
		});
		throttle.retire(0);
		throttle.retire(1);
		assertEquals(4, throttle.tryAcquire(0, 4));
		assertEquals(4, throttle.tryAcquire(1, 4));
		assertTrue(throttle.tryAcquire(1));
	}

	@Test
	void retiringTwiceChangesNothing() {
		final var throttle = new RetiringWeightsThrottle(new int[]{5, 5
		});
		throttle.retire(0);
		assertEquals(2, throttle.tryAcquire(1, 2));
		throttle.retire(0);
		assertEquals(3, throttle.tryAcquire(1, ANY), "origin 1 keeps the rest of its share");
		assertEquals(5, throttle.tryAcquire(1, ANY));
	}

	@Test
	void anOriginWithoutWeightStaysRefusedWhileItIsActive() {
		final var throttle = new RetiringWeightsThrottle(new int[]{0, 5
		});
		assertEquals(0, throttle.tryAcquire(0, 1));
		assertFalse(throttle.tryAcquire(0));
		assertEquals(5, throttle.tryAcquire(1, ANY));
		assertEquals(5, throttle.tryAcquire(1, ANY));
	}
}
