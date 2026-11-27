package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.akurilov.commons.concurrent.throttle.SequentialWeightsThrottle;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class RetiringWeightsThrottleTest {

	private static final int ANY = 1_000;
	private static final int MIN_CYCLE = 100;

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

	@Test
	void aShortStartingCycleIsScaledByOneInteger() {
		assertFirstCycle(floored(1, 1), 50, 50);
		assertFirstCycle(floored(1, 4), 20, 80);
		assertFirstCycle(floored(1, 2), 34, 68);
		assertFirstCycle(floored(1, 1, 1), 34, 34, 34);
		assertFirstCycle(floored(0, 1), 0, 100);
		assertFirstCycle(floored(49, 50), 98, 100);
		assertFirstCycle(floored(99), 198);
		assertFirstCycle(floored(1, 1, 48), 2, 2, 96);
	}

	@Test
	void aStartingCycleAtTheMinimumIsAsWritten() {
		assertFirstCycle(floored(20, 80), 20, 80);
		assertFirstCycle(floored(1, 1, 98), 1, 1, 98);
		assertFirstCycle(floored(100, 0), 100, 0);
		assertFirstCycle(floored(1000, 4000), 1000, 4000);
		assertArrayEquals(ints(Integer.MAX_VALUE, Integer.MAX_VALUE), floored(Integer.MAX_VALUE, Integer.MAX_VALUE).cycleWeights());
	}

	@Test
	void aCycleWithoutPositiveWeightsIsAsWritten() {
		assertArrayEquals(ints(0, 0), floored(0, 0).cycleWeights());
		assertArrayEquals(ints(-1, -1), floored(-1, -1).cycleWeights());
		assertArrayEquals(ints(-1, 0), floored(-1, 0).cycleWeights());
	}

	@Test
	void zeroAndNegativeWeightsStayAsWrittenAndDoNotCount() {
		final var throttle = floored(0, -3, Integer.MIN_VALUE, 1, 1, 48);
		assertArrayEquals(ints(0, -3, Integer.MIN_VALUE, 2, 2, 96), throttle.cycleWeights());
		throttle.retire(5);
		assertArrayEquals(ints(0, -3, Integer.MIN_VALUE, 50, 50, 0), throttle.cycleWeights());
		assertEquals(0, throttle.tryAcquire(0, 1));
		assertFalse(throttle.tryAcquire(1));
		assertFalse(throttle.tryAcquire(2));
	}

	@Test
	void theWrittenArrayIsNotChanged() {
		final int[] weights = ints(1, 1, 48);
		final var throttle = new RetiringWeightsThrottle(weights, MIN_CYCLE);
		throttle.retire(2);
		assertArrayEquals(ints(1, 1, 48), weights);
	}

	/** After each retirement, the origins left get the permits of a throttle built from their written weights. */
	@ParameterizedTest
	@CsvSource({"1;1;48", "1;1;98", "50;50;2400", "60;60;10", "1;2;3;94", "7;48", "1;48"
	})
	void afterARetirementTheOthersGetTheCycleOfTheirOwnWeights(final String written) {
		final int[] weights = parse(written);
		final var throttle = new RetiringWeightsThrottle(weights, MIN_CYCLE);
		final Random random = new Random(20261126L + written.hashCode());
		final boolean[] gone = new boolean[weights.length];
		for (int last = weights.length - 1; last > 0; last--) {
			throttle.retire(last);
			gone[last] = true;
			drainCycle(throttle, gone);
			final int[] survivors = Arrays.copyOf(weights, last);
			final var reference = new RetiringWeightsThrottle(survivors, MIN_CYCLE);
			assertArrayEquals(
							Arrays.copyOf(reference.cycleWeights(), weights.length),
							throttle.cycleWeights(),
							written + " after retiring origin " + last);
			for (int call = 0; call < 2_000; call++) {
				final int origin = random.nextInt(last);
				final int permits = random.nextInt(4) == 0 ? random.nextInt(4_096) : random.nextInt(8);
				assertEquals(
								reference.tryAcquire(origin, permits),
								throttle.tryAcquire(origin, permits),
								written + " after retiring origin " + last + ", call " + call);
			}
			drainCycle(throttle, gone);
		}
	}

	@Test
	void aLiveSumAtTheMinimumKeepsTheWrittenWeights() {
		final var throttle = floored(50, 50, 2400);
		throttle.retire(2);
		for (int cycle = 0; cycle < 3; cycle++) {
			assertEquals(50, throttle.tryAcquire(0, ANY));
			assertEquals(50, throttle.tryAcquire(1, ANY));
		}
		assertArrayEquals(ints(50, 50, 0), throttle.cycleWeights());
	}

	@Test
	void aLoneOriginLeftIsScaledToo() {
		final var throttle = floored(1, 48);
		throttle.retire(1);
		assertEquals(3, throttle.tryAcquire(0, ANY), "the share held at the retirement, from 1/48 scaled to 3/144");
		assertEquals(100, throttle.tryAcquire(0, ANY));
		assertEquals(100, throttle.tryAcquire(0, ANY));
	}

	@Test
	void retiringDoesNotChangeTheCurrentCycle() {
		final var throttle = floored(1, 1, 48);
		assertEquals(1, throttle.tryAcquire(0, 1));
		throttle.retire(2);
		assertEquals(1, throttle.tryAcquire(0, ANY), "origin 0 keeps the rest of its old share");
		assertEquals(0, throttle.tryAcquire(0, ANY), "origin 1 still holds its old share");
		assertEquals(2, throttle.tryAcquire(1, ANY));
		assertEquals(50, throttle.tryAcquire(0, ANY));
		assertEquals(50, throttle.tryAcquire(1, ANY));
	}

	@Test
	void retiringTwiceWithAMinimumChangesNothing() {
		final var throttle = floored(1, 1, 48);
		throttle.retire(2);
		final int[] once = throttle.cycleWeights();
		throttle.retire(2);
		assertArrayEquals(once, throttle.cycleWeights());
		assertEquals(2, throttle.tryAcquire(0, ANY));
		assertEquals(2, throttle.tryAcquire(1, ANY));
		throttle.retire(2);
		assertEquals(50, throttle.tryAcquire(0, ANY));
		assertEquals(50, throttle.tryAcquire(1, ANY));
	}

	@Test
	void retiringEveryOriginWithAMinimumIsSafe() {
		final var throttle = floored(1, 1, 48);
		throttle.retire(2);
		throttle.retire(0);
		throttle.retire(1);
		assertArrayEquals(ints(0, 0, 0), throttle.cycleWeights());
		assertEquals(4, throttle.tryAcquire(0, 4));
		assertEquals(4, throttle.tryAcquire(1, 4));
		assertTrue(throttle.tryAcquire(2));
	}

	@Test
	void withoutAMinimumARetirementKeepsTheWrittenWeights() {
		final var throttle = new RetiringWeightsThrottle(ints(1, 1, 48));
		throttle.retire(2);
		assertArrayEquals(ints(1, 1, 0), throttle.cycleWeights());
		for (int cycle = 0; cycle < 3; cycle++) {
			assertEquals(1, throttle.tryAcquire(0, ANY));
			assertEquals(1, throttle.tryAcquire(1, ANY));
		}
	}

	private static RetiringWeightsThrottle floored(final int... weights) {
		return new RetiringWeightsThrottle(weights, MIN_CYCLE);
	}

	private static int[] ints(final int... values) {
		return values;
	}

	private static int[] parse(final String weights) {
		return Arrays.stream(weights.split(";")).mapToInt(Integer::parseInt).toArray();
	}

	/** Each origin takes all it can of the first cycle, in order: its share. */
	private static void assertFirstCycle(final RetiringWeightsThrottle throttle, final int... expected) {
		assertArrayEquals(expected, throttle.cycleWeights());
		final int[] permits = new int[expected.length];
		for (int i = 0; i < permits.length; i++) {
			permits[i] = throttle.tryAcquire(i, Integer.MAX_VALUE);
		}
		assertArrayEquals(expected, permits);
	}

	/** Uses up what the origins still in the cycle hold of the current one. */
	private static void drainCycle(final RetiringWeightsThrottle throttle, final boolean[] gone) {
		for (int i = 0; i < gone.length; i++) {
			if (!gone[i]) {
				throttle.tryAcquire(i, Integer.MAX_VALUE);
			}
		}
	}
}
