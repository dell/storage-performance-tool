package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class DurationTimeTest {
	@Test
	void signedNanoTimeValuesUseElapsedDifferencesInsteadOfPositivity() {
		assertEquals(-50, DurationTime.deadlineAfter(-100, 50));
		assertTrue(DurationTime.isPositiveRange(-100, -50));
		assertFalse(DurationTime.deadlineReached(-50, -75));
		assertTrue(DurationTime.deadlineReached(-50, -25));
		assertTrue(DurationTime.timestampBeforeDeadline(-75, -50));
		assertEquals(25, DurationTime.remainingNanos(-50, -75));
		assertEquals(50, DurationTime.elapsedNanos(-100, -50));
	}

	@Test
	void finiteDeadlineRemainsBoundedAcrossSignedNanoTimeRollover() {
		final long startNanos = Long.MAX_VALUE - 10;
		final long deadlineNanos = DurationTime.deadlineAfter(startNanos, 20);

		assertEquals(Long.MIN_VALUE + 9, deadlineNanos);
		assertTrue(DurationTime.isPositiveRange(startNanos, deadlineNanos));
		assertFalse(DurationTime.deadlineReached(deadlineNanos, Long.MIN_VALUE));
		assertTrue(DurationTime.deadlineReached(deadlineNanos, Long.MIN_VALUE + 9));
		assertEquals(9, DurationTime.remainingNanos(deadlineNanos, Long.MIN_VALUE));
		assertEquals(20, DurationTime.elapsedNanos(startNanos, deadlineNanos));
	}

	@Test
	void maxValueCanBeAFiniteDeadline() {
		assertEquals(Long.MAX_VALUE, DurationTime.deadlineAfter(Long.MAX_VALUE - 1, 1));
		assertFalse(DurationTime.deadlineReached(Long.MAX_VALUE, Long.MAX_VALUE - 1));
		assertTrue(DurationTime.deadlineReached(Long.MAX_VALUE, Long.MAX_VALUE));
		assertEquals(1, DurationTime.remainingNanos(Long.MAX_VALUE, Long.MAX_VALUE - 1));
	}

	@Test
	void earlierDeadlineUsesRemainingTimeAcrossSignedRollover() {
		final long observedNanos = Long.MAX_VALUE - 10;
		final long earlierDeadlineNanos = DurationTime.deadlineAfter(observedNanos, 5);
		final long laterDeadlineNanos = DurationTime.deadlineAfter(observedNanos, 20);

		assertEquals(
						earlierDeadlineNanos,
						DurationTime.earlierDeadline(
										earlierDeadlineNanos, laterDeadlineNanos, observedNanos));
	}
}
