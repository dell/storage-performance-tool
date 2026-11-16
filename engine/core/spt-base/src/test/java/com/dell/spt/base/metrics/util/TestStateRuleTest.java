package com.dell.spt.base.metrics.util;

import static com.dell.spt.base.metrics.util.TestStateRule.COMPLETED;
import static com.dell.spt.base.metrics.util.TestStateRule.RUNNING;
import static com.dell.spt.base.metrics.util.TestStateRule.activeState;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class TestStateRuleTest {

	@Test
	void noOperationsReadsRunning() {
		assertEquals(RUNNING, activeState(10L, 100L, 60_000L, 0L, 0L, 0L));
	}

	@Test
	void operationsInFlightReadRunningPastTheLimits() {
		assertEquals(RUNNING, activeState(10L, 100L, 60_000L, 200L, 0L, 1L));
	}

	@Test
	void noLimitReadsRunningAtConcurrencyZero() {
		assertEquals(RUNNING, activeState(0L, 0L, 60_000L, 100L, 5L, 0L));
	}

	@Test
	void negativeLimitsCountAsNoLimit() {
		assertEquals(RUNNING, activeState(-1L, -1L, 60_000L, 100L, 5L, 0L));
	}

	@Test
	void timeLimitedBeforeLimitReadsRunning() {
		assertEquals(RUNNING, activeState(10L, 0L, 9_999L, 100L, 0L, 0L));
	}

	@Test
	void timeLimitedAtLimitReadsCompleted() {
		assertEquals(COMPLETED, activeState(10L, 0L, 10_000L, 100L, 0L, 0L));
	}

	@Test
	void countLimitedBelowCountReadsRunning() {
		assertEquals(RUNNING, activeState(0L, 100L, 60_000L, 98L, 1L, 0L));
	}

	@Test
	void countLimitedAtCountReadsCompletedCountingFailures() {
		assertEquals(COMPLETED, activeState(0L, 100L, 1_000L, 99L, 1L, 0L));
	}

	@Test
	void bothLimitsNeedTimeElapsedAndCountReached() {
		assertEquals(RUNNING, activeState(10L, 100L, 5_000L, 100L, 0L, 0L));
		assertEquals(RUNNING, activeState(10L, 100L, 20_000L, 50L, 0L, 0L));
		assertEquals(COMPLETED, activeState(10L, 100L, 20_000L, 100L, 0L, 0L));
	}

	@Test
	void negativeConcurrencySampleIsNotInFlight() {
		assertEquals(COMPLETED, activeState(0L, 100L, 1_000L, 100L, 0L, -16L));
		assertEquals(RUNNING, activeState(0L, 0L, 1_000L, 100L, 0L, -16L));
	}
}
