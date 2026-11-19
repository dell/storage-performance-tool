package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ApiStatusAcceptedRunTest {

	@Test
	void remembersRunsItAcceptedThroughEveryLaterState() {
		final ApiStatus status = new ApiStatus();
		assertFalse(status.acceptedRun(0L));
		assertFalse(status.acceptedRun(5L));

		status.setRunning("s1", 5L);
		assertTrue(status.acceptedRun(5L));
		status.completeIfNotStopped();
		assertTrue(status.acceptedRun(5L));

		status.setRunning("s2", 6L);
		status.setStopped();
		assertTrue(status.acceptedRun(5L), "an earlier run's retained rows keep their role");
		assertTrue(status.acceptedRun(6L));
		assertFalse(status.acceptedRun(0L), "zero is never a run id");
	}

	@Test
	void forgetsARunTheExecutorRefusedUnlessItWasAcceptedBefore() {
		final ApiStatus status = new ApiStatus();
		status.setRunning("s1", 5L);

		final boolean sevenBefore = status.acceptedRun(7L);
		status.setRunning("s2", 7L);
		status.forgetRefusedRun(7L, sevenBefore);
		assertFalse(status.acceptedRun(7L), "a refused run is not remembered");
		assertTrue(status.acceptedRun(5L));

		final boolean fiveBefore = status.acceptedRun(5L);
		status.setRunning("s3", 5L);
		status.forgetRefusedRun(5L, fiveBefore);
		assertTrue(status.acceptedRun(5L), "a refused request with an accepted run's id keeps that run's role");
	}

	@Test
	void refusedRunsPushNoAcceptedRunOut() {
		final ApiStatus status = new ApiStatus();
		for (long runId = 1L; runId <= ApiStatus.ACCEPTED_RUN_IDS; runId++) {
			status.setRunning("s", runId);
		}
		for (long refused = 100L; refused < 100L + 2 * ApiStatus.ACCEPTED_RUN_IDS; refused++) {
			final boolean acceptedBefore = status.acceptedRun(refused);
			status.setRunning("s", refused);
			status.forgetRefusedRun(refused, acceptedBefore);
			assertFalse(status.acceptedRun(refused), "refused run " + refused);
		}
		for (long runId = 1L; runId <= ApiStatus.ACCEPTED_RUN_IDS; runId++) {
			assertTrue(status.acceptedRun(runId), "run " + runId);
		}
		status.setRunning("s", 200L);
		assertFalse(status.acceptedRun(1L), "the next accepted run pushes out the oldest, as before");
		assertTrue(status.acceptedRun(200L));
	}

	@Test
	void keepsTheLatestRunsOnly() {
		final ApiStatus status = new ApiStatus();
		for (long runId = 1L; runId <= ApiStatus.ACCEPTED_RUN_IDS + 1; runId++) {
			status.setRunning("s", runId);
			status.setRunning("s", runId); // a repeated id takes no second place
		}
		assertFalse(status.acceptedRun(1L));
		for (long runId = 2L; runId <= ApiStatus.ACCEPTED_RUN_IDS + 1; runId++) {
			assertTrue(status.acceptedRun(runId), "run " + runId);
		}
	}
}
