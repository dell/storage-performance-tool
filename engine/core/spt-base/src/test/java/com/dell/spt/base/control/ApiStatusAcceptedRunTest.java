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
