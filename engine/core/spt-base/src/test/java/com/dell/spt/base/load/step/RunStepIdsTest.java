package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Which step of a run is the first with its id: what tells an earlier run's files from this run's. */
class RunStepIdsTest {

	@Test
	void onlyTheFirstStepOfAnIdInARunIsTheFirst() {
		final var ids = new RunStepIds();

		assertTrue(ids.firstInRun(100L, "X"));
		assertFalse(ids.firstInRun(100L, "X"), "a second step of the id in the same run");
		assertTrue(ids.firstInRun(100L, "Y"));
		assertTrue(ids.firstInRun(101L, "X"), "the same id in a later run");
		assertFalse(ids.firstInRun(100L, "X"), "the earlier run is still known");
	}

	@Test
	void aStepWithoutARunIdIsNeverTheFirst() {
		final var ids = new RunStepIds();

		assertFalse(ids.firstInRun(0L, "X"));
		assertFalse(ids.firstInRun(-1L, "X"));
	}

	@Test
	void onlyTheLatestRunsAreKeptAndARunInUseStaysKept() {
		final var ids = new RunStepIds();
		assertTrue(ids.firstInRun(1L, "X"));
		for (long run = 2; run <= RunStepIds.RUNS_KEPT; run++) {
			assertTrue(ids.firstInRun(run, "X"));
			// Run 1 starts a step again, so it stays among the latest.
			assertFalse(ids.firstInRun(1L, "X"));
		}
		assertTrue(ids.firstInRun(RunStepIds.RUNS_KEPT + 1L, "X"));

		assertFalse(ids.firstInRun(1L, "X"), "run 1 started a step last, so it is kept");
		assertTrue(ids.firstInRun(2L, "X"), "run 2 was the oldest, so it was dropped");
	}
}
