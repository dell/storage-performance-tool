package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.control.run.RunTerminalException;
import org.junit.jupiter.api.Test;

class BucketMetricsBudgetTest {

	private static final long GIB = 1L << 30;
	/** 1,000 members plus the catch-all, for each of a mixed step's four operation types. */
	private static final long LARGEST_STEP_SETS = 4 * 1001;

	@Test
	void theLargestStepFitsTheDefaultHeap() {
		final long defaultHeap = 4_280L * (1L << 20);
		assertDoesNotThrow(() -> BucketMetricsBudget.check(LARGEST_STEP_SETS, defaultHeap));
		assertTrue(
						BucketMetricsBudget.sustainedBytes(LARGEST_STEP_SETS) < 0.25 * defaultHeap,
						"the decided sustained budget is 25% of the default heap");
	}

	@Test
	void aSetTooLargeForTheHeapFailsWithADiagnosticBeforeAnyAllocation() {
		final var failure = assertThrows(
						RunTerminalException.class, () -> BucketMetricsBudget.check(LARGEST_STEP_SETS, GIB));
		assertTrue(failure.getMessage().contains("--bucket-metrics=false"), failure.getMessage());
		assertTrue(failure.getMessage().contains("maximum heap"), failure.getMessage());
		assertTrue(failure.getMessage().contains("40%"), failure.getMessage());
	}

	@Test
	void theLimitIsFortyPercentOfTheHeap() {
		final long sustained = BucketMetricsBudget.sustainedBytes(1000);
		assertDoesNotThrow(() -> BucketMetricsBudget.check(1000, (long) (sustained / 0.40) + 1));
		assertThrows(RunTerminalException.class, () -> BucketMetricsBudget.check(1000, (long) (sustained / 0.40) - 1000));
	}
}
