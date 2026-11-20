package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationFailureMark;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * When a composite read finalizes, given that a part settles before its completion takes the parent's
 * lock: a count of zero does not mean that a failed part is finished.
 */
final class CompositeFinalizationTest {

	private final CompositeDataOperationImpl<DataItem> read = new CompositeDataOperationImpl<>(
					0, OpType.READ, new DataItemImpl("ranged", 9, 3072L), null, "/bucket", null, null, 0, 1024L);
	private final List<PartialDataOperation<DataItem>> parts = parts();

	@SuppressWarnings("unchecked")
	private List<PartialDataOperation<DataItem>> parts() {
		return (List<PartialDataOperation<DataItem>>) (List<?>) read.subOperations();
	}

	private void settle(final PartialDataOperation<DataItem> part, final Operation.Status status) {
		part.status(status);
		part.startResponse();
		part.finishResponse();
	}

	private void useEveryRetry(final PartialDataOperation<DataItem> part) {
		for (var i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			part.incrementRetryCount();
		}
	}

	/** What the completion of {@code part}, out of retries, records under the read's monitor. */
	private void recordFailure(final PartialDataOperation<DataItem> part) {
		read.put("mpuFailure", part.status().name());
		PartialDataOperationFailureMark.markFailureRecorded((PartialDataOperationImpl<?>) part);
	}

	@Test
	void aFailedPartThatCanStillRetryHoldsFinalizationEvenWithAFailureRecorded() {
		settle(parts.get(0), Operation.Status.FAIL_IO);
		useEveryRetry(parts.get(0));
		recordFailure(parts.get(0));
		settle(parts.get(1), Operation.Status.FAIL_IO);
		settle(parts.get(2), Operation.Status.SUCC);

		assertTrue(read.allSubOperationsDone(), "the count reached zero before the second part's retry");
		assertFalse(CoopStorageDriverBase.shouldEnqueueFinalization(read));
	}

	@Test
	void aFailedPartOneRetryShortOfTheLimitStillHoldsFinalization() {
		settle(parts.get(0), Operation.Status.FAIL_IO);
		useEveryRetry(parts.get(0));
		recordFailure(parts.get(0));
		settle(parts.get(1), Operation.Status.FAIL_IO);
		for (var i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES - 1; i++) {
			parts.get(1).incrementRetryCount();
		}
		settle(parts.get(2), Operation.Status.SUCC);

		assertFalse(CoopStorageDriverBase.shouldEnqueueFinalization(read));
	}

	@Test
	void theLastPartToEndFinalizesOnceAnotherRanOutOfRetries() {
		settle(parts.get(0), Operation.Status.FAIL_IO);
		useEveryRetry(parts.get(0));
		recordFailure(parts.get(0));
		settle(parts.get(1), Operation.Status.SUCC);
		settle(parts.get(2), Operation.Status.SUCC);

		assertTrue(CoopStorageDriverBase.shouldEnqueueFinalization(read));
		assertFalse(CoopStorageDriverBase.shouldEnqueueFinalization(read), "only once");
	}

	@Test
	void aPartOutOfRetriesWhoseFailureIsNotYetRecordedWaitsForItsOwnCompletion() {
		settle(parts.get(0), Operation.Status.FAIL_IO);
		useEveryRetry(parts.get(0));
		settle(parts.get(1), Operation.Status.SUCC);
		settle(parts.get(2), Operation.Status.SUCC);

		assertFalse(CoopStorageDriverBase.shouldEnqueueFinalization(read), "the completion of the failed part decides");
		recordFailure(parts.get(0));
		assertTrue(CoopStorageDriverBase.shouldEnqueueFinalization(read));
	}

	@Test
	void twoPartsOutOfRetriesFinalizeOnce() {
		settle(parts.get(0), Operation.Status.FAIL_IO);
		useEveryRetry(parts.get(0));
		recordFailure(parts.get(0));
		settle(parts.get(1), Operation.Status.FAIL_IO);
		useEveryRetry(parts.get(1));
		recordFailure(parts.get(1));
		settle(parts.get(2), Operation.Status.SUCC);

		assertTrue(CoopStorageDriverBase.shouldEnqueueFinalization(read));
		assertFalse(CoopStorageDriverBase.shouldEnqueueFinalization(read), "only once");
	}

	/**
	 * With one failure recorded, a second part out of retries that has settled but whose completion
	 * has not recorded its own failure yet holds finalization: its completion records the failure the
	 * read ends with. Its attempt count and status alone do not show that it has.
	 */
	@Test
	void aSecondPartOutOfRetriesHoldsFinalizationUntilItsFailureIsRecorded() {
		settle(parts.get(0), Operation.Status.RESP_FAIL_AUTH);
		useEveryRetry(parts.get(0));
		settle(parts.get(1), Operation.Status.RESP_FAIL_SVC);
		useEveryRetry(parts.get(1));
		settle(parts.get(2), Operation.Status.SUCC);
		recordFailure(parts.get(0));

		assertFalse(CoopStorageDriverBase.shouldEnqueueFinalization(read), "the second failed part's completion decides");
		recordFailure(parts.get(1));
		assertTrue(CoopStorageDriverBase.shouldEnqueueFinalization(read));
		assertEquals(Operation.Status.RESP_FAIL_SVC.name(), read.get("mpuFailure"));
	}
}
