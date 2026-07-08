package com.dell.spt.base.item.op.partial.data;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

/**
 * Test gap (load-op-retry code review): opRetryCount()/result() copy preservation was
 * only exercised against base OperationImpl and a few other concrete subclasses, not
 * PartialDataOperationImpl - a multipart-upload part operation. Its own {@code
 * retryCount()}/{@code incrementRetryCount()} track a completely separate concept
 * (MAX_PART_RETRIES, the driver's own internal per-part retry mechanism) from {@code
 * opRetryCount()} (load-op-retry's bounded retry, tracked on the base Operation
 * interface) - this test is specifically about the latter not being lost via this
 * subclass's own copy constructor/result() override.
 */
class PartialDataOperationImplTest {

	@Test
	void resultCopyPreservesOpRetryCount() {
		final var item = new DataItemImpl("retry-part-obj", 0, 1024);
		final var op = new PartialDataOperationImpl<>(0, OpType.CREATE, item, null, "/dst", null, 0, null);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.startRequest();
		op.finishRequest();
		op.startResponse();

		final var copy = op.result();

		assertEquals(2, copy.opRetryCount(), "PartialDataOperationImpl's result() copy must preserve opRetryCount");
		op.incrementOpRetryCount();
		assertEquals(2, copy.opRetryCount(), "copy opRetryCount must not change when original is mutated afterward");

		op.reset();
		assertEquals(3, op.opRetryCount(), "opRetryCount must survive reset() on the original");
	}

	@Test
	void opRetryCountIsIndependentOfPartRetryCount() {
		// The two counters must not be conflated: incrementing one must not affect the
		// other, since they're bounded by completely different, unrelated limits
		// (load-op-retryLimit vs. CoopStorageDriverBase.MAX_PART_RETRIES).
		final var item = new DataItemImpl("retry-part-obj-2", 0, 1024);
		final var op = new PartialDataOperationImpl<>(0, OpType.CREATE, item, null, "/dst", null, 0, null);

		op.incrementOpRetryCount();
		op.incrementRetryCount();
		op.incrementRetryCount();

		assertEquals(1, op.opRetryCount(), "opRetryCount must reflect only its own increments");
		assertEquals(2, op.retryCount(), "retryCount (part-level) must reflect only its own increments");
	}
}
