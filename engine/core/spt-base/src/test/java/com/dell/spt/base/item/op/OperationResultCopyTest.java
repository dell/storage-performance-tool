package com.dell.spt.base.item.op;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.integrity.IntegrityMetadata;
import com.dell.spt.base.item.ItemImpl;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link OperationImpl#result()} produces a truly independent copy
 * whose timing and status fields survive mutation of the original.
 * These guarantees are critical because the completion path calls
 * {@code op.result()} to snapshot metrics, then the original op may be
 * recycled/reset/re-submitted.
 */
class OperationResultCopyTest {

	private OperationImpl<ItemImpl> newTimedOp() {
		final var item = new ItemImpl("test-obj-1");
		final var op = new OperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null);
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		op.status(Operation.Status.SUCC);
		return op;
	}

	@Test
	void resultCopy_isIndependentOfOriginalStatus() {
		final var op = newTimedOp();
		op.finishResponse();
		final var copy = op.result();

		// Mutate the original after copying
		op.status(Operation.Status.FAIL_IO);

		assertEquals(Operation.Status.SUCC, copy.status(),
						"copy status must not change when original is mutated");
	}

	@Test
	void resultCopy_preservesDurationAndLatency() {
		final var op = newTimedOp();
		op.finishResponse();

		final var copy = op.result();

		assertTrue(copy.duration() >= 0, "copy should have non-negative duration");
		assertTrue(copy.latency() >= 0, "copy should have non-negative latency");
		assertEquals(op.duration(), copy.duration(),
						"copy duration must match original at snapshot time");
		assertEquals(op.latency(), copy.latency(),
						"copy latency must match original at snapshot time");
	}

	@Test
	void resultCopy_preservesAllTimingFields() {
		final var op = newTimedOp();
		op.finishResponse();

		final var copy = op.result();

		assertEquals(op.reqTimeStart(), copy.reqTimeStart(), "reqTimeStart must be copied");
		assertEquals(op.reqTimeDone(), copy.reqTimeDone(), "reqTimeDone must be copied");
		assertEquals(op.respTimeStart(), copy.respTimeStart(), "respTimeStart must be copied");
		assertEquals(op.respTimeDone(), copy.respTimeDone(), "respTimeDone must be copied");
	}

	@Test
	void resetAfterResultCopy_doesNotAffectCopy() {
		final var op = newTimedOp();
		op.finishResponse();

		final long origDuration = op.duration();
		final long origReqTimeStart = op.reqTimeStart();
		final var copy = op.result();

		// Simulate recycling: reset clears timing and status
		op.reset();

		assertEquals(Operation.Status.SUCC, copy.status(),
						"copy status must survive original reset");
		assertEquals(origDuration, copy.duration(),
						"copy duration must survive original reset");
		assertEquals(origReqTimeStart, copy.reqTimeStart(),
						"copy reqTimeStart must survive original reset");

		// Verify original was actually reset
		assertEquals(Operation.Status.PENDING, op.status(),
						"original should be PENDING after reset");
		assertEquals(0, op.reqTimeStart(),
						"original timing should be zeroed after reset");
	}

	@Test
	void resultCopy_timingNotAffectedByOriginalRestart() {
		final var op = newTimedOp();
		op.finishResponse();

		final long origReqTimeStart = op.reqTimeStart();
		final long origRespTimeDone = op.respTimeDone();
		final var copy = op.result();

		// Simulate re-submit: startRequest overwrites reqTimeStart
		op.reset();
		op.startRequest();

		assertEquals(origReqTimeStart, copy.reqTimeStart(),
						"copy reqTimeStart must not change when original restarts");
		assertEquals(origRespTimeDone, copy.respTimeDone(),
						"copy respTimeDone must not change when original restarts");
	}

	@Test
	void finishResponse_recordsTimingAtCallTime() throws InterruptedException {
		final var item = new ItemImpl("timing-obj");
		final var op = new OperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null);
		op.startRequest();
		op.finishRequest();
		op.startResponse();

		// Record time just before finishResponse
		final long beforeFinish = Operation.START_OFFSET_MICROS + System.nanoTime() / 1000;
		op.finishResponse();
		final long afterFinish = Operation.START_OFFSET_MICROS + System.nanoTime() / 1000;

		assertTrue(op.respTimeDone() >= beforeFinish,
						"respTimeDone must be >= time just before finishResponse call");
		assertTrue(op.respTimeDone() <= afterFinish,
						"respTimeDone must be <= time just after finishResponse call");
	}

	@Test
	void finishResponse_throwsWhenResponseNotStarted() {
		final var item = new ItemImpl("no-resp-start");
		final var op = new OperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null);
		op.startRequest();
		op.finishRequest();
		// Do NOT call startResponse()

		assertThrows(IllegalStateException.class, op::finishResponse,
						"finishResponse should throw when startResponse was never called");
	}

	@Test
	void resultCopy_opRetryCountPreserved() {
		// Finding: load-op-retry's bounded check (opRetryCount() < retryLimit) reads the
		// *result copy* that flows through LoadStepContextImpl.put(), not the live op - it
		// must see the same count the live op had at snapshot time.
		final var op = newTimedOp();
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.finishResponse();

		final var copy = op.result();

		assertEquals(2, copy.opRetryCount(), "copy must preserve opRetryCount at snapshot time");

		// Mutate the original after copying
		op.incrementOpRetryCount();
		assertEquals(2, copy.opRetryCount(), "copy opRetryCount must not change when original is mutated");
	}

	@Test
	void resultCopyPreservesExplicitVersionAndIntegrityState() {
		final var op = newTimedOp();
		op.requestedVersionId("requested-v1");
		op.returnedVersionId("returned-v2");
		final var metadata = new IntegrityMetadata("1", "sha256", "0".repeat(64), 10);
		op.integrityMetadata(metadata);
		op.finishResponse();

		final var copy = op.result();
		op.reset();

		assertEquals("requested-v1", copy.requestedVersionId());
		assertEquals("returned-v2", copy.returnedVersionId());
		assertSame(metadata, copy.integrityMetadata());
		assertEquals("requested-v1", op.requestedVersionId(), "request identity survives retry reset");
		assertNull(op.returnedVersionId(), "response identity is cleared for retry");
	}

	@Test
	void resetAfterIncrementOpRetryCount_survivesReset() {
		// Finding: reset() is called before every redispatch, including a load-op-retry
		// re-dispatch of a failed attempt - it must deliberately *not* clear opRetryCount,
		// or the bounded-retry limit could never actually be reached.
		final var op = newTimedOp();
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.finishResponse();

		op.reset();

		assertEquals(2, op.opRetryCount(), "opRetryCount must survive reset() for retry re-dispatch");
		assertEquals(Operation.Status.PENDING, op.status(), "reset should still clear status as normal");
	}

	@Test
	void resetOpRetryCount_zeroesTheCounter() {
		// Finding: without an explicit reset hook, a recycled read-loop operation that failed
		// and retried once before eventually succeeding
		// would keep an elevated opRetryCount forever across every future successful cycle,
		// eventually exhausting its retry budget from unrelated, non-consecutive failures.
		final var op = newTimedOp();
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		assertEquals(2, op.opRetryCount());

		op.resetOpRetryCount();

		assertEquals(0, op.opRetryCount(), "resetOpRetryCount() must zero the counter");
	}

	@Test
	void resetOpRetryCount_doesNotSurviveViaResultCopyUnlessCalledBeforeSnapshot() {
		// resetOpRetryCount() must actually mutate the counter the same way
		// incrementOpRetryCount() does - i.e. result() must snapshot whatever the count was
		// *at snapshot time*, matching the general result()-is-a-snapshot contract already
		// covered by the other tests in this class.
		final var op = newTimedOp();
		op.incrementOpRetryCount();
		op.resetOpRetryCount();
		op.finishResponse();

		final var copy = op.result();

		assertEquals(0, copy.opRetryCount(), "copy must reflect a reset that happened before the snapshot");
	}

	@Test
	void supportsOpRetryTracking_trueForRealImplementation() {
		final var op = newTimedOp();
		assertTrue(op.supportsOpRetryTracking(),
						"OperationImpl must report real retry tracking support");
	}

	@Test
	void resultCopy_nodeAddrPreserved() {
		final var op = newTimedOp();
		op.nodeAddr("10.0.0.1:9020");
		op.finishResponse();

		final var copy = op.result();

		assertEquals("10.0.0.1:9020", copy.nodeAddr(),
						"nodeAddr must be preserved in the copy");

		// Mutate original
		op.nodeAddr("10.0.0.2:9020");
		assertEquals("10.0.0.1:9020", copy.nodeAddr(),
						"copy nodeAddr must not change when original is mutated");
	}
}
