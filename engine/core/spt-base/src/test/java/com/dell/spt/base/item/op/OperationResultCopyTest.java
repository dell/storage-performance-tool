package com.dell.spt.base.item.op;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.ItemImpl;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link OperationImpl#result()} produces a truly independent copy
 * whose timing, status, and flag fields survive mutation of the original.
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

		assertTrue(copy.duration() > 0, "copy should have positive duration");
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
	void resultCopy_preservesDriverRecycledFlag() {
		final var op = newTimedOp();
		op.driverRecycled(true);
		op.finishResponse();

		final var copy = op.result();

		assertTrue(copy.driverRecycled(),
						"driverRecycled flag must be preserved in the copy");
	}

	@Test
	void resultCopy_driverRecycledDefaultsFalse() {
		final var op = newTimedOp();
		op.finishResponse();

		final var copy = op.result();

		assertFalse(copy.driverRecycled(),
						"driverRecycled should default to false in the copy");
	}

	@Test
	void resetAfterResultCopy_doesNotAffectCopy() {
		final var op = newTimedOp();
		op.finishResponse();
		op.driverRecycled(true);

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
		assertTrue(copy.driverRecycled(),
						"copy driverRecycled must survive original reset");

		// Verify original was actually reset
		assertEquals(Operation.Status.PENDING, op.status(),
						"original should be PENDING after reset");
		assertEquals(0, op.reqTimeStart(),
						"original timing should be zeroed after reset");
		assertFalse(op.driverRecycled(),
						"original driverRecycled should be false after reset");
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
