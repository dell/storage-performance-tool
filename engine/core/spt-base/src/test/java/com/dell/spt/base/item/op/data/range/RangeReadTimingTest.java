package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeReadTimingTest {
	private static RangeReadOperation<DataItemImpl> operation(AtomicLong clock) {
		return new RangeReadOperation<>(0, new DataItemImpl("item", 0, 10), "/bucket", null, null,
						new RangeReadPolicy(2, 3L, 1), bound -> 0, () -> clock.getAndAdd(10));
	}

	private static void headers(RangeReadAttempt a) {
		assertTrue(a.headers(206, Status.SUCC, List.of("bytes 3-4/10"), List.of(), List.of(), false));
	}

	@Test
	void durationIncludesRetryDelayButLatencyUsesSuccessfulAttempt() {
		var clock = new AtomicLong(100);
		var op = operation(clock);
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var first = c.beginAttempt(null);
		tracker.dispatched(op);
		first.requestHandoff();
		first.requestComplete();
		first.headers(503, Status.RESP_FAIL_SVC, List.of(), List.of(), List.of(), false);
		c.claimOutcome(first);
		op.reset();
		assertEquals(0, op.reqTimeStart());
		var retry = c.beginAttempt(first);
		clock.set(1000);
		retry.requestHandoff();
		assertFalse(retry.requestHandoff());
		retry.requestComplete();
		headers(retry);
		retry.bodyBytes(2);
		retry.finish(true);
		assertEquals(0, op.respTimeDone());
		assertTrue(c.complete(op, tracker, retry));
		assertEquals(100, op.reqTimeStart());
		assertEquals(1010, op.reqTimeDone());
		assertEquals(1020, op.respTimeStart());
		assertEquals(1030, op.respDataTimeStart());
		assertEquals(1040, op.respTimeDone());
		assertEquals(940, op.duration());
		assertEquals(10, op.latency());
		assertEquals(20, op.dataLatency());
		assertFalse(first.requestComplete());
		assertFalse(first.finish(true));
		assertEquals(940, op.duration());
		var recycled = op.result();
		tracker.driverQueued(recycled);
		recycled.reset();
		var next = recycled.circulation().beginAttempt(null);
		tracker.dispatched(recycled);
		clock.set(2000);
		next.requestHandoff();
		next.requestComplete();
		headers(next);
		next.bodyBytes(2);
		next.finish(true);
		assertTrue(recycled.circulation().complete(recycled, tracker, next));
		assertEquals(2000, recycled.reqTimeStart());
		assertEquals(40, recycled.duration());
		assertEquals(940, op.duration());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void absentRequestCompletionRemainsAbsentAndEmptyChunksDoNotStartBodyTiming() {
		var clock = new AtomicLong(100);
		var op = operation(clock);
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var a = op.circulation().beginAttempt(null);
		assertFalse(a.requestComplete());
		tracker.dispatched(op);
		a.requestHandoff();
		headers(a);
		assertFalse(a.requestComplete());
		a.bodyBytes(0);
		a.bodyBytes(2);
		a.finish(true);
		assertTrue(op.circulation().complete(op, tracker, a));
		assertEquals(0, op.reqTimeDone());
		assertEquals(0, op.latency());
		assertEquals(0, op.dataLatency());
		assertEquals(120, op.respDataTimeStart());
		assertEquals(30, op.duration());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void failedAttemptsAndRejectedPublicationDoNotPublishSuccessTiming() {
		var clock = new AtomicLong(100);
		var op = operation(clock);
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var a = op.circulation().beginAttempt(null);
		tracker.dispatched(op);
		a.requestHandoff();
		a.requestComplete();
		headers(a);
		a.bodyBytes(1);
		a.transportFailure(Status.FAIL_IO);
		assertTrue(op.circulation().complete(op, tracker, a));
		assertEquals(RangeReadAttempt.Timing.EMPTY, op.timing());
		assertEquals(100, a.outcome().timing().dispatch());
		assertTrue(tracker.counters().reconciled());
		var other = operation(clock);
		tracker.driverQueued(other);
		var response = other.circulation().beginAttempt(null);
		tracker.dispatched(other);
		response.requestHandoff();
		response.requestComplete();
		headers(response);
		response.bodyBytes(2);
		response.finish(true);
		assertFalse(other.circulation().complete(other, new OperationLifecycleTracker<>(), response));
		assertEquals(RangeReadAttempt.Timing.EMPTY, other.timing());
		assertTrue(other.circulation().complete(other, tracker, response));
		assertTrue(tracker.counters().reconciled());
	}
}
