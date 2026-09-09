package com.dell.spt.base.load.lifecycle;

import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadOperationsBuilder;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeReadCompletionTest {
	private static RangeReadOperation<DataItemImpl> operation() {
		return new RangeReadOperationsBuilder<DataItemImpl>(0, new RangeReadPolicy(2, 3L, 1))
						.buildOp(new DataItemImpl("item", 0, 10));
	}

	private static void success(RangeReadAttempt a) {
		a.requestHandoff();
		a.headers(206, Status.SUCC, List.of("bytes 3-4/10"), List.of(), List.of(), false);
		a.bodyBytes(2);
		a.finish(true);
	}

	@Test
	void retainsValidatedBytesBeforeObserverAndCustodyRelease() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		tracker.dispatched(op);
		success(a);
		var observed = new AtomicInteger();
		tracker.terminalObserver(result -> {
			assertSame(a.outcome(), result.circulation().terminalOutcome());
			assertEquals(2, result.countBytesDone());
			assertEquals(Status.SUCC, result.status());
			assertEquals(1, tracker.outstandingOperationCount());
			observed.incrementAndGet();
		});
		assertTrue(c.complete(op, tracker, a));
		assertFalse(c.complete(op, tracker, a));
		assertEquals(1, observed.get());
		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightCount());
		assertEquals(1, tracker.counters().accepted());
		assertTrue(tracker.counters().reconciled());
		var recycled = op.result();
		tracker.generatorBuffered(recycled);
		assertNull(recycled.circulation().terminalOutcome());
		assertSame(a.outcome(), c.terminalOutcome());
		assertFalse(c.complete(recycled, tracker, a));
		tracker.unattempted(recycled);
	}

	@Test
	void retryDelayFailureRemainsKnownAtExpiredDrainDeadline() {
		var op = operation();
		var tracker = OperationLifecycleTracker.<RangeReadOperation<DataItemImpl>> withDeadlineSettlement(
						(result, owner) -> result.circulation().settleAtDeadline(result, owner));
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		tracker.dispatched(op);
		a.requestHandoff();
		a.transportFailure(Status.FAIL_TIMEOUT);
		assertNotNull(c.claimOutcome(a));
		assertEquals(0, tracker.expireTerminalDeadline());
		assertFalse(c.complete(op, tracker, a));
		assertSame(a.outcome(), c.terminalOutcome());
		assertFalse(tracker.unresolved(op));
		assertEquals(1, tracker.counters().failed());
		assertEquals(0, tracker.counters().unresolved());
		assertEquals(0, op.countBytesDone());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void rejectedSubmissionHasNoDispatchOrTimingAndCannotSucceed() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		assertFalse(tracker.retainedTerminal(op, op.lifecycle(), Status.SUCC));
		a.transportFailure(Status.FAIL_IO);
		assertTrue(c.complete(op, tracker, a));
		assertEquals(0, tracker.snapshot().dispatched());
		assertEquals(0, tracker.inFlightCount());
		assertEquals(0, op.reqTimeStart());
		assertFalse(c.terminalOutcome().requestHandedOff());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void foreignDisabledAndResetTrackersCannotCommit() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		tracker.dispatched(op);
		success(a);
		assertFalse(c.complete(op, new OperationLifecycleTracker<>(), a));
		assertFalse(c.complete(op, OperationLifecycleTracker.disabled(), a));
		assertNull(c.terminalOutcome());
		assertEquals(0, op.countBytesDone());
		tracker.reset();
		assertFalse(c.complete(op, tracker, a));
		assertNull(c.terminalOutcome());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void observerFailurePreservesOutcomeAndReleasesCustody() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		tracker.dispatched(op);
		a.transportFailure(Status.FAIL_IO);
		tracker.terminalObserver(ignored -> {
			throw new IllegalStateException("broken observer");
		});
		assertThrows(IllegalStateException.class, () -> c.complete(op, tracker, a));
		assertSame(a.outcome(), c.terminalOutcome());
		assertFalse(c.complete(op, tracker, a));
		assertEquals(0, tracker.outstandingOperationCount());
		assertEquals(0, tracker.inFlightCount());
		assertEquals(1, tracker.counters().failed());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void recoveryAndCompletionRaceCommitsOneLogicalOutcome() throws Exception {
		try (var pool = Executors.newFixedThreadPool(2)) {
			for (int i = 0; i < 100; i++) {
				var op = operation();
				var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
				tracker.driverQueued(op);
				var c = op.circulation();
				var a = c.beginAttempt(null);
				tracker.dispatched(op);
				success(a);
				var start = new CountDownLatch(1);
				var complete = pool.submit(() -> {
					start.await();
					return c.complete(op, tracker, a);
				});
				var recover = pool.submit(() -> {
					start.await();
					return c.settleAtDeadline(op, tracker);
				});
				start.countDown();
				assertNotEquals(complete.get(5, TimeUnit.SECONDS), recover.get(5, TimeUnit.SECONDS));
				assertEquals(1, tracker.counters().accepted());
				assertEquals(0, tracker.counters().unresolved());
				assertEquals(0, tracker.inFlightCount());
				assertTrue(tracker.counters().reconciled());
			}
		}
	}

	@Test
	void deadlineRetainsPreviousFailureBeforeRetryHandoff() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var first = c.beginAttempt(null);
		tracker.dispatched(op);
		first.requestHandoff();
		first.transportFailure(Status.FAIL_IO);
		c.claimOutcome(first);
		var queuedRetry = c.beginAttempt(first);
		assertNotNull(queuedRetry);
		assertFalse(queuedRetry.wasHandedOff());
		assertTrue(c.settleAtDeadline(op, tracker));
		assertSame(first.outcome(), c.terminalOutcome());
		assertFalse(queuedRetry.requestHandoff());
		assertNull(queuedRetry.outcome());
		assertEquals(1, tracker.counters().failed());
		assertEquals(0, tracker.counters().unresolved());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void deadlineSeparatesNeverSentAndIndeterminateBody() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		var neverSent = operation();
		tracker.driverQueued(neverSent);
		var queued = neverSent.circulation().beginAttempt(null);
		assertTrue(neverSent.circulation().settleAtDeadline(neverSent, tracker));
		assertEquals(1, tracker.counters().unattempted());
		assertFalse(queued.requestHandoff());
		var op = operation();
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		tracker.dispatched(op);
		a.requestHandoff();
		a.headers(206, Status.SUCC, List.of("bytes 3-4/10"), List.of(), List.of(), false);
		a.bodyBytes(1);
		assertTrue(c.settleAtDeadline(op, tracker));
		assertEquals(RangeReadAttempt.Category.UNRESOLVED, c.terminalOutcome().category());
		assertEquals(1, c.terminalOutcome().receivedBytes());
		assertEquals(0, op.countBytesDone());
		assertFalse(a.finish(true));
		assertFalse(c.complete(op, tracker, a));
		assertEquals(1, tracker.counters().unresolved());
		assertEquals(0, tracker.counters().failed());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void foreignDeadlineCannotFenceLiveAttempt() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var c = op.circulation();
		var a = c.beginAttempt(null);
		tracker.dispatched(op);
		a.requestHandoff();
		assertFalse(c.settleAtDeadline(op, new OperationLifecycleTracker<>()));
		assertFalse(c.settleAtDeadline(op, OperationLifecycleTracker.disabled()));
		assertNull(a.outcome());
		assertTrue(c.settleAtDeadline(op, tracker));
		assertTrue(tracker.counters().reconciled());
	}
}
