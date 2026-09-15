package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeReadCirculationTest {
	private static RangeReadOperation<DataItemImpl> operation() {
		return new RangeReadOperationsBuilder<DataItemImpl>(0, new RangeReadPolicy(2, 3L, 1))
						.buildOp(new DataItemImpl("item", 0, 10));
	}

	@Test
	void copiesShareAttemptClaimAndRetryPreservesLogicalCustody() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		assertNull(op.circulation().beginAttempt(null));
		tracker.driverQueued(op);
		var copy = op.result();
		assertSame(op.circulation(), copy.circulation());
		var first = op.circulation().beginAttempt(null);
		assertNotNull(first);
		assertNull(copy.circulation().beginAttempt(null));
		tracker.dispatched(op);
		first.requestHandoff();
		first.transportFailure(Status.FAIL_IO);
		assertNull(copy.circulation().beginAttempt(first));
		assertSame(first.outcome(), copy.circulation().claimOutcome(first));
		assertNull(op.circulation().claimOutcome(first));
		op.reset();
		var retry = op.circulation().beginAttempt(first);
		assertNotNull(retry);
		assertSame(first.lifecycle(), retry.lifecycle());
		assertEquals(first.range(), retry.range());
		assertNull(copy.circulation().claimOutcome(first));
		assertEquals(1, tracker.snapshot().dispatched());
		assertEquals(1, tracker.inFlightCount());
		assertTrue(op.circulation().close(retry));
		assertFalse(copy.circulation().close(retry));
		assertNull(op.circulation().beginAttempt(retry));
		tracker.unresolved(op);
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void successRecyclingStartsIndependentCirculationAndFencesOldToken() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var old = op.circulation();
		var attempt = old.beginAttempt(null);
		tracker.dispatched(op);
		attempt.requestHandoff();
		attempt.headers(206, Status.SUCC, List.of("bytes 3-4/10"), List.of(), List.of(), false);
		attempt.bodyBytes(2);
		attempt.finish(true);
		assertNotNull(old.claimOutcome(attempt));
		assertNull(old.beginAttempt(attempt));
		assertTrue(old.close(attempt));
		tracker.completionStarted(op);
		op.status(Status.SUCC);
		var result = op.result();
		tracker.terminal(op);
		tracker.driverQueued(result);
		assertNotSame(old, result.circulation());
		assertNull(result.circulation().beginAttempt(attempt));
		assertNotNull(result.circulation().beginAttempt(null));
		assertNull(old.claimOutcome(attempt));
		tracker.unattempted(result);
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void recoveryFencesKnownOutcomeAndInvalidSelectionNeverCreatesAttempt() {
		var op = operation();
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.driverQueued(op);
		var attempt = op.circulation().beginAttempt(null);
		attempt.transportFailure(Status.FAIL_IO);
		tracker.unattempted(op);
		assertNull(op.circulation().claimOutcome(attempt));
		assertNull(op.circulation().beginAttempt(attempt));
		var invalid = new RangeReadOperationsBuilder<DataItemImpl>(0, new RangeReadPolicy(2, null, 1))
						.buildOp(new DataItemImpl("empty", 0, 0));
		tracker.driverQueued(invalid);
		assertNull(invalid.circulation().beginAttempt(null));
		assertTrue(tracker.localFailure(invalid, invalid.lifecycle()));
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void competingCopiesCannotCreateOverlappingAttempts() throws Exception {
		try (var pool = Executors.newFixedThreadPool(2)) {
			for (int i = 0; i < 100; i++) {
				var op = operation();
				var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
				tracker.driverQueued(op);
				var copy = op.result();
				var start = new CountDownLatch(1);
				var a = pool.submit(() -> {
					start.await();
					return op.circulation().beginAttempt(null);
				});
				var b = pool.submit(() -> {
					start.await();
					return copy.circulation().beginAttempt(null);
				});
				start.countDown();
				assertNotEquals(a.get(5, TimeUnit.SECONDS) == null, b.get(5, TimeUnit.SECONDS) == null);
				tracker.unattempted(op);
				assertTrue(tracker.counters().reconciled());
			}
		}
	}
}
