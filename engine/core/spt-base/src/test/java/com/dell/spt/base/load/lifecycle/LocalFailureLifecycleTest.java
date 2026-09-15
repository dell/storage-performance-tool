package com.dell.spt.base.load.lifecycle;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import org.junit.jupiter.api.Test;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class LocalFailureLifecycleTest {
	private static DataOperationImpl<DataItemImpl> operation() {
		return new DataOperationImpl<>(0, OpType.READ, new DataItemImpl("item", 0, 1), "/bucket", null, null, null, 0);
	}

	@Test
	void localFailureReconcilesWithoutTransportOrTiming() {
		for (final boolean queued : new boolean[]{false, true
		}) {
			final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
			final var op = operation();
			assertTrue(tracker.generatorBuffered(op));
			if (queued) {
				assertTrue(tracker.driverQueued(op));
			}
			final AtomicInteger observed = new AtomicInteger();
			tracker.dispatchObserver(ignored -> fail("local error must never dispatch"));
			tracker.terminalObserver(result -> {
				assertEquals(Operation.Status.RESP_FAIL_CLIENT, result.status());
				assertEquals(1, tracker.counters().failed());
				observed.incrementAndGet();
			});
			assertTrue(tracker.localFailure(op, op.lifecycle()));
			assertFalse(tracker.localFailure(op, op.lifecycle()));
			assertFalse(tracker.unattempted(op));
			assertFalse(tracker.dispatched(op));
			assertFalse(tracker.unresolved(op));
			assertEquals(1, observed.get());
			assertEquals(0, tracker.snapshot().dispatched());
			assertEquals(0, tracker.inFlightCount());
			assertEquals(0, tracker.outstandingOperationCount());
			assertEquals(0, op.reqTimeStart());
			assertEquals(0, op.respTimeStart());
			assertEquals(0, op.countBytesDone());
			assertEquals(1, tracker.counters().failed());
			assertTrue(tracker.counters().reconciled());
		}
	}

	@Test
	void recoveryAndDispatchPreventLocalCommit() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var recovered = operation();
		assertTrue(tracker.driverQueued(recovered));
		assertTrue(tracker.unattempted(recovered));
		assertFalse(tracker.localFailure(recovered, recovered.lifecycle()));
		final var sent = operation();
		assertTrue(tracker.driverQueued(sent));
		assertTrue(tracker.dispatched(sent));
		assertFalse(tracker.localFailure(sent, sent.lifecycle()));
		assertEquals(1, tracker.inFlightCount());
		assertEquals(0, tracker.counters().failed());
		assertTrue(tracker.unresolved(sent));
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void oldCirculationAndResetCannotContaminateNewCounters() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation();
		assertTrue(tracker.driverQueued(op));
		final var old = op.lifecycle();
		assertTrue(tracker.localFailure(op, old));
		assertTrue(tracker.generatorBuffered(op));
		assertNotSame(old, op.lifecycle());
		assertFalse(tracker.localFailure(op, old));
		assertTrue(tracker.localFailure(op, op.lifecycle()));
		assertEquals(2, tracker.counters().failed());
		assertTrue(tracker.counters().reconciled());
		final var pending = operation();
		assertTrue(tracker.driverQueued(pending));
		tracker.reset();
		assertFalse(tracker.localFailure(pending, pending.lifecycle()));
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void localKnownFailureDoesNotBecomeUnresolvedAtDrainDeadline() {
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final var op = operation();
		assertTrue(tracker.driverQueued(op));
		tracker.expireTerminalDeadline();
		assertTrue(tracker.localFailure(op, op.lifecycle()));
		assertEquals(0, tracker.counters().unresolved());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void disabledUnselectedAndForeignOperationsFailClosed() {
		final var op = operation();
		assertFalse(OperationLifecycleTracker.<DataOperationImpl<DataItemImpl>> disabled().localFailure(op, op.lifecycle()));
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		assertFalse(tracker.localFailure(op, op.lifecycle()));
		assertTrue(tracker.driverQueued(op));
		assertFalse(tracker.localFailure(op, operation().lifecycle()));
		assertFalse(tracker.localFailure(op, null));
		final var otherTracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		assertFalse(otherTracker.localFailure(op, op.lifecycle()));
		assertTrue(tracker.localFailure(op, op.lifecycle()));
	}

	@Test
	void admissionRecoveryRaceKeepsExactlyOneAccountedState() throws Exception {
		try (final var pool = Executors.newFixedThreadPool(2)) {
			for (int i = 0; i < 100; i++) {
				final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
				final var op = operation();
				assertTrue(tracker.driverQueued(op));
				final var expected = op.lifecycle();
				final var start = new CountDownLatch(1);
				final var failure = pool.submit(() -> {
					start.await();
					return tracker.localFailure(op, expected);
				});
				final var recovery = pool.submit(() -> {
					start.await();
					return tracker.unattempted(op);
				});
				start.countDown();
				assertNotEquals(failure.get(5, TimeUnit.SECONDS), recovery.get(5, TimeUnit.SECONDS));
				assertTrue(tracker.counters().reconciled());
				assertEquals(0, tracker.snapshot().dispatched());
			}
		}
	}
}
