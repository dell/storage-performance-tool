package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RetainedRangeDriverHooksTest {
	private static final RangeReadPolicy POLICY = new RangeReadPolicy(2, 0L, 1);

	/** Real cooperative queues, with the background dispatcher paused for exact boundary tests. */
	private static final class Driver extends CoopStorageDriverMock<DataItem, RangeReadOperation<DataItem>> {
		final RangeReadMetrics metrics = new RangeReadMetrics(POLICY);
		RangeReadOperation<DataItem> retryOperation;
		RangeReadAttempt retry;
		boolean retryQueued;

		Driver() throws Exception {
			super("range-hooks", new SeedDataInput(1, 1024, 1, true), config(), false, 4);
			enableOperationLifecycle(OperationLifecycleTracker.withDeadlineSettlement(
							(op, owner) -> op.circulation().settleAtDeadline(op, owner)));
			operationLifecycle().terminalObserver(op -> metrics.recordFinal(op.circulation()));
		}

		@Override
		protected void doStart() {}

		@Override
		protected boolean supportsDirectDispatch() {
			return true;
		}

		@Override
		protected boolean successfulSubmitStartsTransport(RangeReadOperation<DataItem> op) {
			return false;
		}

		@Override
		protected boolean offerIncomingOperationLocked(RangeReadOperation<DataItem> op,
						BlockingQueue<RangeReadOperation<DataItem>> queue) {
			if (op != retryOperation)
				return super.offerIncomingOperationLocked(op, queue);
			if (retryQueued || !op.circulation().isPendingAttempt(retry) || !queue.offer(op))
				return false;
			retryQueued = true;
			return true;
		}

		@Override
		protected boolean claimDispatchOwnership(RangeReadOperation<DataItem> op) {
			return op == retryOperation ? op.circulation().isPendingAttempt(retry)
							: super.claimDispatchOwnership(op);
		}

		@Override
		protected boolean recoverQueuedOperation(RangeReadOperation<DataItem> op) {
			if (op != retryOperation)
				return super.recoverQueuedOperation(op);
			op.circulation().settleAtDeadline(op, operationLifecycle());
			return false;
		}

		RangeReadOperation<DataItem> take() {
			return pollForDirectDispatch();
		}

		boolean handoffRetry() {
			return withDispatchAdmission(() -> retryOperation.circulation().isPendingAttempt(retry)
							&& metrics.requestHandoff(retry));
		}

		RangeReadOperation<DataItem> prepareRetry() {
			var op = new RangeReadOperationsBuilder<DataItem>(0, POLICY)
							.buildOp(new DataItemImpl("object", 0, 8));
			var tracker = operationLifecycle();
			assertTrue(tracker.generatorBuffered(op));
			assertTrue(put(op));
			var first = op.circulation().beginAttempt(null);
			assertSame(op, take());
			assertTrue(metrics.requestHandoff(first));
			assertTrue(first.transportFailure(Operation.Status.FAIL_IO));
			metrics.recordAttempt(op.circulation(), first);
			retryOperation = op;
			retry = op.circulation().beginAttempt(first);
			assertNotNull(retry);
			return op;
		}
	}

	private static Config config() {
		var storage = mock(Config.class);
		var driver = mock(Config.class);
		var limit = mock(Config.class);
		var auth = mock(Config.class);
		var integrity = mock(Config.class);
		var input = mock(Config.class);
		when(storage.configVal("driver")).thenReturn(driver);
		when(driver.configVal("limit")).thenReturn(limit);
		when(storage.configVal("auth")).thenReturn(auth);
		when(storage.configVal("integrity")).thenReturn(integrity);
		when(integrity.stringVal("mode")).thenReturn("none");
		when(integrity.stringVal("algorithm")).thenReturn("sha256");
		when(integrity.configVal("input")).thenReturn(input);
		when(input.stringVal("provenance")).thenReturn("none");
		when(input.stringVal("expectedProducerId")).thenReturn("");
		when(storage.intVal("driver-limit-queue-input")).thenReturn(4);
		when(limit.intVal("concurrency")).thenReturn(1);
		return storage;
	}

	@Test
	void retainedRetryQueueAndDirectDispatchDoNotInventAnotherLogicalDispatch() throws Exception {
		try (var driver = new Driver()) {
			driver.start();
			var op = driver.prepareRetry();
			assertEquals(1, driver.put(List.of(op, op)));
			assertSame(op, driver.take());
			assertNull(driver.take());
			assertEquals(1, driver.operationLifecycle().snapshot().dispatched());
			assertEquals(1, driver.metrics.snapshot(driver.operationLifecycle().counters()).requestsSent());
			assertTrue(driver.handoffRetry());
			assertFalse(driver.handoffRetry());
			assertTrue(driver.retry.transportFailure(Operation.Status.FAIL_TIMEOUT));
			assertTrue(op.circulation().complete(op, driver.operationLifecycle(), driver.retry));
			assertEquals(2, driver.metrics.snapshot(driver.operationLifecycle().counters()).requestsSent());
			assertTrue(driver.metrics.snapshot(driver.operationLifecycle().counters()).reconciled());
		}
	}

	@Test
	void queueRecoveryRetainsKnownFailureAndClosedAdmissionNeverInvokesHandoff() throws Exception {
		try (var driver = new Driver()) {
			driver.start();
			var op = driver.prepareRetry();
			assertTrue(driver.put(op));
			driver.closeAdmission();
			assertTrue(driver.recoverQueuedOperations().isEmpty());
			assertEquals(1, driver.operationLifecycle().counters().failed());
			assertEquals(0, driver.operationLifecycle().counters().unattempted());
			assertFalse(driver.handoffRetry());
			assertFalse(driver.retry.requestHandoff());
			var invoked = new AtomicBoolean();
			assertFalse(driver.withDispatchAdmission(() -> {
				invoked.set(true);
				return true;
			}));
			assertFalse(invoked.get());
			assertEquals(1, driver.metrics.snapshot(driver.operationLifecycle().counters()).requestsSent());
			assertTrue(driver.metrics.snapshot(driver.operationLifecycle().counters()).reconciled());
		}
	}
}
