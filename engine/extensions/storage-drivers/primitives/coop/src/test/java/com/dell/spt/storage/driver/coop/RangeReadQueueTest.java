package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.dell.spt.storage.driver.coop.range.RangeReadQueue;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RangeReadQueueTest {
	private static final RangeReadPolicy POLICY = new RangeReadPolicy(2, 0L, 1);

	private record Dispatch(RangeReadOperation<DataItem> operation, RangeReadAttempt token) {}

	private static final class Driver extends CoopStorageDriverMock<DataItem, RangeReadOperation<DataItem>> {
		final RangeReadRuntime<DataItem> runtime;
		final RangeReadQueue<DataItem> ranges;
		final boolean background;
		final BlockingQueue<Dispatch> submitted = new ArrayBlockingQueue<>(4);

		@SuppressWarnings("unchecked")
		Driver(boolean background) throws Exception {
			super("range-queue", new SeedDataInput(1, 1024, 1, true), config(), false, 4);
			this.background = background;
			runtime = new RangeReadRuntime<>(POLICY, 2, this, this::publishRetainedRangeResult);
			ranges = new RangeReadQueue<>(runtime);
			enableOperationLifecycle(runtime.tracker());
			LoadGenerator<DataItem, RangeReadOperation<DataItem>> generator = mock(LoadGenerator.class);
			when(generator.supportsRangeRetry()).thenReturn(true);
			when(generator.retryRange(any(), any())).thenAnswer(call -> {
				RangeReadOperation<DataItem> op = call.getArgument(0);
				RangeReadAttempt attempt = call.getArgument(1);
				return op.circulation().claimRetryQueue(attempt) && runtime.admission().put(op);
			});
			operationResultOutput(runtime.bind(generator, operationLifecycle(), true, 1,
							(delay, task) -> {
								task.run();
								return CompletableFuture.completedFuture(null);
							},
							op -> {}, op -> {}));
		}

		@Override
		protected void doStart() {
			if (background)
				super.doStart();
		}

		@Override
		protected boolean supportsDirectDispatch() {
			return !background;
		}

		@Override
		protected boolean successfulSubmitStartsTransport(RangeReadOperation<DataItem> op) {
			return false;
		}

		@Override
		protected boolean offerIncomingOperationLocked(RangeReadOperation<DataItem> op,
						BlockingQueue<RangeReadOperation<DataItem>> queue) {
			return ranges.offer(op, queue);
		}

		@Override
		protected boolean claimDispatchOwnership(RangeReadOperation<DataItem> op) {
			return ranges.capture(op) != null;
		}

		@Override
		protected boolean recoverQueuedOperation(RangeReadOperation<DataItem> op) {
			return ranges.recover(op);
		}

		@Override
		protected List<RangeReadOperation<DataItem>> recoverAdditionalQueuedOperations() {
			return ranges.recoveryCandidates();
		}

		@Override
		protected boolean submit(RangeReadOperation<DataItem> op) {
			if (!beginDispatch(op))
				return false;
			var token = ranges.capture(op);
			assertNotNull(token);
			assertTrue(submitted.offer(new Dispatch(op, token)));
			return true; // Models asynchronous connection acquisition, before execute/write.
		}

		@Override
		protected int submit(List<RangeReadOperation<DataItem>> ops, int from, int to) {
			int count = 0;
			for (int i = from; i < to && submit(ops.get(i)); i++)
				count++;
			return count;
		}

		RangeReadOperation<DataItem> operation(String name) {
			var op = new RangeReadOperationsBuilder<DataItem>(0, POLICY).buildOp(new DataItemImpl(name, 0, 10));
			assertTrue(operationLifecycle().generatorBuffered(op));
			return op;
		}

		Dispatch take() throws Exception {
			if (background) {
				var dispatch = submitted.poll(5, TimeUnit.SECONDS);
				assertNotNull(dispatch, "background dispatcher must submit the queued operation");
				return dispatch;
			}
			var op = pollForDirectDispatch();
			assertNotNull(op);
			return new Dispatch(op, ranges.capture(op));
		}

		boolean handoff(Dispatch dispatch) {
			return withDispatchAdmission(() -> ranges.requestHandoff(dispatch.operation(), dispatch.token()));
		}

		boolean fail(Dispatch dispatch) {
			assertTrue(dispatch.token().transportFailure(Status.FAIL_IO));
			final boolean accepted = ranges.completed(dispatch.operation(), dispatch.operation().circulation(), dispatch.token());
			if (accepted)
				recordRetainedAttemptCompletion(true);
			return accepted;
		}

		@Override
		protected void doClose() throws IOException {
			try {
				super.doClose();
			} finally {
				operationLifecycle().expireTerminalDeadline();
				runtime.close();
			}
		}
	}

	@Test
	void fullQueueAndDuplicateRefusalPreserveOriginalTokenAndReservation() throws Exception {
		try (var driver = new Driver(false)) {
			driver.start();
			var first = driver.operation("first");
			var second = driver.operation("second");
			assertFalse(driver.put(first), "queue admission requires a bounded runtime reservation");
			assertEquals(1, driver.runtime.admission().put(List.of(first, first)));
			var original = driver.ranges.capture(first);
			assertNotNull(original);
			assertFalse(driver.ranges.completed(first, first.circulation(), original));
			assertSame(original, driver.ranges.capture(first));
			assertFalse(driver.runtime.admission().put(second));
			assertEquals(OperationLifecycleState.GENERATOR_BUFFERED, second.lifecycle().state());
			assertNull(second.circulation().pendingAttempt());
			assertSame(original, driver.ranges.capture(first));
			assertEquals(1, driver.runtime.admission().admittedCirculations());
			assertEquals(1, driver.ranges.pendingCount());
			var dispatch = driver.take();
			assertSame(original, dispatch.token());
			assertEquals(0, driver.operationLifecycle().snapshot().dispatched());
			driver.closeAdmission();
			assertEquals(List.of(first), driver.recoverQueuedOperations());
			assertFalse(driver.handoff(dispatch));
			assertFalse(original.requestHandoff());
			assertEquals(0, driver.ranges.pendingCount());
			assertEquals(0, driver.runtime.admission().admittedCirculations());
		}
	}

	@Test
	void queuedRetryAfterBackgroundSubmissionIsRecoveredAsKnownFailure() throws Exception {
		try (var driver = new Driver(true)) {
			driver.start();
			var op = driver.operation("background");
			assertTrue(driver.runtime.admission().put(op));
			var first = driver.take();
			assertTrue(driver.handoff(first));
			assertFalse(driver.handoff(first));
			assertTrue(driver.fail(first));
			assertEquals(1, driver.completedOpCount());
			var retry = driver.take();
			assertNotSame(first.token(), retry.token());
			assertEquals(1, driver.ranges.pendingCount());
			assertEquals(1, driver.operationLifecycle().snapshot().dispatched());
			assertFalse(driver.handoff(first));
			driver.closeAdmission();
			assertTrue(driver.recoverQueuedOperations().isEmpty());
			assertEquals(1, driver.operationLifecycle().counters().failed());
			assertEquals(0, driver.operationLifecycle().counters().unattempted());
			assertEquals(0, driver.operationLifecycle().counters().unresolved());
			assertFalse(driver.handoff(retry));
			assertFalse(retry.token().requestHandoff());
			assertEquals(1, driver.runtime.snapshot().requestsSent());
			assertEquals(0, driver.ranges.pendingCount());
			assertTrue(driver.runtime.snapshot().reconciled());
		}
	}

	@Test
	void preHandoffFailureCanRetryWithoutInventingTheFirstRequestOrLogicalDispatch() throws Exception {
		try (var driver = new Driver(false)) {
			driver.start();
			var op = driver.operation("pre-handoff");
			assertTrue(driver.runtime.admission().put(op));
			var first = driver.take();
			assertTrue(driver.fail(first));
			assertEquals(1, driver.completedOpCount());
			var retry = driver.take();
			assertEquals(0, driver.runtime.snapshot().requestsSent());
			assertEquals(0, driver.operationLifecycle().snapshot().dispatched());
			assertFalse(driver.handoff(first));
			assertTrue(driver.handoff(retry));
			assertEquals(1, driver.runtime.snapshot().requestsSent());
			assertEquals(1, driver.operationLifecycle().snapshot().dispatched());
			assertTrue(driver.fail(retry));
			assertEquals(2, driver.completedOpCount());
			assertFalse(driver.ranges.completed(op, op.circulation(), first.token()));
			assertEquals(1, driver.operationLifecycle().counters().failed());
			assertEquals(2, driver.runtime.snapshot().transportAttemptFailures());
			assertEquals(0, driver.ranges.pendingCount());
			assertTrue(driver.runtime.snapshot().reconciled());
		}
	}

	@Test
	void activeTransportRemainsInDriverDrainInsteadOfQueueRecovery() throws Exception {
		try (var driver = new Driver(false)) {
			driver.start();
			var op = driver.operation("active");
			assertTrue(driver.runtime.admission().put(op));
			var request = driver.take();
			assertTrue(driver.handoff(request));
			driver.closeAdmission();
			assertTrue(driver.recoverQueuedOperations().isEmpty());
			assertEquals(1, driver.operationLifecycle().inFlightCount());
			assertEquals(0, driver.operationLifecycle().counters().unattempted());
			assertEquals(0, driver.operationLifecycle().counters().unresolved());
			assertEquals(1, driver.runtime.admission().admittedCirculations());
			driver.operationLifecycle().expireTerminalDeadline();
			assertEquals(1, driver.operationLifecycle().counters().unresolved());
			assertEquals(0, driver.runtime.admission().admittedCirculations());
			assertTrue(driver.runtime.snapshot().reconciled());
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
		when(storage.intVal("driver-limit-queue-input")).thenReturn(1);
		when(limit.intVal("concurrency")).thenReturn(1);
		return storage;
	}

}
