package com.dell.spt.base.load.generator;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.dell.spt.base.load.generator.range.RangeReadAdmission;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import java.io.EOFException;
import java.util.concurrent.CompletableFuture;
import com.dell.spt.base.load.step.local.context.range.RangeReadRetryCoordinator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class RangeReadRetryTest {
	private static final class Fixture implements AutoCloseable {
		final RangeReadPolicy policy = new RangeReadPolicy(2, 0L, 1);
		final OperationLifecycleTracker<RangeReadOperation<DataItemImpl>> tracker = OperationLifecycleTracker.withDeadlineSettlement((op, owner) -> op.circulation().settleAtDeadline(op, owner));
		final RangeReadMetrics metrics = new RangeReadMetrics(policy);
		final Output<RangeReadOperation<DataItemImpl>> output = mock(Output.class);
		final RangeReadAdmission<DataItemImpl> admission = new RangeReadAdmission<>(output, tracker, 1);
		final AtomicInteger quota = new AtomicInteger(1);
		final AtomicReference<RangeReadOperation<DataItemImpl>> operation = new AtomicReference<>();
		final LoadGeneratorImpl<DataItemImpl, RangeReadOperation<DataItemImpl>> generator;

		Fixture() throws Exception {
			Input<DataItemImpl> input = mock(Input.class);
			when(input.toString()).thenReturn("range-retry-input");
			var reads = new AtomicInteger();
			doAnswer(call -> {
				if (reads.getAndIncrement() != 0)
					throw new EOFException();
				List<DataItemImpl> items = call.getArgument(0);
				items.add(new DataItemImpl("object", 0, 10));
				return 1;
			}).when(input).get(anyList(), anyInt());
			Throttle throttle = mock(Throttle.class);
			when(throttle.tryAcquire(anyInt())).thenAnswer(call -> {
				int allowed = Math.min((Integer) call.getArgument(0), quota.get());
				quota.addAndGet(-allowed);
				return allowed;
			});
			tracker.terminalObserver(op -> {
				metrics.recordFinal(op.circulation());
				admission.releaseSettled(op.circulation());
			});
			when(output.put(any(RangeReadOperation.class))).thenAnswer(call -> {
				RangeReadOperation<DataItemImpl> op = call.getArgument(0);
				operation.set(op);
				return tracker.driverQueued(op);
			});
			generator = new LoadGeneratorImpl<>(input, new RangeReadOperationsBuilder<>(0, policy),
							List.of(throttle), admission, 1, 1, 1, false, false, true);
			generator.operationLifecycle(tracker);
			generator.doWork();
			assertNotNull(operation.get());
		}

		RangeReadAttempt failedAttempt(boolean handoff) {
			var op = operation.get();
			var attempt = op.circulation().beginAttempt(null);
			if (handoff) {
				assertTrue(tracker.explicitlyDispatched(op));
				assertTrue(metrics.requestHandoff(attempt));
			}
			assertTrue(attempt.transportFailure(Operation.Status.FAIL_IO));
			metrics.recordAttempt(op.circulation(), attempt);
			return attempt;
		}

		@Override
		public void close() throws Exception {
			generator.closeAdmission();
			generator.recoverBufferedOperations();
			tracker.resolveOutstandingAsUnresolved();
			generator.close();
			admission.close();
		}
	}

	@Test
	void coordinatorRetriesThroughRealGeneratorWithoutConsumingAnotherLogicalCount() throws Exception {
		try (var f = new Fixture()) {
			var op = f.operation.get();
			var circulation = op.circulation();
			var published = new AtomicReference<RangeReadOperation<DataItemImpl>>();
			try (var coordinator = new RangeReadRetryCoordinator<>(true, 1, 1, f.generator, f.tracker, f.metrics,
							(delay, task) -> {
								task.run();
								return CompletableFuture.completedFuture(null);
							}, (owner, token) -> {
								var result = owner.claimTerminalResult(token);
								if (result != null) {
									assertEquals(0, f.admission.admittedCirculations());
									assertEquals(1, f.tracker.counters().accepted());
									assertTrue(published.compareAndSet(null, result));
								}
							})) {
				assertNull(circulation.pendingAttempt());
				var first = circulation.beginAttempt(null);
				assertSame(first, circulation.pendingAttempt());
				assertTrue(f.tracker.explicitlyDispatched(op));
				assertTrue(f.metrics.requestHandoff(first));
				assertNull(circulation.pendingAttempt());
				assertTrue(first.transportFailure(Operation.Status.FAIL_IO));
				assertTrue(coordinator.completed(op, circulation, first));
				var retry = circulation.pendingAttempt();
				assertNotNull(retry);
				assertNotSame(first, retry);
				assertEquals(1, f.admission.admittedCirculations());
				assertFalse(f.generator.isNothingPendingRetry());
				f.generator.doWork(); // No rate permit left after the one initial logical read.
				verify(f.output, times(1)).put(op);
				doAnswer(call -> {
					assertSame(retry, circulation.pendingAttempt());
					assertTrue(circulation.isPendingAttempt(retry));
					assertTrue(f.metrics.requestHandoff(retry));
					assertTrue(retry.headers(206, Operation.Status.SUCC,
									List.of("bytes 0-1/10"), List.of("2"), List.of(), false));
					assertTrue(retry.bodyBytes(2));
					assertTrue(retry.finish(true));
					assertTrue(coordinator.completed(op, circulation, retry));
					return true;
				}).when(f.output).put(op);
				f.quota.set(1);
				f.generator.doWork();
				verify(f.output, times(2)).put(op);
				assertEquals(1, f.generator.generatedOpCount());
				assertEquals(1, f.tracker.counters().selected());
				assertEquals(1, f.tracker.snapshot().dispatched());
				assertEquals(2, f.metrics.snapshot(f.tracker.counters()).requestsSent());
				assertEquals(1, f.metrics.snapshot(f.tracker.counters()).transportAttemptFailures());
				assertEquals(2, published.get().countBytesDone());
				assertEquals(10, op.item().size());
				assertNull(circulation.pendingAttempt());
				assertTrue(f.generator.isNothingPendingRetry());
				assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
				assertNull(coordinator.failure());
			}
		}
	}

	@Test
	void countExhaustedRetryUsesRatePermitAndSynchronousNextAttemptSurvivesRecovery() throws Exception {
		try (var f = new Fixture()) {
			var op = f.operation.get();
			var life = op.lifecycle();
			var first = f.failedAttempt(true);
			var retry = op.circulation().beginAttempt(first);
			assertTrue(f.generator.retryRange(op, retry));
			assertFalse(f.generator.retryRange(op, retry));
			assertFalse(f.generator.retryRange(op, first));
			assertThrows(IllegalArgumentException.class, () -> f.generator.retry(op));
			f.generator.doWork();
			verify(f.output, times(1)).put(op);
			var next = new AtomicReference<RangeReadAttempt>();
			when(f.output.put(op)).thenAnswer(call -> {
				assertSame(life, op.lifecycle());
				assertFalse(f.generator.isNothingPendingRetry());
				assertTrue(f.metrics.requestHandoff(retry));
				assertTrue(retry.transportFailure(Operation.Status.FAIL_TIMEOUT));
				f.metrics.recordAttempt(op.circulation(), retry);
				next.set(op.circulation().beginAttempt(retry));
				assertTrue(f.generator.retryRange(op, next.get()));
				return true;
			});
			f.quota.set(1);
			f.generator.doWork();
			verify(f.output, times(2)).put(op);
			assertEquals(1, f.generator.generatedOpCount());
			assertEquals(1, f.tracker.counters().selected());
			assertFalse(f.generator.isNothingPendingRetry());
			f.generator.closeAdmission();
			f.generator.recoverBufferedOperations();
			assertFalse(next.get().requestHandoff());
			assertEquals(1, f.tracker.counters().failed());
			assertEquals(0, f.tracker.counters().unattempted());
			assertEquals(2, f.metrics.snapshot(f.tracker.counters()).requestsSent());
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

	@Test
	void closedAdmissionRetainsPreHandoffSubmissionFailureWithZeroRequests() throws Exception {
		try (var f = new Fixture()) {
			var op = f.operation.get();
			var retry = op.circulation().beginAttempt(f.failedAttempt(false));
			f.generator.closeAdmission();
			assertFalse(f.generator.retryRange(op, retry));
			assertFalse(retry.requestHandoff());
			assertEquals(1, f.tracker.counters().failed());
			assertEquals(0, f.metrics.snapshot(f.tracker.counters()).requestsSent());
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

	@Test
	void outputEofSettlesKnownFailureInsteadOfAbandoningPolledRetry() throws Exception {
		try (var f = new Fixture()) {
			var op = f.operation.get();
			var retry = op.circulation().beginAttempt(f.failedAttempt(true));
			assertTrue(f.generator.retryRange(op, retry));
			when(f.output.put(op)).thenAnswer(call -> {
				throw new EOFException();
			});
			f.quota.set(1);
			f.generator.doWork();
			assertFalse(retry.requestHandoff());
			assertEquals(1, f.tracker.counters().failed());
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

	@Test
	void shutdownDrainFindsPolledRetryBeforeTransportHandoff() throws Exception {
		try (var f = new Fixture()) {
			var op = f.operation.get();
			var retry = op.circulation().beginAttempt(f.failedAttempt(true));
			assertTrue(f.generator.retryRange(op, retry));
			when(f.output.put(op)).thenAnswer(call -> {
				assertFalse(f.generator.isNothingPendingRetry());
				f.generator.closeAdmission();
				assertTrue(f.generator.drainPendingRetries().isEmpty());
				assertFalse(retry.requestHandoff());
				assertTrue(f.generator.isNothingPendingRetry());
				return false;
			});
			f.quota.set(1);
			f.generator.doWork();
			assertEquals(1, f.tracker.counters().failed());
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

}
