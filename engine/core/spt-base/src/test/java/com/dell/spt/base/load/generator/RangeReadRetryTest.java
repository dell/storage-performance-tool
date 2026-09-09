package com.dell.spt.base.load.generator;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import java.io.EOFException;
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
			tracker.terminalObserver(op -> metrics.recordFinal(op.circulation()));
			when(output.put(any(RangeReadOperation.class))).thenAnswer(call -> {
				RangeReadOperation<DataItemImpl> op = call.getArgument(0);
				operation.set(op);
				return tracker.driverQueued(op);
			});
			generator = new LoadGeneratorImpl<>(input, new RangeReadOperationsBuilder<>(0, policy),
							List.of(throttle), output, 1, 1, 1, false, false, true);
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
		public void close() {
			generator.closeAdmission();
			generator.recoverBufferedOperations();
			tracker.resolveOutstandingAsUnresolved();
			generator.close();
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
