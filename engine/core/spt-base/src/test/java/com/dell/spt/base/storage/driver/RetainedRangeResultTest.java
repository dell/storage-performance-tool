package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.github.akurilov.commons.io.Output;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

class RetainedRangeResultTest {
	private static final class Fixture implements AutoCloseable {
		final StorageDriverBase<DataItem, RangeReadOperation<DataItem>> driver;
		final OperationLifecycleTracker<RangeReadOperation<DataItem>> tracker = OperationLifecycleTracker.withDeadlineSettlement((op, owner) -> op.circulation().settleAtDeadline(op, owner));
		final RangeReadMetrics metrics;
		final RangeReadOperation<DataItem> op;
		final Output<RangeReadOperation<DataItem>> output;

		@SuppressWarnings("unchecked")
		Fixture(boolean localError) throws Exception {
			driver = mock(StorageDriverBase.class, withSettings().useConstructor("range-output",
							new SeedDataInput(1, 1024, 1, true), TestConfigBuilder.config().configVal("storage"), false)
							.defaultAnswer(CALLS_REAL_METHODS));
			driver.enableOperationLifecycle(tracker);
			var policy = new RangeReadPolicy(2, localError ? null : 0L, 1);
			metrics = new RangeReadMetrics(policy);
			tracker.terminalObserver(value -> metrics.recordFinal(value.circulation()));
			op = new RangeReadOperationsBuilder<DataItem>(0, policy).buildOp(new DataItemImpl("object", 0, localError ? 0 : 10));
			output = mock(Output.class);
			driver.operationResultOutput(output);
			assertTrue(tracker.generatorBuffered(op));
		}

		void success() {
			assertTrue(tracker.driverQueued(op));
			var attempt = op.circulation().beginAttempt(null);
			assertTrue(tracker.explicitlyDispatched(op));
			assertTrue(metrics.requestHandoff(attempt));
			assertTrue(attempt.requestComplete());
			assertTrue(attempt.headers(206, Status.SUCC, List.of("bytes 0-1/10"), List.of("2"), List.of(), false));
			assertTrue(attempt.bodyBytes(2));
			assertTrue(attempt.finish(true));
			assertTrue(op.circulation().complete(op, tracker, attempt));
		}

		@Override
		public void close() throws Exception {
			driver.close();
		}
	}

	@Test
	void localOutputSeesCommittedMetricsWithoutLocksOrInventedDispatch() throws Exception {
		try (var f = new Fixture(true)) {
			var token = f.op.circulation();
			assertFalse(f.driver.publishRetainedRangeResult(f.op, token));
			assertTrue(f.tracker.localFailure(f.op, f.op.lifecycle()));
			when(f.output.put(ArgumentMatchers.<RangeReadOperation<DataItem>> any())).thenAnswer(call -> {
				assertFalse(Thread.holdsLock(f.op));
				assertFalse(Thread.holdsLock(token.lifecycle()));
				var result = (RangeReadOperation<?>) call.getArgument(0);
				assertNotSame(f.op, result);
				assertEquals(Status.RESP_FAIL_CLIENT, result.status());
				assertEquals(0, result.countBytesDone());
				assertEquals(0, f.tracker.snapshot().dispatched());
				assertEquals(1, f.metrics.snapshot(f.tracker.counters()).localSelectionErrors());
				assertFalse(f.tracker.hasOutstandingOperations());
				return true;
			});
			assertTrue(f.driver.publishRetainedRangeResult(f.op, token));
			assertFalse(f.driver.publishRetainedRangeResult(f.op.result(), token));
			verify(f.output, times(1)).put(ArgumentMatchers.<RangeReadOperation<DataItem>> any());
		}
	}

	@Test
	void rejectionAndExceptionPreserveTerminalOutcomeAndNeverRetryOutput() throws Exception {
		for (boolean throwsFailure : List.of(false, true)) {
			try (var f = new Fixture(false)) {
				f.success();
				var token = f.op.circulation();
				var expected = new IllegalStateException("test reporting failure");
				when(f.output.put(ArgumentMatchers.<RangeReadOperation<DataItem>> any())).thenAnswer(call -> {
					f.tracker.expireTerminalDeadline();
					if (throwsFailure)
						throw expected;
					return false;
				});
				var failure = assertThrows(IllegalStateException.class, () -> f.driver.publishRetainedRangeResult(f.op, token));
				if (throwsFailure)
					assertSame(expected, failure);
				assertFalse(f.driver.publishRetainedRangeResult(f.op, token));
				assertEquals(1, f.tracker.counters().accepted());
				assertEquals(0, f.tracker.counters().unresolved());
				assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
				verify(f.output, times(1)).put(ArgumentMatchers.<RangeReadOperation<DataItem>> any());
			}
		}
	}

	@Test
	void earlyCopyPublishesRetainedValuesAndSynchronousRecycleFencesOldCallback() throws Exception {
		try (var f = new Fixture(false)) {
			var copy = f.op.result();
			var token = copy.circulation();
			f.success();
			when(f.output.put(ArgumentMatchers.<RangeReadOperation<DataItem>> any())).thenAnswer(call -> {
				RangeReadOperation<DataItem> result = call.getArgument(0);
				assertEquals(Status.SUCC, result.status());
				assertEquals(2, result.countBytesDone());
				assertEquals(f.op.reqTimeStart(), result.reqTimeStart());
				assertEquals(f.op.respTimeDone(), result.respTimeDone());
				assertTrue(f.tracker.generatorBuffered(result));
				assertNotSame(token, result.circulation());
				assertFalse(f.driver.publishRetainedRangeResult(result, token));
				assertTrue(f.tracker.unattempted(result));
				return true;
			});
			assertTrue(f.driver.publishRetainedRangeResult(copy, token));
			assertFalse(f.driver.publishRetainedRangeResult(f.op, token));
			assertEquals(1, f.tracker.counters().accepted());
			assertEquals(1, f.tracker.counters().unattempted());
			verify(f.output, times(1)).put(ArgumentMatchers.<RangeReadOperation<DataItem>> any());
		}
	}

	@Test
	void shutdownPublishesLastKnownRetryFailureWithoutSuccessfulTimingOrBytes() throws Exception {
		try (var f = new Fixture(false)) {
			assertTrue(f.tracker.driverQueued(f.op));
			var token = f.op.circulation();
			var attempt = token.beginAttempt(null);
			assertTrue(f.tracker.explicitlyDispatched(f.op));
			assertTrue(f.metrics.requestHandoff(attempt));
			assertTrue(attempt.transportFailure(Status.FAIL_IO));
			assertTrue(f.metrics.recordAttempt(token, attempt));
			var retry = token.beginAttempt(attempt);
			assertNotNull(retry);
			f.tracker.expireTerminalDeadline();
			when(f.output.put(ArgumentMatchers.<RangeReadOperation<DataItem>> any())).thenAnswer(call -> {
				RangeReadOperation<DataItem> result = call.getArgument(0);
				assertEquals(Status.FAIL_IO, result.status());
				assertEquals(0, result.countBytesDone());
				assertEquals(0, result.reqTimeStart());
				assertEquals(0, result.respTimeDone());
				assertEquals(1, f.tracker.counters().failed());
				assertEquals(1, f.metrics.snapshot(f.tracker.counters()).transportFailures());
				return true;
			});
			assertTrue(f.driver.publishRetainedRangeResult(f.op, token));
			assertFalse(retry.requestHandoff());
			assertFalse(f.driver.publishRetainedRangeResult(f.op, token));
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

	@Test
	void recoveredAndIndeterminateOperationsCannotPublishTerminalResults() throws Exception {
		for (boolean dispatched : List.of(false, true)) {
			try (var f = new Fixture(false)) {
				assertTrue(f.tracker.driverQueued(f.op));
				if (dispatched) {
					var attempt = f.op.circulation().beginAttempt(null);
					assertTrue(f.tracker.explicitlyDispatched(f.op));
					assertTrue(f.metrics.requestHandoff(attempt));
				}
				f.tracker.expireTerminalDeadline();
				assertFalse(f.driver.publishRetainedRangeResult(f.op, f.op.circulation()));
				verifyNoInteractions(f.output);
			}
		}
	}

	@Test
	void blockedOutputDoesNotHoldCustodyOrAllowConcurrentDuplicatePublication() throws Exception {
		try (var f = new Fixture(false); var pool = Executors.newSingleThreadExecutor()) {
			f.success();
			var token = f.op.circulation();
			var entered = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			when(f.output.put(ArgumentMatchers.<RangeReadOperation<DataItem>> any())).thenAnswer(call -> {
				entered.countDown();
				assertTrue(release.await(5, TimeUnit.SECONDS));
				return true;
			});
			var publication = pool.submit(() -> f.driver.publishRetainedRangeResult(f.op, token));
			try {
				assertTrue(entered.await(5, TimeUnit.SECONDS));
				f.tracker.expireTerminalDeadline();
				assertEquals(1, f.tracker.counters().accepted());
				assertEquals(0, f.tracker.counters().unresolved());
				assertFalse(f.tracker.hasOutstandingOperations());
				assertFalse(f.driver.publishRetainedRangeResult(f.op.result(), token));
			} finally {
				release.countDown();
			}
			assertTrue(publication.get(5, TimeUnit.SECONDS));
			verify(f.output, times(1)).put(ArgumentMatchers.<RangeReadOperation<DataItem>> any());
		}
	}
}
