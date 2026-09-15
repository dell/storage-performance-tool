package com.dell.spt.base.load.step.local.context.range;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class RangeReadRetryCoordinatorTest {
	private static final RangeReadPolicy POLICY = new RangeReadPolicy(2, 0L, 1);

	private static final class Fixture {
		final OperationLifecycleTracker<RangeReadOperation<DataItemImpl>> tracker = OperationLifecycleTracker.withDeadlineSettlement((op, owner) -> op.circulation().settleAtDeadline(op, owner));
		final RangeReadMetrics metrics = new RangeReadMetrics(POLICY);
		final List<Runnable> tasks = new ArrayList<>();
		final List<Long> delays = new ArrayList<>();
		final ConcurrentLinkedQueue<RangeReadAttempt> handed = new ConcurrentLinkedQueue<>();
		final ConcurrentLinkedQueue<RangeReadOperation<DataItemImpl>> results = new ConcurrentLinkedQueue<>();
		final LoadGenerator<DataItemImpl, RangeReadOperation<DataItemImpl>> generator;
		final RangeReadRetryCoordinator<DataItemImpl> coordinator;

		Fixture(boolean enabled, int limit, int capacity) {
			this(enabled, limit, capacity, null);
		}

		@SuppressWarnings("unchecked")
		Fixture(boolean enabled, int limit, int capacity, RangeReadRetryCoordinator.Scheduler scheduler) {
			generator = mock(LoadGenerator.class);
			tracker.terminalObserver(op -> metrics.recordFinal(op.circulation()));
			when(generator.retryRange(any(), any())).thenAnswer(call -> {
				RangeReadOperation<DataItemImpl> op = call.getArgument(0);
				RangeReadAttempt attempt = call.getArgument(1);
				if (!op.circulation().claimRetryQueue(attempt))
					return false;
				handed.add(attempt);
				return true;
			});
			coordinator = new RangeReadRetryCoordinator<>(enabled, limit, capacity, generator, tracker, metrics,
							scheduler == null ? (delay, task) -> {
								delays.add(delay);
								tasks.add(task);
								return new CompletableFuture<>();
							} : scheduler,
							(op, token) -> {
								assertFalse(Thread.holdsLock(token.lifecycle()));
								var result = op.claimTerminalResult(token);
								if (result != null)
									results.add(result);
							});
		}

		RangeReadOperation<DataItemImpl> operation() {
			var op = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY).buildOp(new DataItemImpl("object", 0, 10));
			assertTrue(tracker.generatorBuffered(op));
			assertTrue(tracker.driverQueued(op));
			return op;
		}

		RangeReadAttempt fail(RangeReadOperation<DataItemImpl> op) {
			var attempt = op.circulation().beginAttempt(null);
			assertTrue(tracker.explicitlyDispatched(op));
			fail(attempt);
			return attempt;
		}

		void fail(RangeReadAttempt attempt) {
			assertTrue(metrics.requestHandoff(attempt));
			assertTrue(attempt.transportFailure(Status.FAIL_IO));
		}
	}

	@Test
	void disabledOrZeroLimitNeverSchedulesAndCountsOneFinalFailure() {
		for (boolean enabled : List.of(false, true)) {
			var f = new Fixture(enabled, enabled ? 0 : 10, 1);
			var op = f.operation();
			var attempt = f.fail(op);
			assertTrue(f.coordinator.completed(op, op.circulation(), attempt));
			assertFalse(f.coordinator.completed(op, op.circulation(), attempt));
			assertTrue(f.tasks.isEmpty());
			assertEquals(1, f.results.size());
			assertEquals(1, f.tracker.counters().failed());
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
			verifyNoInteractions(f.generator);
		}
	}

	@Test
	void configuredLimitRetainsOneSelectionAndCountsEveryFailedAttempt() {
		var f = new Fixture(true, 2, 1);
		var op = f.operation();
		var token = op.circulation();
		var attempt = f.fail(op);
		assertFalse(f.coordinator.completed(op.result(), token, attempt), "a copy cannot steal the registered owner's outcome");
		assertTrue(f.coordinator.completed(op, token, attempt));
		for (int retry = 1; retry <= 2; retry++) {
			assertEquals(1, f.coordinator.pendingCount());
			assertEquals(0, f.tracker.counters().failed());
			assertEquals(retry, token.retryCount());
			assertTrue(f.delays.get(retry - 1) >= 0 && f.delays.get(retry - 1) <= (retry == 1 ? 200 : 400));
			f.tasks.get(retry - 1).run();
			f.tasks.get(retry - 1).run(); // duplicate scheduler callback is inert
			attempt = f.handed.remove();
			assertEquals(token.selection().range(), attempt.range());
			f.fail(attempt);
			assertTrue(f.coordinator.completed(op, token, attempt));
		}
		assertEquals(0, f.coordinator.pendingCount());
		assertEquals(2, f.tasks.size());
		assertEquals(1, f.results.size());
		assertEquals(1, f.tracker.snapshot().dispatched());
		assertEquals(3, f.metrics.snapshot(f.tracker.counters()).transportAttemptFailures());
		assertEquals(3, f.metrics.snapshot(f.tracker.counters()).requestsSent());
		assertEquals(1, f.tracker.counters().failed());
		assertNull(f.coordinator.failure());
		assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
	}

	@Test
	void protocolFailureNeverRetriesAndSuccessAfterRetryIsOneAcceptedResult() {
		for (boolean success : List.of(false, true)) {
			var f = new Fixture(true, 10, 1);
			var op = f.operation();
			var attempt = f.fail(op);
			assertTrue(f.coordinator.completed(op, op.circulation(), attempt));
			f.tasks.getFirst().run();
			var retry = f.handed.remove();
			assertTrue(f.metrics.requestHandoff(retry));
			assertEquals(success, retry.headers(success ? 206 : 200, Status.SUCC,
							success ? List.of("bytes 0-1/10") : List.of(), List.of(), List.of(), false));
			if (success) {
				assertTrue(retry.bodyBytes(2));
				assertTrue(retry.finish(true));
			}
			assertTrue(f.coordinator.completed(op, op.circulation(), retry));
			assertEquals(1, f.tasks.size());
			assertEquals(1, f.results.size());
			assertEquals(success ? 1 : 0, f.tracker.counters().accepted());
			assertEquals(success ? 0 : 1, f.metrics.snapshot(f.tracker.counters()).responseValidationFailures());
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

	@Test
	void closeCancelsQueuedRetryAndLateTimerCannotResubmit() {
		var f = new Fixture(true, 10, 1);
		var op = f.operation();
		assertTrue(f.coordinator.completed(op, op.circulation(), f.fail(op)));
		f.coordinator.close();
		f.coordinator.close();
		f.tasks.getFirst().run();
		assertEquals(0, f.coordinator.pendingCount());
		assertEquals(1, f.tracker.counters().failed());
		assertEquals(0, f.tracker.counters().unattempted());
		assertEquals(1, f.results.size());
		verifyNoInteractions(f.generator);
	}

	@Test
	void schedulerRejectionAndCapacityViolationSettleKnownFailuresAndReportExecutionFailure() {
		var rejected = new Fixture(true, 10, 1, (delay, task) -> {
			throw new IllegalStateException("rejected");
		});
		var op = rejected.operation();
		assertTrue(rejected.coordinator.completed(op, op.circulation(), rejected.fail(op)));
		assertNotNull(rejected.coordinator.failure());
		assertEquals(1, rejected.tracker.counters().failed());
		assertEquals(0, rejected.coordinator.pendingCount());
		var bounded = new Fixture(true, 10, 1);
		var first = bounded.operation();
		var second = bounded.operation();
		assertTrue(bounded.coordinator.completed(first, first.circulation(), bounded.fail(first)));
		assertTrue(bounded.coordinator.completed(second, second.circulation(), bounded.fail(second)));
		assertNotNull(bounded.coordinator.failure());
		assertEquals(1, bounded.coordinator.pendingCount());
		assertEquals(1, bounded.tracker.counters().failed());
		bounded.coordinator.close();
		assertEquals(2, bounded.results.size());
	}

	@Test
	void refusedOrThrowingGeneratorHandoffSettlesThePriorFailure() {
		for (boolean throwsFailure : List.of(false, true)) {
			var f = new Fixture(true, 10, 1);
			var op = f.operation();
			doAnswer(call -> {
				if (throwsFailure)
					throw new IllegalStateException("handoff failed");
				return false;
			}).when(f.generator).retryRange(any(), any());
			assertTrue(f.coordinator.completed(op, op.circulation(), f.fail(op)));
			f.tasks.getFirst().run();
			assertEquals(1, f.tracker.counters().failed());
			assertEquals(1, f.results.size());
			assertEquals(0, f.coordinator.pendingCount());
			assertEquals(throwsFailure, f.coordinator.failure() != null);
			assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
		}
	}

	@Test
	void reportingFailureIsRetainedWithoutErasingTheFinalOutcome() {
		var f = new Fixture(false, 10, 1);
		var coordinator = new RangeReadRetryCoordinator<>(false, 10, 1, f.generator, f.tracker, f.metrics,
						(delay, task) -> {
							throw new AssertionError("retry disabled");
						},
						(op, token) -> {
							throw new IllegalStateException("output failed");
						});
		var op = f.operation();
		assertTrue(coordinator.completed(op, op.circulation(), f.fail(op)));
		assertNotNull(coordinator.failure());
		assertEquals(1, f.tracker.counters().failed());
		assertEquals(0, f.tracker.counters().unresolved());
		assertTrue(f.metrics.snapshot(f.tracker.counters()).reconciled());
	}

	@Test
	void synchronousSchedulingAndCompletionPreserveSuccessorRetry() {
		var f = new Fixture(true, 2, 1, (delay, task) -> {
			task.run();
			return CompletableFuture.completedFuture(null);
		});
		var op = f.operation();
		doAnswer(call -> {
			RangeReadAttempt attempt = call.getArgument(1);
			f.fail(attempt);
			assertTrue(f.coordinator.completed(op, op.circulation(), attempt));
			assertEquals(0, f.coordinator.pendingCount(), "terminal settlement releases the slot before handoff returns");
			return true;
		}).when(f.generator).retryRange(any(), any());
		assertTrue(f.coordinator.completed(op, op.circulation(), f.fail(op)));
		assertEquals(2, op.circulation().retryCount());
		assertEquals(0, f.coordinator.pendingCount());
		assertEquals(1, f.results.size());
		assertEquals(1, f.tracker.counters().failed());
		assertNull(f.coordinator.failure());
	}

	@Test
	void closeDuringBlockedHandoffCancelsOnlyPreTransportWork() throws Exception {
		for (boolean transported : List.of(false, true)) {
			var f = new Fixture(true, 10, 1);
			var op = f.operation();
			var entered = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			doAnswer(call -> {
				RangeReadAttempt attempt = call.getArgument(1);
				if (transported)
					assertTrue(f.metrics.requestHandoff(attempt));
				entered.countDown();
				assertTrue(release.await(5, TimeUnit.SECONDS));
				return true;
			}).when(f.generator).retryRange(any(), any());
			assertTrue(f.coordinator.completed(op, op.circulation(), f.fail(op)));
			try (var pool = Executors.newSingleThreadExecutor()) {
				var handoff = pool.submit(f.tasks.getFirst());
				try {
					assertTrue(entered.await(5, TimeUnit.SECONDS));
					assertEquals(1, f.coordinator.pendingCount());
					f.coordinator.close();
					assertEquals(transported ? 0 : 1, f.tracker.counters().failed());
					assertEquals(0, f.tracker.counters().unresolved(), "active transport belongs to the driver drain");
				} finally {
					release.countDown();
				}
				handoff.get(5, TimeUnit.SECONDS);
			}
			f.tracker.expireTerminalDeadline();
			assertEquals(transported ? 1 : 0, f.tracker.counters().unresolved());
		}
	}
}
