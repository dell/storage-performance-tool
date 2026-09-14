package com.dell.spt.base.load.generator;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadOperationsBuilder;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.generator.range.RangeReadAdmission;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
class RangeReadAdmissionTest {
	private static final RangeReadPolicy POLICY = new RangeReadPolicy(2, null, 1);

	private static Input<DataItemImpl> input(List<DataItemImpl> items) {
		Input<DataItemImpl> input = mock(Input.class);
		when(input.toString()).thenReturn("range-admission-input");
		var next = new AtomicInteger();
		doAnswer(call -> {
			if (next.get() == items.size()) {
				throw new EOFException();
			}
			List<DataItemImpl> target = call.getArgument(0);
			int limit = call.getArgument(1);
			int start = next.get();
			int end = Math.min(items.size(), start + limit);
			target.addAll(items.subList(start, end));
			next.set(end);
			return end - start;
		}).when(input).get(anyList(), anyInt());
		return input;
	}

	private static DataItemImpl item(String name, long size) {
		return new DataItemImpl(name, 0, size);
	}

	@Test
	void localFailureObserverDoesNotHoldAdmissionLock() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 1, () -> 0L);
		var executor = Executors.newSingleThreadExecutor();
		tracker.terminalObserver(op -> {
			try {
				// Another terminal observer must be able to release/query admission capacity.
				assertEquals(0, executor.submit(admission::admittedCirculations).get(2, TimeUnit.SECONDS));
			} catch (Exception failure) {
				throw new IllegalStateException("Terminal observer runs under admission lock", failure);
			}
		});
		try {
			var op = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY).buildOp(item("invalid", 0));
			assertTrue(tracker.generatorBuffered(op));
			assertTrue(admission.put(op));
			assertEquals(1, tracker.counters().failed());
		} finally {
			admission.closeAdmission();
			executor.shutdownNow();
			assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
		}
	}

	@Test
	void pendingLocalObserverAllowsCapacityReleaseRefusalAndShutdown() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 1, () -> 0L);
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var first = builder.buildOp(item("first", 0));
		var second = builder.buildOp(item("second", 0));
		tracker.generatorBuffered(first);
		tracker.generatorBuffered(second);
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		tracker.terminalObserver(op -> {
			entered.countDown();
			try {
				assertTrue(release.await(5, TimeUnit.SECONDS));
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		});
		var pool = Executors.newFixedThreadPool(2);
		try {
			var pending = pool.submit(() -> admission.put(first));
			assertTrue(entered.await(2, TimeUnit.SECONDS));
			assertFalse(pool.submit(() -> admission.put(second)).get(2, TimeUnit.SECONDS));
			pool.submit(admission::closeAdmission).get(2, TimeUnit.SECONDS);
			assertThrows(EOFException.class, () -> admission.put(second));
			release.countDown();
			assertTrue(pending.get(2, TimeUnit.SECONDS));
			assertTrue(tracker.unattempted(second));
			assertEquals(1, tracker.counters().failed());
			assertEquals(1, tracker.counters().unattempted());
			assertTrue(tracker.counters().reconciled());
			verifyNoInteractions(driver);
		} finally {
			release.countDown();
			pool.shutdownNow();
			assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
			admission.closeAdmission();
		}
	}

	@Test
	void localPublicationIsOutsideLifecycleLockAndBoundsPendingLocalErrors() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var publications = new AtomicInteger();
		var admission = new RangeReadAdmission<>(driver, tracker, 1, () -> 0L, op -> {
			assertFalse(Thread.holdsLock(op.lifecycle()));
			assertEquals(OperationLifecycleState.TERMINAL, op.lifecycle().state());
			assertEquals(0, tracker.inFlightCount());
			publications.incrementAndGet();
			entered.countDown();
			try {
				assertTrue(release.await(5, TimeUnit.SECONDS));
			} catch (InterruptedException failure) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(failure);
			}
		});
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var first = builder.buildOp(item("first", 0));
		var second = builder.buildOp(item("second", 0));
		tracker.generatorBuffered(first);
		tracker.generatorBuffered(second);
		try (var pool = Executors.newFixedThreadPool(2)) {
			var pending = pool.submit(() -> admission.put(first));
			try {
				assertTrue(entered.await(2, TimeUnit.SECONDS));
				assertFalse(pool.submit(() -> admission.put(second)).get(2, TimeUnit.SECONDS));
				pool.submit(admission::closeAdmission).get(2, TimeUnit.SECONDS);
				assertTrue(tracker.unattempted(second));
			} finally {
				release.countDown();
			}
			assertTrue(pending.get(2, TimeUnit.SECONDS));
		}
		assertEquals(1, publications.get());
		assertTrue(tracker.counters().reconciled());
		verifyNoInteractions(driver);
	}

	@Test
	void recoveredLocalFailureDoesNotConsumePacingSlot() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 1, () -> 0L);
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var recovered = builder.buildOp(item("recovered", 0));
		var next = builder.buildOp(item("next", 0));
		tracker.generatorBuffered(recovered);
		tracker.generatorBuffered(next);
		assertTrue(tracker.unattempted(recovered));
		assertFalse(admission.put(recovered));
		assertTrue(admission.put(next));
		assertEquals(1, tracker.counters().failed());
		assertEquals(1, tracker.counters().unattempted());
		assertTrue(tracker.counters().reconciled());
		verifyNoInteractions(driver);
		admission.closeAdmission();
	}

	@Test
	void realGeneratorPreservesBatchPrefixWhilePacingOnlyLocalFailures() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		var metrics = new RangeReadMetrics(POLICY);
		tracker.terminalObserver(result -> metrics.recordFinal(result.circulation()));
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		List<RangeReadOperation<DataItemImpl>> received = new ArrayList<>();
		when(driver.put(any(RangeReadOperation.class))).thenAnswer(call -> {
			RangeReadOperation<DataItemImpl> op = call.getArgument(0);
			received.add(op);
			return tracker.driverQueued(op);
		});
		var clock = new AtomicLong();
		var admission = new RangeReadAdmission<>(driver, tracker, 4, clock::get);
		var generator = new LoadGeneratorImpl<>(input(List.of(item("bad-a", 0), item("bad-b", 0),
						item("valid", 2), item("bad-c", 0))), new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY),
						List.of(), admission, 4, 4, 4, false, false);
		generator.operationLifecycle(tracker);
		try {
			generator.doWork();
			assertEquals(4, generator.generatedOpCount());
			assertEquals(1, tracker.counters().failed());
			assertTrue(received.isEmpty());
			generator.doWork(); // Frozen time: a refusal cannot consume another logical result.
			assertEquals(1, tracker.counters().failed());
			clock.addAndGet(1_000_000);
			generator.doWork();
			assertEquals(2, tracker.counters().failed());
			assertEquals(1, received.size());
			assertEquals("valid", received.getFirst().item().name());
			clock.addAndGet(1_000_000);
			generator.doWork();
			assertEquals(3, tracker.counters().failed());
			assertEquals(0, tracker.snapshot().dispatched());
			assertEquals(0, metrics.snapshot(tracker.counters()).requestsSent());
			assertTrue(tracker.unattempted(received.getFirst()));
			assertTrue(metrics.snapshot(tracker.counters()).reconciled());
		} finally {
			admission.closeAdmission();
			generator.close();
		}
	}

	@Test
	void generatorRatePermitsStillApplyAndCloseRecoversPacedWork() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		var metrics = new RangeReadMetrics(POLICY);
		tracker.terminalObserver(result -> metrics.recordFinal(result.circulation()));
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var clock = new AtomicLong();
		var quota = new AtomicInteger();
		Throttle throttle = new Throttle() {
			@Override
			public boolean tryAcquire() {
				return tryAcquire(1) == 1;
			}

			@Override
			public int tryAcquire(int count) {
				int allowed = Math.min(count, quota.get());
				quota.addAndGet(-allowed);
				return allowed;
			}
		};
		var admission = new RangeReadAdmission<>(driver, tracker, 4, clock::get);
		var generator = new LoadGeneratorImpl<>(input(List.of(item("a", 0), item("b", 0))),
						new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY), List.of(throttle), admission,
						2, 2, 2, false, false);
		generator.operationLifecycle(tracker);
		try {
			generator.doWork();
			assertEquals(0, tracker.counters().failed());
			quota.set(1);
			generator.doWork();
			assertEquals(1, tracker.counters().failed());
			clock.addAndGet(1_000_000);
			generator.doWork();
			assertEquals(1, tracker.counters().failed());
			admission.closeAdmission();
			generator.closeAdmission();
			generator.recoverBufferedOperations();
			assertEquals(1, tracker.counters().unattempted());
			assertTrue(metrics.snapshot(tracker.counters()).reconciled());
			verifyNoInteractions(driver);
		} finally {
			admission.closeAdmission();
			generator.close();
		}
	}

	@Test
	void closingDuringValidBatchHandoffReturnsAcceptedPrefix() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 4);
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var first = builder.buildOp(item("a", 2));
		var second = builder.buildOp(item("b", 0));
		tracker.generatorBuffered(first);
		tracker.generatorBuffered(second);
		when(driver.put(first)).thenAnswer(call -> {
			admission.closeAdmission();
			return tracker.driverQueued(first);
		});
		assertEquals(1, admission.put(List.of(first, second)));
		assertThrows(EOFException.class, () -> admission.put(second));
		assertEquals(0, tracker.counters().failed());
		tracker.unattempted(first);
		tracker.unattempted(second);
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void localPacingSurvivesNanoTimeWrapAndDisabledTrackingFailsClosed() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		assertThrows(IllegalArgumentException.class,
						() -> new RangeReadAdmission<>(driver, OperationLifecycleTracker.disabled(), 4));
		var clock = new AtomicLong(Long.MAX_VALUE - 500_000);
		var admission = new RangeReadAdmission<>(driver, tracker, 4, clock::get);
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var a = builder.buildOp(item("a", 0));
		var b = builder.buildOp(item("b", 0));
		tracker.generatorBuffered(a);
		tracker.generatorBuffered(b);
		assertTrue(admission.put(a));
		clock.addAndGet(999_999);
		assertFalse(admission.put(b));
		clock.incrementAndGet();
		assertTrue(admission.put(b));
		assertEquals(0, a.reqTimeStart());
		assertEquals(0, b.reqTimeStart());
		assertTrue(tracker.counters().reconciled());
		verifyNoInteractions(driver);
	}

	@Test
	void observerFailurePropagatesTerminalErrorInsteadOfRetryingCommittedLocalWork() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		tracker.terminalObserver(result -> {
			throw new IllegalStateException("broken counter observer");
		});
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 4);
		var generator = new LoadGeneratorImpl<>(input(List.of(item("bad", 0))),
						new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY), List.of(), admission,
						1, 1, 1, false, false);
		generator.operationLifecycle(tracker);
		try {
			assertThrows(IntegrityTerminalException.class, generator::doWork);
			assertEquals(1, tracker.counters().failed());
			assertEquals(0, tracker.snapshot().dispatched());
			assertTrue(tracker.counters().reconciled());
			verifyNoInteractions(driver);
		} finally {
			admission.closeAdmission();
			generator.close();
		}
	}

	@Test
	void concurrentCallersShareOneLocalErrorPacingBudget() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		var metrics = new RangeReadMetrics(POLICY);
		tracker.terminalObserver(result -> metrics.recordFinal(result.circulation()));
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 4, () -> 0L);
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		List<RangeReadOperation<DataItemImpl>> operations = new ArrayList<>();
		for (int i = 0; i < 64; i++) {
			var op = builder.buildOp(item("bad-" + i, 0));
			tracker.generatorBuffered(op);
			operations.add(op);
		}
		int accepted = 0;
		try (var pool = Executors.newFixedThreadPool(8)) {
			List<Future<Boolean>> pending = new ArrayList<>();
			for (var op : operations) {
				pending.add(pool.submit(() -> admission.put(op)));
			}
			for (var future : pending) {
				if (future.get(5, TimeUnit.SECONDS)) {
					accepted++;
				}
			}
		}
		assertEquals(1, accepted);
		admission.closeAdmission();
		for (var op : operations) {
			tracker.unattempted(op);
		}
		assertEquals(1, tracker.counters().failed());
		assertEquals(63, tracker.counters().unattempted());
		assertTrue(metrics.snapshot(tracker.counters()).reconciled());
		verifyNoInteractions(driver);
	}

	@Test
	void capacitySurvivesRetriesAndFinalReleaseUsesExactCirculation() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		var metrics = new RangeReadMetrics(POLICY);
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 1);
		tracker.terminalObserver(op -> {
			metrics.recordFinal(op.circulation());
			assertTrue(admission.releaseSettled(op.circulation()));
		});
		when(driver.put(any(RangeReadOperation.class))).thenAnswer(call -> {
			RangeReadOperation<DataItemImpl> op = call.getArgument(0);
			return op.lifecycle().state() == OperationLifecycleState.DISPATCHED || tracker.driverQueued(op);
		});
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var first = builder.buildOp(item("first", 2));
		var second = builder.buildOp(item("second", 2));
		tracker.generatorBuffered(first);
		tracker.generatorBuffered(second);
		assertTrue(admission.put(first));
		var circulation = first.circulation();
		var attempt = circulation.beginAttempt(null);
		assertTrue(tracker.explicitlyDispatched(first));
		assertTrue(metrics.requestHandoff(attempt));
		assertTrue(attempt.transportFailure(Operation.Status.FAIL_IO));
		metrics.recordAttempt(circulation, attempt);
		var retry = circulation.beginAttempt(attempt);
		assertFalse(admission.releaseSettled(circulation));
		assertFalse(admission.put(second));
		assertTrue(admission.put(first));
		assertEquals(1, admission.admittedCirculations());
		assertTrue(metrics.requestHandoff(retry));
		assertTrue(retry.transportFailure(Operation.Status.FAIL_IO));
		assertTrue(circulation.complete(first, tracker, retry));
		assertEquals(0, admission.admittedCirculations());
		assertTrue(admission.put(second));
		assertFalse(admission.releaseSettled(circulation));
		assertEquals(1, admission.admittedCirculations());
		admission.closeAdmission();
		assertTrue(tracker.unattempted(second));
		assertTrue(admission.releaseSettled(second.circulation()));
		assertEquals(0, admission.admittedCirculations());
		assertTrue(metrics.snapshot(tracker.counters()).reconciled());
	}

	@Test
	void downstreamRefusalReturnsOnlyItsOwnReservation() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		assertThrows(IllegalArgumentException.class, () -> new RangeReadAdmission<>(driver, tracker, 0));
		var admission = new RangeReadAdmission<>(driver, tracker, 1);
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var op = builder.buildOp(item("refused", 2));
		tracker.generatorBuffered(op);
		assertFalse(admission.put(op));
		assertEquals(0, admission.admittedCirculations());
		when(driver.put(op)).thenAnswer(call -> tracker.driverQueued(op));
		assertTrue(admission.put(op));
		assertEquals(1, admission.admittedCirculations());
		admission.closeAdmission();
		tracker.unattempted(op);
		assertTrue(admission.releaseSettled(op.circulation()));
	}

	@Test
	void concurrentValidAdmissionsCannotExceedCapacity() throws Exception {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 3);
		when(driver.put(any(RangeReadOperation.class))).thenAnswer(call -> tracker.driverQueued(call.getArgument(0)));
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		List<RangeReadOperation<DataItemImpl>> operations = new ArrayList<>();
		for (int i = 0; i < 64; i++) {
			var op = builder.buildOp(item("valid-" + i, 2));
			tracker.generatorBuffered(op);
			operations.add(op);
		}
		int accepted = 0;
		try (var pool = Executors.newFixedThreadPool(8)) {
			List<Future<Boolean>> pending = new ArrayList<>();
			for (var op : operations)
				pending.add(pool.submit(() -> admission.put(op)));
			for (var result : pending)
				if (result.get(5, TimeUnit.SECONDS))
					accepted++;
		}
		assertEquals(3, accepted);
		assertEquals(3, admission.admittedCirculations());
		admission.closeAdmission();
		for (var op : operations) {
			tracker.unattempted(op);
			admission.releaseSettled(op.circulation());
		}
		assertEquals(0, admission.admittedCirculations());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void synchronousCompletionAndRecycleCannotReleaseNewCirculationsSlot() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 1);
		tracker.terminalObserver(op -> assertTrue(admission.releaseSettled(op.circulation())));
		var builder = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY);
		var op = builder.buildOp(item("recycled", 2));
		tracker.generatorBuffered(op);
		var old = op.circulation();
		var calls = new AtomicInteger();
		when(driver.put(op)).thenAnswer(call -> {
			assertTrue(tracker.driverQueued(op));
			if (calls.getAndIncrement() == 0) {
				var attempt = old.beginAttempt(null);
				assertTrue(tracker.explicitlyDispatched(op));
				assertTrue(attempt.requestHandoff());
				assertTrue(attempt.headers(206, Operation.Status.SUCC, List.of("bytes 0-1/2"),
								List.of("2"), List.of(), false));
				assertTrue(attempt.bodyBytes(2));
				assertTrue(attempt.finish(true));
				assertTrue(old.complete(op, tracker, attempt));
				assertTrue(tracker.generatorBuffered(op));
				assertNotSame(old, op.circulation());
				assertTrue(admission.put(op));
			}
			return true;
		});
		assertTrue(admission.put(op));
		assertEquals(1, admission.admittedCirculations());
		assertFalse(admission.releaseSettled(old));
		assertEquals(1, admission.admittedCirculations());
		admission.closeAdmission();
		tracker.unattempted(op);
		assertTrue(admission.releaseSettled(op.circulation()));
		assertEquals(0, admission.admittedCirculations());
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void exceptionalHandoffKeepsCapacityUntilRecoveryResolvesOwnership() {
		var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		Output<RangeReadOperation<DataItemImpl>> driver = mock(Output.class);
		var admission = new RangeReadAdmission<>(driver, tracker, 1);
		var op = new RangeReadOperationsBuilder<DataItemImpl>(0, POLICY).buildOp(item("exception", 2));
		tracker.generatorBuffered(op);
		when(driver.put(op)).thenThrow(new IllegalStateException("handoff contract violation"));
		assertThrows(IntegrityTerminalException.class, () -> admission.put(op));
		assertEquals(1, admission.admittedCirculations());
		assertFalse(admission.releaseSettled(op.circulation()));
		admission.closeAdmission();
		tracker.unattempted(op);
		assertTrue(admission.releaseSettled(op.circulation()));
		assertEquals(0, admission.admittedCirculations());
	}

}
