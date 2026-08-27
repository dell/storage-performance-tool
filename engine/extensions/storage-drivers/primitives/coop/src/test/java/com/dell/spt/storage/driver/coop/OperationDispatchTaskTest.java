package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.concurrent.PlatformThreadExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.DurationTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

@SuppressWarnings("unchecked")
class OperationDispatchTaskTest {

	private static final int BATCH_SIZE = 4;
	private static final String STEP_ID = "test-step";

	private VirtualThreadExecutor executor;
	private CoopStorageDriverBase<Item, Operation<Item>> driverMock;
	private BlockingQueue<Operation<Item>> inOpQueue;
	private BlockingQueue<Operation<Item>> childOpQueue;
	private ReentrantLock dispatchLock;
	private Condition dispatchReady;
	private OperationDispatchTask<Item, Operation<Item>> task;

	@Test
	void legacyVirtualThreadConstructorDescriptorRemainsAvailable() throws Exception {
		assertNotNull(OperationDispatchTask.class.getConstructor(
						VirtualThreadExecutor.class,
						CoopStorageDriverBase.class,
						BlockingQueue.class,
						BlockingQueue.class,
						String.class,
						int.class,
						Lock.class,
						Condition.class,
						int.class));
	}

	@BeforeEach
	void setUp() {
		executor = new VirtualThreadExecutor();
		driverMock = mock(CoopStorageDriverBase.class);
		inOpQueue = new ArrayBlockingQueue<>(16);
		childOpQueue = new ArrayBlockingQueue<>(16);
		dispatchLock = new ReentrantLock();
		dispatchReady = dispatchLock.newCondition();
		when(driverMock.dispatchAttemptLimit(anyInt())).thenAnswer(invocation -> invocation.getArgument(0));
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 16);
	}

	@AfterEach
	void tearDown() {
		task.close();
		executor.close();
	}

	private void configurePermitGate(final AtomicInteger availableMpuPermits) {
		when(driverMock.tryAcquireMpuObjectPermit(any(Operation.class))).thenAnswer(inv -> {
			final int permits = availableMpuPermits.get();
			if (permits > 0) {
				availableMpuPermits.decrementAndGet();
				return true;
			}
			return false;
		});
	}

	private void captureSubmittedOps(final List<Operation<Item>> submitted) {
		when(driverMock.submit(any(Operation.class))).thenAnswer(inv -> {
			submitted.add(inv.getArgument(0));
			return true;
		});
		when(driverMock.submit(anyList(), anyInt(), anyInt())).thenAnswer(inv -> {
			final List<Operation<Item>> buff = inv.getArgument(0);
			final int from = inv.getArgument(1);
			final int to = inv.getArgument(2);
			for (int i = from; i < to; i++) {
				submitted.add(buff.get(i));
			}
			return to - from;
		});
	}

	@Test
	void singleOpDispatched() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class))).thenReturn(true);

		inOpQueue.add(op);
		task.doWork();

		verify(driverMock).submit(op);
	}

	@Test
	void batchSubmissionSnapshotIsBoundedByAvailableDispatchCapacity() throws Exception {
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		when(driverMock.dispatchAttemptLimit(4)).thenReturn(1);
		final var ops = List.<Operation<Item>> of(
						new OperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("bounded-1", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("bounded-2", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("bounded-3", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("bounded-4", 0, 1), null, "/bucket", null));
		for (final var op : ops) {
			assertTrue(lifecycle.driverQueued(op));
		}
		when(driverMock.submit(anyList(), eq(0), eq(1))).thenAnswer(invocation -> {
			final var submitting = task.submittingOperations();
			assertEquals(1, submitting.size(),
							"shutdown recovery should snapshot only operations which this call may submit");
			assertSame(ops.get(0), submitting.get(0).operation());
			return 1;
		});

		inOpQueue.addAll(ops);
		task.doWork();

		verify(driverMock).submit(anyList(), eq(0), eq(1));
	}

	@Test
	void successfulLegacySubmitMarksActualDispatchWithoutNewExtensionHook() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("legacy-submit", 0, 1), null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		when(driverMock.successfulSubmitStartsTransport(op)).thenReturn(true);
		assertTrue(lifecycle.driverQueued(op));
		when(driverMock.submit(op)).thenReturn(true);

		inOpQueue.add(op);
		task.doWork();

		assertEquals(1, lifecycle.inFlightCount());
		assertEquals(1, lifecycle.snapshot().dispatched());
	}

	@Test
	void successfulLegacySingleSubmitCrossingDeadlineBecomesUnresolved() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("legacy-deadline-single", 0, 1),
						null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		when(driverMock.successfulSubmitStartsTransport(op)).thenReturn(true);
		assertTrue(lifecycle.driverQueued(op));
		final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
		lifecycle.enforceDispatchDeadline(deadlineNanos);
		when(driverMock.submit(op)).thenAnswer(invocation -> {
			assertFalse(DurationTime.deadlineReached(deadlineNanos, System.nanoTime()));
			while (!DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
				Thread.onSpinWait();
			}
			return true;
		});

		inOpQueue.add(op);
		task.doWork();

		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(op));
		assertEquals(1, lifecycle.snapshot().unresolved());
		assertEquals(0, lifecycle.snapshot().unattempted());
	}

	@Test
	void expiredLegacyChildQueueOperationIsRejectedBeforeSubmit() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("expired-legacy-child", 0, 1),
						null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		when(driverMock.successfulSubmitStartsTransport(op)).thenReturn(true);
		when(driverMock.submit(op)).thenReturn(true);
		lifecycle.enforceDispatchDeadline(System.nanoTime());
		childOpQueue = spy(childOpQueue);
		when(childOpQueue.isEmpty()).thenReturn(false);
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 16);
		childOpQueue.add(op);

		task.doWork();

		verify(driverMock, never()).submit(op);
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op));
		assertEquals(1, lifecycle.snapshot().unattempted());
	}

	@Test
	void successfulLegacyBatchSubmitCrossingDeadlineBecomesUnresolved() throws Exception {
		final Operation<Item> first = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("legacy-deadline-batch-1", 0, 1),
						null, "/bucket", null);
		final Operation<Item> second = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("legacy-deadline-batch-2", 0, 1),
						null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		when(driverMock.successfulSubmitStartsTransport(any(Operation.class))).thenReturn(true);
		assertTrue(lifecycle.driverQueued(first));
		assertTrue(lifecycle.driverQueued(second));
		final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
		lifecycle.enforceDispatchDeadline(deadlineNanos);
		when(driverMock.submit(anyList(), anyInt(), anyInt())).thenAnswer(invocation -> {
			assertFalse(DurationTime.deadlineReached(deadlineNanos, System.nanoTime()));
			while (!DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
				Thread.onSpinWait();
			}
			return invocation.getArgument(2, Integer.class)
							- invocation.getArgument(1, Integer.class);
		});

		inOpQueue.add(first);
		inOpQueue.add(second);
		task.doWork();

		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(first));
		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(second));
		assertEquals(2, lifecycle.snapshot().unresolved());
		assertEquals(0, lifecycle.snapshot().unattempted());
	}

	@Test
	void successfulSubmitDoesNotDispatchNextLegacyCirculation() throws Exception {
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		when(driverMock.successfulSubmitStartsTransport(legacy)).thenReturn(true);
		assertTrue(lifecycle.driverQueued(legacy));
		when(driverMock.submit(legacy)).thenAnswer(invocation -> {
			assertTrue(lifecycle.dispatched(legacy));
			assertTrue(lifecycle.completionStarted(legacy));
			assertTrue(lifecycle.terminal(legacy));
			assertTrue(lifecycle.generatorBuffered(legacy));
			assertTrue(lifecycle.driverQueued(legacy));
			return true;
		});

		inOpQueue.add(legacy);
		task.doWork();

		assertEquals(OperationLifecycleState.DRIVER_QUEUED, lifecycle.stateOf(legacy),
						"a late fallback from the prior submit must not dispatch the next circulation");
		assertEquals(1, lifecycle.snapshot().dispatched());
		assertEquals(1, lifecycle.snapshot().terminal());
		assertEquals(0, lifecycle.inFlightCount());
	}

	@Test
	void successfulCompositeExpansionMayRetainUndispatchedParentOwnership() throws Exception {
		final Operation<Item> parent = new OperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("expanded-parent", 0, 1), null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		assertTrue(lifecycle.driverQueued(parent));
		when(driverMock.submit(parent)).thenReturn(true);
		when(driverMock.successfulSubmitStartsTransport(parent)).thenReturn(false);

		inOpQueue.add(parent);
		task.doWork();

		assertEquals(OperationLifecycleState.DRIVER_QUEUED, lifecycle.stateOf(parent));
		assertEquals(0, lifecycle.snapshot().dispatched());
		assertEquals(0, lifecycle.inFlightCount());
	}

	@Test
	void compatibilityTerminalStateIsRemovedUsingTrackerSidecar() throws Exception {
		final Operation<Item> legacy = mock(Operation.class, Answers.CALLS_REAL_METHODS);
		final Operation<Item> next = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("next", 0, 1), null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		when(driverMock.operationLifecycle()).thenReturn(lifecycle);
		assertTrue(lifecycle.driverQueued(legacy));
		when(driverMock.submit(legacy)).thenAnswer(invocation -> {
			assertTrue(lifecycle.dispatched(legacy));
			assertTrue(lifecycle.completionStarted(legacy));
			assertTrue(lifecycle.terminal(legacy));
			return false;
		});
		when(driverMock.submit(next)).thenReturn(true);

		inOpQueue.add(legacy);
		task.doWork();
		inOpQueue.add(next);
		task.doWork();

		verify(driverMock).submit(next);
		verify(driverMock, never()).submit(anyList(), anyInt(), anyInt());
	}

	@Test
	void terminalResultIsNotRetainedWhenSubmitReportsNoQueueProgress() throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("terminal", 0, 1), null, "/bucket", null);
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		assertTrue(lifecycle.driverQueued(op));
		when(driverMock.submit(op)).thenAnswer(invocation -> {
			assertTrue(lifecycle.dispatched(op));
			assertTrue(lifecycle.completionStarted(op));
			assertTrue(lifecycle.terminal(op));
			return false;
		});

		inOpQueue.add(op);
		task.doWork();

		verify(driverMock).submit(op);
	}

	@Test
	void platformThreadDispatcherWakesForIncomingOperation() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(op)).thenReturn(true);
		try (final var platformExecutor = new PlatformThreadExecutor()) {
			final var platformTask = new OperationDispatchTask<>(
							platformExecutor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
							dispatchLock, dispatchReady, 16);
			try {
				platformTask.start();
				inOpQueue.add(op);
				dispatchLock.lock();
				try {
					dispatchReady.signal();
				} finally {
					dispatchLock.unlock();
				}
				verify(driverMock, timeout(5_000)).submit(op);
			} finally {
				platformTask.stop();
				assertTrue(platformTask.awaitStop(5, TimeUnit.SECONDS));
			}
		}
	}

	@Test
	void batchOpsDispatched() throws Exception {
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);
		final Operation<Item> op3 = mock(Operation.class);
		when(driverMock.submit(anyList(), anyInt(), anyInt())).thenReturn(3);

		inOpQueue.add(op1);
		inOpQueue.add(op2);
		inOpQueue.add(op3);
		task.doWork();

		verify(driverMock).submit(anyList(), eq(0), eq(3));
	}

	@Test
	void inputScratchIsReusedAndClearedAcrossIterations() throws Exception {
		final BlockingQueue<Operation<Item>> scratchQueue = mock(BlockingQueue.class);
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);
		final List<Collection<Operation<Item>>> drainTargets = new ArrayList<>();
		final AtomicInteger drainCalls = new AtomicInteger();
		when(scratchQueue.drainTo(anyCollection(), anyInt())).thenAnswer(invocation -> {
			final Collection<Operation<Item>> target = invocation.getArgument(0);
			assertTrue(target.isEmpty(), "scratch must be empty before each input drain");
			drainTargets.add(target);
			target.add(drainCalls.getAndIncrement() == 0 ? op1 : op2);
			return 1;
		});
		when(driverMock.submit(any(Operation.class))).thenReturn(true);
		task = new OperationDispatchTask<>(
						executor, driverMock, scratchQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 16);

		task.doWork();
		task.doWork();

		assertEquals(2, drainTargets.size());
		assertSame(drainTargets.get(0), drainTargets.get(1),
						"dispatcher should retain one task-owned input scratch collection");
		assertTrue(drainTargets.get(0).isEmpty(),
						"scratch should not retain operations between dispatch iterations");
		verify(driverMock).submit(op1);
		verify(driverMock).submit(op2);
	}

	@Test
	void backpressureRetainsBufferedOp() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class)))
						.thenReturn(false)
						.thenReturn(true);
		when(driverMock.hasAvailableDispatchCapacity()).thenReturn(false);

		inOpQueue.add(op);

		// doWork() will submit (fail), then block on untimed backpressure await.
		// Run it on a background thread and signal after a brief delay.
		final var worker = Thread.ofVirtual().start(() -> {
			try {
				task.doWork();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		Thread.sleep(50); // let it reach the await

		// First submit attempt should have happened
		verify(driverMock, times(1)).submit(any(Operation.class));
		assertTrue(inOpQueue.isEmpty(), "op should have been drained from the queue");

		// Signal to release the backpressure await so doWork() returns
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}
		worker.join(5000);

		task.doWork(); // retry succeeds — buffer clears

		verify(driverMock, times(2)).submit(any(Operation.class));
	}

	@Test
	void batchBackpressureRetainsUnsubmittedOps() throws Exception {
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);
		final Operation<Item> op3 = mock(Operation.class);
		when(driverMock.submit(anyList(), anyInt(), anyInt()))
						.thenReturn(1) // first call: only 1 of 3 accepted
						.thenReturn(2); // second call: remaining 2 accepted

		inOpQueue.add(op1);
		inOpQueue.add(op2);
		inOpQueue.add(op3);
		task.doWork(); // submits 1 of 3

		verify(driverMock).submit(anyList(), eq(0), eq(3));

		task.doWork(); // retries remaining 2

		verify(driverMock).submit(anyList(), eq(0), eq(2));
	}

	@Test
	void childOpsDispatchedBeforeInOps() throws Exception {
		final Operation<Item> childOp = mock(Operation.class);
		final Operation<Item> inOp = mock(Operation.class);

		final List<Operation<Item>> dispatched = new ArrayList<>();
		when(driverMock.submit(anyList(), anyInt(), anyInt())).thenAnswer(inv -> {
			final List<Operation<Item>> buff = inv.getArgument(0);
			final int from = inv.getArgument(1);
			final int to = inv.getArgument(2);
			for (int i = from; i < to; i++) {
				dispatched.add(buff.get(i));
			}
			return to - from;
		});

		childOpQueue.add(childOp);
		inOpQueue.add(inOp);
		task.doWork();

		assertEquals(2, dispatched.size());
		assertSame(childOp, dispatched.get(0), "child op should be dispatched first");
		assertSame(inOp, dispatched.get(1), "in op should be dispatched second");
	}

	@Test
	void stopExitsCleanly() throws Exception {
		task.start();
		Thread.sleep(50); // let the VT run a few iterations

		assertTrue(task.isStarted());

		task.stop();
		assertTrue(task.await(5, TimeUnit.SECONDS), "task should stop within timeout");
		assertTrue(task.isStopped());
	}

	@Test
	void emptyQueuesDoNotSubmit() throws Exception {
		// With untimed await, doWork() blocks when queues are empty.
		// Run the task, wait, then verify no submit calls were made.
		task.start();
		Thread.sleep(100); // let the task enter await()

		verify(driverMock, never()).submit(any(Operation.class));
		verify(driverMock, never()).submit(anyList(), anyInt(), anyInt());
		verify(driverMock, never()).submit(anyList());

		task.stop();
		assertTrue(task.await(5, TimeUnit.SECONDS), "task should stop within timeout");
	}

	@Test
	void signalWakesDispatchInstantly() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class))).thenReturn(true);

		task.start();
		Thread.sleep(100); // let the task enter Condition.await(); CI runners need more headroom

		// Add op and signal — dispatch should wake instantly
		inOpQueue.add(op);
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}

		verify(driverMock, timeout(1000)).submit(any(Operation.class));

		task.stop();
		assertTrue(task.await(5, TimeUnit.SECONDS), "task should stop within timeout");
	}

	@Test
	void backpressureRecoveryViaSignal() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class)))
						.thenReturn(false)
						.thenReturn(true);
		when(driverMock.hasAvailableDispatchCapacity()).thenReturn(false);

		task.start();

		// Add op and signal
		inOpQueue.add(op);
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}

		// Wait until the first attempt has entered backpressure before signaling the completion.
		// Otherwise a busy suite may deliver this signal before the dispatcher starts awaiting it.
		verify(driverMock, timeout(1000)).submit(any(Operation.class));
		when(driverMock.hasAvailableDispatchCapacity()).thenReturn(true);
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}

		// Should eventually succeed on retry
		verify(driverMock, timeout(500).atLeast(2)).submit(any(Operation.class));

		task.stop();
		assertTrue(task.await(5, TimeUnit.SECONDS), "task should stop within timeout");
	}

	@Test
	void untimedAwaitWakesOnSignal() throws Exception {
		// The dispatch task uses untimed await() — verify it still wakes
		// promptly when signaled.

		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class))).thenReturn(true);

		task.start();
		Thread.sleep(100); // let the task enter await()

		// Signal should wake the untimed await instantly
		inOpQueue.add(op);
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}

		verify(driverMock, timeout(1000)).submit(any(Operation.class));

		task.stop();
		assertTrue(task.await(5, TimeUnit.SECONDS), "task should stop within timeout");
	}

	@Test
	void deferredMpuQueueCapacityPreservesInputBackpressure() throws Exception {
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 2);
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);
		final Operation<Item> op3 = mock(Operation.class);
		final Operation<Item> op4 = mock(Operation.class);
		final Operation<Item> op5 = mock(Operation.class);
		when(driverMock.isMpuInit(any(Operation.class))).thenReturn(true);
		when(driverMock.tryAcquireMpuObjectPermit(any(Operation.class))).thenReturn(false);

		inOpQueue.add(op1);
		inOpQueue.add(op2);
		inOpQueue.add(op3);
		inOpQueue.add(op4);
		inOpQueue.add(op5);
		task.doWork();

		assertEquals(3, inOpQueue.size(), "dispatcher should stop draining input once deferred MPU queue reaches capacity");
		verify(driverMock, never()).submit(any(Operation.class));
		verify(driverMock, never()).submit(anyList(), anyInt(), anyInt());
	}

	@Test
	void deferredMpuOnlyWorkAwaitsWhenPermitsUnavailable() throws Exception {
		final Operation<Item> mpuOp = mock(Operation.class);
		when(driverMock.isMpuInit(any(Operation.class))).thenReturn(true);
		when(driverMock.tryAcquireMpuObjectPermit(any(Operation.class))).thenReturn(false);

		inOpQueue.add(mpuOp);
		task.doWork();
		assertTrue(inOpQueue.isEmpty(), "precondition: MPU init op should be deferred");

		final var worker = Thread.ofVirtual().start(() -> {
			try {
				task.doWork();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		Thread.sleep(50);
		assertTrue(worker.isAlive(), "dispatcher should await when only deferred MPU ops exist and no permits are available");

		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}
		worker.join(5000);
		assertFalse(worker.isAlive(), "dispatcher should wake and finish iteration after signal");
	}

	@Test
	void deferredQueueFullAndInputPendingAwaitsWithoutPermits() throws Exception {
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 2);
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);
		final Operation<Item> op3 = mock(Operation.class);
		when(driverMock.isMpuInit(any(Operation.class))).thenReturn(true);
		when(driverMock.tryAcquireMpuObjectPermit(any(Operation.class))).thenReturn(false);

		inOpQueue.add(op1);
		inOpQueue.add(op2);
		inOpQueue.add(op3);
		task.doWork();
		assertEquals(1, inOpQueue.size(), "precondition: one input op should remain when deferred queue is full");

		final var worker = Thread.ofVirtual().start(() -> {
			try {
				task.doWork();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		Thread.sleep(50);
		assertTrue(worker.isAlive(), "dispatcher should await when deferred queue is full and MPU permits are unavailable");
		assertEquals(1, inOpQueue.size(), "input queue should not be drained while deferred queue is full");

		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}
		worker.join(5000);
		assertFalse(worker.isAlive(), "dispatcher should wake and finish iteration after signal");
	}

	@Test
	void deferredQueueFullResumesWithoutDroppingMpuOpsInFifoOrder() throws Exception {
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 2);
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);
		final Operation<Item> op3 = mock(Operation.class);
		final List<Operation<Item>> submitted = new ArrayList<>();
		final AtomicInteger availableMpuPermits = new AtomicInteger(0);

		when(driverMock.isMpuInit(any(Operation.class))).thenReturn(true);
		configurePermitGate(availableMpuPermits);
		captureSubmittedOps(submitted);

		inOpQueue.add(op1);
		inOpQueue.add(op2);
		inOpQueue.add(op3);

		task.doWork();
		assertEquals(1, inOpQueue.size(), "input queue should preserve backpressure while deferred MPU queue is full");
		assertTrue(submitted.isEmpty(), "no MPU op should be dispatched without permits");

		availableMpuPermits.set(1);
		task.doWork();
		assertEquals(1, submitted.size(), "one permit should resume exactly one deferred MPU op");
		assertSame(op1, submitted.get(0), "deferred MPU ops should resume in FIFO order");
		assertTrue(inOpQueue.isEmpty(), "pending input should move only when deferred queue capacity reopens");

		availableMpuPermits.set(1);
		task.doWork();
		assertEquals(2, submitted.size(), "second permit should dispatch the second deferred MPU op");
		assertSame(op2, submitted.get(1), "second dispatched op should preserve FIFO order");

		availableMpuPermits.set(1);
		task.doWork();
		assertEquals(3, submitted.size(), "third permit should dispatch the last preserved MPU op");
		assertSame(op3, submitted.get(2), "all deferred/pending MPU ops should eventually dispatch without loss");
	}

	@Test
	void deferredQueueFullPausesIntakeUntilPermitThenResumesMpuAndNonMpu() throws Exception {
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady, 1);
		final Operation<Item> mpuOp = mock(Operation.class);
		final Operation<Item> simpleOp = mock(Operation.class);
		final List<Operation<Item>> submitted = new ArrayList<>();
		final AtomicInteger availableMpuPermits = new AtomicInteger(0);

		when(driverMock.isMpuInit(any(Operation.class))).thenAnswer(inv -> inv.getArgument(0) == mpuOp);
		configurePermitGate(availableMpuPermits);
		captureSubmittedOps(submitted);

		inOpQueue.add(mpuOp);
		inOpQueue.add(simpleOp);

		task.doWork();
		assertEquals(1, inOpQueue.size(), "non-MPU input should remain queued while deferred MPU queue is full");
		assertTrue(submitted.isEmpty(), "dispatcher should not submit work without MPU permits");

		final var worker = Thread.ofVirtual().start(() -> {
			try {
				task.doWork();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
		Thread.sleep(50);
		assertTrue(worker.isAlive(), "dispatcher should await when deferred MPU queue is full and permits are unavailable");
		assertEquals(1, inOpQueue.size(), "input queue should remain paused while awaiting permits");

		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}
		worker.join(5000);
		assertFalse(worker.isAlive(), "dispatcher should wake and finish iteration after signal");
		assertEquals(1, inOpQueue.size(), "signal without permits should not drain paused input");
		assertTrue(submitted.isEmpty(), "signal without permits should not dispatch queued ops");

		availableMpuPermits.set(1);
		task.doWork();
		assertTrue(inOpQueue.isEmpty(), "paused input should resume once deferred MPU capacity opens");
		assertEquals(2, submitted.size(), "resume should dispatch both the deferred MPU op and queued non-MPU op");
		assertSame(mpuOp, submitted.get(0), "deferred MPU op should dispatch first on resume");
		assertSame(simpleOp, submitted.get(1), "queued non-MPU op should dispatch immediately after resume");
	}
}
