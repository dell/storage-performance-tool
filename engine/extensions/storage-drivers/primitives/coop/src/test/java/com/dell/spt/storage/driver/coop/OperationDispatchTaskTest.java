package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

	@BeforeEach
	void setUp() {
		executor = new VirtualThreadExecutor();
		driverMock = mock(CoopStorageDriverBase.class);
		inOpQueue = new ArrayBlockingQueue<>(16);
		childOpQueue = new ArrayBlockingQueue<>(16);
		dispatchLock = new ReentrantLock();
		dispatchReady = dispatchLock.newCondition();
		task = new OperationDispatchTask<>(
						executor, driverMock, inOpQueue, childOpQueue, STEP_ID, BATCH_SIZE,
						dispatchLock, dispatchReady);
	}

	@AfterEach
	void tearDown() {
		task.close();
		executor.close();
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
	void backpressureRetainsBufferedOp() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class)))
						.thenReturn(false)
						.thenReturn(true);

		inOpQueue.add(op);
		task.doWork(); // submit returns false — op stays buffered

		verify(driverMock, times(1)).submit(any(Operation.class));
		assertTrue(inOpQueue.isEmpty(), "op should have been drained from the queue");

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
		task.doWork(); // both queues empty — should not call submit

		verify(driverMock, never()).submit(any(Operation.class));
		verify(driverMock, never()).submit(anyList(), anyInt(), anyInt());
		verify(driverMock, never()).submit(anyList());
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

		task.start();

		// Add op and signal
		inOpQueue.add(op);
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}

		// First attempt fails (backpressure). Signal again to simulate completion callback.
		Thread.sleep(20);
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
	void fastRecycleEnabledExtendsIdleWait() throws Exception {
		// Enable fast-recycle quiesce on the driver mock — this signals that
		// concurrency is low and the dispatch task may use the 100ms timeout
		when(driverMock.isFastRecycleQuiesceActive()).thenReturn(true);

		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class))).thenReturn(true);

		task.start();
		Thread.sleep(100); // let the task enter the extended await (100ms vs 1ms)

		// Even with the extended timeout, signal() still provides instant wake-up
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
	void fastRecycleDisabledUsesShortWait() throws Exception {
		// Fast-recycle quiesce NOT active — dispatch task uses 1ms timeout
		when(driverMock.isFastRecycleQuiesceActive()).thenReturn(false);

		final Operation<Item> op = mock(Operation.class);
		when(driverMock.submit(any(Operation.class))).thenReturn(true);

		task.start();
		Thread.sleep(50); // let the task enter await

		// With 1ms timeout, the task wakes up quickly even without signal
		inOpQueue.add(op);

		verify(driverMock, timeout(500)).submit(any(Operation.class));

		task.stop();
		assertTrue(task.await(5, TimeUnit.SECONDS), "task should stop within timeout");
	}
}
