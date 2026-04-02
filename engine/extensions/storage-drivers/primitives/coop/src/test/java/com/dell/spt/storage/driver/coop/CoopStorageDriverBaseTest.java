package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * Tests for CoopStorageDriverBase concurrency management and part-level retry.
 */
@SuppressWarnings("unchecked")
class CoopStorageDriverBaseTest {

	@Test
	void scheduledAndCompletedCountersAreIndependent() throws Exception {
		final var scheduledField = CoopStorageDriverBase.class.getDeclaredField("scheduledOpCount");
		scheduledField.setAccessible(true);
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);

		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		scheduledField.set(driver, new LongAdder());
		completedField.set(driver, new LongAdder());

		assertEquals(0, driver.scheduledOpCount());
		assertEquals(0, driver.completedOpCount());
	}

	@Test
	void activeOpCountReflectsSemaphoreState() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		final var sem = new Semaphore(4, true);
		semField.set(driver, sem);

		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertEquals(0, driver.activeOpCount(), "all permits free = 0 active");

		sem.acquire(2);
		assertEquals(2, driver.activeOpCount(), "2 permits taken = 2 active");

		sem.release(2);
		assertEquals(0, driver.activeOpCount(), "all permits released = 0 active");
	}

	@Test
	void isIdleWhenAllPermitsFree() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		final var sem = new Semaphore(4, true);
		semField.set(driver, sem);

		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertTrue(driver.isIdle(), "should be idle when all permits free");

		sem.acquire(1);
		assertFalse(driver.isIdle(), "should not be idle with permit held");

		sem.release(1);
		assertTrue(driver.isIdle(), "should be idle after release");
	}

	// ---------- Part-level retry tests ----------

	/** Set up a mock CoopStorageDriverBase with the fields needed by handleCompleted(). */
	private CoopStorageDriverBase<Item, Operation<Item>> newRetryTestDriver() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

		// childOpQueue
		final var childQueueField = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		childQueueField.setAccessible(true);
		childQueueField.set(driver, new ArrayBlockingQueue<>(100));

		// completedOpCount
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);
		completedField.set(driver, new LongAdder());

		// dispatch lock and condition (needed by signalDispatch)
		final var lockField = CoopStorageDriverBase.class.getDeclaredField("dispatchLock");
		lockField.setAccessible(true);
		final var lock = new ReentrantLock();
		lockField.set(driver, lock);
		final var condField = CoopStorageDriverBase.class.getDeclaredField("dispatchReady");
		condField.setAccessible(true);
		condField.set(driver, lock.newCondition());

		// opResultOut on StorageDriverBase (parent) — mock that accepts anything
		final Output<Operation<Item>> mockOutput = mock(Output.class);
		when(mockOutput.put(any(Operation.class))).thenReturn(true);
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, mockOutput);

		return driver;
	}

	private BlockingQueue<Operation<Item>> childQueueOf(CoopStorageDriverBase<Item, Operation<Item>> driver) throws Exception {
		final var field = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		field.setAccessible(true);
		return (BlockingQueue<Operation<Item>>) field.get(driver);
	}

	@Test
	void handleCompleted_retriesFailedPartWhenRetriesRemain() throws Exception {
		final var driver = newRetryTestDriver();
		// Build parent (4096 bytes, 1024-byte threshold → 4 parts)
		final var baseItem = new DataItemImpl("obj1", 0, 4096);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false));
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // initializes pendingSubTasksCount = 4

		// Grab first part and simulate a failed IO
		final var part = (PartialDataOperationImpl<DataItem>) parent.subOperations().get(0);
		part.status(Operation.Status.FAIL_IO);
		// Simulate finishResponse() having been called (decrements parent pending count)
		parent.markSubTaskCompleted(); // pending = 3

		// Act
		boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		// Assert
		assertTrue(result, "handleCompleted should return true");
		assertEquals(1, part.retryCount(), "retryCount should be incremented to 1");
		assertEquals(Operation.Status.PENDING, part.status(), "part should be reset to PENDING");
		assertNull(parent.get("mpuAbort"), "mpuAbort should NOT be set when retries remain");
		// Part should be re-enqueued in the child op queue
		final var queue = childQueueOf(driver);
		assertTrue(queue.contains(part), "failed part should be re-enqueued for retry");
		// Parent should NOT be in the queue (other parts still pending)
		assertFalse(queue.contains(parent), "parent should not be re-enqueued yet");
	}

	@Test
	void handleCompleted_abortsWhenRetriesExhausted() throws Exception {
		final var driver = newRetryTestDriver();
		final var baseItem = new DataItemImpl("obj2", 0, 2048);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false));
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // pendingSubTasksCount = 2

		final var part = (PartialDataOperationImpl<DataItem>) parent.subOperations().get(0);
		// Exhaust retries
		for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			part.incrementRetryCount();
		}
		part.status(Operation.Status.FAIL_IO);
		parent.markSubTaskCompleted(); // simulate finishResponse

		boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		assertTrue(result, "handleCompleted should return true");
		assertEquals("true", parent.get("mpuAbort"), "mpuAbort should be set when retries exhausted");
		assertEquals(CoopStorageDriverBase.MAX_PART_RETRIES, part.retryCount(),
						"retryCount should not be incremented further");
	}

	@Test
	void handleCompleted_successfulPartDoesNotRetryOrAbort() throws Exception {
		final var driver = newRetryTestDriver();
		final var baseItem = new DataItemImpl("obj3", 0, 2048);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false));
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // pendingSubTasksCount = 2

		final var part = (PartialDataOperationImpl<DataItem>) parent.subOperations().get(0);
		part.status(Operation.Status.SUCC);
		parent.markSubTaskCompleted(); // simulate finishResponse; pending = 1

		boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		assertTrue(result);
		assertEquals(0, part.retryCount(), "retryCount should remain 0 for successful part");
		assertNull(parent.get("mpuAbort"), "mpuAbort should not be set for successful part");
		// Part should NOT be re-enqueued
		final var queue = childQueueOf(driver);
		assertFalse(queue.contains(part), "successful part should not be re-enqueued");
	}

	@Test
	void handleCompleted_reEnqueuesParentWhenAllPartsDone() throws Exception {
		final var driver = newRetryTestDriver();
		final var baseItem = new DataItemImpl("obj4", 0, 1024);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false));
		// Single part (1024-byte item, 1024-byte threshold → 1 part)
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // pendingSubTasksCount = 1

		final var part = (PartialDataOperationImpl<DataItem>) parent.subOperations().get(0);
		part.status(Operation.Status.SUCC);
		parent.markSubTaskCompleted(); // pending = 0, allSubOperationsDone() → true

		driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		final var queue = childQueueOf(driver);
		assertTrue(queue.contains(parent), "parent should be re-enqueued when all parts done");
	}
}
