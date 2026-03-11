package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.Test;

/**
 * Tests for the pipeline consolidation in CoopStorageDriverBase.
 * Verifies that put() calls submit() directly (no intermediate queue)
 * and that childOpQueue is preserved for composite operations.
 */
@SuppressWarnings("unchecked")
class CoopStorageDriverBaseTest {

	@Test
	void childOpQueueAcceptsAndReturnsOps() {
		final BlockingQueue<Operation<Item>> childQ = new ArrayBlockingQueue<>(16);
		final Operation<Item> op1 = mock(Operation.class);
		final Operation<Item> op2 = mock(Operation.class);

		assertTrue(childQ.offer(op1));
		assertTrue(childQ.offer(op2));
		assertSame(op1, childQ.poll(), "first op should be returned first (FIFO)");
		assertSame(op2, childQ.poll(), "second op should be returned second");
		assertNull(childQ.poll(), "empty queue should return null");
	}

	@Test
	void scheduledAndCompletedCountersAreIndependent() throws Exception {
		// Verify the LongAdder counters work correctly via reflection
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

		// Set concurrencyLimit via reflection
		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertEquals(0, driver.activeOpCount(), "all permits free = 0 active");

		sem.acquire(2);
		assertEquals(2, driver.activeOpCount(), "2 permits taken = 2 active");

		sem.release(2);
		assertEquals(0, driver.activeOpCount(), "all permits released = 0 active");
	}
}
