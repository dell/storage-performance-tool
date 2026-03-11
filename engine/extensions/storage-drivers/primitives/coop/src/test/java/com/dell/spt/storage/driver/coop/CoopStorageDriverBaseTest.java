package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class CoopStorageDriverBaseTest {

	/**
	 * Access the protected pollNextOp() via a mock. Mockito's mock of an abstract class
	 * inherits the concrete methods, so we can call pollNextOp() directly and test the
	 * queue priority behavior.
	 */
	private CoopStorageDriverBase<Item, Operation<Item>> createDriverWithQueues(
					BlockingQueue<Operation<Item>> childOpQueue,
					BlockingQueue<Operation<Item>> inOpQueue) throws Exception {
		// Use Mockito to create a partial mock that has real concrete method behavior.
		// We inject queues via reflection since the constructor requires a Config object.
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var childField = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		childField.setAccessible(true);
		childField.set(driver, childOpQueue);
		final var inField = CoopStorageDriverBase.class.getDeclaredField("inOpQueue");
		inField.setAccessible(true);
		inField.set(driver, inOpQueue);
		return driver;
	}

	@Test
	void pollNextOpReturnsChildOpFirst() throws Exception {
		final BlockingQueue<Operation<Item>> childQ = new ArrayBlockingQueue<>(16);
		final BlockingQueue<Operation<Item>> inQ = new ArrayBlockingQueue<>(16);

		final var driver = createDriverWithQueues(childQ, inQ);
		final Operation<Item> childOp = mock(Operation.class);
		final Operation<Item> inOp = mock(Operation.class);

		childQ.add(childOp);
		inQ.add(inOp);

		assertSame(childOp, driver.pollNextOp(), "child op should be returned first");
		assertSame(inOp, driver.pollNextOp(), "in op should be returned second");
		assertNull(driver.pollNextOp(), "should return null when both queues are empty");
	}

	@Test
	void pollNextOpReturnsInOpWhenNoChildren() throws Exception {
		final BlockingQueue<Operation<Item>> childQ = new ArrayBlockingQueue<>(16);
		final BlockingQueue<Operation<Item>> inQ = new ArrayBlockingQueue<>(16);

		final var driver = createDriverWithQueues(childQ, inQ);
		final Operation<Item> inOp = mock(Operation.class);

		inQ.add(inOp);

		assertSame(inOp, driver.pollNextOp(), "in op should be returned when child queue is empty");
	}

	@Test
	void pollNextOpReturnsNullWhenBothQueuesEmpty() throws Exception {
		final BlockingQueue<Operation<Item>> childQ = new ArrayBlockingQueue<>(16);
		final BlockingQueue<Operation<Item>> inQ = new ArrayBlockingQueue<>(16);

		final var driver = createDriverWithQueues(childQ, inQ);

		assertNull(driver.pollNextOp(), "should return null when both queues are empty");
	}
}
