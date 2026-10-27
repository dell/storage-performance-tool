package com.dell.spt.base.storage.driver.mock;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.github.akurilov.commons.io.Output;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock.DEFAULT_CONCURRENCY_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class demonstrating the simplified usage of DummyStorageDriverMock
 * with the new convenience constructors and factory methods.
 * 
 * These improvements make it much easier to create mock storage drivers
 * for unit testing without dealing with complex configuration structures.
 */
class DummyStorageDriverMockTest {

	@Test
	void testCreateFactoryMethod() {
		// Using static factory method - the preferred approach
		var mockDriver = DummyStorageDriverMock.<DataItem, DataOperation<DataItem>> create();

		assertNotNull(mockDriver);
		assertEquals(DEFAULT_CONCURRENCY_LIMIT, mockDriver.concurrencyLimit());
	}

	@Test
	void testCreateFactoryMethodWithConcurrency() {
		// Factory method with custom concurrency
		var mockDriver = DummyStorageDriverMock.<DataItem, DataOperation<DataItem>> create(50);

		assertNotNull(mockDriver);
		assertEquals(50, mockDriver.concurrencyLimit());
	}

	/**
	 * Example showing how the old way required complex configuration setup.
	 * The new factory methods eliminate this complexity.
	 * 
	 * OLD WAY (not recommended):
	 * - Create nested Map structures
	 * - Understand "driver-limit" vs "driver.limit" differences
	 * - Deal with Config object creation
	 * 
	 * NEW WAY (recommended):
	 * - Use factory method: DummyStorageDriverMock.create()
	 * - Use factory method with concurrency: DummyStorageDriverMock.create(concurrency)
	 */
	@Test
	void testSimplifiedUsage() {
		// The improvements make creating mock drivers trivial for unit tests
		var mock1 = DummyStorageDriverMock.<DataItem, DataOperation<DataItem>> create();
		var mock2 = DummyStorageDriverMock.<DataItem, DataOperation<DataItem>> create(100);

		assertNotNull(mock1);
		assertNotNull(mock2);
		assertEquals(DEFAULT_CONCURRENCY_LIMIT, mock1.concurrencyLimit());
		assertEquals(100, mock2.concurrencyLimit());

		// Note: Direct constructor usage is discouraged - use factory methods instead
		// The constructor is package-private to enforce this pattern
	}

	@Test
	void anOperationAfterAdmissionClosesIsRefusedAndNeverOutput() throws Exception {
		final Output<Operation<Item>> results = acceptingOutput();
		final var driver = startedDriver(results);

		driver.closeAdmission();

		assertFalse(driver.put(operation()));
		assertEquals(0, driver.put(List.of(operation(), operation()), 0, 2));
		assertEquals(0, driver.put(List.of(operation())));
		verify(results, never()).put(any(Operation.class));
		assertEquals(0, driver.activeOpCount());
		assertEquals(0, driver.completedOpCount());
	}

	@Test
	@Timeout(10)
	void anOperationStaysActiveUntilItsResultIsOutput() throws Exception {
		final var outputEntered = new CountDownLatch(1);
		final var outputRelease = new CountDownLatch(1);
		@SuppressWarnings("unchecked")
		final Output<Operation<Item>> results = mock(Output.class);
		when(results.put(any(Operation.class))).thenAnswer(invocation -> {
			outputEntered.countDown();
			return outputRelease.await(10, TimeUnit.SECONDS);
		});
		final var driver = startedDriver(results);

		// Its own thread: the common pool may be busy with other tests in this JVM.
		final var put = new FutureTask<>(() -> driver.put(operation()));
		new Thread(put, "mock-driver-put").start();
		assertTrue(outputEntered.await(10, TimeUnit.SECONDS));
		// A step that closes admission now must still see this operation to wait for it.
		driver.closeAdmission();
		assertEquals(1, driver.activeOpCount());
		outputRelease.countDown();

		assertTrue(put.get(10, TimeUnit.SECONDS));
		assertEquals(0, driver.activeOpCount());
		assertEquals(1, driver.completedOpCount());
	}

	@Test
	void anOperationTheOutputRejectsIsNotCounted() throws Exception {
		@SuppressWarnings("unchecked")
		final Output<Operation<Item>> results = mock(Output.class);
		when(results.put(any(Operation.class))).thenReturn(false);
		final var driver = startedDriver(results);

		assertFalse(driver.put(operation()));
		assertEquals(0, driver.activeOpCount());
		assertEquals(0, driver.scheduledOpCount());
	}

	private static Output<Operation<Item>> acceptingOutput() throws Exception {
		@SuppressWarnings("unchecked")
		final Output<Operation<Item>> results = mock(Output.class);
		when(results.put(any(Operation.class))).thenReturn(true);
		return results;
	}

	private static DummyStorageDriverMock<Item, Operation<Item>> startedDriver(final Output<Operation<Item>> results)
					throws Exception {
		final var driver = DummyStorageDriverMock.<Item, Operation<Item>> create();
		driver.operationResultOutput(results);
		driver.start();
		return driver;
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> operation() {
		return mock(Operation.class);
	}
}
