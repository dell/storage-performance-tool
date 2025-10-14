package com.dell.spt.base.storage.driver.mock;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.data.DataOperation;
import org.junit.jupiter.api.Test;

import static com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock.DEFAULT_CONCURRENCY_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
}
