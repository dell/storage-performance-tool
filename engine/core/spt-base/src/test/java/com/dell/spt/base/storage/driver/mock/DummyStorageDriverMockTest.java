package com.dell.spt.base.storage.driver.mock;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.storage.driver.CompositeResults;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.ArrayList;
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
import static org.mockito.Mockito.withSettings;

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

	/** The mock publishes one result for a composite, and says so. */
	@Test
	void testDeclaresOneResultPerComposite() {
		final var mockDriver = DummyStorageDriverMock.<DataItem, DataOperation<DataItem>> create();
		assertEquals(CompositeResults.ONE_RESULT, mockDriver.compositeResults());
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

	/**
	 * Threads complete operations as fast as the mock allows while a reader samples the gauge.
	 * Reading the two counters one after the other read below 0 here. Bounded by operation count,
	 * not time, so a slow runner only takes longer.
	 */
	@Test
	@Timeout(120)
	void theActiveCountStaysWithinZeroAndTheCompletingThreadsUnderFastCompletions() throws Exception {
		final int threadCount = 4;
		final int opsPerThread = 20_000;
		final var driver = DummyStorageDriverMock.<Item, Operation<Item>> create(threadCount);
		driver.operationResultOutput(new AcceptingOutput());
		driver.start();
		final var op = stubOnlyOperation();
		final var ready = new CountDownLatch(threadCount + 1);
		final var completers = new ArrayList<FutureTask<Integer>>();
		for (int t = 0; t < threadCount; t++) {
			final var completer = new FutureTask<>(() -> {
				ready.countDown();
				ready.await();
				int done = 0;
				for (int i = 0; i < opsPerThread; i++) {
					if (driver.put(op)) {
						done++;
					}
				}
				return done;
			});
			completers.add(completer);
			new Thread(completer, "mock-driver-completer-" + t).start();
		}

		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		long samples = 0;
		ready.countDown();
		while (!completers.stream().allMatch(FutureTask::isDone)) {
			final int active = driver.activeOpCount();
			min = Math.min(min, active);
			max = Math.max(max, active);
			samples++;
		}

		for (final var completer : completers) {
			assertEquals(opsPerThread, completer.get(10, TimeUnit.SECONDS));
		}
		assertTrue(samples > 0);
		assertTrue(min >= 0, "lowest of " + samples + " samples: " + min);
		assertTrue(max <= threadCount, "highest of " + samples + " samples: " + max);
		assertEquals(0, driver.activeOpCount());
		assertEquals((long) threadCount * opsPerThread, driver.completedOpCount());
	}

	/** A put refused at admission close takes back its scheduled count, so the gauge reads 0. */
	@Test
	void theActiveCountReadsZeroAfterAPutRefusedAtAdmissionClose() throws Exception {
		final var driver = startedDriver(new AcceptingOutput());
		for (int i = 0; i < 3; i++) {
			assertTrue(driver.put(operation()));
		}

		driver.closeAdmission();

		assertFalse(driver.put(operation()));
		assertEquals(0, driver.activeOpCount());
		assertEquals(3, driver.scheduledOpCount());
		assertEquals(3, driver.completedOpCount());
	}

	/** Accepts every result at once, without Mockito's per-call cost, so completions are fast. */
	private static final class AcceptingOutput implements Output<Operation<Item>> {

		@Override
		public boolean put(final Operation<Item> op) {
			return true;
		}

		@Override
		public int put(final List<Operation<Item>> ops, final int from, final int to) {
			return to - from;
		}

		@Override
		public int put(final List<Operation<Item>> ops) {
			return ops.size();
		}

		@Override
		public Input<Operation<Item>> getInput() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() {}
	}

	/** An operation mock that records no invocations, so one can be completed many times. */
	@SuppressWarnings("unchecked")
	private static Operation<Item> stubOnlyOperation() {
		return mock(Operation.class, withSettings().stubOnly());
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
