package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Characterization tests for the recycle-poll path in {@link LoadGeneratorImpl#doWork()}.
 * <p>
 * These tests document the current behavior of the spin-wait, yield, and output
 * logic in the recycle branch, plus the fast-recycle quiesce behavior (park/unpark).
 */
@SuppressWarnings("unchecked")
class LoadGeneratorImplRecycleTest {

	private static final int BATCH_SIZE = 4;

	private Input<DataItem> itemInput;
	private OperationsBuilder<DataItem, DataOperation<DataItem>> opsBuilder;
	private CollectingOutput<DataOperation<DataItem>> output;
	private LoadGeneratorImpl<DataItem, DataOperation<DataItem>> generator;

	@BeforeEach
	void setUp() throws Exception {
		// Default mock: get(List,int) returns 0 and doesn't populate the list,
		// so items stays empty and itemInputFinishFlag is set on first doWork().
		itemInput = mock(Input.class);
		when(itemInput.toString()).thenReturn("EmptyInput");

		opsBuilder = mock(OperationsBuilder.class);
		when(opsBuilder.opType()).thenReturn(OpType.READ);
		when(opsBuilder.originIndex()).thenReturn(0);

		output = new CollectingOutput<>();

		generator = new LoadGeneratorImpl<>(
						itemInput,
						opsBuilder,
						List.of(),
						output,
						BATCH_SIZE,
						0, // countLimit -> Long.MAX_VALUE
						1000, // recycleQueueCapacity
						true, // recycleFlag
						false); // shuffleFlag
	}

	@AfterEach
	void tearDown() {
		generator.close();
	}

	/**
	 * Exhaust item input, then recycle one op, then call doWork() again.
	 * The recycled op should appear in the output via the single-op path.
	 */
	@Test
	void recycleEnqueueThenDoWorkOutputsOp() throws Exception {
		// First doWork: exhausts item input, sets itemInputFinishFlag
		generator.doWork();
		assertTrue(generator.isItemInputFinished());
		assertEquals(0, output.received.size());

		// Recycle one op
		final DataOperation<DataItem> op = newOp("item-1");
		generator.recycle(op);
		assertFalse(generator.isNothingToRecycle());

		// Second doWork: enters recycle branch, picks up op, outputs it
		generator.doWork();
		assertEquals(1, output.received.size());
		assertSame(op, output.received.get(0));
		assertTrue(generator.isNothingToRecycle());
		assertEquals(1, generator.generatedOpCount());
	}

	/**
	 * With empty recycle queue, doWork() should return promptly via the
	 * spin-wait + yield path. This verifies the yield doesn't block
	 * indefinitely.
	 */
	@Test
	void emptyRecycleQueueDoWorkReturnsPromptly() throws Exception {
		// Exhaust item input
		generator.doWork();
		assertTrue(generator.isItemInputFinished());

		// doWork with empty recycle queue -- should return quickly
		final long start = System.nanoTime();
		generator.doWork();
		final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

		// Spin-wait (128x ~10ns) + yield is well under 100ms
		assertTrue(elapsedMs < 100, "doWork took " + elapsedMs + "ms; expected < 100ms");
		assertEquals(0, output.received.size());
	}

	/**
	 * Recycle multiple ops and verify a single doWork() picks up all of them
	 * (up to batchSize) and outputs them as a batch.
	 */
	@Test
	void batchRecycleOutputsAllOps() throws Exception {
		// Exhaust item input
		generator.doWork();

		// Recycle BATCH_SIZE ops
		final List<DataOperation<DataItem>> ops = new ArrayList<>();
		for (int i = 0; i < BATCH_SIZE; i++) {
			final DataOperation<DataItem> op = newOp("item-" + i);
			ops.add(op);
			generator.recycle(op);
		}

		// Single doWork should pick up and output all of them
		generator.doWork();
		assertEquals(BATCH_SIZE, output.received.size());
		for (int i = 0; i < BATCH_SIZE; i++) {
			assertSame(ops.get(i), output.received.get(i));
		}
		assertEquals(BATCH_SIZE, generator.generatedOpCount());
	}

	/**
	 * Start the generator VT loop, recycle ops from the test thread,
	 * and verify all ops eventually reach the output.
	 */
	@Test
	void concurrentRecycleDuringRunningGenerator() throws Exception {
		final int opCount = 20;
		final ConcurrentCollectingOutput<DataOperation<DataItem>> concurrentOutput =
						new ConcurrentCollectingOutput<>(opCount);

		final LoadGeneratorImpl<DataItem, DataOperation<DataItem>> runningGen =
						new LoadGeneratorImpl<>(
										itemInput,
										opsBuilder,
										List.of(),
										concurrentOutput,
										BATCH_SIZE,
										0,
										1000,
										true,
										false);

		try {
			runningGen.start();

			// Wait for item input to be exhausted
			assertEventually(runningGen::isItemInputFinished, 2000);

			// Recycle ops from test thread
			for (int i = 0; i < opCount; i++) {
				runningGen.recycle(newOp("concurrent-" + i));
			}

			// Wait for all ops to be output
			assertTrue(
							concurrentOutput.latch.await(5, TimeUnit.SECONDS),
							"Expected " + opCount + " ops but got " + concurrentOutput.received.size());
			assertEquals(opCount, concurrentOutput.received.size());
		} finally {
			runningGen.stop();
			runningGen.await(5, TimeUnit.SECONDS);
			runningGen.close();
		}
	}

	/**
	 * Verify isNothingToRecycle() and generatedOpCount() track queue state
	 * correctly across recycle() and doWork() calls.
	 */
	@Test
	void recycleQueueStateTracking() throws Exception {
		// Initially empty
		assertTrue(generator.isNothingToRecycle());
		assertEquals(0, generator.generatedOpCount());

		// Exhaust item input
		generator.doWork();

		// Recycle 3 ops
		for (int i = 0; i < 3; i++) {
			generator.recycle(newOp("track-" + i));
		}
		assertFalse(generator.isNothingToRecycle());

		// doWork drains the queue and outputs all ops
		generator.doWork();
		assertTrue(generator.isNothingToRecycle());
		assertEquals(3, generator.generatedOpCount());
		assertEquals(3, output.received.size());
	}

	// --- fast-recycle quiesce tests ---

	/**
	 * With quiesce enabled and empty recycle queue, doWork() parks for up to 10ms
	 * instead of yielding.  Verify it returns within a bounded time.
	 */
	@Test
	void quiesceParksOnEmptyRecycleQueue() throws Exception {
		generator.enableFastRecycleQuiesce();
		// Exhaust item input
		generator.doWork();
		assertTrue(generator.isItemInputFinished());

		// doWork with quiesce + empty queue parks for up to 10ms
		final long start = System.nanoTime();
		generator.doWork();
		final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

		// Should park for ~10ms (timeout), bounded well under 100ms
		assertTrue(elapsedMs < 100, "doWork took " + elapsedMs + "ms; expected < 100ms");
		assertEquals(0, output.received.size());
	}

	/**
	 * With quiesce enabled, recycle() unparks the generator VT so it picks
	 * up the op quickly rather than waiting the full 10ms timeout.
	 */
	@Test
	void recycleUnparksQuiescedGenerator() throws Exception {
		final int opCount = 10;
		final ConcurrentCollectingOutput<DataOperation<DataItem>> concurrentOutput =
						new ConcurrentCollectingOutput<>(opCount);

		final LoadGeneratorImpl<DataItem, DataOperation<DataItem>> quiescedGen =
						new LoadGeneratorImpl<>(
										itemInput,
										opsBuilder,
										List.of(),
										concurrentOutput,
										BATCH_SIZE,
										0,
										1000,
										true,
										false);
		quiescedGen.enableFastRecycleQuiesce();

		try {
			quiescedGen.start();
			assertEventually(quiescedGen::isItemInputFinished, 2000);

			// Recycle ops with slight spacing to exercise the unpark path
			final long t0 = System.nanoTime();
			for (int i = 0; i < opCount; i++) {
				quiescedGen.recycle(newOp("quiesce-" + i));
				Thread.sleep(1);
			}

			// All ops should arrive well under 10ms * opCount since unpark wakes
			// the generator immediately for each one
			assertTrue(
							concurrentOutput.latch.await(5, TimeUnit.SECONDS),
							"Expected " + opCount + " ops but got " + concurrentOutput.received.size());
			final long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
			assertTrue(
							elapsedMs < 2000,
							"Took " + elapsedMs + "ms for " + opCount + " ops; unpark may not be working");
			assertEquals(opCount, concurrentOutput.received.size());
		} finally {
			quiescedGen.stop();
			quiescedGen.await(5, TimeUnit.SECONDS);
			quiescedGen.close();
		}
	}

	/**
	 * Without quiesce, the existing yield path is used (characterization of
	 * the non-quiesce path when fast-recycle is not enabled).
	 */
	@Test
	void nonQuiesceUsesYieldPath() throws Exception {
		// quiesce NOT enabled — default
		generator.doWork(); // exhaust item input

		// doWork with empty queue should return very quickly (yield, not 10ms park)
		final long start = System.nanoTime();
		generator.doWork();
		final long elapsedMs = (System.nanoTime() - start) / 1_000_000;

		// Yield returns in microseconds, not milliseconds
		assertTrue(elapsedMs < 5, "doWork took " + elapsedMs + "ms; yield should be sub-millisecond");
	}

	// --- helpers ---

	private DataOperation<DataItem> newOp(final String name) {
		final DataItem item = new DataItemImpl(name, 0, 1024);
		return new DataOperationImpl<>(0, OpType.READ, item, "/bucket", null, null, List.of(), 0);
	}

	private void assertEventually(
					final java.util.function.BooleanSupplier condition,
					final long timeoutMs)
					throws InterruptedException {
		final long deadline = System.currentTimeMillis() + timeoutMs;
		while (!condition.getAsBoolean()) {
			if (System.currentTimeMillis() > deadline) {
				fail("Condition not met within " + timeoutMs + "ms");
			}
			Thread.sleep(10);
		}
	}

	/** Simple collecting output for single-threaded doWork() tests. */
	private static class CollectingOutput<O> implements Output<O> {
		final List<O> received = new ArrayList<>();

		@Override
		public boolean put(final O item) {
			if (item != null) {
				received.add(item);
			}
			return true;
		}

		@Override
		public int put(final List<O> buffer, final int from, final int to) {
			for (int i = from; i < to; i++) {
				received.add(buffer.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<O> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<O> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}

	/** Thread-safe collecting output with a latch for concurrent tests. */
	private static class ConcurrentCollectingOutput<O> implements Output<O> {
		final List<O> received = new CopyOnWriteArrayList<>();
		final CountDownLatch latch;

		ConcurrentCollectingOutput(final int expectedCount) {
			latch = new CountDownLatch(expectedCount);
		}

		@Override
		public boolean put(final O item) {
			if (item != null) {
				received.add(item);
				latch.countDown();
			}
			return true;
		}

		@Override
		public int put(final List<O> buffer, final int from, final int to) {
			for (int i = from; i < to; i++) {
				received.add(buffer.get(i));
				latch.countDown();
			}
			return to - from;
		}

		@Override
		public int put(final List<O> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<O> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}
}
