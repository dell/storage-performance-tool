package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.io.collection.ListInput;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A batch put whose later operation fails to prepare keeps the operations it already queued
 * (GitHub #486). A bucket check that fails to connect leaves the path uncached and still queues the
 * operation; the next operation's check then threw a {@link LinkageError}, the put lost the queued
 * operation, and the lifecycle tracker refused every retry of it, so the step hung.
 */
@Timeout(60)
class PartialBatchAdmissionTest {

	private static final String UNCACHED = "uncached";
	private static final String ERROR = "error";

	/** Scripts {@code requestNewPath}: an uncached path, as after a failed connect, or an error. */
	private static final class ScriptedPathDriver extends CoopStorageDriverMock<DataItem, Operation<DataItem>> {
		private final List<String> script;
		private final AtomicInteger calls = new AtomicInteger();

		ScriptedPathDriver(final int batchSize, final String... script) throws Exception {
			super("partial-batch", dataInput(), config(), false, batchSize);
			this.script = List.of(script);
		}

		@Override
		protected String requestNewPath(final String path) {
			final int n = calls.getAndIncrement();
			final String step = n < script.size() ? script.get(n) : "ok";
			if (UNCACHED.equals(step)) {
				return null;
			}
			if (ERROR.equals(step)) {
				throw new LinkageError("attempted duplicate class definition");
			}
			return path;
		}
	}

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	@Test
	void aRangedBatchPutReportsTheOperationsQueuedBeforeAPrepareError() throws Exception {
		final var driver = new ScriptedPathDriver(4, UNCACHED, ERROR);
		final var results = new CopyOnWriteArrayList<Operation<DataItem>>();
		driver.operationResultOutput(collector(results));
		try {
			driver.start();
			final var ops = ops(3);
			assertEquals(1, driver.put(ops, 0, 3));
			assertEquals(2, driver.put(ops, 1, 3));
			await(() -> results.size() == 3);
			assertEachOnce(ops, results);
		} finally {
			driver.close();
		}
	}

	@Test
	void aListBatchPutReportsTheOperationsQueuedBeforeAPrepareError() throws Exception {
		final var driver = new ScriptedPathDriver(4, UNCACHED, ERROR);
		final var results = new CopyOnWriteArrayList<Operation<DataItem>>();
		driver.operationResultOutput(collector(results));
		try {
			driver.start();
			final var ops = ops(3);
			assertEquals(1, driver.put(ops));
			assertEquals(2, driver.put(ops.subList(1, 3)));
			await(() -> results.size() == 3);
			assertEachOnce(ops, results);
		} finally {
			driver.close();
		}
	}

	@Test
	void aPrepareErrorOnTheFirstOperationOfABatchStillPropagates() throws Exception {
		final var driver = new ScriptedPathDriver(4, ERROR, ERROR);
		try {
			driver.start();
			final var ops = ops(3);
			assertThrows(LinkageError.class, () -> driver.put(ops, 0, 3));
			assertThrows(LinkageError.class, () -> driver.put(ops));
			assertEquals(0, driver.scheduledOpCount());
		} finally {
			driver.close();
		}
	}

	@Test
	void aGeneratorRunsEveryOperationOnceAfterAnUncachedPathThenAPrepareError() throws Exception {
		final var driver = new ScriptedPathDriver(4, UNCACHED, ERROR);
		final var results = new CopyOnWriteArrayList<Operation<DataItem>>();
		driver.operationResultOutput(collector(results));
		final var items = new ArrayList<DataItem>();
		for (var i = 0; i < 6; i++) {
			items.add(new DataItemImpl("obj-" + i, 0, 1));
		}
		final Input<DataItem> input = new ListInput<>(items);
		final var generator = new LoadGeneratorImpl<DataItem, Operation<DataItem>>(
						input, builder(), List.of(), driver, 4, 6, 4, false, false);
		generator.operationLifecycle(driver.operationLifecycle());
		try {
			driver.start();
			generator.start();
			await(() -> results.size() == 6);
			Thread.sleep(100);
			assertEquals(items.stream().map(DataItem::name).sorted().toList(), names(results));
			assertNull(generator.terminalFailure());
		} finally {
			generator.close();
			driver.close();
		}
	}

	private static void assertEachOnce(final List<Operation<DataItem>> ops, final List<Operation<DataItem>> results)
					throws InterruptedException {
		Thread.sleep(100);
		assertEquals(names(ops), names(results));
	}

	/** The driver publishes copies of its operations, so results are matched by item name. */
	private static List<String> names(final List<Operation<DataItem>> ops) {
		return ops.stream().map(op -> op.item().name()).sorted().toList();
	}

	private static DataInput dataInput() throws Exception {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	private static Operation<DataItem> op(final DataItem item) {
		return new DataOperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null, List.of(), 0);
	}

	private static List<Operation<DataItem>> ops(final int n) {
		final var ops = new ArrayList<Operation<DataItem>>();
		for (var i = 0; i < n; i++) {
			ops.add(op(new DataItemImpl("obj-" + i, 0, 1)));
		}
		return ops;
	}

	private static OperationsBuilderImpl<DataItem, Operation<DataItem>> builder() {
		final var builder = new OperationsBuilderImpl<DataItem, Operation<DataItem>>(0) {
			@Override
			public Operation<DataItem> buildOp(final DataItem item) {
				return op(item);
			}

			@Override
			public void buildOps(final List<DataItem> items, final List<Operation<DataItem>> buff) {
				for (final var item : items) {
					buff.add(op(item));
				}
			}
		};
		builder.opType(OpType.CREATE).credentialInput(new ConstantValueInputImpl<>(Credential.NONE));
		return builder;
	}

	private static Output<Operation<DataItem>> collector(final List<Operation<DataItem>> results) {
		return new Output<>() {
			@Override
			public boolean put(final Operation<DataItem> op) {
				results.add(op);
				return true;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer, final int from, final int to) {
				results.addAll(buffer.subList(from, to));
				return to - from;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer) {
				results.addAll(buffer);
				return buffer.size();
			}

			@Override
			public Input<Operation<DataItem>> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};
	}

	private static void await(final BooleanSupplier condition) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean());
	}

	private static Config config() {
		final var storage = mock(Config.class);
		final var driver = mock(Config.class);
		final var limit = mock(Config.class);
		final var auth = mock(Config.class);
		final var integrity = mock(Config.class);
		final var input = mock(Config.class);
		when(storage.configVal("driver")).thenReturn(driver);
		when(driver.configVal("limit")).thenReturn(limit);
		when(storage.stringVal("namespace")).thenReturn("test-ns");
		when(storage.configVal("auth")).thenReturn(auth);
		when(storage.configVal("integrity")).thenReturn(integrity);
		when(integrity.stringVal("mode")).thenReturn("none");
		when(integrity.stringVal("algorithm")).thenReturn("sha256");
		when(integrity.configVal("input")).thenReturn(input);
		when(input.stringVal("provenance")).thenReturn("none");
		when(input.stringVal("expectedProducerId")).thenReturn("");
		when(auth.stringVal("uid")).thenReturn("user");
		when(auth.stringVal("secret")).thenReturn("secret");
		when(storage.intVal("driver-limit-queue-input")).thenReturn(16);
		when(limit.intVal("concurrency")).thenReturn(2);
		return storage;
	}
}
