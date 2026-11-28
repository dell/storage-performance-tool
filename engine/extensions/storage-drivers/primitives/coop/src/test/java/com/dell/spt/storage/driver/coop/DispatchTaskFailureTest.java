package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
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
import com.github.akurilov.commons.lang.Exceptions;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A dispatch task that meets an {@link Error} or an unexpected exception fails the step once
 * and leaves no backlog; before, it ended without a trace and the driver stopped sending.
 */
@Timeout(60)
class DispatchTaskFailureTest {

	private static final String STEP_ID = "dispatch-failure";
	private static final int OPS = 100;
	private static final int FAIL_AT = 5;

	/** Throws {@code injected} from the {@value #FAIL_AT}th submit. */
	private static final class FailingSubmitDriver extends CoopStorageDriverMock<DataItem, Operation<DataItem>> {
		private final AtomicInteger submits = new AtomicInteger();
		private final Throwable injected;

		FailingSubmitDriver(final int batchSize, final Throwable injected) throws Exception {
			super(STEP_ID, dataInput(), config(), false, batchSize);
			this.injected = injected;
		}

		@Override
		protected boolean submit(final Operation<DataItem> op) {
			if (submits.incrementAndGet() == FAIL_AT) {
				Exceptions.throwUnchecked(injected);
			}
			return super.submit(op);
		}
	}

	/** Throws while the driver closes admission, as a transport shutting down does. */
	private static final class ThrowsWhileClosingDriver extends CoopStorageDriverMock<DataItem, Operation<DataItem>> {
		private volatile boolean entered;

		ThrowsWhileClosingDriver() throws Exception {
			super(STEP_ID, dataInput(), config(), false, 4);
		}

		@Override
		protected boolean submit(final Operation<DataItem> op) {
			entered = true;
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (isAdmissionOpen() && System.nanoTime() < deadline) {
				Thread.onSpinWait();
			}
			throw new RejectedExecutionException("event loop shutting down");
		}
	}

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	@Test
	void anErrorFailsTheStepOnceAndLeavesNoBacklog() throws Exception {
		failsTheStep(new LinkageError("attempted duplicate class definition"), 4);
	}

	@Test
	void anErrorInSingleOperationDispatchFailsTheStep() throws Exception {
		failsTheStep(new LinkageError("attempted duplicate class definition"), 1);
	}

	@Test
	void anUnexpectedRuntimeExceptionFailsTheStep() throws Exception {
		failsTheStep(new NullPointerException("driver bug"), 4);
	}

	@Test
	void aCheckedExceptionThrownUncheckedFailsTheStep() throws Exception {
		failsTheStep(new IOException("thrown unchecked"), 4);
	}

	@Test
	void aStoppedTaskLeavesNoBacklogToRefuseDirectDispatch() throws Exception {
		final var driver = new FailingSubmitDriver(4, new LinkageError("attempted duplicate class definition"));
		final var generator = generator(driver, new CopyOnWriteArrayList<>());
		try {
			driver.start();
			generator.start();
			final var task = dispatchTask(driver);
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS), "the dispatch task stops");
			assertEquals(0, task.backlog());
			assertEquals(0, driver.dispatcherBacklog());
		} finally {
			generator.close();
			driver.close();
		}
	}

	@Test
	void aTerminalFailureFromDispatchIsRecordedAsItIs() throws Exception {
		final var injected = new IntegrityTerminalException(IntegrityTerminalException.Category.EXECUTION, "terminal");
		final var driver = new FailingSubmitDriver(4, injected);
		final var results = new CopyOnWriteArrayList<Operation<DataItem>>();
		final var generator = generator(driver, results);
		try {
			driver.start();
			generator.start();
			await(() -> driver.terminalFailure() != null);
			assertSame(injected, driver.terminalFailure().getCause());
			assertEquals(STEP_ID, driver.terminalFailure().stepId());
		} finally {
			generator.close();
			driver.close();
		}
	}

	@Test
	void anIllegalStateExceptionKeepsTheTaskRunning() throws Exception {
		final var driver = new FailingSubmitDriver(4, new IllegalStateException("illegal driver state"));
		final var results = new CopyOnWriteArrayList<Operation<DataItem>>();
		final var generator = generator(driver, results);
		try {
			driver.start();
			generator.start();
			await(() -> results.size() == OPS);
			assertNull(driver.terminalFailure());
			assertFalse(dispatchTask(driver).isStopped());
		} finally {
			generator.close();
			driver.close();
		}
	}

	@Test
	void anExceptionWhileAdmissionClosesRecordsNothing() throws Exception {
		final var driver = new ThrowsWhileClosingDriver();
		driver.operationResultOutput(collector(new CopyOnWriteArrayList<>()));
		try (var err = new LogCapture(Loggers.ERR); var msg = new LogCapture(Loggers.MSG)) {
			driver.start();
			assertTrue(driver.put(op(new DataItemImpl("obj-0", 0, 1))));
			await(() -> driver.entered);
			driver.closeAdmission();
			final var task = dispatchTask(driver);
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS), "the dispatch task stops");
			assertNull(driver.terminalFailure());
			assertEquals(List.of(), err.messagesAbove(Level.DEBUG));
			assertEquals(List.of(), msg.messagesAbove(Level.DEBUG));
		} finally {
			driver.close();
		}
	}

	private static void failsTheStep(final Throwable injected, final int batchSize) throws Exception {
		final var driver = new FailingSubmitDriver(batchSize, injected);
		final var results = new CopyOnWriteArrayList<Operation<DataItem>>();
		final var generator = generator(driver, results);
		try (var err = new LogCapture(Loggers.ERR)) {
			driver.start();
			generator.start();
			await(() -> driver.terminalFailure() != null);
			final var failure = driver.terminalFailure();
			assertSame(injected, failure.getCause());
			assertEquals(STEP_ID, failure.stepId());
			assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
			assertTrue(failure.getMessage().contains(injected.getClass().getName()), failure.getMessage());
			final var task = dispatchTask(driver);
			assertTrue(task.awaitStop(5, TimeUnit.SECONDS), "the dispatch task stops");
			assertEquals(0, task.backlog());
			final var errors = err.messagesAbove(Level.WARN);
			assertEquals(1, errors.size(), errors.toString());
			assertTrue(errors.get(0).contains(injected.toString()), errors.get(0));
		} finally {
			generator.close();
			driver.close();
		}
	}

	private static LoadGeneratorImpl<DataItem, Operation<DataItem>> generator(
					final FailingSubmitDriver driver, final List<Operation<DataItem>> results) throws Exception {
		driver.operationResultOutput(collector(results));
		final var items = new ArrayList<DataItem>();
		for (var i = 0; i < OPS; i++) {
			items.add(new DataItemImpl("obj-" + i, 0, 1));
		}
		final var generator = new LoadGeneratorImpl<DataItem, Operation<DataItem>>(
						new ListInput<>(items), builder(), List.of(), driver, 4, OPS, 4, false, false);
		generator.operationLifecycle(driver.operationLifecycle());
		return generator;
	}

	private static OperationDispatchTask<?, ?> dispatchTask(final CoopStorageDriverBase<?, ?> driver)
					throws ReflectiveOperationException {
		final var field = CoopStorageDriverBase.class.getDeclaredField("opDispatchTask");
		field.setAccessible(true);
		return (OperationDispatchTask<?, ?>) field.get(driver);
	}

	/**
	 * Collects one engine logger's records. The logger is asynchronous, so a read first sends a
	 * marker through it and waits for the marker to arrive.
	 */
	private static final class LogCapture implements AutoCloseable {
		private static final AtomicInteger MARKERS = new AtomicInteger();

		private final List<LogEvent> events = new CopyOnWriteArrayList<>();
		private final Logger logger;
		private final AbstractAppender appender;

		LogCapture(final Logger logger) {
			this.logger = logger;
			appender = new AbstractAppender("dispatchFailureCapture", null, null, true, Property.EMPTY_ARRAY) {
				@Override
				public void append(final LogEvent event) {
					events.add(event.toImmutable());
				}
			};
			appender.start();
			LoggerContext.getContext(false).getLogger(logger.getName()).addAppender(appender);
		}

		/** The messages logged so far more severe than {@code level}, without the read marker. */
		List<String> messagesAbove(final Level level) throws InterruptedException {
			final String marker = "dispatch-failure-capture-marker-" + MARKERS.incrementAndGet();
			logger.fatal(marker);
			await(() -> events.stream().anyMatch(event -> marker.equals(event.getMessage().getFormattedMessage())));
			final List<String> messages = new ArrayList<>();
			for (final LogEvent event : events) {
				final String message = event.getMessage().getFormattedMessage();
				if (event.getLevel().isMoreSpecificThan(level) && !event.getLevel().equals(level)
								&& !message.startsWith("dispatch-failure-capture-marker-")) {
					messages.add(message);
				}
			}
			return messages;
		}

		@Override
		public void close() {
			LoggerContext.getContext(false).getLogger(logger.getName()).removeAppender(appender);
			appender.stop();
		}
	}

	private static DataInput dataInput() throws Exception {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	private static Operation<DataItem> op(final DataItem item) {
		return new DataOperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null, List.of(), 0);
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
