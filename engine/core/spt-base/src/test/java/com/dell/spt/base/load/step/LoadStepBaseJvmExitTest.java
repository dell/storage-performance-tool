package com.dell.spt.base.load.step;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.buildinfo.EngineBuildInfoPublisher;
import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.nic.EventLoopsArtifact;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Once the JVM has begun to exit, a load step does not start, and it ends as a stop. */
class LoadStepBaseJvmExitTest {

	private static final String STEP_ID = "jvm-exit-step";
	private static final String STOPPED_LINE = STEP_ID + ": step start stopped, the engine is exiting";
	private static final String END_OF_LOG = "jvm-exit-test: end of the log";

	@TempDir
	Path logHome;

	private String originalLogHome;

	@BeforeEach
	void useTemporaryLogHome() {
		originalLogHome = ThreadContext.get(KEY_HOME_DIR);
		ThreadContext.put(KEY_HOME_DIR, logHome.toString());
	}

	@AfterEach
	void restore() {
		JvmExitTestSupport.reset();
		if (originalLogHome == null) {
			ThreadContext.remove(KEY_HOME_DIR);
		} else {
			ThreadContext.put(KEY_HOME_DIR, originalLogHome);
		}
	}

	@Test
	void startIsRefusedBeforeTheStepTouchesAnything() throws Exception {
		try (var step = new RecordingStep(config())) {
			JvmExit.markBegun();

			final Throwable refusal = assertThrows(Throwable.class, step::start);

			assertInstanceOf(InterruptedException.class, refusal);
			assertTrue(refusal.getMessage().contains(STEP_ID), refusal.getMessage());
			assertEquals(List.of(), step.calls);
			assertFalse(step.isStarted());
			assertNoStepDirectory();
		}
	}

	@Test
	void runEndsAsAStopAndClosesTheStep() {
		final var step = new RecordingStep(config());
		JvmExit.markBegun();

		final Throwable refusal = assertThrows(Throwable.class, step::run);

		assertInstanceOf(InterruptedException.class, refusal);
		assertEquals(List.of(), step.calls);
		assertInstanceOf(InterruptedException.class, step.runFailure());
		assertTrue(step.isClosed());
		assertNoStepDirectory();
	}

	@Test
	void aSecondRunEndsAsAStopAndKeepsTheRecordOfTheFirst() {
		final var step = new RecordingStep(config());
		step.run();
		JvmExit.markBegun();

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, step::run));

		assertEquals(null, step.runFailure());
	}

	@Test
	void aStoppedStepStartedAgainEndsAsAStop() throws Exception {
		final Config config = config();
		config.val("load-step-node-addrs", List.of());
		final List<Extension> extensions = List.of(new LinearLoadStepExtension<>(), new DummyStorageDriverMockExtension<>());
		try (var step = new LinearLoadStepClient(config, extensions, List.of(), mock(MetricsManager.class))) {
			step.stop();
			JvmExit.markBegun();

			assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, step::start));
		}
	}

	@Test
	void aStepStartsAsBeforeWhileTheJvmIsNotExiting() {
		final var step = new RecordingStep(config());

		step.run();

		assertEquals(List.of("provision", "init", "start"), step.calls);
		assertEquals(null, step.runFailure());
		assertTrue(Files.exists(buildRecord()), "the check above must look where the step writes");
	}

	@Test
	void aLinearStepIsRefusedInBothStartForms() throws Exception {
		JvmExit.markBegun();
		final Config config = config();
		config.val("load-step-node-addrs", List.of());
		final List<Extension> extensions = List.of(new LinearLoadStepExtension<>(), new DummyStorageDriverMockExtension<>());
		try (var started = new LinearLoadStepClient(config, extensions, List.of(), mock(MetricsManager.class))) {
			assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, started::start));
			assertFalse(started.isStarted());
		}
		final var run = new LinearLoadStepClient(config, extensions, List.of(), mock(MetricsManager.class));
		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, run::run));
		assertTrue(run.isClosed());
		assertNoStepDirectory();
	}

	@Test
	void aSliceRefusedOnAWorkerPublishesNothingWhenClosed() throws Exception {
		// A slice's files go where the logging configuration puts them, which depends on the test order.
		final Path lifecycle = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), STEP_ID));
		final Path eventLoops = lifecycle.resolveSibling(EventLoopsArtifact.FILE_NAME);
		Files.deleteIfExists(lifecycle);
		Files.deleteIfExists(eventLoops);
		JvmExit.markBegun();
		try (var slice = new LinearLoadStepLocal(config(), List.of(), null, mock(MetricsManager.class))) {
			assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, slice::start));
		}
		assertNoStepDirectory();
		assertFalse(Files.exists(lifecycle), "the refused slice wrote its lifecycle file");
		assertFalse(Files.exists(eventLoops), "the refused slice wrote its event loops file");
	}

	@Test
	void aStartStoppedByTheExitEndsAsAStopWithOneInfoLine() throws Exception {
		final var step = new RecordingStep(config());
		step.duringInit = () -> {
			JvmExit.markBegun();
			Thread.currentThread().interrupt();
			throw new IllegalStateException("cut short by the interrupt");
		};

		final List<LoggedLine> lines = logOf(() -> assertStoppedRun(step));

		assertEquals(1, lines.stream().filter(line -> line.message.equals(STOPPED_LINE) && Level.INFO.equals(line.level)).count(), lines::toString);
		assertNoStartFailureLine(lines);
	}

	@Test
	void aStartStoppedByTheNodeApiEndsAsAStopWithoutTheExitLine() throws Exception {
		final var step = new RecordingStep(config());
		step.duringInit = () -> {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("cut short by the interrupt");
		};

		final List<LoggedLine> lines = logOf(() -> assertStoppedRun(step));

		assertTrue(lines.stream().noneMatch(line -> line.message.equals(STOPPED_LINE)), lines::toString);
		assertNoStartFailureLine(lines);
	}

	@Test
	void anInterruptedFailureInsideTheWrappedStartLogsNoWarning() throws Exception {
		final var step = new RecordingStep(config());
		step.duringStart = () -> {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("cut short by the interrupt");
		};

		final List<LoggedLine> lines = logOf(() -> assertStoppedRun(step));

		assertNoStartFailureLine(lines);
	}

	@Test
	void aTypedFailureOfAnInterruptedStartStaysAFailure() {
		final var step = new RecordingStep(config());
		step.duringInit = () -> {
			Thread.currentThread().interrupt();
			throw new RunTerminalException(IntegrityTerminalException.Category.EXECUTION, "the target refused", null);
		};
		try {
			assertThrows(RunTerminalException.class, step::run);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	void aFailedStartThatNoStopInterruptedStaysAFailure() throws Exception {
		final var step = new RecordingStep(config());
		step.duringStart = () -> {
			JvmExit.markBegun();
			throw new IllegalStateException("a real failure during the exit");
		};

		final List<LoggedLine> lines = logOf(() -> assertEquals(
						"step failed to start; see the engine log",
						assertInstanceOf(RunTerminalException.class, assertThrows(Throwable.class, step::run)).getMessage()));

		assertTrue(lines.stream().noneMatch(line -> line.message.equals(STOPPED_LINE)), lines::toString);
		assertEquals("a real failure during the exit", step.runFailure().getMessage());
		// The log level of a logger can differ between test orders; the report is still unclaimed.
		assertTrue(JvmExit.claimStartStopReport(), "a failed start claimed the exit's stop report");
	}

	private static void assertStoppedRun(final RecordingStep step) {
		try {
			assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, step::run));
			assertInstanceOf(InterruptedException.class, step.runFailure());
			assertTrue(step.isClosed());
		} finally {
			Thread.interrupted();
		}
	}

	private static void assertNoStartFailureLine(final List<LoggedLine> lines) {
		assertTrue(
						lines.stream().noneMatch(line -> line.message.toLowerCase(java.util.Locale.ROOT).contains("failed to start")),
						lines::toString);
	}

	/** What the message and error loggers received while {@code action} ran, up to a closing marker. */
	private static List<LoggedLine> logOf(final Runnable action) throws InterruptedException {
		final var context = LoggerContext.getContext(false);
		final Logger messages = context.getLogger(Loggers.MSG.getName());
		final Logger errors = context.getLogger(Loggers.ERR.getName());
		final var capture = new LogCapture();
		capture.start();
		messages.addAppender(capture);
		errors.addAppender(capture);
		try {
			action.run();
			// The loggers may be asynchronous: this line comes after everything the action logged.
			Loggers.ERR.error(END_OF_LOG);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
			while (capture.lines.stream().noneMatch(line -> line.message.equals(END_OF_LOG))) {
				assertTrue(System.nanoTime() < deadline, "the log did not reach its end: " + capture.lines);
				Thread.sleep(10);
			}
			return List.copyOf(capture.lines);
		} finally {
			messages.removeAppender(capture);
			errors.removeAppender(capture);
			capture.stop();
		}
	}

	private record LoggedLine(Level level, String message) {}

	private static final class LogCapture extends AbstractAppender {

		private final List<LoggedLine> lines = new CopyOnWriteArrayList<>();

		private LogCapture() {
			super("jvmExitTestCapture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			lines.add(new LoggedLine(event.getLevel(), event.getMessage().getFormattedMessage()));
		}
	}

	private void assertNoStepDirectory() {
		assertFalse(Files.exists(buildRecord().getParent()), "the refused step wrote its directory");
	}

	private Path buildRecord() {
		return logHome.resolve("log").resolve(STEP_ID).resolve(EngineBuildInfoPublisher.FILE_NAME);
	}

	private static Config config() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", STEP_ID);
		config.val("load-step-idAutoGenerated", false);
		config.val("run-id", 123L);
		return config;
	}

	private static final class RecordingStep extends LoadStepBase {

		private final List<String> calls = new ArrayList<>();
		private Runnable duringInit = () -> {};
		private Runnable duringStart = () -> {};

		private RecordingStep(final Config config) {
			super(config, Collections.emptyList(), Collections.emptyList(), mock(MetricsManager.class));
		}

		@Override
		protected void provisionTargetBuckets() {
			calls.add("provision");
		}

		@Override
		protected void init() {
			calls.add("init");
			duringInit.run();
		}

		@Override
		protected void doStartWrapped() {
			calls.add("start");
			duringStart.run();
		}

		@Override
		protected void initMetrics(
						final int originIndex,
						final OpType opType,
						final int concurrency,
						final Config metricsConfig,
						final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit timeUnit) {
			return true;
		}

		@Override
		public String getTypeName() {
			return "test";
		}
	}
}
