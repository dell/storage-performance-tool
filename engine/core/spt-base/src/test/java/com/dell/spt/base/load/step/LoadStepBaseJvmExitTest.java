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
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Once the JVM has begun to exit, a load step does not start, and it ends as a stop. */
class LoadStepBaseJvmExitTest {

	private static final String STEP_ID = "jvm-exit-step";

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
		}

		@Override
		protected void doStartWrapped() {
			calls.add("start");
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
