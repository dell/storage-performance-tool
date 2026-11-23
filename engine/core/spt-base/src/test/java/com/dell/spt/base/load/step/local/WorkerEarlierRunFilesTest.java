package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.EarlierRunFiles;
import com.dell.spt.base.load.step.RunStepIds;
import com.dell.spt.base.load.step.StepIdCheckException;
import com.dell.spt.base.load.step.StepIdFiles;
import com.dell.spt.base.load.step.StepIdInUseException;
import com.dell.spt.base.load.step.client.StepIdReuseAtStartTest;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A worker engine, asked by the Entry Node to prepare its files for the first step of an id in a run
 * (before any slice of the step exists), moves aside the files an earlier run left under that id and
 * warns once; the slice then starts as on a clean folder. A later step of the id in the same run, a
 * step without a run id, and a slice the Entry Node did not prepare for are refused as before; so
 * is a worker that cannot move the files, before any slice is created.
 */
class WorkerEarlierRunFilesTest {

	private static final long RUN_ID = 1_763_892_000_000L;
	private static final Instant NOW = Instant.parse("2026-11-23T10:15:30Z");

	private final String stepId = "worker-earlier-run-" + UUID.randomUUID();
	private final List<WorkerSlice> slices = new ArrayList<>();

	@AfterEach
	void removeTheStepFolders() throws Exception {
		for (final WorkerSlice slice : slices) {
			try {
				slice.close();
			} catch (final Exception ignored) {
				// A refused slice may fail its close; the folders go all the same.
			}
		}
		final Path stepDir = logFile(Loggers.OPERATION_LIFECYCLE).toAbsolutePath().getParent();
		try (Stream<Path> siblings = Files.list(stepDir.getParent())) {
			for (final Path dir : siblings.filter(dir -> dir.getFileName().toString().startsWith(stepId)).toList()) {
				Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwxr-xr-x"));
				StepIdReuseAtStartTest.removeFolder(dir);
			}
		}
	}

	@Test
	void aPreparedCreateStepMovesAnEarlierRunsLifecycleFileAsideWarnsOnceAndItsSliceRuns() throws Exception {
		final Path lifecycle = earlierFile(logFile(Loggers.OPERATION_LIFECYCLE));
		final Config config = config("create", RUN_ID);

		final var firstInRun = new boolean[1];
		final List<String> warnings = warnings(() -> firstInRun[0] = prepare(new RunStepIds(), config));

		assertTrue(firstInRun[0]);
		final Path moved = onlyMovedFolder();
		assertEquals(List.of(
						stepId + ": this worker's folder for the step holds operation.lifecycle.csv from an earlier run; "
										+ "moved it aside to " + moved + ", and the step runs"),
						warnings);
		assertEquals("earlier", Files.readString(moved.resolve(lifecycle.getFileName())));
		assertFalse(Files.exists(lifecycle), "the earlier run's file is still in the step's folder");

		final LoadStepContext context = mockContext();
		final WorkerSlice slice = slice(config, context);
		slice.start();
		verify(context).start();
		slice.stop();

		final List<String> rows = Files.readAllLines(lifecycle);
		assertEquals(2, rows.size(), rows.toString());
		assertTrue(rows.get(1).startsWith("1," + RUN_ID + "," + stepId + ","), rows.toString());
		assertEquals("earlier", Files.readString(moved.resolve(lifecycle.getFileName())), "the moved file changed");
	}

	@Test
	void aPartialReadStepMovesAnEarlierRunsRangeReadFileAside() throws Exception {
		final Path ranges = earlierFile(logFile(Loggers.RANGE_READ));
		final Config config = config("read", RUN_ID);
		config.val("load-op-read-range-size", "1KiB");
		config.val("storage-driver-type", "s3");

		assertEquals(1, warnings(() -> prepare(new RunStepIds(), config)).size());

		assertEquals("earlier", Files.readString(onlyMovedFolder().resolve(ranges.getFileName())));
		assertFalse(Files.exists(ranges));
		final LoadStepContext context = mockContext();
		slice(config, context).start();
		verify(context).start();
	}

	@Test
	void aNicSetStepMovesAnEarlierRunsNicFilesAside() throws Exception {
		final Path nicPaths = earlierFile(logFile(Loggers.NIC_PATHS));
		final Path nicSet = earlierFile(logFile(Loggers.NIC_SET));
		final Config config = config("update", RUN_ID);
		config.val(NicSetConfig.SLICE_INTERFACES_PATH, List.of("lo"));

		final List<String> warnings = warnings(() -> prepare(new RunStepIds(), config));

		final Path moved = onlyMovedFolder();
		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).contains("holds nic.paths.csv, nic.set.json from an earlier run; moved them aside"), warnings.toString());
		assertEquals("earlier", Files.readString(moved.resolve(nicPaths.getFileName())));
		assertEquals("earlier", Files.readString(moved.resolve(nicSet.getFileName())));
		final LoadStepContext context = mockContext();
		slice(config, context).start();
		verify(context).start();
	}

	@Test
	void aSecondStepOfTheIdInTheSameRunMovesNothingAndItsSliceIsRefusedBeforeItsContextsStart() throws Exception {
		final var runStepIds = new RunStepIds();
		final Config config = config("create", RUN_ID);
		assertTrue(prepare(runStepIds, config));
		final WorkerSlice first = slice(config, mockContext());
		first.start();
		first.stop();
		final Path lifecycle = logFile(Loggers.OPERATION_LIFECYCLE);
		assertTrue(Files.exists(lifecycle), "the first slice published its lifecycle file");
		final String published = Files.readString(lifecycle);

		assertFalse(prepare(runStepIds, config("create", RUN_ID)));

		final LoadStepContext context = mockContext();
		final var failure = refused(slice(config("create", RUN_ID), context));
		assertEquals(StepIdInUseException.class, failure.getClass());
		assertTrue(failure.getMessage().contains("(" + lifecycle + " exists)"), failure.getMessage());
		verify(context, never()).start();
		assertEquals(published, Files.readString(lifecycle));
		assertNoMovedFolder();
	}

	@Test
	void aSameRunFileThatAppearsAfterThePreparationIsRefusedAtTheSliceStart() throws Exception {
		final Config config = config("create", RUN_ID);
		assertTrue(prepare(new RunStepIds(), config));
		// A concurrent step of the id in this run publishes its file between the two.
		final Path lifecycle = earlierFile(logFile(Loggers.OPERATION_LIFECYCLE));

		final LoadStepContext context = mockContext();
		final var failure = refused(slice(config, context));

		assertEquals(StepIdInUseException.class, failure.getClass());
		verify(context, never()).start();
		assertEquals("earlier", Files.readString(lifecycle));
		assertNoMovedFolder();
	}

	@Test
	void theNextRunsStepOfTheIdMovesThePreviousRunsFileAside() throws Exception {
		final var runStepIds = new RunStepIds();
		final Config config = config("create", RUN_ID);
		assertTrue(prepare(runStepIds, config));
		final WorkerSlice first = slice(config, mockContext());
		first.start();
		first.stop();
		final String published = Files.readString(logFile(Loggers.OPERATION_LIFECYCLE));

		final Config next = config("create", RUN_ID + 1);
		assertTrue(prepare(runStepIds, next));
		final LoadStepContext context = mockContext();
		slice(next, context).start();

		verify(context).start();
		assertEquals(published, Files.readString(onlyMovedFolder().resolve("operation.lifecycle.csv")));
	}

	@Test
	void aSliceTheEntryNodeDidNotPrepareForIsRefusedAsBefore() throws Exception {
		final Path lifecycle = earlierFile(logFile(Loggers.OPERATION_LIFECYCLE));
		final LoadStepContext context = mockContext();

		final var failure = refused(slice(config("create", RUN_ID), context));

		assertEquals("step id \"" + stepId + "\" is already used in this log directory (" + lifecycle
						+ " exists); give each step its own load.step.id", failure.getMessage());
		verify(context, never()).start();
		assertEquals("earlier", Files.readString(lifecycle));
		assertNoMovedFolder();
	}

	@Test
	void aStepWithoutARunIdMovesNothing() throws Exception {
		final Path lifecycle = earlierFile(logFile(Loggers.OPERATION_LIFECYCLE));

		assertFalse(prepare(new RunStepIds(), config("create", 0L)));

		assertEquals("earlier", Files.readString(lifecycle));
		assertNoMovedFolder();
	}

	@Test
	void aFileOfAnotherKindIsNeverMoved() throws Exception {
		final Path messages = earlierFile(logFile(Loggers.MSG));

		final var failure = assertThrows(StepIdCheckException.class, () -> EarlierRunFiles.prepare(
						new RunStepIds(), RUN_ID, stepId, List.of(Loggers.MSG.getName()), NOW));

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("earlier", Files.readString(messages));
		assertNoMovedFolder();
	}

	@Test
	void aWorkerThatCannotMoveTheFilesRefusesTheStepAndKeepsThemInPlace() throws Exception {
		assumeFalse("root".equals(System.getProperty("user.name")), "root writes a read-only folder");
		final Path lifecycle = earlierFile(logFile(Loggers.OPERATION_LIFECYCLE));
		final Path stepDir = lifecycle.toAbsolutePath().getParent();
		Files.setPosixFilePermissions(stepDir, PosixFilePermissions.fromString("r-xr-xr-x"));
		try {
			final var failure = assertThrows(StepIdCheckException.class, () -> prepare(new RunStepIds(), config("create", RUN_ID)));

			assertEquals("step id \"" + stepId + "\" was used by an earlier run in this worker's log directory, and its "
							+ "files could not be moved aside; see the engine log", failure.getMessage());
			assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		} finally {
			Files.setPosixFilePermissions(stepDir, PosixFilePermissions.fromString("rwxr-xr-x"));
		}
		assertEquals("earlier", Files.readString(lifecycle));
		assertNoMovedFolder();
	}

	/** As the Entry Node asks the worker: the loggers of the files the slice of {@code config} publishes. */
	private boolean prepare(final RunStepIds runStepIds, final Config config) {
		final var loggerNames = StepIdFiles.sliceLoggerNames(
						StepIdFiles.merged(config, List.of()), StepIdFiles.nicSet(config, List.of()));
		return EarlierRunFiles.prepare(runStepIds, config.longVal("run-id"), stepId, loggerNames, Instant.now());
	}

	private RunTerminalException refused(final WorkerSlice slice) {
		final var thrown = assertThrows(RuntimeException.class, slice::start);
		final var failure = RunTerminalException.find(thrown);
		assertTrue(failure instanceof StepIdInUseException || failure instanceof StepIdCheckException, thrown.toString());
		return failure;
	}

	private Path onlyMovedFolder() throws IOException {
		final List<Path> moved = movedFolders();
		assertEquals(1, moved.size(), moved.toString());
		return moved.get(0);
	}

	private void assertNoMovedFolder() throws IOException {
		assertEquals(List.of(), movedFolders());
	}

	private List<Path> movedFolders() throws IOException {
		final Path stepDir = logFile(Loggers.OPERATION_LIFECYCLE).toAbsolutePath().getParent();
		try (Stream<Path> siblings = Files.list(stepDir.getParent())) {
			return siblings.filter(dir -> dir.getFileName().toString().startsWith(stepId + ".earlier-")).toList();
		}
	}

	/** The warnings {@code action} logs for this step to errors.log and the console. */
	private List<String> warnings(final ThrowingAction action) throws Exception {
		final List<String> warnings = Collections.synchronizedList(new ArrayList<>());
		final String end = "worker earlier run end " + UUID.randomUUID();
		final var ended = new CountDownLatch(1);
		final var appender = new AbstractAppender("workerEarlierRun-" + stepId, null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final String message = event.getMessage().getFormattedMessage();
				if (message.equals(end)) {
					ended.countDown();
				} else if (event.getLevel().name().equals("WARN") && message.contains(stepId)) {
					warnings.add(message);
				}
			}
		};
		appender.start();
		final var logger = (org.apache.logging.log4j.core.Logger) LoggerContext.getContext(false)
						.getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			action.run();
			// One thread's events arrive in order: after this one, every warning the action logged.
			Loggers.ERR.info(end);
			assertTrue(ended.await(10, TimeUnit.SECONDS), "the end marker never arrived");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		synchronized (warnings) {
			return List.copyOf(warnings);
		}
	}

	@FunctionalInterface
	private interface ThrowingAction {
		void run() throws Exception;
	}

	private Config config(final String opType, final long runId) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-op-type", opType);
		config.val("run-id", runId);
		return config;
	}

	private WorkerSlice slice(final Config config, final LoadStepContext context) {
		final var slice = new WorkerSlice(config, mock(MetricsManager.class), context);
		slices.add(slice);
		return slice;
	}

	private static LoadStepContext mockContext() {
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.activeOpCount()).thenReturn(0);
		return context;
	}

	private Path logFile(final Logger logger) throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
	}

	private static Path earlierFile(final Path path) throws IOException {
		Files.createDirectories(path.toAbsolutePath().getParent());
		Files.writeString(path, "earlier");
		return path;
	}

	private static final class WorkerSlice extends LoadStepLocalBase {

		WorkerSlice(final Config config, final MetricsManager metricsManager, final LoadStepContext context) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
			stepContexts.add(context);
		}

		@Override
		public String getTypeName() {
			return "worker-earlier-run";
		}

		@Override
		protected void init() {
			// The context is given.
		}
	}
}
