package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.logging.Loggers;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A worker engine moves an earlier run's step files into a new folder next to the step's, keeps
 * them as they were, and leaves the step's other files in place; when it cannot, it refuses the step.
 */
class EarlierRunFilesTest {

	private static final Instant NOW = Instant.parse("2026-11-23T10:15:30.250Z");

	@TempDir
	Path log;

	@Test
	void theFilesMoveUnchangedToANewFolderNamedForTheTimeAndTheStepsOtherFilesStay() throws Exception {
		final Path stepDir = Files.createDirectory(log.resolve("MAXT-W-10KB-C-T10"));
		final Path lifecycle = Files.writeString(stepDir.resolve("operation.lifecycle.csv"), "earlier lifecycle\n");
		final Path ranges = Files.writeString(stepDir.resolve("range.read.csv"), "earlier ranges\n");
		final Path messages = Files.writeString(stepDir.resolve("messages.log"), "this run's messages\n");

		final Path moved = EarlierRunFiles.moveAside("MAXT-W-10KB-C-T10", List.of(lifecycle, ranges), NOW);

		assertEquals(log.resolve("MAXT-W-10KB-C-T10.earlier-20261123T101530Z").toAbsolutePath(), moved);
		assertEquals("earlier lifecycle\n", Files.readString(moved.resolve("operation.lifecycle.csv")));
		assertEquals("earlier ranges\n", Files.readString(moved.resolve("range.read.csv")));
		assertFalse(Files.exists(lifecycle));
		assertFalse(Files.exists(ranges));
		assertEquals("this run's messages\n", Files.readString(messages), "the step's own log stays where it is written");
		assertEquals(List.of("operation.lifecycle.csv", "range.read.csv"), names(moved));
	}

	@Test
	void aTakenFolderNameGetsTheNextNumberAndIsLeftAsItWas() throws Exception {
		final Path stepDir = Files.createDirectory(log.resolve("X"));
		final Path taken = Files.createDirectory(log.resolve("X.earlier-20261123T101530Z"));
		Files.writeString(taken.resolve("operation.lifecycle.csv"), "older\n");
		Files.createDirectory(log.resolve("X.earlier-20261123T101530Z-2"));
		final Path lifecycle = Files.writeString(stepDir.resolve("operation.lifecycle.csv"), "earlier\n");

		final Path moved = EarlierRunFiles.moveAside("X", List.of(lifecycle), NOW);

		assertEquals(log.resolve("X.earlier-20261123T101530Z-3").toAbsolutePath(), moved);
		assertEquals("earlier\n", Files.readString(moved.resolve("operation.lifecycle.csv")));
		assertEquals("older\n", Files.readString(taken.resolve("operation.lifecycle.csv")));
	}

	@Test
	void aFileThisEngineHoldsOpenIsNotMovedAndTheStepIsRefused() throws Exception {
		assumeTrue(Files.isDirectory(Path.of("/proc/self/fd")), "lists its open files on Linux only");
		final Path stepDir = Files.createDirectory(log.resolve("X"));
		final Path performance = Files.writeString(stepDir.resolve("integrity.performance.csv"), "earlier\n");

		try (FileChannel appender = FileChannel.open(performance, StandardOpenOption.APPEND)) {
			final var failure = assertThrows(StepIdCheckException.class,
							() -> EarlierRunFiles.moveAside("X", List.of(performance), NOW));

			assertNotMoved(failure, performance);
			assertTrue(appender.isOpen());
		}
	}

	@Test
	void theRefusalNamesTheFileThatIsOpenNotTheFirstFile() throws Exception {
		assumeTrue(Files.isDirectory(Path.of("/proc/self/fd")), "lists its open files on Linux only");
		final Path stepDir = Files.createDirectory(log.resolve("X"));
		final Path lifecycle = Files.writeString(stepDir.resolve("operation.lifecycle.csv"), "earlier\n");
		final Path performance = Files.writeString(stepDir.resolve("integrity.performance.csv"), "earlier\n");

		try (FileChannel appender = FileChannel.open(performance, StandardOpenOption.APPEND)) {
			final var failure = new StepIdCheckException[1];
			final List<String> logged = errors(() -> failure[0] = assertThrows(StepIdCheckException.class,
							() -> EarlierRunFiles.moveAside("X", List.of(lifecycle, performance), NOW)));

			assertNotMoved(failure[0], lifecycle);
			final String cause = failure[0].getCause().getMessage();
			assertTrue(cause.contains(performance.toRealPath().toString()), cause);
			assertFalse(cause.contains(lifecycle.getFileName().toString()), cause);
			assertEquals(List.of("X: could not move aside " + performance.toRealPath() + ", which an earlier run left"), logged);
		}
	}

	/** The first lines of the "could not move aside" errors {@code action} logs. */
	private static List<String> errors(final Runnable action) throws Exception {
		final List<String> lines = Collections.synchronizedList(new ArrayList<>());
		final String end = "earlier run files end " + UUID.randomUUID();
		final var ended = new CountDownLatch(1);
		final var appender = new AbstractAppender("earlierRunFiles-" + end, null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final String message = event.getMessage().getFormattedMessage();
				if (message.equals(end)) {
					ended.countDown();
				} else if (message.contains("could not move aside")) {
					lines.add(message.lines().findFirst().orElseThrow());
				}
			}
		};
		appender.start();
		final var logger = (org.apache.logging.log4j.core.Logger) LoggerContext.getContext(false)
						.getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			action.run();
			Loggers.ERR.info(end);
			assertTrue(ended.await(10, TimeUnit.SECONDS), "the end marker never arrived");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		synchronized (lines) {
			return List.copyOf(lines);
		}
	}

	@Test
	void outsideLinuxAFreshProcessMovesItsIntegrityLogsAside() throws Exception {
		final String stepId = "fresh-" + UUID.randomUUID();
		final Path stepDir = Files.createDirectory(log.resolve(stepId));
		final Path performance = Files.writeString(stepDir.resolve("integrity.performance.csv"), "earlier\n");

		final Path moved = EarlierRunFiles.moveAside(stepId, List.of(performance), NOW, log.resolve("no-proc-fd"));

		assertEquals("earlier\n", Files.readString(moved.resolve("integrity.performance.csv")));
		assertFalse(Files.exists(performance));
	}

	@Test
	void outsideLinuxAnIntegrityLogOfAStepThatStartedHereIsRefusedAndALifecycleFileStillMoves() throws Exception {
		final String stepId = "started-here-" + UUID.randomUUID();
		EarlierRunFiles.noteAppenderStepForTest(stepId);
		final Path stepDir = Files.createDirectory(log.resolve(stepId));
		final Path lifecycle = Files.writeString(stepDir.resolve("operation.lifecycle.csv"), "earlier\n");
		final Path performance = Files.writeString(stepDir.resolve("integrity.performance.csv"), "earlier\n");
		final Path noOpenFiles = log.resolve("no-proc-fd");

		final var failure = assertThrows(StepIdCheckException.class,
						() -> EarlierRunFiles.moveAside(stepId, List.of(lifecycle, performance), NOW, noOpenFiles));

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getCause().getMessage().contains(performance.toString()), failure.getCause().getMessage());
		assertEquals("earlier\n", Files.readString(lifecycle));
		assertEquals("earlier\n", Files.readString(performance));
		assertEquals(List.of(stepId), names(log));

		final Path moved = EarlierRunFiles.moveAside(stepId, List.of(lifecycle), NOW, noOpenFiles);
		assertEquals("earlier\n", Files.readString(moved.resolve("operation.lifecycle.csv")));
	}

	@Test
	void aReadOnlyStepFolderRefusesTheStepAndKeepsTheFileInPlace() throws Exception {
		assumeFalse("root".equals(System.getProperty("user.name")), "root writes a read-only folder");
		final Path stepDir = Files.createDirectory(log.resolve("X"));
		final Path lifecycle = Files.writeString(stepDir.resolve("operation.lifecycle.csv"), "earlier\n");
		Files.setPosixFilePermissions(stepDir, PosixFilePermissions.fromString("r-xr-xr-x"));
		try {
			final var failure = assertThrows(StepIdCheckException.class,
							() -> EarlierRunFiles.moveAside("X", List.of(lifecycle), NOW));

			assertNotMoved(failure, lifecycle);
			assertEquals(List.of("X"), names(log), "the empty new folder was removed");
		} finally {
			Files.setPosixFilePermissions(stepDir, PosixFilePermissions.fromString("rwxr-xr-x"));
		}
	}

	@Test
	void aReadOnlyLogFolderRefusesTheStepAndKeepsTheFileInPlace() throws Exception {
		assumeFalse("root".equals(System.getProperty("user.name")), "root writes a read-only folder");
		final Path stepDir = Files.createDirectory(log.resolve("X"));
		final Path lifecycle = Files.writeString(stepDir.resolve("operation.lifecycle.csv"), "earlier\n");
		Files.setPosixFilePermissions(log, PosixFilePermissions.fromString("r-xr-xr-x"));
		try {
			final var failure = assertThrows(StepIdCheckException.class,
							() -> EarlierRunFiles.moveAside("X", List.of(lifecycle), NOW));

			assertNotMoved(failure, lifecycle);
		} finally {
			Files.setPosixFilePermissions(log, PosixFilePermissions.fromString("rwxr-xr-x"));
		}
	}

	private static void assertNotMoved(final StepIdCheckException failure, final Path file) throws Exception {
		assertEquals("step id \"X\" was used by an earlier run in this worker's log directory, and its files could not "
						+ "be moved aside; see the engine log", failure.getMessage());
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("earlier\n", Files.readString(file));
	}

	private static List<String> names(final Path dir) throws Exception {
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(file -> file.getFileName().toString()).sorted().toList();
		}
	}
}
