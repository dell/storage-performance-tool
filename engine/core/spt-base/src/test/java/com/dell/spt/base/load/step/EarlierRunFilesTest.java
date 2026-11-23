package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.List;
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
