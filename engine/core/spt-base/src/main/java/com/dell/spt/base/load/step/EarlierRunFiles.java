package com.dell.spt.base.load.step;

import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Moves aside, on a worker engine, the {@link StepIdFiles} an earlier run left under a step's id, so
 * that the step runs as on a clean folder and the earlier run's files are kept as they were. The
 * Entry Node asks for it before it creates or starts any slice of the step (see {@link
 * LoadStepManagerService#prepareStepIdFiles}), so a worker that cannot move them refuses the step
 * before any load on any node. The files go to a new folder next to the step's, {@code <step
 * id>.earlier-<UTC time>}, that no step reads or writes. Only those files move: the step's own logs
 * stay in the step's folder. None of the moved files is replaced or deleted.
 */
public final class EarlierRunFiles {

	static final String FOLDER_INFIX = ".earlier-";
	private static final DateTimeFormatter FOLDER_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
	private static final int FOLDER_ATTEMPTS = 1_000;
	private static final Path OPEN_FILES = Path.of("/proc/self/fd");
	/** The step files a log appender writes, and may still hold open after its step ended. */
	private static final Set<String> APPENDER_FILE_NAMES = Set.of(
					IntegrityCsvArtifacts.FAILURES_FILE_NAME,
					IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME,
					IntegrityCsvArtifacts.MULTIPART_LIFECYCLE_FILE_NAME);
	/**
	 * Where this process cannot list its open files: the ids of the steps that started a slice here
	 * with an appender-written file, whose appender may still hold it open.
	 */
	private static final Set<String> APPENDER_STEP_IDS = ConcurrentHashMap.newKeySet();

	private EarlierRunFiles() {}

	/**
	 * Prepares a worker's files for step {@code stepId} of run {@code runId}: when it is the first
	 * step of its id in that run, by {@code runStepIds}, moves aside the files of {@code loggerNames}
	 * an earlier run left under the id.
	 *
	 * @param loggerNames the loggers of the {@link StepIdFiles} the step's slice here publishes
	 * @return whether the step is the first of its id in its run here; when not, nothing is moved
	 * @throws StepIdCheckException when a file cannot be checked or moved aside
	 */
	public static boolean prepare(
					final RunStepIds runStepIds,
					final long runId,
					final String stepId,
					final List<String> loggerNames,
					final Instant now) {
		if (!runStepIds.firstInRun(runId, stepId)) {
			return false;
		}
		final var paths = new ArrayList<Path>(loggerNames.size());
		for (final var loggerName : loggerNames) {
			try {
				if (!StepIdFiles.LOGGER_NAMES.contains(loggerName)) {
					throw new IOException("not a step file this engine knows");
				}
				paths.add(Path.of(FileManager.INSTANCE.logFileName(loggerName, stepId)));
			} catch (final IOException | RuntimeException unknown) {
				throw StepIdCheckException.here(stepId, "the " + loggerName + " file", unknown);
			}
		}
		final var earlier = StepIdInUseException.existing(stepId, paths);
		if (!earlier.isEmpty()) {
			moveAside(stepId, earlier, now);
		}
		return true;
	}

	/**
	 * Notes, where this process cannot list its open files, that a slice of step {@code stepId}
	 * starts here with the files of {@code loggerNames}: an appender may hold one of them open later.
	 */
	public static void noteSliceStart(final String stepId, final List<String> loggerNames) {
		if (!Files.isDirectory(OPEN_FILES) && loggerNames.stream().anyMatch(StepIdFiles.APPENDER_LOGGER_NAMES::contains)) {
			APPENDER_STEP_IDS.add(stepId);
		}
	}

	/**
	 * Moves {@code files}, which an earlier run left in the folder of step {@code stepId}, into a new
	 * folder next to it, and logs one warning that says where they went.
	 *
	 * @param files existing files, at least one, all in the step's folder
	 * @return the new folder
	 * @throws StepIdCheckException when a file may still be open in this engine, or the new folder or
	 *     a move fails; files moved before the failure stay in the new folder
	 */
	public static Path moveAside(final String stepId, final List<Path> files, final Instant now) {
		return moveAside(stepId, files, now, OPEN_FILES);
	}

	static Path moveAside(final String stepId, final List<Path> files, final Instant now, final Path openFiles) {
		if (files.isEmpty()) {
			throw new IllegalArgumentException(stepId + ": no file to move aside");
		}
		final Path stepDir = files.get(0).toAbsolutePath().normalize().getParent();
		try {
			final var open = openHere(stepId, files, openFiles);
			if (!open.isEmpty()) {
				throw StepIdCheckException.notMovedAside(stepId, open.get(0).toString(), new IOException(
								"this engine still has, or may still have, " + open + " open, from an earlier step with this id"));
			}
		} catch (final IOException unknown) {
			throw StepIdCheckException.notMovedAside(stepId, files.get(0).toString(), unknown);
		}
		final Path earlierDir;
		try {
			earlierDir = newFolder(stepDir, now);
		} catch (final IOException failed) {
			throw StepIdCheckException.notMovedAside(stepId, stepDir.toString(), failed);
		}
		final var moved = new ArrayList<String>(files.size());
		for (final Path file : files) {
			final Path target = earlierDir.resolve(file.getFileName());
			try {
				Files.move(file, target);
			} catch (final IOException failed) {
				if (moved.isEmpty()) {
					try {
						Files.deleteIfExists(earlierDir);
					} catch (final IOException ignored) {
						// An empty folder of its own is left; the step is refused all the same.
					}
				}
				throw StepIdCheckException.notMovedAside(stepId, file.toString(), failed);
			}
			moved.add(file.getFileName().toString());
		}
		Loggers.ERR.warn(
						"{}: this worker's folder for the step holds {} from an earlier run; moved {} aside to {} before the step starts",
						stepId, String.join(", ", moved), moved.size() == 1 ? "it" : "them", earlierDir);
		return earlierDir;
	}

	/** A new folder next to {@code stepDir}, named for {@code now}, with a number when that name is taken. */
	static Path newFolder(final Path stepDir, final Instant now) throws IOException {
		final String name = stepDir.getFileName() + FOLDER_INFIX + FOLDER_TIME.format(now);
		for (var i = 1; i <= FOLDER_ATTEMPTS; i++) {
			final Path candidate = stepDir.resolveSibling(i == 1 ? name : name + "-" + i);
			try {
				return Files.createDirectory(candidate);
			} catch (final FileAlreadyExistsException taken) {
				// The next number.
			}
		}
		throw new FileAlreadyExistsException(stepDir.resolveSibling(name) + " and " + (FOLDER_ATTEMPTS - 1) + " more");
	}

	/**
	 * The {@code files} this process holds open, for example a log appender of an earlier step with
	 * the same id: a file moved under it would take this run's rows. Where the process cannot list its
	 * open files ({@code openFiles} absent, outside Linux), its appender-written files are taken to be
	 * open when a slice of the step started here before; a fresh process holds none.
	 */
	static List<Path> openHere(final String stepId, final List<Path> files, final Path openFiles) throws IOException {
		if (!Files.isDirectory(openFiles)) {
			if (!APPENDER_STEP_IDS.contains(stepId)) {
				return List.of();
			}
			return files.stream().filter(file -> APPENDER_FILE_NAMES.contains(file.getFileName().toString())).toList();
		}
		final Set<Path> wanted = new HashSet<>();
		for (final Path file : files) {
			wanted.add(file.toRealPath());
		}
		final Set<Path> open = new LinkedHashSet<>();
		try (Stream<Path> fds = Files.list(openFiles)) {
			for (final Path fd : (Iterable<Path>) fds::iterator) {
				final Path target;
				try {
					target = Files.readSymbolicLink(fd);
				} catch (final IOException closed) {
					// Closed since it was listed, the listing's own among them.
					continue;
				}
				if (wanted.contains(target)) {
					open.add(target);
				}
			}
		}
		return List.copyOf(open);
	}

	/** For a test of the rule outside Linux: as {@link #noteSliceStart} there. */
	static void noteAppenderStepForTest(final String stepId) {
		APPENDER_STEP_IDS.add(stepId);
	}
}
