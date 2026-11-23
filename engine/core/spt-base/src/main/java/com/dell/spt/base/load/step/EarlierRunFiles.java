package com.dell.spt.base.load.step;

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
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Moves aside, on a worker engine, the {@link StepIdFiles} an earlier run left under a step's id, so
 * that the step runs as on a clean folder and the earlier run's files are kept as they were. They
 * go to a new folder next to the step's, {@code <step id>.earlier-<UTC time>}, that no step reads or
 * writes. Only those files move: the step's own logs of this run, which the worker opened before
 * the slice starts, stay in the step's folder. Nothing is replaced or deleted.
 */
public final class EarlierRunFiles {

	static final String FOLDER_INFIX = ".earlier-";
	private static final DateTimeFormatter FOLDER_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
	private static final int FOLDER_ATTEMPTS = 1_000;
	private static final Path OPEN_FILES = Path.of("/proc/self/fd");

	private EarlierRunFiles() {}

	/**
	 * Moves {@code files}, which an earlier run left in the folder of step {@code stepId}, into a new
	 * folder next to it, and logs one warning that says where they went.
	 *
	 * @param files existing files, all in the step's folder
	 * @return the new folder
	 * @throws StepIdCheckException when a file is still open in this engine, or the new folder or a
	 *     move fails; files moved before the failure stay in the new folder
	 */
	public static Path moveAside(final String stepId, final List<Path> files, final Instant now) {
		final Path stepDir = files.get(0).toAbsolutePath().getParent();
		final Path firstFile = files.get(0);
		try {
			final var open = openHere(files);
			if (!open.isEmpty()) {
				throw new IOException("this engine still has " + open + " open, from an earlier step with this id");
			}
		} catch (final IOException unknown) {
			throw StepIdCheckException.notMovedAside(stepId, firstFile.toString(), unknown);
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
						"{}: this worker's folder for the step holds {} from an earlier run; moved {} aside to {}, and the step runs",
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
	 * open files (outside Linux), none is taken to be open.
	 */
	static List<Path> openHere(final List<Path> files) throws IOException {
		if (!Files.isDirectory(OPEN_FILES)) {
			return List.of();
		}
		final Set<Path> wanted = new HashSet<>();
		for (final Path file : files) {
			wanted.add(file.toRealPath());
		}
		final Set<Path> open = new HashSet<>();
		try (Stream<Path> fds = Files.list(OPEN_FILES)) {
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
}
