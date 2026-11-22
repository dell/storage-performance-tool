package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.integrity.FailurePreservingCleanup;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;

/** Safely collects and merges one role-applicable logger-backed CSV from every step slice. */
public final class CsvLoggerArtifactAggregator implements AutoCloseable {

	private final String loadStepId;
	private final String artifactName;
	private final List<String> expectedHeader;
	private final List<FileManager> fileManagers;
	private final List<String> sourceNames;
	private final Path canonicalPath;

	public CsvLoggerArtifactAggregator(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final String loggerName,
					final String artifactName,
					final String expectedHeader)
					throws IOException {
		this.loadStepId = loadStepId;
		this.artifactName = artifactName;
		this.expectedHeader = CSVFormat.RFC4180.parse(new java.io.StringReader(expectedHeader + "\r\n"))
						.getRecords().get(0).toList();
		this.fileManagers = List.copyOf(fileManagers);
		this.sourceNames = new ArrayList<>(fileManagers.size());
		for (final FileManager fileManager : fileManagers) {
			sourceNames.add(fileManager.logFileName(loggerName, loadStepId));
		}
		canonicalPath = Path.of(sourceNames.get(0));
		if (!artifactName.equals(canonicalPath.getFileName().toString())) {
			throw new IOException("logger path " + canonicalPath + " does not match artifact " + artifactName);
		}
	}

	/**
	 * The files {@link #close()} and the workers refuse to replace for this artifact of step
	 * {@code loadStepId}, on the Entry Node: its canonical file, the node source of each of
	 * {@code sliceCount} slices, and any other node source in its directory, which an earlier step
	 * with more slices left when it could not publish the canonical file. A step refuses its id at
	 * start when any of them exists.
	 *
	 * @param entry the Entry Node's file manager
	 */
	static List<Path> publishedPaths(
					final FileManager entry, final String loggerName, final String loadStepId, final int sliceCount)
					throws IOException {
		final Path canonical = Path.of(entry.logFileName(loggerName, loadStepId));
		final List<Path> paths = new ArrayList<>(1 + sliceCount);
		paths.add(canonical);
		for (int i = 0; i < sliceCount; i++) {
			paths.add(nodeSourcePath(canonical, i));
		}
		final Path directory = canonical.toAbsolutePath().getParent();
		if (directory != null && Files.isDirectory(directory)) {
			final Pattern nodeSource = Pattern.compile(Pattern.quote(stem(canonical)) + "\\.node-[0-9]{3,}\\.csv");
			try (Stream<Path> files = Files.list(directory)) {
				files.map(file -> file.getFileName().toString())
								.filter(name -> nodeSource.matcher(name).matches())
								.sorted()
								.map(canonical::resolveSibling)
								.filter(path -> !paths.contains(path))
								.forEachOrdered(paths::add);
			}
		}
		return paths;
	}

	@Override
	public void close() {
		publish(sourceNames.size());
	}

	/**
	 * Publishes the artifact of a step whose start ended as a stop, from its first
	 * {@code startedSlices} slices: those whose start returned. The others wrote nothing to publish.
	 * With none, publishes nothing. A missing source of a started slice is still a failure.
	 */
	void closeAfterStoppedStart(final int startedSlices) {
		if (startedSlices < 0 || startedSlices > sourceNames.size()) {
			throw new IllegalArgumentException(
							"started slices " + startedSlices + " not in 0.." + sourceNames.size());
		}
		if (startedSlices > 0) {
			publish(startedSlices);
		}
	}

	private void publish(final int sliceCount) {
		try {
			collectAndPublish(sliceCount);
		} catch (final IntegrityTerminalException e) {
			throw e;
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			throw terminal(Category.AGGREGATION, "failed to aggregate " + artifactName, e);
		}
	}

	private void collectAndPublish(final int sliceCount) throws IOException {
		LogUtil.flushAll();
		final Path parent = canonicalPath.toAbsolutePath().getParent();
		Files.createDirectories(parent);
		for (int i = 0; i < sliceCount; i++) {
			final Path nodePath = nodeSourcePath(canonicalPath, i);
			if (Files.exists(nodePath)) {
				throw terminal(Category.PUBLICATION, "stale node source exists: " + nodePath, null);
			}
		}
		// Each slice on its own, so a missing one costs only its own file.
		final List<Path> nodeSources = new ArrayList<>(sliceCount);
		final List<String> missingNodes = new ArrayList<>();
		final List<Exception> failures = new ArrayList<>();
		boolean entryMissing = false;
		for (int i = 0; i < sliceCount; i++) {
			final Path nodePath = nodeSourcePath(canonicalPath, i);
			try {
				if (i == 0) {
					if (!Files.isRegularFile(canonicalPath)) {
						entryMissing = true;
						throw new IOException("entry-node logger artifact is missing: " + canonicalPath);
					}
					IntegrityManifestCompletion.atomicMove(canonicalPath, nodePath);
				} else {
					copyRemoteSource(fileManagers.get(i), sourceNames.get(i), nodePath);
				}
				nodeSources.add(nodePath);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				final String node = String.format(Locale.ROOT, "node-%03d", i);
				missingNodes.add(node);
				if (sliceCount > 1) {
					Loggers.ERR.warn("{}: failed to collect {} from {}: {}", loadStepId, artifactName, node, e.getMessage());
				}
				if (!(entryMissing && i == 0)) {
					failures.add(e);
				}
			}
		}
		if (!missingNodes.isEmpty()) {
			throw missingSlices(sliceCount, nodeSources.size(), missingNodes, entryMissing, failures);
		}

		final Path staging = Files.createTempFile(parent, "." + artifactName, ".staging");
		FailurePreservingCleanup.always(() -> {
			try (final CSVPrinter printer = new CSVPrinter(
							Files.newBufferedWriter(staging, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING),
							IntegrityCsvFormat.RFC4180_LF)) {
				printer.printRecord(expectedHeader);
				for (final Path source : nodeSources) {
					copyRecords(source, printer);
				}
			}
			IntegrityManifestCompletion.atomicMove(staging, canonicalPath);
			return null;
		}, () -> Files.deleteIfExists(staging));
	}

	/**
	 * The one failure of a close that could not collect every slice: it names each missing node. A
	 * missing entry file has no cause, as before; otherwise the first failure is the cause and the
	 * later ones are suppressed.
	 */
	private IntegrityTerminalException missingSlices(
					final int sliceCount,
					final int collected,
					final List<String> missingNodes,
					final boolean entryMissing,
					final List<Exception> failures) {
		final String prefix = entryMissing
						? "entry-node logger artifact is missing: " + canonicalPath
						: "failed to aggregate " + artifactName;
		final String message = sliceCount > 1
						? prefix + (entryMissing ? "; " : ": ") + "not published, missing " + String.join(", ", missingNodes)
										+ "; collected the other " + collected + " of " + sliceCount
						: prefix;
		final Throwable cause = entryMissing || failures.isEmpty() ? null : failures.get(0);
		final IntegrityTerminalException failure = terminal(Category.AGGREGATION, message, cause);
		for (final Exception other : failures) {
			if (other != cause) {
				failure.addSuppressed(other);
			}
		}
		return failure;
	}

	private void copyRecords(final Path source, final CSVPrinter printer) throws IOException {
		try (Reader reader = Files.newBufferedReader(source, StandardCharsets.UTF_8);
						CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final var iterator = parser.iterator();
			if (!iterator.hasNext() || !expectedHeader.equals(iterator.next().toList())) {
				throw new IOException("logger artifact has a noncanonical header: " + source);
			}
			while (iterator.hasNext()) {
				final var record = iterator.next();
				if (record.size() != expectedHeader.size()) {
					throw new IOException("logger artifact has an invalid record: " + source);
				}
				if (expectedHeader.equals(record.toList())) {
					throw new IOException("logger artifact contains a duplicate header: " + source);
				}
				printer.printRecord(record);
			}
		}
	}

	/**
	 * Fetches a remote node's file into a temporary file beside {@code target}, created at the first
	 * chunk, and moves it to {@code target} only when complete, so a failed fetch leaves no file.
	 */
	private static void copyRemoteSource(
					final FileManager fileManager, final String remoteName, final Path target) throws IOException {
		final Path[] staging = new Path[1];
		FailurePreservingCleanup.always(() -> {
			long offset = 0;
			try {
				while (true) {
					final byte[] bytes = fileManager.readFromFile(remoteName, offset);
					if (bytes.length == 0) {
						break;
					}
					if (staging[0] == null) {
						staging[0] = Files.createTempFile(
										target.toAbsolutePath().getParent(), "." + target.getFileName() + ".", ".fetching");
					}
					Files.write(staging[0], bytes, StandardOpenOption.APPEND);
					offset += bytes.length;
				}
			} catch (final LostNodeFileManager.NotCollected notRead) {
				throw new IOException("remote logger artifact was not collected, as the entry lost its node's step slice: " + remoteName, notRead);
			} catch (final EOFException expected) {
				// FileManager signals normal EOF with this exception.
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				throw new IOException("failed to fetch remote logger artifact " + remoteName, e);
			}
			if (staging[0] == null) {
				throw new IOException("remote logger artifact was empty or missing: " + remoteName);
			}
			IntegrityManifestCompletion.atomicMove(staging[0], target);
			return null;
		}, () -> {
			if (staging[0] != null) {
				Files.deleteIfExists(staging[0]);
			}
		});
	}

	static Path nodeSourcePath(final Path artifact, final int nodeIndex) {
		return artifact.resolveSibling(String.format(Locale.ROOT, "%s.node-%03d.csv", stem(artifact), nodeIndex));
	}

	private static String stem(final Path artifact) {
		final String name = artifact.getFileName().toString();
		final int suffix = name.toLowerCase(Locale.ROOT).lastIndexOf(".csv");
		return suffix >= 0 ? name.substring(0, suffix) : name;
	}

	private IntegrityTerminalException terminal(
					final Category category, final String message, final Throwable cause) {
		return new IntegrityTerminalException(category, loadStepId, message, cause);
	}
}
