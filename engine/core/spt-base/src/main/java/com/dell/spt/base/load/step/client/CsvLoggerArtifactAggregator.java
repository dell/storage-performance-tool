package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.LogUtil;
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

	@Override
	public void close() {
		try {
			collectAndPublish();
		} catch (final IntegrityTerminalException e) {
			throw e;
		} catch (final Exception e) {
			throw terminal(Category.AGGREGATION, "failed to aggregate " + artifactName, e);
		}
	}

	private void collectAndPublish() throws IOException {
		LogUtil.flushAll();
		final Path parent = canonicalPath.toAbsolutePath().getParent();
		Files.createDirectories(parent);
		final List<Path> nodeSources = new ArrayList<>(sourceNames.size());
		for (int i = 0; i < sourceNames.size(); i++) {
			final Path nodePath = nodeSourcePath(canonicalPath, i);
			if (Files.exists(nodePath)) {
				throw terminal(Category.PUBLICATION, "stale node source exists: " + nodePath, null);
			}
			if (i == 0) {
				if (!Files.isRegularFile(canonicalPath)) {
					throw terminal(Category.AGGREGATION, "entry-node logger artifact is missing: " + canonicalPath, null);
				}
				IntegrityManifestCompletion.atomicMove(canonicalPath, nodePath);
			} else {
				copyRemoteSource(fileManagers.get(i), sourceNames.get(i), nodePath);
			}
			nodeSources.add(nodePath);
		}

		final Path staging = Files.createTempFile(parent, "." + artifactName, ".staging");
		try (final CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(staging, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING),
						IntegrityCsvFormat.RFC4180_LF)) {
			printer.printRecord(expectedHeader);
			for (final Path source : nodeSources) {
				copyRecords(source, printer);
			}
		}
		IntegrityManifestCompletion.atomicMove(staging, canonicalPath);
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

	private static void copyRemoteSource(
					final FileManager fileManager, final String remoteName, final Path target) throws IOException {
		long offset = 0;
		boolean sawData = false;
		try {
			while (true) {
				final byte[] bytes = fileManager.readFromFile(remoteName, offset);
				if (bytes.length == 0) {
					break;
				}
				Files.write(
								target,
								bytes,
								sawData
												? new StandardOpenOption[]{StandardOpenOption.APPEND
												}
												: new StandardOpenOption[]{StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE
												});
				sawData = true;
				offset += bytes.length;
			}
		} catch (final EOFException expected) {
			// FileManager signals normal EOF with this exception.
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			throw new IOException("failed to fetch remote logger artifact " + remoteName, e);
		}
		if (!sawData) {
			throw new IOException("remote logger artifact was empty or missing: " + remoteName);
		}
	}

	static Path nodeSourcePath(final Path artifact, final int nodeIndex) {
		final String name = artifact.getFileName().toString();
		final int suffix = name.toLowerCase(Locale.ROOT).lastIndexOf(".csv");
		final String stem = suffix >= 0 ? name.substring(0, suffix) : name;
		return artifact.resolveSibling(String.format(Locale.ROOT, "%s.node-%03d.csv", stem, nodeIndex));
	}

	private IntegrityTerminalException terminal(
					final Category category, final String message, final Throwable cause) {
		return new IntegrityTerminalException(category, loadStepId, message, cause);
	}
}
