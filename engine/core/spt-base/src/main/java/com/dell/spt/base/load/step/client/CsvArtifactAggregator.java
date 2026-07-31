package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/** CSV-aware distributed collector and atomic publisher for canonical integrity manifests. */
public final class CsvArtifactAggregator implements AutoCloseable {

	private record ManifestRecord(String bucket, String key, long size, String versionId) {
        private String identity() {
            return bucket + '\u0000' + key + '\u0000' + versionId;
        }
    }

	private final String loadStepId;
	private final long runId;
	private final Path manifestPath;
	private final List<FileManager> fileManagers;
	private final List<String> sourceNames;
	private final long selectionLimit;

	public CsvArtifactAggregator(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<Config> configSlices,
					final String itemOutputFile,
					final long runId,
					final long selectionLimit) {
		this.loadStepId = loadStepId;
		this.runId = runId;
		this.manifestPath = Path.of(itemOutputFile);
		this.fileManagers = List.copyOf(fileManagers);
		this.selectionLimit = Math.max(0, selectionLimit);
		this.sourceNames = new ArrayList<>(fileManagers.size());
		for (int i = 0; i < fileManagers.size(); i++) {
			final FileManager fileManager = fileManagers.get(i);
			if (fileManager == null) {
				throw terminal(Category.AGGREGATION, "file manager for slice " + i + " is unavailable", null);
			}
			if (i == 0) {
				if (fileManager instanceof FileManagerService) {
					throw new AssertionError("entry-node file manager must be local");
				}
				sourceNames.add(itemOutputFile);
			} else {
				if (!(fileManager instanceof FileManagerService)) {
					throw new AssertionError("worker file manager must be remote");
				}
				try {
					final String remoteName = fileManager.newTmpFileName();
					configSlices.get(i).val("item-output-file", remoteName);
					sourceNames.add(remoteName);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					throw terminal(Category.AGGREGATION, "failed to allocate manifest source for slice " + i, e);
				}
			}
		}
	}

	@Override
	public void close() {
		try {
			collectAndPublish();
		} catch (final IntegrityTerminalException e) {
			throw e;
		} catch (final Exception e) {
			throw terminal(Category.AGGREGATION, "failed to aggregate " + manifestPath.getFileName(), e);
		}
	}

	private void collectAndPublish() throws IOException {
		final Path parent = manifestPath.toAbsolutePath().getParent();
		Files.createDirectories(parent);
		final Path marker = IntegrityManifestCompletion.completionPath(manifestPath);
		if (Files.exists(marker)) {
			throw terminal(Category.PUBLICATION, "stale completion record exists: " + marker, null);
		}

		final List<Path> nodeSources = new ArrayList<>(sourceNames.size());
		for (int i = 0; i < sourceNames.size(); i++) {
			final Path nodePath = nodeSourcePath(manifestPath, i);
			if (Files.exists(nodePath)) {
				throw terminal(Category.PUBLICATION, "stale node source exists: " + nodePath, null);
			}
			if (i == 0) {
				if (!Files.isRegularFile(manifestPath)) {
					throw terminal(Category.AGGREGATION, "entry-node manifest source is missing", null);
				}
				IntegrityManifestCompletion.atomicMove(manifestPath, nodePath);
			} else {
				copyRemoteSource(fileManagers.get(i), sourceNames.get(i), nodePath);
			}
			nodeSources.add(nodePath);
		}

		long sourceCount = 0;
		final Map<String, ManifestRecord> unique = new LinkedHashMap<>();
		for (final Path source : nodeSources) {
			final List<ManifestRecord> records = readSource(source);
			sourceCount += records.size();
			for (final ManifestRecord record : records) {
				unique.putIfAbsent(record.identity(), record);
			}
		}

		final List<ManifestRecord> selected = new ArrayList<>(unique.values());
		if ("verify-input.csv".equals(manifestPath.getFileName().toString())) {
			selected.sort(Comparator.comparing(ManifestRecord::bucket)
							.thenComparing(ManifestRecord::key)
							.thenComparing(ManifestRecord::versionId));
			if (selectionLimit > 0 && selected.size() > selectionLimit) {
				selected.subList((int) selectionLimit, selected.size()).clear();
			}
		}

		final Path staging = Files.createTempFile(parent, "." + manifestPath.getFileName(), ".staging");
		try (final CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(staging, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING),
						CSVFormat.RFC4180)) {
			printer.printRecord(IntegrityManifestItemInput.HEADER);
			for (final ManifestRecord record : selected) {
				printer.printRecord(record.bucket, record.key, record.size, record.versionId);
			}
		}
		IntegrityManifestCompletion.atomicMove(staging, manifestPath);
		final IntegrityManifestCompletion completion = IntegrityManifestCompletion.create(
						manifestPath,
						runId,
						IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
						loadStepId,
						sourceCount,
						unique.size(),
						selected.size());
		try {
			completion.publish(manifestPath);
		} catch (final IOException e) {
			throw terminal(Category.PUBLICATION, "failed to publish completion record for " + manifestPath, e);
		}
		if ("written.csv".equals(manifestPath.getFileName().toString()) && selected.isEmpty()) {
			throw terminal(Category.EXECUTION, "write verification produced zero successful objects", null);
		}
	}

	private void copyRemoteSource(
					final FileManager fileManager, final String remoteName, final Path target) throws IOException {
		long offset = 0;
		boolean sawData = false;
		try {
			while (true) {
				final byte[] bytes = fileManager.readFromFile(remoteName, offset);
				if (bytes.length == 0) {
					break;
				}
				if (sawData) {
					Files.write(target, bytes, StandardOpenOption.APPEND);
				} else {
					Files.write(
									target,
									bytes,
									StandardOpenOption.CREATE_NEW,
									StandardOpenOption.WRITE);
					sawData = true;
				}
				offset += bytes.length;
			}
		} catch (final EOFException expected) {
			// The file-manager contract signals normal EOF with this exception.
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			throw new IOException("failed to fetch remote manifest source " + remoteName, e);
		}
		if (!sawData && !Files.exists(target)) {
			throw new IOException("remote manifest source was empty or missing: " + remoteName);
		}
	}

	private static List<ManifestRecord> readSource(final Path source) throws IOException {
		try (Reader reader = Files.newBufferedReader(source, StandardCharsets.UTF_8);
						CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final List<CSVRecord> csv = parser.getRecords();
			if (csv.isEmpty() || !IntegrityManifestItemInput.HEADER.equals(csv.get(0).toList())) {
				throw new IOException("manifest source has a noncanonical header: " + source);
			}
			final List<ManifestRecord> records = new ArrayList<>(csv.size() - 1);
			for (int i = 1; i < csv.size(); i++) {
				final CSVRecord record = csv.get(i);
				if (record.size() != 4 || record.get(0).isEmpty() || record.get(1).isEmpty()) {
					throw new IOException("invalid manifest record " + record.getRecordNumber() + " in " + source);
				}
				final long size;
				try {
					size = Long.parseLong(record.get(2));
				} catch (final NumberFormatException e) {
					throw new IOException("invalid manifest size at record " + record.getRecordNumber(), e);
				}
				if (size < 0) {
					throw new IOException("negative manifest size at record " + record.getRecordNumber());
				}
				records.add(new ManifestRecord(record.get(0), record.get(1), size, record.get(3)));
			}
			return records;
		}
	}

	static Path nodeSourcePath(final Path manifest, final int nodeIndex) {
		final String name = manifest.getFileName().toString();
		final int suffix = name.toLowerCase(Locale.ROOT).lastIndexOf(".csv");
		final String stem = suffix >= 0 ? name.substring(0, suffix) : name;
		return manifest.resolveSibling(String.format(Locale.ROOT, "%s.node-%03d.csv", stem, nodeIndex));
	}

	private IntegrityTerminalException terminal(
					final Category category, final String message, final Throwable cause) {
		return new IntegrityTerminalException(category, loadStepId, message, cause);
	}
}
