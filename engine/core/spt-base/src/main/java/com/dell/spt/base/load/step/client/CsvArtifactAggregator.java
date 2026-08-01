package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityCsvFormat;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.item.op.OpType;
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
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Locale;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/** CSV-aware distributed collector and atomic publisher for canonical integrity manifests. */
public final class CsvArtifactAggregator implements AutoCloseable {

	private record ManifestRecord(String bucket, String key, long size, String versionId) {}

	private record AggregationCounts(long source, long unique, long selected) {}

	static final int SORT_CHUNK_RECORDS = 64 * 1024;
	private static final int MERGE_FAN_IN = 64;
	private static final Comparator<ManifestRecord> RECORD_ORDER = Comparator
					.comparing(ManifestRecord::bucket)
					.thenComparing(ManifestRecord::key)
					.thenComparing(ManifestRecord::versionId);

	private final String loadStepId;
	private final long runId;
	private final Path manifestPath;
	private final List<FileManager> fileManagers;
	private final List<String> sourceNames;
	private final long selectionLimit;
	private final OpType artifactOpType;

	public CsvArtifactAggregator(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<Config> configSlices,
					final String itemOutputFile,
					final long runId,
					final long selectionLimit) {
		this(
						loadStepId,
						fileManagers,
						configSlices,
						itemOutputFile,
						runId,
						selectionLimit,
						legacyArtifactOpType(itemOutputFile));
	}

	public CsvArtifactAggregator(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<Config> configSlices,
					final String itemOutputFile,
					final long runId,
					final long selectionLimit,
					final OpType artifactOpType) {
		this.loadStepId = loadStepId;
		this.runId = runId;
		this.manifestPath = Path.of(itemOutputFile);
		this.fileManagers = List.copyOf(fileManagers);
		this.selectionLimit = Math.max(0, selectionLimit);
		this.artifactOpType = artifactOpType;
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

	private static OpType legacyArtifactOpType(final String itemOutputFile) {
		final String name = Path.of(itemOutputFile).getFileName().toString();
		if ("verify-input.csv".equals(name)) {
			return OpType.LIST;
		}
		return "written.csv".equals(name) ? OpType.CREATE : OpType.READ;
	}

	private void collectAndPublish() throws IOException {
		final Path parent = manifestPath.toAbsolutePath().getParent();
		Files.createDirectories(parent);
		final Path marker = IntegrityManifestCompletion.completionPath(manifestPath);
		if (Files.exists(marker)) {
			throw terminal(Category.PUBLICATION, "stale completion record exists: " + marker, null);
		}

		final boolean discovery = OpType.LIST.equals(artifactOpType);
		final List<Path> nodeSources = new ArrayList<>(sourceNames.size());
		final List<Path> emissionCounts = new ArrayList<>(sourceNames.size());
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
			if (discovery) {
				final Path nodeCount = IntegrityManifestCompletion.emissionCountPath(nodePath);
				if (Files.exists(nodeCount)) {
					throw terminal(Category.PUBLICATION, "stale node emission count exists: " + nodeCount, null);
				}
				if (i == 0) {
					final Path sourceCount = IntegrityManifestCompletion.emissionCountPath(manifestPath);
					if (!Files.isRegularFile(sourceCount)) {
						throw terminal(Category.AGGREGATION, "entry-node LIST emission count is missing", null);
					}
					IntegrityManifestCompletion.atomicMove(sourceCount, nodeCount);
				} else {
					final String remoteCount = IntegrityManifestCompletion.emissionCountPath(
									Path.of(sourceNames.get(i))).toString();
					copyRemoteSource(fileManagers.get(i), remoteCount, nodeCount);
				}
				emissionCounts.add(nodeCount);
			}

		}

		final Path staging = Files.createTempFile(parent, "." + manifestPath.getFileName(), ".staging");
		final AggregationCounts counts;
		try {
			counts = aggregateBounded(nodeSources, staging, discovery ? selectionLimit : 0);
		} catch (final IOException | RuntimeException e) {
			try {
				Files.deleteIfExists(staging);
			} catch (final IOException cleanupFailure) {
				e.addSuppressed(cleanupFailure);
			}
			throw e;
		}
		long emittedCount = counts.source();
		if (discovery) {
			emittedCount = 0;
			for (final Path countPath : emissionCounts) {
				emittedCount = Math.addExact(emittedCount, readEmissionCount(countPath));
			}
			if (emittedCount != counts.source()) {
				Files.deleteIfExists(staging);
				throw terminal(
								Category.AGGREGATION,
								"LIST emitted " + emittedCount + " records but parsed "
												+ counts.source() + " manifest rows",
								null);
			}
		}
		IntegrityManifestCompletion.atomicMove(staging, manifestPath);
		final IntegrityManifestCompletion completion = IntegrityManifestCompletion.create(
						manifestPath,
						runId,
						IntegrityManifestCompletion.PRODUCER_ENGINE_STEP,
						loadStepId,
						counts.source(),
						counts.unique(),
						counts.selected());
		try {
			completion.publish(manifestPath);
		} catch (final IOException e) {
			throw terminal(Category.PUBLICATION, "failed to publish completion record for " + manifestPath, e);
		}
		if (OpType.CREATE.equals(artifactOpType) && counts.selected() == 0) {
			throw terminal(Category.EXECUTION, "write verification produced zero successful objects", null);
		}
	}

	private static long readEmissionCount(final Path path) throws IOException {
		final String value = Files.readString(path, StandardCharsets.US_ASCII).trim();
		try {
			final long count = Long.parseLong(value);
			if (count < 0) {
				throw new IOException("negative LIST emission count in " + path);
			}
			return count;
		} catch (final NumberFormatException e) {
			throw new IOException("invalid LIST emission count in " + path, e);
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

	private record MergeCounts(long unique, long selected) {}

	private static final class ChunkCursor implements AutoCloseable {
		private final Path path;
		private final CSVParser parser;
		private final Iterator<CSVRecord> records;
		private ManifestRecord current;

		private ChunkCursor(final Path path) throws IOException {
			this.path = path;
			final Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
			try {
				parser = CSVFormat.RFC4180.parse(reader);
			} catch (final RuntimeException e) {
				reader.close();
				throw e;
			}
			records = parser.iterator();
		}

		private boolean advance() throws IOException {
			if (!records.hasNext()) {
				current = null;
				return false;
			}
			current = parseRecord(records.next(), path);
			return true;
		}

		public void close() throws IOException {
			parser.close();
		}
	}

	private record MergeEntry(int source, ManifestRecord record) {}

	private static AggregationCounts aggregateBounded(
					final List<Path> sources, final Path staging, final long selectionLimit)
					throws IOException {
		final Path tempDir = Files.createTempDirectory("spt-integrity-aggregate-");
		final List<Path> chunks = new ArrayList<>();
		long sourceCount = 0;
		try {
			for (final Path source : sources) {
				sourceCount = Math.addExact(sourceCount, createSortedChunks(source, tempDir, chunks));
			}
			final MergeCounts merged = mergeSortedChunks(tempDir, chunks, staging, selectionLimit);
			return new AggregationCounts(sourceCount, merged.unique(), merged.selected());
		} finally {
			deleteTempTree(tempDir);
		}
	}

	private static long createSortedChunks(
					final Path source, final Path tempDir, final List<Path> chunks) throws IOException {
		long sourceCount = 0;
		try (Reader reader = Files.newBufferedReader(source, StandardCharsets.UTF_8);
						CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final Iterator<CSVRecord> records = parser.iterator();
			if (!records.hasNext()
							|| !IntegrityManifestItemInput.HEADER.equals(records.next().toList())) {
				throw new IOException("manifest source has a noncanonical header: " + source);
			}
			final List<ManifestRecord> batch = new ArrayList<>(SORT_CHUNK_RECORDS);
			while (records.hasNext()) {
				checkInterrupted();
				batch.add(parseRecord(records.next(), source));
				sourceCount = Math.addExact(sourceCount, 1);
				if (batch.size() == SORT_CHUNK_RECORDS) {
					chunks.add(writeSortedChunk(tempDir, batch));
					batch.clear();
				}
			}
			if (!batch.isEmpty()) {
				chunks.add(writeSortedChunk(tempDir, batch));
			}
		}
		return sourceCount;
	}

	private static Path writeSortedChunk(
					final Path tempDir, final List<ManifestRecord> records) throws IOException {
		records.sort(RECORD_ORDER);
		final Path chunk = Files.createTempFile(tempDir, "chunk-", ".csv");
		boolean success = false;
		try (CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(chunk, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING),
						IntegrityCsvFormat.RFC4180_LF)) {
			ManifestRecord prior = null;
			for (final ManifestRecord record : records) {
				if (prior != null && RECORD_ORDER.compare(prior, record) == 0) {
					requireSameSize(prior, record);
					continue;
				}
				printer.printRecord(record.bucket(), record.key(), record.size(), record.versionId());
				prior = record;
			}
			success = true;
		} finally {
			if (!success) {
				Files.deleteIfExists(chunk);
			}
		}
		return chunk;
	}

	private static MergeCounts mergeSortedChunks(
					final Path tempDir, final List<Path> chunks, final Path staging, final long selectionLimit)
					throws IOException {
		List<Path> working = new ArrayList<>(chunks);
		while (working.size() > MERGE_FAN_IN) {
			final List<Path> next = new ArrayList<>((working.size() + MERGE_FAN_IN - 1) / MERGE_FAN_IN);
			for (int start = 0; start < working.size(); start += MERGE_FAN_IN) {
				final int end = Math.min(start + MERGE_FAN_IN, working.size());
				final Path intermediate = Files.createTempFile(tempDir, "merge-", ".csv");
				mergeChunkGroup(working.subList(start, end), intermediate, false, 0);
				next.add(intermediate);
			}
			working = next;
		}
		return mergeChunkGroup(working, staging, true, selectionLimit);
	}

	private static MergeCounts mergeChunkGroup(
					final List<Path> chunks,
					final Path target,
					final boolean includeHeader,
					final long selectionLimit)
					throws IOException {
		final List<ChunkCursor> cursors = new ArrayList<>(chunks.size());
		final PriorityQueue<MergeEntry> queue = new PriorityQueue<>((left, right) -> {
			final int compared = RECORD_ORDER.compare(left.record(), right.record());
			return compared != 0 ? compared : Integer.compare(left.source(), right.source());
		});
		try (CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(target, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING),
						IntegrityCsvFormat.RFC4180_LF)) {
			if (includeHeader) {
				printer.printRecord(IntegrityManifestItemInput.HEADER);
			}
			for (final Path chunk : chunks) {
				final ChunkCursor cursor = new ChunkCursor(chunk);
				final int source = cursors.size();
				cursors.add(cursor);
				if (cursor.advance()) {
					queue.add(new MergeEntry(source, cursor.current));
				}
			}
			long unique = 0;
			long selected = 0;
			ManifestRecord prior = null;
			while (!queue.isEmpty()) {
				checkInterrupted();
				final MergeEntry entry = queue.remove();
				final ManifestRecord record = entry.record();
				if (prior != null && RECORD_ORDER.compare(prior, record) == 0) {
					requireSameSize(prior, record);
				} else {
					unique = Math.addExact(unique, 1);
					if (selectionLimit == 0 || selected < selectionLimit) {
						printer.printRecord(record.bucket(), record.key(), record.size(), record.versionId());
						selected = Math.addExact(selected, 1);
					}
					prior = record;
				}
				final ChunkCursor cursor = cursors.get(entry.source());
				if (cursor.advance()) {
					queue.add(new MergeEntry(entry.source(), cursor.current));
				}
			}
			return new MergeCounts(unique, selected);
		} finally {
			IOException failure = null;
			for (final ChunkCursor cursor : cursors) {
				try {
					cursor.close();
				} catch (final IOException e) {
					if (failure == null) {
						failure = e;
					} else {
						failure.addSuppressed(e);
					}
				}
			}
			if (failure != null) {
				throw failure;
			}
		}
	}

	private static ManifestRecord parseRecord(final CSVRecord record, final Path source) throws IOException {
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
		return new ManifestRecord(record.get(0), record.get(1), size, record.get(3));
	}

	private static void requireSameSize(final ManifestRecord left, final ManifestRecord right)
					throws IOException {
		if (left.size() != right.size()) {
			throw new IOException(
							"manifest identity has conflicting sizes: "
											+ left.bucket() + "/" + left.key() + ":" + left.versionId());
		}
	}

	private static void checkInterrupted() throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException("manifest aggregation interrupted");
		}
	}

	private static void deleteTempTree(final Path tempDir) throws IOException {
		try (final var paths = Files.walk(tempDir)) {
			final List<Path> ordered = paths.sorted(Comparator.reverseOrder()).toList();
			IOException failure = null;
			for (final Path path : ordered) {
				try {
					Files.deleteIfExists(path);
				} catch (final IOException e) {
					if (failure == null) {
						failure = e;
					} else {
						failure.addSuppressed(e);
					}
				}
			}
			if (failure != null) {
				throw failure;
			}
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
