package com.dell.spt.base.load.step.client;

import com.dell.spt.base.integrity.IntegrityCsvFormat;
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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/** Fixed-memory RFC 4180 external sorter for exact DELETE artifact reconciliation. */
final class DeleteCsvExternalSorter {
	static final int SORT_CHUNK_RECORDS = 1_024;
	private static final int MERGE_FAN_IN = 64;

	private record MergeEntry(int source, List<String> row) {}

	private static final class Cursor implements AutoCloseable {
		private final CSVParser parser;
		private final Iterator<CSVRecord> records;
		private List<String> current;

		private Cursor(final Path path) throws IOException {
			parser = CSVFormat.RFC4180.parse(Files.newBufferedReader(path, StandardCharsets.UTF_8));
			records = parser.iterator();
		}

		private boolean advance() {
			if (!records.hasNext()) {
				current = null;
				return false;
			}
			current = records.next().toList();
			return true;
		}

		@Override
		public void close() throws IOException {
			parser.close();
		}
	}

	private static final class CursorCollection implements AutoCloseable {
		private final List<Cursor> cursors = new ArrayList<>();

		private Cursor open(final Path path) throws IOException {
			final Cursor cursor = new Cursor(path);
			cursors.add(cursor);
			return cursor;
		}

		@Override
		public void close() throws IOException {
			IOException failure = null;
			for (final Cursor cursor : cursors) {
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

	private DeleteCsvExternalSorter() {}

	static long sort(
					final List<Path> sources,
					final List<String> expectedHeader,
					final Comparator<List<String>> comparator,
					final Path target,
					final Path tempDirectory,
					final String prefix)
					throws IOException {
		final Path sortDirectory = Files.createTempDirectory(tempDirectory, prefix + "-");
		final List<List<String>> batch = new ArrayList<>(SORT_CHUNK_RECORDS);
		long rows = 0;
		long chunks = 0;
		for (final Path source : sources) {
			try (Reader reader = Files.newBufferedReader(source, StandardCharsets.UTF_8);
							CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
				final Iterator<CSVRecord> records = parser.iterator();
				if (!records.hasNext() || !expectedHeader.equals(records.next().toList())) {
					throw new IOException("DELETE artifact has a noncanonical header: " + source);
				}
				while (records.hasNext()) {
					checkInterrupted();
					final List<String> row = records.next().toList();
					if (row.size() != expectedHeader.size()) {
						throw new IOException("DELETE artifact has an invalid row: " + source);
					}
					batch.add(List.copyOf(row));
					rows = Math.addExact(rows, 1);
					if (batch.size() == SORT_CHUNK_RECORDS) {
						writeChunk(batch, comparator, chunkPath(sortDirectory, 0, chunks++));
						batch.clear();
					}
				}
			}
		}
		if (!batch.isEmpty()) {
			writeChunk(batch, comparator, chunkPath(sortDirectory, 0, chunks++));
		}
		merge(chunks, comparator, target, sortDirectory, expectedHeader);
		return rows;
	}

	private static void writeChunk(
					final List<List<String>> batch,
					final Comparator<List<String>> comparator,
					final Path chunk)
					throws IOException {
		batch.sort(comparator);
		try (CSVPrinter printer = printer(chunk)) {
			for (final List<String> row : batch) {
				printer.printRecord(row);
			}
		}
	}

	private static void merge(
					final long initialChunkCount,
					final Comparator<List<String>> comparator,
					final Path target,
					final Path sortDirectory,
					final List<String> header)
					throws IOException {
		long workingCount = initialChunkCount;
		int round = 0;
		while (workingCount > MERGE_FAN_IN) {
			long nextCount = 0;
			for (long start = 0; start < workingCount; start += MERGE_FAN_IN) {
				final List<Path> group = chunkGroup(sortDirectory, round, start, workingCount);
				mergeGroup(group, comparator, chunkPath(sortDirectory, round + 1, nextCount++), null);
				deleteChunks(group);
			}
			workingCount = nextCount;
			round++;
		}
		final List<Path> finalGroup = chunkGroup(sortDirectory, round, 0, workingCount);
		mergeGroup(finalGroup, comparator, target, header);
		deleteChunks(finalGroup);
		Files.delete(sortDirectory);
	}

	private static List<Path> chunkGroup(
					final Path directory,
					final int round,
					final long start,
					final long count) {
		final List<Path> group = new ArrayList<>(MERGE_FAN_IN);
		for (long index = start; index < Math.min(Math.addExact(start, MERGE_FAN_IN), count); index++) {
			group.add(chunkPath(directory, round, index));
		}
		return group;
	}

	private static Path chunkPath(final Path directory, final int round, final long index) {
		return directory.resolve("r" + round + "-" + index + ".csv");
	}

	private static void deleteChunks(final List<Path> chunks) throws IOException {
		IOException failure = null;
		for (final Path chunk : chunks) {
			try {
				Files.deleteIfExists(chunk);
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

	private static void mergeGroup(
					final List<Path> chunks,
					final Comparator<List<String>> comparator,
					final Path target,
					final List<String> header)
					throws IOException {
		final PriorityQueue<MergeEntry> queue = new PriorityQueue<>((left, right) -> {
			final int compared = comparator.compare(left.row(), right.row());
			return compared != 0 ? compared : Integer.compare(left.source(), right.source());
		});
		try (CursorCollection cursors = new CursorCollection();
						CSVPrinter printer = printer(target)) {
			if (header != null) {
				printer.printRecord(header);
			}
			for (final Path chunk : chunks) {
				final int source = cursors.cursors.size();
				final Cursor cursor = cursors.open(chunk);
				if (cursor.advance()) {
					queue.add(new MergeEntry(source, cursor.current));
				}
			}
			while (!queue.isEmpty()) {
				checkInterrupted();
				final MergeEntry entry = queue.remove();
				printer.printRecord(entry.row());
				final Cursor cursor = cursors.cursors.get(entry.source());
				if (cursor.advance()) {
					queue.add(new MergeEntry(entry.source(), cursor.current));
				}
			}
		}
	}

	private static CSVPrinter printer(final Path path) throws IOException {
		return new CSVPrinter(
						Files.newBufferedWriter(
										path, StandardCharsets.UTF_8,
										StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
										StandardOpenOption.WRITE),
						IntegrityCsvFormat.RFC4180_LF);
	}

	private static void checkInterrupted() throws IOException {
		if (Thread.currentThread().isInterrupted()) {
			throw new IOException("DELETE artifact sort interrupted");
		}
	}
}
