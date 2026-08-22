package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.github.akurilov.commons.io.file.FileInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** RFC 4180 reader for canonical {@code bucket,key,size,version_id} manifests. */
public final class IntegrityManifestItemInput
				implements FileInput<IntegrityManifestDataItem>, RemainingItemCountInput<IntegrityManifestDataItem> {

	public static final List<String> HEADER = List.of("bucket", "key", "size", "version_id");

	private static final CSVFormat FORMAT = CSVFormat.RFC4180.builder()
					.setHeader(HEADER.toArray(String[]::new))
					.setSkipHeaderRecord(true)
					.get();

	private final Path manifestPath;
	private final long itemCount;
	private Reader reader;
	private CSVParser parser;
	private Iterator<CSVRecord> records;
	private long remainingItemCount;

	public IntegrityManifestItemInput(final Path manifestPath) throws IOException {
		this.manifestPath = manifestPath;
		this.itemCount = countRows(manifestPath);
		this.remainingItemCount = itemCount;
		open();
	}

	public static boolean hasCanonicalHeader(final Path path) throws IOException {
		try (final Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
						final CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final Iterator<CSVRecord> records = parser.iterator();
			if (!records.hasNext()) {
				return false;
			}
			final CSVRecord first = records.next();
			return first.size() == HEADER.size()
							&& HEADER.equals(first.toList());
		}
	}

	@Override
	public IntegrityManifestDataItem get() {
		try {
			if (!records.hasNext()) {
				return null;
			}
			final var item = item(records.next());
			remainingItemCount--;
			return item;
		} catch (final RuntimeException e) {
			throw manifestFailure(e);
		}
	}

	@Override
	public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
		int count = 0;
		try {
			while (count < limit && records.hasNext()) {
				buffer.add(item(records.next()));
				count++;
			}
			if (count == 0) {
				throwUnchecked(new EOFException());
			}
		} catch (final RuntimeException e) {
			throw manifestFailure(e);
		}
		remainingItemCount -= count;
		return count;
	}

	@Override
	public long skip(final long itemsCount) {
		long skipped = 0;
		while (skipped < itemsCount && records.hasNext()) {
			records.next();
			skipped++;
		}
		remainingItemCount -= skipped;
		return skipped;
	}

	@Override
	public long remainingItemCount() {
		return remainingItemCount;
	}

	@Override
	public void reset() {
		close();
		try {
			open();
			remainingItemCount = itemCount;
		} catch (final IOException e) {
			throwUnchecked(e);
		}
	}

	@Override
	public void close() {
		try {
			if (parser != null) {
				parser.close();
			} else if (reader != null) {
				reader.close();
			}
		} catch (final IOException e) {
			throwUnchecked(e);
		} finally {
			parser = null;
			reader = null;
			records = null;
		}
	}

	@Override
	public Path filePath() {
		return manifestPath;
	}

	@Override
	public String toString() {
		return "IntegrityManifestFromFile(" + manifestPath + ")";
	}

	private void open() throws IOException {
		reader = Files.newBufferedReader(manifestPath, StandardCharsets.UTF_8);
		parser = FORMAT.parse(reader);
		records = parser.iterator();
	}

	private static long countRows(final Path path) throws IOException {
		try (final Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
						final CSVParser parser = CSVFormat.RFC4180.parse(reader)) {
			final Iterator<CSVRecord> records = parser.iterator();
			if (!records.hasNext() || !HEADER.equals(records.next().toList())) {
				throw new IOException("integrity manifest does not have the exact canonical header");
			}
			long count = 0;
			while (records.hasNext()) {
				validateRecord(records.next());
				count = Math.addExact(count, 1);
			}
			return count;
		} catch (final RuntimeException e) {
			throw new IOException("failed to count canonical integrity manifest rows", e);
		}
	}

	private static IntegrityManifestDataItem item(final CSVRecord record) {
		final long size = validateRecord(record);
		return new IntegrityManifestDataItem(
						record.get(0), record.get(1), size, record.get(3));
	}

	private static long validateRecord(final CSVRecord record) {
		if (record.size() != HEADER.size()) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " must have four fields");
		}
		if (record.get(0).isEmpty()) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " has an empty bucket");
		}
		if (record.get(1).isEmpty()) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " has an empty key");
		}
		return parseSize(record);
	}

	private static long parseSize(final CSVRecord record) {
		final long size;
		try {
			size = Long.parseLong(record.get(2));
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " has invalid size", e);
		}
		if (size < 0) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " has negative size");
		}
		return size;
	}

	private IllegalArgumentException manifestFailure(final RuntimeException cause) {
		return new IllegalArgumentException(
						"invalid integrity manifest " + manifestPath + ": " + cause.getMessage(), cause);
	}
}
