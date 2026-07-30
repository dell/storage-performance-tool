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
				implements FileInput<IntegrityManifestDataItem> {

	public static final List<String> HEADER = List.of("bucket", "key", "size", "version_id");

	private static final CSVFormat FORMAT = CSVFormat.RFC4180.builder()
					.setHeader(HEADER.toArray(String[]::new))
					.setSkipHeaderRecord(true)
					.get();

	private final Path manifestPath;
	private Reader reader;
	private CSVParser parser;
	private Iterator<CSVRecord> records;

	public IntegrityManifestItemInput(final Path manifestPath) throws IOException {
		this.manifestPath = manifestPath;
		if (!hasCanonicalHeader(manifestPath)) {
			throw new IOException("integrity manifest does not have the exact canonical header");
		}
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
			return records.hasNext() ? item(records.next()) : null;
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
		return count;
	}

	@Override
	public long skip(final long itemsCount) {
		long skipped = 0;
		while (skipped < itemsCount && records.hasNext()) {
			records.next();
			skipped++;
		}
		return skipped;
	}

	@Override
	public void reset() {
		close();
		try {
			open();
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

	private static IntegrityManifestDataItem item(final CSVRecord record) {
		if (record.size() != HEADER.size()) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " must have four fields");
		}
		final long size;
		try {
			size = Long.parseLong(record.get(2));
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException(
							"integrity manifest record " + record.getRecordNumber() + " has invalid size", e);
		}
		return new IntegrityManifestDataItem(
						record.get(0), record.get(1), size, record.get(3));
	}

	private IllegalArgumentException manifestFailure(final RuntimeException cause) {
		return new IllegalArgumentException(
						"invalid integrity manifest " + manifestPath + ": " + cause.getMessage(), cause);
	}
}
