package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.file.FileInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Hands out a multi-bucket canonical manifest one row per bucket in turn, so a DELETE that sends one
 * target per request works on every bucket at once instead of finishing them one after another.
 *
 * <p>The manifest on disk stays as it is. One reader starts at each bucket's first row, and each item
 * carries its row position because DELETE evidence is reconciled against that position.
 */
public final class BucketRotatingManifestInput
				implements FileInput<IntegrityManifestDataItem>, RemainingItemCountInput<IntegrityManifestDataItem> {

	/** One open file per bucket; the same limit as the largest bucket set a run can name. */
	static final int MAX_BUCKETS = 1000;

	private static final CSVFormat ROW_FORMAT = CSVFormat.RFC4180;

	private record BucketRows(long byteOffset, long firstRow, long rowCount) {}

	private static final class Cursor implements AutoCloseable {
		private final CSVParser parser;
		private final Iterator<CSVRecord> records;
		private long nextRow;
		private long rowsLeft;

		private Cursor(final Path manifest, final BucketRows rows) throws IOException {
			final InputStream stream = Files.newInputStream(manifest);
			try {
				stream.skipNBytes(rows.byteOffset());
				final Reader reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
				parser = ROW_FORMAT.parse(reader);
			} catch (final IOException | RuntimeException e) {
				stream.close();
				throw e;
			}
			records = parser.iterator();
			nextRow = rows.firstRow();
			rowsLeft = rows.rowCount();
		}

		private IntegrityManifestDataItem next() {
			final var item = IntegrityManifestItemInput.item(records.next());
			item.selectionIndex(nextRow);
			nextRow++;
			rowsLeft--;
			return item;
		}

		@Override
		public void close() throws IOException {
			parser.close();
		}
	}

	private final Path manifestPath;
	private final List<BucketRows> buckets;
	private final long itemCount;
	private final ArrayDeque<Cursor> turns = new ArrayDeque<>();
	private long remainingItemCount;

	private BucketRotatingManifestInput(final Path manifestPath, final List<BucketRows> buckets) throws IOException {
		this.manifestPath = manifestPath;
		this.buckets = buckets;
		this.itemCount = buckets.stream().mapToLong(BucketRows::rowCount).sum();
		this.remainingItemCount = itemCount;
		open();
	}

	/**
	 * Returns an input that takes the source's rows in turn from each bucket and closes the source. Returns
	 * the source itself when its rows are one bucket, or when they change bucket more than {@link
	 * #MAX_BUCKETS} times, which is either a very large bucket set or a file that is already mixed.
	 */
	public static Input<IntegrityManifestDataItem> across(final IntegrityManifestItemInput source) throws IOException {
		final Path manifest = source.filePath();
		final List<BucketRows> buckets = bucketRows(manifest);
		if (buckets.size() < 2) {
			return source;
		}
		if (buckets.size() > MAX_BUCKETS) {
			Loggers.MSG.info(
							"DELETE keeps the order of {}: its rows change bucket more than {} times",
							manifest,
							MAX_BUCKETS);
			return source;
		}
		// The source closes first: a failure to close it, or to open the bucket readers, then leaves
		// nothing open that the caller cannot close.
		source.close();
		return new BucketRotatingManifestInput(manifest, buckets);
	}

	/**
	 * Finds where each bucket's rows start, stopping one past the limit. Single-byte decoding makes a
	 * record's character position its byte offset, and records split in the same places because a
	 * multi-byte UTF-8 sequence has no delimiter, quote or line-end byte in it.
	 */
	private static List<BucketRows> bucketRows(final Path manifest) throws IOException {
		final var buckets = new ArrayList<BucketRows>();
		try (final Reader reader = Files.newBufferedReader(manifest, StandardCharsets.ISO_8859_1);
						final CSVParser parser = IntegrityManifestItemInput.FORMAT.parse(reader)) {
			String bucket = null;
			long byteOffset = 0;
			long firstRow = 0;
			long row = 0;
			for (final CSVRecord record : parser) {
				if (!record.get(0).equals(bucket)) {
					if (bucket != null) {
						buckets.add(new BucketRows(byteOffset, firstRow, row - firstRow));
						if (buckets.size() > MAX_BUCKETS) {
							return buckets;
						}
					}
					bucket = record.get(0);
					byteOffset = record.getCharacterPosition();
					firstRow = row;
				}
				row++;
			}
			if (bucket != null) {
				buckets.add(new BucketRows(byteOffset, firstRow, row - firstRow));
			}
		}
		return buckets;
	}

	@Override
	public IntegrityManifestDataItem get() {
		try {
			return nextItem();
		} catch (final RuntimeException e) {
			throw manifestFailure(e);
		}
	}

	@Override
	public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
		int count = 0;
		try {
			while (count < limit) {
				final var item = nextItem();
				if (item == null) {
					break;
				}
				buffer.add(item);
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
		try {
			while (skipped < itemsCount && nextItem() != null) {
				skipped++;
			}
		} catch (final RuntimeException e) {
			throw manifestFailure(e);
		}
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
		IOException failure = null;
		for (Cursor cursor = turns.pollFirst(); cursor != null; cursor = turns.pollFirst()) {
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
			throwUnchecked(failure);
		}
	}

	@Override
	public Path filePath() {
		return manifestPath;
	}

	@Override
	public String toString() {
		return "BucketRotatingManifestFromFile(" + manifestPath + ")";
	}

	private void open() throws IOException {
		try {
			for (final BucketRows rows : buckets) {
				turns.addLast(new Cursor(manifestPath, rows));
			}
		} catch (final IOException | RuntimeException e) {
			try {
				close();
			} catch (final RuntimeException closeFailure) {
				e.addSuppressed(closeFailure);
			}
			throw e;
		}
	}

	private IntegrityManifestDataItem nextItem() {
		final Cursor cursor = turns.pollFirst();
		if (cursor == null) {
			return null;
		}
		// Back in the queue first: a bad row must not leave this cursor open with nothing to close it.
		turns.addLast(cursor);
		final var item = cursor.next();
		remainingItemCount--;
		if (cursor.rowsLeft == 0) {
			turns.removeLast();
			try {
				cursor.close();
			} catch (final IOException e) {
				throwUnchecked(e);
			}
		}
		return item;
	}

	private IllegalArgumentException manifestFailure(final RuntimeException cause) {
		return new IllegalArgumentException(
						"invalid integrity manifest " + manifestPath + ": " + cause.getMessage(), cause);
	}
}
