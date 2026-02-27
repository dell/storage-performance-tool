package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.nio.ByteBuffer;

public class ParquetRowGroupWriterTest {

	@Test
	void write_returnsNonEmptyBytes() throws IOException {
		final ParquetRowGroupWriter writer = new ParquetRowGroupWriter(64 * 1024);
		final byte[] bytes = writer.write(0);
		assertNotNull(bytes);
		assertTrue(bytes.length > 0, "Parquet output should be non-empty");
	}

	@Test
	void write_validParquetMagicBytes() throws IOException {
		final ParquetRowGroupWriter writer = new ParquetRowGroupWriter(64 * 1024);
		final byte[] bytes = writer.write(0);
		// Parquet files start with magic bytes "PAR1"
		assertEquals('P', bytes[0]);
		assertEquals('A', bytes[1]);
		assertEquals('R', bytes[2]);
		assertEquals('1', bytes[3]);
		// And end with magic bytes "PAR1"
		final int len = bytes.length;
		assertEquals('P', bytes[len - 4]);
		assertEquals('A', bytes[len - 3]);
		assertEquals('R', bytes[len - 2]);
		assertEquals('1', bytes[len - 1]);
	}

	@Test
	void write_sizeRoughlyMatchesTarget() throws IOException {
		final long targetBytes = 128 * 1024;
		final ParquetRowGroupWriter writer = new ParquetRowGroupWriter(targetBytes);
		final byte[] bytes = writer.write(0);
		// Parquet metadata and framing adds overhead; allow 5x variance in either direction
		assertTrue(bytes.length > targetBytes / 5,
						"Output " + bytes.length + " too small for target " + targetBytes);
	}

	@Test
	void write_smallTarget_producesAtLeastOneRow() throws IOException {
		final ParquetRowGroupWriter writer = new ParquetRowGroupWriter(1);
		final byte[] bytes = writer.write(0);
		assertTrue(bytes.length > 4, "Even a 1-byte target must produce a valid Parquet file");
		assertEquals('P', bytes[0]);
		assertEquals('A', bytes[1]);
		assertEquals('R', bytes[2]);
		assertEquals('1', bytes[3]);
	}

	@Test
	void write_canBeReadByParquetReader() throws IOException {
		final ParquetRowGroupWriter writer = new ParquetRowGroupWriter(64 * 1024);
		final byte[] bytes = writer.write(42);
		final InputFile inputFile = new ByteArrayInputFile(bytes);
		try (final ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
			final var meta = reader.getFooter();
			assertNotNull(meta, "Parquet footer should be readable");
			assertEquals(1, meta.getBlocks().size(), "Should have exactly 1 row group");
			assertTrue(meta.getBlocks().get(0).getRowCount() >= 1, "Row group must have at least 1 row");
		}
	}

	@Test
	void schema_hasExpectedFields() {
		final var schema = ParquetRowGroupWriter.SCHEMA;
		assertNotNull(schema.getField("id"));
		assertNotNull(schema.getField("timestamp"));
		assertNotNull(schema.getField("value"));
		assertNotNull(schema.getField("payload"));
		assertEquals(4, schema.getFields().size());
	}

	@Test
	void byteArrayOutputFile_supportsMultipleWrites() throws IOException {
		final ParquetRowGroupWriter.ByteArrayOutputFile outputFile = new ParquetRowGroupWriter.ByteArrayOutputFile();
		final var stream = outputFile.createOrOverwrite(0);
		stream.write(new byte[]{1, 2, 3
		}, 0, 3);
		assertEquals(3, stream.getPos());
		final byte[] result = outputFile.toByteArray();
		assertEquals(3, result.length);
		assertEquals(1, result[0]);
		assertEquals(2, result[1]);
		assertEquals(3, result[2]);
	}

	/** Minimal in-memory InputFile for Parquet reader tests. */
	private static final class ByteArrayInputFile implements InputFile {

		private final byte[] data;

		ByteArrayInputFile(final byte[] data) {
			this.data = data;
		}

		@Override
		public long getLength() {
			return data.length;
		}

		@Override
		public SeekableInputStream newStream() {
			return new ByteArraySeekableInputStream(data);
		}
	}

	private static final class ByteArraySeekableInputStream extends SeekableInputStream {

		private final byte[] data;
		private int pos = 0;

		ByteArraySeekableInputStream(final byte[] data) {
			this.data = data;
		}

		@Override
		public long getPos() {
			return pos;
		}

		@Override
		public void seek(final long newPos) {
			this.pos = (int) newPos;
		}

		@Override
		public void readFully(final byte[] bytes) throws IOException {
			readFully(bytes, 0, bytes.length);
		}

		@Override
		public void readFully(final byte[] bytes, final int start, final int len) throws IOException {
			if (pos + len > data.length)
				throw new EOFException();
			System.arraycopy(data, pos, bytes, start, len);
			pos += len;
		}

		@Override
		public int read(final ByteBuffer buf) throws IOException {
			final int available = Math.min(buf.remaining(), data.length - pos);
			if (available <= 0)
				return -1;
			buf.put(data, pos, available);
			pos += available;
			return available;
		}

		@Override
		public void readFully(final ByteBuffer buf) throws IOException {
			final int needed = buf.remaining();
			if (pos + needed > data.length)
				throw new EOFException();
			buf.put(data, pos, needed);
			pos += needed;
		}

		@Override
		public int read() {
			if (pos >= data.length)
				return -1;
			return data[pos++] & 0xFF;
		}
	}
}
