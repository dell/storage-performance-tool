package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Writes a single Parquet row group into an in-memory byte array using the
 * official parquet-avro library.
 *
 * Fixed synthetic schema:
 *   id:        INT64   (sequential row ID)
 *   timestamp: INT64   (epoch ms at write time)
 *   value:     DOUBLE  (random float)
 *   payload:   BYTES   (random bytes — sized to hit targetFileSizeBytes)
 *
 * Row count = targetFileSizeBytes / estimatedRowBytes.
 * The payload column absorbs the remainder to hit the target size.
 */
final class ParquetRowGroupWriter {

	static final Schema SCHEMA = new Schema.Parser().parse(
					"{"
									+ "\"type\":\"record\","
									+ "\"name\":\"SptRow\","
									+ "\"namespace\":\"com.dell.spt\","
									+ "\"fields\":["
									+ "  {\"name\":\"id\",       \"type\":\"long\"},"
									+ "  {\"name\":\"timestamp\",\"type\":\"long\"},"
									+ "  {\"name\":\"value\",    \"type\":\"double\"},"
									+ "  {\"name\":\"payload\",  \"type\":\"bytes\"}"
									+ "]}");

	// Overhead per row excluding payload (id 8 + ts 8 + value 8 + framing ~20)
	private static final int ROW_OVERHEAD_BYTES = 44;
	private static final int MIN_ROWS = 1;

	private final long targetFileSizeBytes;

	ParquetRowGroupWriter(final long targetFileSizeBytes) {
		this.targetFileSizeBytes = targetFileSizeBytes;
	}

	/**
	 * Writes one Parquet file and returns its bytes.
	 *
	 * @param baseRowId starting row ID for this file
	 * @return Parquet file bytes
	 */
	byte[] write(final long baseRowId) throws IOException {
		final long payloadBytesPerRow = Math.max(0, targetFileSizeBytes / Math.max(MIN_ROWS,
						targetFileSizeBytes / (ROW_OVERHEAD_BYTES + 64)) - ROW_OVERHEAD_BYTES);
		final int rowCount = (int) Math.max(MIN_ROWS,
						targetFileSizeBytes / (ROW_OVERHEAD_BYTES + payloadBytesPerRow));

		final ByteArrayOutputFile outputFile = new ByteArrayOutputFile();
		final Configuration conf = new Configuration();
		// Disable Hadoop filesystem completely — all I/O goes through outputFile
		conf.set("fs.defaultFS", "file:///");

		try (final ParquetWriter<GenericRecord> writer = AvroParquetWriter
						.<GenericRecord> builder(outputFile)
						.withSchema(SCHEMA)
						.withConf(conf)
						.withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
						.withRowGroupSize((long) Integer.MAX_VALUE)
						.build()) {

			final Random rng = ThreadLocalRandom.current();
			final long nowMs = System.currentTimeMillis();
			final byte[] payloadBuf = new byte[(int) Math.max(1, payloadBytesPerRow)];

			for (int i = 0; i < rowCount; i++) {
				rng.nextBytes(payloadBuf);
				final GenericRecord record = new GenericData.Record(SCHEMA);
				record.put("id", baseRowId + i);
				record.put("timestamp", nowMs);
				record.put("value", rng.nextDouble());
				record.put("payload", ByteBuffer.wrap(payloadBuf.clone()));
				writer.write(record);
			}
		}

		return outputFile.toByteArray();
	}

	/**
	 * In-memory {@link OutputFile} — no Hadoop FileSystem involvement.
	 */
	static final class ByteArrayOutputFile implements OutputFile {

		private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

		@Override
		public PositionOutputStream create(final long blockSizeHint) {
			return new ByteArrayPositionOutputStream(baos);
		}

		@Override
		public PositionOutputStream createOrOverwrite(final long blockSizeHint) {
			baos.reset();
			return new ByteArrayPositionOutputStream(baos);
		}

		@Override
		public boolean supportsBlockSize() {
			return false;
		}

		@Override
		public long defaultBlockSize() {
			return 0;
		}

		byte[] toByteArray() {
			return baos.toByteArray();
		}
	}

	static final class ByteArrayPositionOutputStream extends PositionOutputStream {

		private final ByteArrayOutputStream baos;
		private long pos = 0;

		ByteArrayPositionOutputStream(final ByteArrayOutputStream baos) {
			this.baos = baos;
		}

		@Override
		public long getPos() {
			return pos;
		}

		@Override
		public void write(final int b) {
			baos.write(b);
			pos++;
		}

		@Override
		public void write(final byte[] b, final int off, final int len) {
			baos.write(b, off, len);
			pos += len;
		}

		@Override
		public void flush() {}

		@Override
		public void close() {}
	}
}
