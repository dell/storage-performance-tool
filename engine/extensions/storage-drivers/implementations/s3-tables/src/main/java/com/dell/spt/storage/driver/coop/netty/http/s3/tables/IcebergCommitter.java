package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import com.dell.spt.base.logging.Loggers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Executes a full Iceberg snapshot commit for one tableWrite operation:
 *
 *   1. Generate Parquet bytes   (ParquetRowGroupWriter)
 *   2. PUT data file            → warehouseLocation/data/<uuid>.parquet
 *   3. Build manifest file      (Avro)
 *   4. PUT manifest file        → warehouseLocation/metadata/<uuid>-m0.avro
 *   5. Build manifest list      (Avro)
 *   6. PUT manifest list        → warehouseLocation/metadata/snap-<snapshotId>.avro
 *   7. Build table metadata JSON
 *   8. PUT metadata JSON        → warehouseLocation/metadata/v<N>.metadata.json
 *   9. UpdateTableMetadataLocation (old versionToken → new S3 URI)
 *      → HTTP 409: jitter-sleep, re-fetch versionToken, retry from step 7
 *
 * Thread-safe: multiple threads share one IcebergCommitter per driver instance.
 * The versionToken AtomicReference is per-JVM (per engine node) — see design doc §5.2.
 */
final class IcebergCommitter {

	private static final ObjectMapper MAPPER = new ObjectMapper();
	private static final int HTTP_CONFLICT = 409;
	private static final int MAX_RETRIES = 64;
	private static final int JITTER_MIN_MS = 5;
	private static final int JITTER_MAX_MS = 50;

	// Iceberg format version
	private static final int ICEBERG_FORMAT_VERSION = 2;

	// Avro schemas for Iceberg manifest structures
	private static final Schema MANIFEST_ENTRY_SCHEMA = parseAvroSchema(
					"{"
									+ "\"type\":\"record\","
									+ "\"name\":\"manifest_entry\","
									+ "\"fields\":["
									+ "  {\"name\":\"status\",          \"type\":\"int\"},"
									+ "  {\"name\":\"snapshot_id\",     \"type\":[\"null\",\"long\"], \"default\":null},"
									+ "  {\"name\":\"sequence_number\", \"type\":[\"null\",\"long\"], \"default\":null},"
									+ "  {\"name\":\"file_sequence_number\", \"type\":[\"null\",\"long\"], \"default\":null},"
									+ "  {\"name\":\"data_file\", \"type\":{"
									+ "    \"type\":\"record\","
									+ "    \"name\":\"r2\","
									+ "    \"fields\":["
									+ "      {\"name\":\"content\",         \"type\":\"int\"},"
									+ "      {\"name\":\"file_path\",        \"type\":\"string\"},"
									+ "      {\"name\":\"file_format\",      \"type\":\"string\"},"
									+ "      {\"name\":\"record_count\",     \"type\":\"long\"},"
									+ "      {\"name\":\"file_size_in_bytes\",\"type\":\"long\"},"
									+ "      {\"name\":\"column_sizes\",     \"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\",\"key-id\":4,\"value-id\":5}], \"default\":null},"
									+ "      {\"name\":\"value_counts\",     \"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\",\"key-id\":6,\"value-id\":7}], \"default\":null},"
									+ "      {\"name\":\"null_value_counts\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\",\"key-id\":8,\"value-id\":9}], \"default\":null},"
									+ "      {\"name\":\"nan_value_counts\", \"type\":[\"null\",{\"type\":\"map\",\"values\":\"long\",\"key-id\":10,\"value-id\":11}], \"default\":null},"
									+ "      {\"name\":\"lower_bounds\",     \"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\",\"key-id\":12,\"value-id\":13}], \"default\":null},"
									+ "      {\"name\":\"upper_bounds\",     \"type\":[\"null\",{\"type\":\"map\",\"values\":\"bytes\",\"key-id\":14,\"value-id\":15}], \"default\":null},"
									+ "      {\"name\":\"key_metadata\",     \"type\":[\"null\",\"bytes\"], \"default\":null},"
									+ "      {\"name\":\"split_offsets\",    \"type\":[\"null\",{\"type\":\"array\",\"items\":\"long\"}], \"default\":null},"
									+ "      {\"name\":\"equality_ids\",     \"type\":[\"null\",{\"type\":\"array\",\"items\":\"int\"}],  \"default\":null},"
									+ "      {\"name\":\"sort_order_id\",    \"type\":[\"null\",\"int\"], \"default\":null}"
									+ "    ]"
									+ "  }}"
									+ "]}");

	private static final Schema MANIFEST_LIST_SCHEMA = parseAvroSchema(
					"{"
									+ "\"type\":\"record\","
									+ "\"name\":\"manifest_file\","
									+ "\"fields\":["
									+ "  {\"name\":\"manifest_path\",        \"type\":\"string\"},"
									+ "  {\"name\":\"manifest_length\",      \"type\":\"long\"},"
									+ "  {\"name\":\"partition_spec_id\",    \"type\":\"int\"},"
									+ "  {\"name\":\"content\",              \"type\":\"int\"},"
									+ "  {\"name\":\"sequence_number\",      \"type\":\"long\"},"
									+ "  {\"name\":\"min_sequence_number\",  \"type\":\"long\"},"
									+ "  {\"name\":\"added_snapshot_id\",    \"type\":\"long\"},"
									+ "  {\"name\":\"added_files_count\",    \"type\":\"int\"},"
									+ "  {\"name\":\"existing_files_count\", \"type\":\"int\"},"
									+ "  {\"name\":\"deleted_files_count\",  \"type\":\"int\"},"
									+ "  {\"name\":\"added_rows_count\",     \"type\":\"long\"},"
									+ "  {\"name\":\"existing_rows_count\",  \"type\":\"long\"},"
									+ "  {\"name\":\"deleted_rows_count\",   \"type\":\"long\"},"
									+ "  {\"name\":\"key_metadata\",         \"type\":[\"null\",\"bytes\"], \"default\":null},"
									+ "  {\"name\":\"partitions\",           \"type\":[\"null\",{\"type\":\"array\",\"items\":{"
									+ "    \"type\":\"record\",\"name\":\"r508\",\"fields\":["
									+ "      {\"name\":\"contains_null\",\"type\":\"boolean\"},"
									+ "      {\"name\":\"contains_nan\",\"type\":[\"null\",\"boolean\"],\"default\":null},"
									+ "      {\"name\":\"lower_bound\",\"type\":[\"null\",\"bytes\"],\"default\":null},"
									+ "      {\"name\":\"upper_bound\",\"type\":[\"null\",\"bytes\"],\"default\":null}"
									+ "    ]}}], \"default\":null},"
									+ "  {\"name\":\"first_row_id\",         \"type\":[\"null\",\"long\"], \"default\":null}"
									+ "]}");

	private final S3TablesStorageDriver<?, ?> driver;
	private final String warehouseLocation; // s3://bucket/prefix
	private final String warehouseS3Path;   // /bucket/prefix (for HTTP PUT URI)

	// Shared mutable state — safe because AtomicReference / AtomicLong
	private final AtomicReference<String> versionToken;
	private final AtomicLong snapshotIdCounter;
	private final AtomicLong rowIdCounter = new AtomicLong(0);

	IcebergCommitter(
					final S3TablesStorageDriver<?, ?> driver,
					final String warehouseLocation,
					final String versionToken) {
		this.driver = driver;
		this.warehouseLocation = warehouseLocation.endsWith("/")
						? warehouseLocation.substring(0, warehouseLocation.length() - 1)
						: warehouseLocation;
		this.warehouseS3Path = warehouseUriToPath(this.warehouseLocation);
		this.versionToken = new AtomicReference<>(versionToken);
		this.snapshotIdCounter = new AtomicLong(System.currentTimeMillis());
	}

	/**
	 * Holds intermediate state between commitDataPlane() and commitMetadata().
	 */
	static final class DataPlaneResult {
		final long snapshotId;
		final String manifestListS3Uri;
		final long fileSizeBytes;

		DataPlaneResult(final long snapshotId, final String manifestListS3Uri, final long fileSizeBytes) {
			this.snapshotId = snapshotId;
			this.manifestListS3Uri = manifestListS3Uri;
			this.fileSizeBytes = fileSizeBytes;
		}
	}

	/**
	 * Steps 1–6: generate Parquet bytes and PUT all data-plane files
	 * (data file, manifest, manifest list). Returns a DataPlaneResult
	 * for use by commitMetadata().
	 *
	 * @param parquetBytes Parquet file bytes (already generated)
	 * @param fileSizeBytes size of the Parquet file for manifest metadata
	 */
	DataPlaneResult commitDataPlane(final byte[] parquetBytes, final long fileSizeBytes) throws Exception {
		final String dataFileUuid = UUID.randomUUID().toString();
		final String dataFileKey = warehouseS3Path + "/data/" + dataFileUuid + ".parquet";
		final String dataFileS3Uri = warehouseLocation + "/data/" + dataFileUuid + ".parquet";

		// Step 2: PUT data file
		driver.executeDataPlaneRequest(dataFileKey, parquetBytes, "application/octet-stream");

		// Steps 3–4: manifest file
		final long snapshotId = snapshotIdCounter.incrementAndGet();
		final long baseRowId = rowIdCounter.getAndAdd(parquetBytes.length / 44 + 1);

		final String manifestUuid = UUID.randomUUID().toString();
		final String manifestKey = warehouseS3Path + "/metadata/" + manifestUuid + "-m0.avro";
		final String manifestS3Uri = warehouseLocation + "/metadata/" + manifestUuid + "-m0.avro";
		final byte[] manifestBytes = buildManifestFile(dataFileS3Uri, fileSizeBytes, snapshotId, baseRowId);
		driver.executeDataPlaneRequest(manifestKey, manifestBytes, "application/octet-stream");

		// Steps 5–6: manifest list
		final String snapUuid = UUID.randomUUID().toString();
		final String manifestListKey = warehouseS3Path + "/metadata/snap-" + snapshotId + "-" + snapUuid + ".avro";
		final String manifestListS3Uri = warehouseLocation + "/metadata/snap-" + snapshotId + "-" + snapUuid + ".avro";
		final byte[] manifestListBytes = buildManifestList(manifestS3Uri, manifestBytes.length, snapshotId);
		driver.executeDataPlaneRequest(manifestListKey, manifestListBytes, "application/octet-stream");

		return new DataPlaneResult(snapshotId, manifestListS3Uri, fileSizeBytes);
	}

	/**
	 * Steps 7–9: build and PUT table metadata JSON, then UpdateTableMetadataLocation.
	 * Retries on HTTP 409 with randomized jitter backoff.
	 * Must be called with the DataPlaneResult from a prior commitDataPlane() call.
	 */
	void commitMetadata(final DataPlaneResult dp) throws Exception {
		commitMetadataInternal(dp.snapshotId, dp.manifestListS3Uri, dp.fileSizeBytes);
	}

	/**
	 * Full commit (steps 1–9) — convenience for tests and any caller that doesn't
	 * need the timing split.
	 */
	long commit(final byte[] parquetBytes, final long fileSizeBytes) throws Exception {
		final DataPlaneResult dp = commitDataPlane(parquetBytes, fileSizeBytes);
		commitMetadataInternal(dp.snapshotId, dp.manifestListS3Uri, dp.fileSizeBytes);
		return dp.snapshotId;
	}

	/**
	 * Steps 7–9 implementation: build and PUT table metadata JSON, then UpdateTableMetadataLocation.
	 * Retries on HTTP 409 with randomized jitter backoff.
	 */
	private void commitMetadataInternal(
					final long snapshotId,
					final String manifestListS3Uri,
					final long fileSizeBytes) throws Exception {

		for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
			final String metaUuid = UUID.randomUUID().toString();
			final String metaKey = warehouseS3Path + "/metadata/v" + snapshotId + "-" + metaUuid + ".metadata.json";
			final String metaS3Uri = warehouseLocation + "/metadata/v" + snapshotId + "-" + metaUuid + ".metadata.json";

			final byte[] metaBytes = buildTableMetadata(snapshotId, manifestListS3Uri, metaS3Uri, fileSizeBytes);
			driver.executeDataPlaneRequest(metaKey, metaBytes, "application/json");

			// Step 9: UpdateTableMetadataLocation
			final String currentToken = versionToken.get();
			final int status = driver.getControlPlane().updateTableMetadataLocation(currentToken, metaS3Uri);

			if (status >= 200 && status < 300) {
				return; // committed
			}

			if (status == HTTP_CONFLICT) {
				driver.recordCommitRetry();
				// Jitter backoff then re-fetch token
				final int jitterMs = JITTER_MIN_MS +
								ThreadLocalRandom.current().nextInt(JITTER_MAX_MS - JITTER_MIN_MS);
				Thread.sleep(jitterMs);

				final String freshToken = driver.getControlPlane().getTableMetadataLocation().versionToken;
				versionToken.set(freshToken);

				Loggers.MSG.debug("{}: Iceberg commit 409, retry {}/{}, new token fetched",
								driver.getStepId(), attempt + 1, MAX_RETRIES);
				continue;
			}

			throw new Exception("UpdateTableMetadataLocation failed: HTTP " + status);
		}

		throw new Exception("UpdateTableMetadataLocation: exceeded max retries (" + MAX_RETRIES + ")");
	}

	// --- Avro/JSON builders ---

	private byte[] buildManifestFile(
					final String dataFileS3Uri,
					final long fileSizeBytes,
					final long snapshotId,
					final long baseRowId) throws IOException {

		final Schema dataFileSchema = MANIFEST_ENTRY_SCHEMA.getField("data_file").schema();

		final GenericRecord dataFile = new GenericData.Record(dataFileSchema);
		dataFile.put("content", 0); // DATA
		dataFile.put("file_path", dataFileS3Uri);
		dataFile.put("file_format", "PARQUET");
		dataFile.put("record_count", Math.max(1L, fileSizeBytes / 44));
		dataFile.put("file_size_in_bytes", fileSizeBytes);
		dataFile.put("column_sizes", null);
		dataFile.put("value_counts", null);
		dataFile.put("null_value_counts", null);
		dataFile.put("nan_value_counts", null);
		dataFile.put("lower_bounds", null);
		dataFile.put("upper_bounds", null);
		dataFile.put("key_metadata", null);
		dataFile.put("split_offsets", null);
		dataFile.put("equality_ids", null);
		dataFile.put("sort_order_id", null);

		final GenericRecord entry = new GenericData.Record(MANIFEST_ENTRY_SCHEMA);
		entry.put("status", 1); // ADDED
		entry.put("snapshot_id", snapshotId);
		entry.put("sequence_number", 1L);
		entry.put("file_sequence_number", 1L);
		entry.put("data_file", dataFile);

		return writeAvro(MANIFEST_ENTRY_SCHEMA, entry);
	}

	private byte[] buildManifestList(
					final String manifestS3Uri,
					final long manifestLength,
					final long snapshotId) throws IOException {

		final GenericRecord manifestFile = new GenericData.Record(MANIFEST_LIST_SCHEMA);
		manifestFile.put("manifest_path", manifestS3Uri);
		manifestFile.put("manifest_length", manifestLength);
		manifestFile.put("partition_spec_id", 0);
		manifestFile.put("content", 0); // DATA
		manifestFile.put("sequence_number", 1L);
		manifestFile.put("min_sequence_number", 1L);
		manifestFile.put("added_snapshot_id", snapshotId);
		manifestFile.put("added_files_count", 1);
		manifestFile.put("existing_files_count", 0);
		manifestFile.put("deleted_files_count", 0);
		manifestFile.put("added_rows_count", 1L);
		manifestFile.put("existing_rows_count", 0L);
		manifestFile.put("deleted_rows_count", 0L);
		manifestFile.put("key_metadata", null);
		manifestFile.put("partitions", null);
		manifestFile.put("first_row_id", null);

		return writeAvro(MANIFEST_LIST_SCHEMA, manifestFile);
	}

	private byte[] buildTableMetadata(
					final long snapshotId,
					final String manifestListS3Uri,
					final String metaS3Uri,
					final long fileSizeBytes) throws IOException {

		final ObjectNode meta = MAPPER.createObjectNode();
		meta.put("format-version", ICEBERG_FORMAT_VERSION);
		meta.put("table-uuid", UUID.randomUUID().toString());
		meta.put("location", warehouseLocation);
		meta.put("last-sequence-number", snapshotId);
		meta.put("last-updated-ms", System.currentTimeMillis());
		meta.put("last-column-id", 4);
		meta.put("current-snapshot-id", snapshotId);

		// schema
		final ObjectNode schema = MAPPER.createObjectNode();
		schema.put("type", "struct");
		schema.put("schema-id", 0);
		final ArrayNode fields = schema.putArray("fields");
		addField(fields, 1, "id", "required", "long");
		addField(fields, 2, "timestamp", "required", "long");
		addField(fields, 3, "value", "required", "double");
		addField(fields, 4, "payload", "required", "binary");
		meta.set("schema", schema);
		meta.putArray("schemas").add(schema);
		meta.put("default-spec-id", 0);
		meta.putArray("partition-specs").add(
						MAPPER.createObjectNode().put("spec-id", 0).putArray("fields"));
		meta.put("last-partition-id", 999);
		meta.put("default-sort-order-id", 0);
		meta.putArray("sort-orders").add(
						MAPPER.createObjectNode().put("order-id", 0).putArray("fields"));

		// snapshot
		final ObjectNode snapshot = MAPPER.createObjectNode();
		snapshot.put("snapshot-id", snapshotId);
		snapshot.put("timestamp-ms", System.currentTimeMillis());
		snapshot.put("manifest-list", manifestListS3Uri);
		snapshot.put("sequence-number", snapshotId);
		final ObjectNode summary = snapshot.putObject("summary");
		summary.put("operation", "append");
		summary.put("added-data-files", "1");
		summary.put("added-records", String.valueOf(Math.max(1, fileSizeBytes / 44)));
		summary.put("added-files-size", String.valueOf(fileSizeBytes));
		snapshot.put("schema-id", 0);
		meta.putArray("snapshots").add(snapshot);
		meta.putArray("refs");

		// current metadata file location (self-referential, required by spec)
		meta.put("metadata-log", metaS3Uri);
		meta.putArray("statistics");
		meta.putArray("partition-statistics");

		return MAPPER.writeValueAsBytes(meta);
	}

	private static void addField(
					final ArrayNode fields,
					final int id,
					final String name,
					final String required,
					final String type) {
		final ObjectNode f = fields.addObject();
		f.put("id", id);
		f.put("name", name);
		f.put("required", "required".equals(required));
		f.put("type", type);
	}

	@SuppressWarnings("unchecked")
	private static <T extends GenericRecord> byte[] writeAvro(
					final Schema schema,
					final T record) throws IOException {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DatumWriter<T> writer = new GenericDatumWriter<>(schema);
		try (final DataFileWriter<T> dfw = new DataFileWriter<>(writer)) {
			dfw.create(schema, baos);
			dfw.append(record);
		}
		return baos.toByteArray();
	}

	/** Converts s3://bucket/path to /bucket/path for HTTP PUT URI. */
	private static String warehouseUriToPath(final String s3Uri) {
		if (s3Uri.startsWith("s3://")) {
			return "/" + s3Uri.substring(5);
		}
		if (s3Uri.startsWith("s3a://")) {
			return "/" + s3Uri.substring(6);
		}
		return s3Uri;
	}

	private static Schema parseAvroSchema(final String json) {
		return new Schema.Parser().parse(json);
	}

	/** Result from GetTableMetadataLocation. */
	static final class MetadataLocationResult {
		final String metadataLocation;
		final String versionToken;

		MetadataLocationResult(final String metadataLocation, final String versionToken) {
			this.metadataLocation = metadataLocation;
			this.versionToken = versionToken;
		}
	}
}
