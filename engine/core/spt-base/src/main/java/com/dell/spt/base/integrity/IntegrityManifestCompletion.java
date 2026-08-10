package com.dell.spt.base.integrity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Identity-bound completion record for a canonical integrity manifest.
 *
 * <p>Every supported version is a closed schema: unknown members and trailing JSON values are rejected.
 */
public final class IntegrityManifestCompletion {

	@JsonProperty("version")
	private final int version;

	@JsonProperty("status")
	private final String status;

	@JsonProperty("run_id")
	private final long runId;

	@JsonProperty("producer_kind")
	private final String producerKind;

	@JsonProperty("producer_id")
	private final String producerId;

	@JsonProperty("artifact")
	private final String artifact;

	@JsonProperty("source_record_count")
	private final long sourceRecordCount;

	@JsonProperty("unique_record_count")
	private final long uniqueRecordCount;

	@JsonProperty("selected_record_count")
	private final long selectedRecordCount;

	@JsonProperty("excluded_delete_marker_count")
	private final long excludedDeleteMarkerCount;

	@JsonProperty("manifest_bytes")
	private final long manifestBytes;

	@JsonProperty("manifest_sha256")
	private final String manifestSha256;

	@JsonCreator
	public IntegrityManifestCompletion(
					@JsonProperty("version") final int version,
					@JsonProperty("status") final String status,
					@JsonProperty("run_id") final long runId,
					@JsonProperty("producer_kind") final String producerKind,
					@JsonProperty("producer_id") final String producerId,
					@JsonProperty("artifact") final String artifact,
					@JsonProperty("source_record_count") final long sourceRecordCount,
					@JsonProperty("unique_record_count") final long uniqueRecordCount,
					@JsonProperty("selected_record_count") final long selectedRecordCount,
					@JsonProperty("excluded_delete_marker_count") final long excludedDeleteMarkerCount,
					@JsonProperty("manifest_bytes") final long manifestBytes,
					@JsonProperty("manifest_sha256") final String manifestSha256) {
		this.version = version;
		this.status = status;
		this.runId = runId;
		this.producerKind = producerKind;
		this.producerId = producerId;
		this.artifact = artifact;
		this.sourceRecordCount = sourceRecordCount;
		this.uniqueRecordCount = uniqueRecordCount;
		this.selectedRecordCount = selectedRecordCount;
		this.excludedDeleteMarkerCount = excludedDeleteMarkerCount;
		this.manifestBytes = manifestBytes;
		this.manifestSha256 = manifestSha256;
	}

	public int version() {
		return version;
	}

	public String status() {
		return status;
	}

	public long runId() {
		return runId;
	}

	public String producerKind() {
		return producerKind;
	}

	public String producerId() {
		return producerId;
	}

	public String artifact() {
		return artifact;
	}

	public long sourceRecordCount() {
		return sourceRecordCount;
	}

	public long uniqueRecordCount() {
		return uniqueRecordCount;
	}

	public long selectedRecordCount() {
		return selectedRecordCount;
	}

	public long excludedDeleteMarkerCount() {
		return excludedDeleteMarkerCount;
	}

	public long manifestBytes() {
		return manifestBytes;
	}

	public String manifestSha256() {
		return manifestSha256;
	}

	public static final int LEGACY_VERSION = 1;
	public static final int VERSION = 2;
	public static final String STATUS_COMPLETE = "complete";
	public static final String PRODUCER_ENGINE_STEP = "engine_step";
	public static final String PRODUCER_CLI_STAGER = "cli_stager";

	private static final ObjectMapper JSON = new ObjectMapper()
					.enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
					.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

	public static Path emissionCountPath(final Path manifest) {
		return manifest.resolveSibling(manifest.getFileName() + ".emitted.count");
	}

	public static Path deleteMarkerCountPath(final Path manifest) {
		return manifest.resolveSibling(manifest.getFileName() + ".delete-markers.count");
	}

	public static Path completionPath(final Path manifest) {
		final String name = manifest.getFileName().toString();
		final int suffix = name.lastIndexOf(".csv");
		final String base = suffix >= 0 ? name.substring(0, suffix) : name;
		return manifest.resolveSibling(base + ".complete.json");
	}

	public static IntegrityManifestCompletion create(
					final Path manifest,
					final long runId,
					final String producerKind,
					final String producerId,
					final long sourceCount,
					final long uniqueCount,
					final long selectedCount)
					throws IOException {
		return create(
						manifest, runId, producerKind, producerId,
						sourceCount, uniqueCount, selectedCount, 0);
	}

	public static IntegrityManifestCompletion create(
					final Path manifest,
					final long runId,
					final String producerKind,
					final String producerId,
					final long sourceCount,
					final long uniqueCount,
					final long selectedCount,
					final long excludedDeleteMarkerCount)
					throws IOException {
		requirePositiveRunId(runId);
		validateCounts(sourceCount, uniqueCount, selectedCount);
		if (excludedDeleteMarkerCount < 0) {
			throw new IOException("excluded delete marker count must be nonnegative");
		}
		final IntegrityManifestValidator.Evidence evidence = IntegrityManifestValidator.validate(manifest);
		if (evidence.rows() != selectedCount) {
			throw new IOException(
							"integrity manifest row count " + evidence.rows()
											+ " does not match selected count " + selectedCount);
		}
		return new IntegrityManifestCompletion(
						VERSION,
						STATUS_COMPLETE,
						runId,
						producerKind,
						requireText(producerId, "producer_id"),
						manifest.getFileName().toString(),
						sourceCount,
						uniqueCount,
						selectedCount,
						excludedDeleteMarkerCount,
						evidence.bytes(),
						evidence.sha256());
	}

	public void publish(final Path manifest) throws IOException {
		publish(manifest, null);
	}

	void publish(
					final Path manifest,
					final CrashDurableFilePublisher.Operations testOperations)
					throws IOException {
		publish(manifest, testOperations, null);
	}

	void publish(
					final Path manifest,
					final CrashDurableFilePublisher.Operations testOperations,
					final FailurePreservingCleanup.IOAction testCleanup)
					throws IOException {
		final Path marker = completionPath(manifest);
		if (Files.exists(marker)) {
			throw new IOException("refusing to replace existing completion record " + marker);
		}
		final Path parent = marker.toAbsolutePath().getParent();
		Files.createDirectories(parent);
		final Path staging = Files.createTempFile(parent, "." + marker.getFileName(), ".staging");
		final FailurePreservingCleanup.IOAction cleanup = testCleanup == null ? () -> Files.deleteIfExists(staging) : testCleanup;
		FailurePreservingCleanup.onFailure(
						() -> {
							JSON.writerWithDefaultPrettyPrinter().writeValue(staging.toFile(), this);
							if (testOperations == null) {
								atomicMove(staging, marker);
							} else {
								CrashDurableFilePublisher.publish(staging, marker, testOperations);
							}
							return null;
						},
						cleanup);
	}

	public static IntegrityManifestCompletion validate(
					final Path manifest,
					final long expectedRunId,
					final IntegrityInputProvenance provenance,
					final String expectedProducerId)
					throws IOException {
		requirePositiveRunId(expectedRunId);
		if (!Files.isRegularFile(manifest)) {
			throw new IOException("integrity input manifest is missing: " + manifest);
		}
		final Path marker = completionPath(manifest);
		if (!Files.isRegularFile(marker)) {
			throw new IOException("integrity input completion record is missing: " + marker);
		}
		final IntegrityManifestCompletion record = JSON.readValue(marker.toFile(), IntegrityManifestCompletion.class);
		validateCounts(record.sourceRecordCount, record.uniqueRecordCount, record.selectedRecordCount);
		final String expectedKind = switch (provenance) {
		case ENGINE_STEP -> PRODUCER_ENGINE_STEP;
		case CLI_STAGER -> PRODUCER_CLI_STAGER;
		default -> throw new IOException("completion records are not valid for provenance " + provenance);
		};
		if ((record.version != VERSION && record.version != LEGACY_VERSION)
						|| !STATUS_COMPLETE.equals(record.status)
						|| record.runId != expectedRunId
						|| !expectedKind.equals(record.producerKind)
						|| !requireText(expectedProducerId, "expected producer id").equals(record.producerId)
						|| !manifest.getFileName().toString().equals(record.artifact)
						|| record.manifestBytes < 0
						|| record.excludedDeleteMarkerCount < 0
						|| (record.version == LEGACY_VERSION && record.excludedDeleteMarkerCount != 0)) {
			throw new IOException("integrity input completion record does not match manifest identity");
		}
		final IntegrityManifestValidator.Evidence evidence = IntegrityManifestValidator.validate(manifest);
		if (evidence.bytes() != record.manifestBytes
						|| !evidence.sha256().equals(record.manifestSha256)) {
			throw new IOException("integrity input completion record does not match manifest identity");
		}
		if (evidence.rows() != record.selectedRecordCount) {
			throw new IOException(
							"integrity input row count " + evidence.rows() + " does not match completion record "
											+ record.selectedRecordCount);
		}
		return record;
	}

	public static void atomicMove(final Path source, final Path target) throws IOException {
		CrashDurableFilePublisher.publish(source, target);
	}

	private static String requireText(final String value, final String field) throws IOException {
		if (value == null || value.isBlank()) {
			throw new IOException(field + " must not be empty");
		}
		return value;
	}

	private static void validateCounts(
					final long sourceCount, final long uniqueCount, final long selectedCount)
					throws IOException {
		if (sourceCount < 0 || uniqueCount < 0 || selectedCount < 0
						|| sourceCount < uniqueCount || uniqueCount < selectedCount) {
			throw new IOException("completion counts must satisfy source >= unique >= selected >= 0");
		}
	}

	private static void requirePositiveRunId(final long runId) throws IOException {
		if (runId <= 0) {
			throw new IOException("run_id must be positive");
		}
	}

}
