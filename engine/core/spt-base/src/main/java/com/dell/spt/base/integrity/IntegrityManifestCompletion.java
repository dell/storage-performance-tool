package com.dell.spt.base.integrity;

import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Identity-bound completion record for a canonical integrity manifest.
 *
 * <p>Version 1 is a closed schema: unknown members and trailing JSON values are rejected.
 */
public record IntegrityManifestCompletion(
                int version,
                String status,
                @JsonProperty("run_id") long runId,
                @JsonProperty("producer_kind") String producerKind,
                @JsonProperty("producer_id") String producerId,
                String artifact,
                @JsonProperty("source_record_count") long sourceRecordCount,
                @JsonProperty("unique_record_count") long uniqueRecordCount,
                @JsonProperty("selected_record_count") long selectedRecordCount,
                @JsonProperty("manifest_bytes") long manifestBytes,
                @JsonProperty("manifest_sha256") String manifestSha256) {

    public static final int VERSION = 1;
    public static final String STATUS_COMPLETE = "complete";
    public static final String PRODUCER_ENGINE_STEP = "engine_step";
    public static final String PRODUCER_CLI_STAGER = "cli_stager";

    private static final ObjectMapper JSON = new ObjectMapper()
                    .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    public static Path emissionCountPath(final Path manifest) {
        return manifest.resolveSibling(manifest.getFileName() + ".emitted.count");
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
        requirePositiveRunId(runId);
        validateCounts(sourceCount, uniqueCount, selectedCount);
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
                        Files.size(manifest),
                        sha256(manifest));
    }

    public void publish(final Path manifest) throws IOException {
		publish(manifest, null);
	}

	void publish(
				final Path manifest,
				final CrashDurableFilePublisher.Operations testOperations)
				throws IOException {
        final Path marker = completionPath(manifest);
        if (Files.exists(marker)) {
            throw new IOException("refusing to replace existing completion record " + marker);
        }
        final Path parent = marker.toAbsolutePath().getParent();
        Files.createDirectories(parent);
        final Path staging = Files.createTempFile(parent, "." + marker.getFileName(), ".staging");
        boolean committed = false;
        try {
            JSON.writerWithDefaultPrettyPrinter().writeValue(staging.toFile(), this);
			if (testOperations == null) {
				atomicMove(staging, marker);
			} else {
				CrashDurableFilePublisher.publish(staging, marker, testOperations);
			}
            committed = true;
        } finally {
            if (!committed) {
                Files.deleteIfExists(staging);
            }
        }
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
        if (!IntegrityManifestItemInput.hasCanonicalHeader(manifest)) {
            throw new IOException("integrity input manifest has a noncanonical header: " + manifest);
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
        if (record.version != VERSION
                        || !STATUS_COMPLETE.equals(record.status)
                        || record.runId != expectedRunId
                        || !expectedKind.equals(record.producerKind)
                        || !requireText(expectedProducerId, "expected producer id").equals(record.producerId)
                        || !manifest.getFileName().toString().equals(record.artifact)
                        || record.manifestBytes < 0
                        || record.manifestBytes != Files.size(manifest)
                        || !sha256(manifest).equals(record.manifestSha256)) {
            throw new IOException("integrity input completion record does not match manifest identity");
        }
        long rows = 0;
        try (final var input = new IntegrityManifestItemInput(manifest)) {
            while (input.get() != null) {
                rows++;
            }
        }
        if (rows != record.selectedRecordCount) {
            throw new IOException(
                            "integrity input row count " + rows + " does not match completion record "
                                            + record.selectedRecordCount);
        }
        return record;
    }

    public static void atomicMove(final Path source, final Path target) throws IOException {
        CrashDurableFilePublisher.publish(source, target);
    }

    private static String sha256(final Path path) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
        try (InputStream in = Files.newInputStream(path); DigestInputStream hashing = new DigestInputStream(in, digest)) {
            hashing.transferTo(OutputStreamNull.INSTANCE.stream);
        }
        return HexFormat.of().formatHex(digest.digest());
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

    private enum OutputStreamNull {
        INSTANCE;

        private final java.io.OutputStream stream = java.io.OutputStream.nullOutputStream();
    }}
