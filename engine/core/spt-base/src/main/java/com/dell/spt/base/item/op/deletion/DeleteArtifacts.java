package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.integrity.IntegrityCsvFormat;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import org.apache.commons.csv.CSVPrinter;

/** Stable versioned DELETE artifact names, schemas, units, and identities. */
public final class DeleteArtifacts {
	public static final String SCHEMA_VERSION = "1";
	public static final String REQUEST_UNIT = "logical_api_requests";
	public static final String OBJECT_UNIT = "object_identities";
	public static final String METRICS_FILE_NAME = "delete.metrics.total.csv";
	public static final String REQUESTS_FILE_NAME = "delete.requests.csv";
	public static final String OBJECTS_FILE_NAME = "delete.objects.csv";
	public static final String VERIFICATION_FILE_NAME = "delete.verification.csv";
	public static final String RESIDUAL_FILE_NAME = "items.csv";
	public static final String SELECTION_FILE_NAME = "verify-input.csv";
	public static final String SELECTION_COMPLETION_FILE_NAME = "verify-input.complete.json";
	public static final String COMPLETION_FILE_NAME = "delete.complete.json";

	public static final List<String> METRICS_HEADER = List.of(
					"schema_version", "request_unit", "object_unit", "batch_unit", "mode",
					"configured_batch_size", "selection_order", "requests_attempted",
					"requests_full_success", "requests_partial", "requests_failed",
					"requests_unresolved", "objects_selected", "objects_attempted",
					"objects_accepted", "objects_failed", "objects_unattempted",
					"objects_unresolved", "batch_actual_requests", "batch_actual_objects",
					"batch_full_count", "batch_partial_count", "terminal_reconciled");
	public static final List<String> REQUESTS_HEADER = List.of(
					"schema_version", "request_id", "batch_id", "target_count", "outcome",
					"node", "start_us", "duration_us", "latency_us");
	public static final List<String> OBJECTS_HEADER = List.of(
					"schema_version", "request_id", "target_id", "target_index", "bucket", "key",
					"size", "version_id", "outcome", "error_classification", "error");
	public static final List<String> VERIFICATION_HEADER = List.of(
					"schema_version", "target_id", "target_index", "bucket", "key", "size", "version_id",
					"operational_outcome", "pre_enabled", "pre_presence", "post_enabled", "post_presence",
					"correctness_failure", "inconclusive", "residual");

	private DeleteArtifacts() {}

	/** Returns a content-stable identity for one immutable logical request/batch. */
	public static String requestId(final DeleteRequest request) {
		return "delete-request-" + digest(request.targets());
	}

	/** Returns a content-stable identity for one immutable target. */
	public static String targetId(final DeleteTarget target) {
		return "delete-target-" + digest(List.of(target));
	}

	/** Returns whether an unverified target remains conservative recovery input. */
	public static boolean isResidual(final DeleteTargetOutcome outcome) {
		return outcome != DeleteTargetOutcome.ACCEPTED;
	}

	/** Encodes exactly one RFC 4180 record without its trailing line separator. */
	public static String csvLine(final Object... values) {
		final var output = new StringWriter();
		try (final var printer = new CSVPrinter(output, IntegrityCsvFormat.RFC4180_LF)) {
			printer.printRecord(values);
		} catch (final IOException impossible) {
			throw new AssertionError(impossible);
		}
		final String line = output.toString();
		return line.endsWith("\n") ? line.substring(0, line.length() - 1) : line;
	}

	private static String digest(final List<DeleteTarget> targets) {
		try {
			final var bytes = new ByteArrayOutputStream();
			try (final var output = new DataOutputStream(bytes)) {
				output.writeInt(targets.size());
				for (final DeleteTarget target : targets) {
					writeText(output, target.bucket());
					writeText(output, target.key());
					output.writeLong(target.size());
					writeText(output, target.versionId() == null ? "" : target.versionId());
				}
			}
			return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
		} catch (final IOException impossible) {
			throw new AssertionError(impossible);
		} catch (final NoSuchAlgorithmException impossible) {
			throw new AssertionError("SHA-256 is unavailable", impossible);
		}
	}

	private static void writeText(final DataOutputStream output, final String value) throws IOException {
		final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
		output.writeInt(bytes.length);
		output.write(bytes);
	}
}
