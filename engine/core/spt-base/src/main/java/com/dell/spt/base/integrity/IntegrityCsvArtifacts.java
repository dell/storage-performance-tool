package com.dell.spt.base.integrity;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** Canonical RFC 4180 rows for logger-backed integrity artifacts. */
public final class IntegrityCsvArtifacts {

	public static final String FAILURES_FILE_NAME = "integrity.failures.csv";
	public static final String PERFORMANCE_FILE_NAME = "integrity.performance.csv";
	public static final String MULTIPART_LIFECYCLE_FILE_NAME = "multipart.lifecycle.csv";

	public static final String FAILURES_HEADER = "timestamp,node,step,driver,key,requested_version_id,returned_version_id,request_id,reason,algorithm,expected_digest,actual_digest,expected_size,actual_size,first_mismatch_offset,attempt";
	public static final String PERFORMANCE_HEADER = "node,step,driver,phase,algorithm,objects,bytes,hash_worker_seconds,mean_worker_hash_mib_per_second,time_to_first_request_seconds,additional_payload_passes";
	public static final String MULTIPART_LIFECYCLE_HEADER = "timestamp,node,step,driver,bucket,key,upload_id,state,abort_attempted,abort_succeeded,error";

	private static final double BYTES_PER_MIB = 1024.0 * 1024.0;
	private static final String MULTIPART_LIFECYCLE_EMITTED_CONTEXT_KEY = "spt.integrity.multipart.lifecycle.emitted";

	private IntegrityCsvArtifacts() {}

	public static List<Artifact> applicableHeaders(
					final OpType opType, final String driverType, final boolean metadataMode) {
		return applicableHeaders(opType, driverType, metadataMode, true);
	}

	public static List<Artifact> applicableHeaders(
					final OpType opType,
					final String driverType,
					final boolean metadataMode,
					final boolean multipartEnabled) {
		if (!metadataMode) {
			return List.of();
		}
		if (opType == OpType.READ) {
			return List.of(
							new Artifact(Kind.FAILURES, FAILURES_HEADER),
							new Artifact(Kind.PERFORMANCE, PERFORMANCE_HEADER));
		}
		if (opType == OpType.CREATE) {
			if (multipartEnabled && driverType != null && driverType.toLowerCase(Locale.ROOT).startsWith("s3")) {
				return List.of(
								new Artifact(Kind.PERFORMANCE, PERFORMANCE_HEADER),
								new Artifact(Kind.MULTIPART_LIFECYCLE, MULTIPART_LIFECYCLE_HEADER));
			}
			return List.of(new Artifact(Kind.PERFORMANCE, PERFORMANCE_HEADER));
		}
		return List.of();
	}

	public static String failureRecord(
					final String node,
					final String step,
					final String driver,
					final Operation<?> operation) {
		final IntegrityVerificationResult result = operation.integrityVerificationResult();
		if (result == null || result.failureReason() == null) {
			throw new IllegalArgumentException("terminal integrity failure result is required");
		}
		final IntegrityMetadata expected = result.expected();
		return record(
						Instant.now().toString(), node, step, driver,
						operation.item() == null ? "" : operation.item().name(),
						operation.requestedVersionId(), operation.returnedVersionId(), operation.responseRequestId(),
						result.failureReason().value(),
						expected == null ? IntegrityMetadataCodec.ALGORITHM_SHA256 : expected.algorithm(),
						expected == null ? null : expected.digest(), result.actualDigest(),
						expected == null ? null : expected.size(), result.actualSize(), null,
						operation.opRetryCount() + 1);
	}

	public static String performanceRecord(
					final String node,
					final String step,
					final String driver,
					final String phase,
					final IntegrityPerformanceAccumulator.Snapshot snapshot) {
		final double workerSeconds = snapshot.workerNanos() / 1_000_000_000.0;
		final String meanRate = workerSeconds <= 0
						? null
						: decimal((snapshot.bytes() / BYTES_PER_MIB) / workerSeconds);
		final String firstRequestSeconds = snapshot.firstRequestDelayNanos() < 0
						? null
						: decimal(snapshot.firstRequestDelayNanos() / 1_000_000_000.0);
		return record(
						node, step, driver, phase, IntegrityMetadataCodec.ALGORITHM_SHA256,
						snapshot.objects(), snapshot.bytes(), decimal(workerSeconds), meanRate,
						firstRequestSeconds, snapshot.additionalPayloadPasses());
	}

	public static boolean logMultipartLifecycleOnce(
					final CompositeOperation<?> operation,
					final String node,
					final String step,
					final String driver,
					final String bucket,
					final String key,
					final String uploadId,
					final String state,
					final Boolean abortAttempted,
					final Boolean abortSucceeded,
					final String error) {
		synchronized (operation) {
			if (Boolean.parseBoolean(operation.get(MULTIPART_LIFECYCLE_EMITTED_CONTEXT_KEY))) {
				return false;
			}
			operation.put(MULTIPART_LIFECYCLE_EMITTED_CONTEXT_KEY, Boolean.TRUE.toString());
		}
		Loggers.MULTIPART_LIFECYCLE.info(multipartLifecycleRecord(
						node, step, driver, bucket, key, uploadId, state,
						abortAttempted, abortSucceeded, error));
		return true;
	}

	public static String multipartLifecycleRecord(
					final String node,
					final String step,
					final String driver,
					final String bucket,
					final String key,
					final String uploadId,
					final String state,
					final Boolean abortAttempted,
					final Boolean abortSucceeded,
					final String error) {
		return record(
						Instant.now().toString(), node, step, driver, bucket, key, uploadId,
						state, abortAttempted, abortSucceeded, sanitize(error));
	}

	public static String nodeIdentity() {
		final String rmiHost = System.getProperty("java.rmi.server.hostname");
		if (rmiHost != null && !rmiHost.isBlank()) {
			return rmiHost;
		}
		final String host = System.getenv("HOSTNAME");
		return host == null || host.isBlank() ? "local" : host;
	}

	private static String decimal(final double value) {
		return String.format(Locale.ROOT, "%.9f", value);
	}

	private static String sanitize(final String value) {
		if (value == null) {
			return null;
		}
		final var result = new StringBuilder(Math.min(value.length(), 1024));
		for (var i = 0; i < value.length() && result.length() < 1024; i++) {
			final char ch = value.charAt(i);
			result.append(Character.isISOControl(ch) ? ' ' : ch);
		}
		return result.toString();
	}

	private static String record(final Object... values) {
		final var output = new StringWriter();
		try (final var printer = new CSVPrinter(output, CSVFormat.RFC4180)) {
			printer.printRecord(values);
		} catch (final IOException e) {
			throw new IllegalStateException("failed to format integrity CSV record", e);
		}
		final String value = output.toString();
		return value.endsWith("\r\n") ? value.substring(0, value.length() - 2) : value;
	}

	public enum Kind {
		FAILURES, PERFORMANCE, MULTIPART_LIFECYCLE
	}

	public record Artifact(Kind kind, String header) {}
}
