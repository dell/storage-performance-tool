package com.dell.spt.base.metrics;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import java.util.List;

/** Cached snapshot of a finished step used to keep /metrics/json non-empty at idle. */
public final class TerminalStepEntry {
	public final String stepId;
	public final OpType opType;
	public final long runId;
	public final long recordedAtMillis;

	public final long successCount;
	public final long failedCount;
	public final long corruptCount;
	public final long bytesTotal;
	public final double latencyMeanUs;
	public final double durationMeanUs;
	public final TimingMetricSnapshot latencySnapshot;
	public final TimingMetricSnapshot durationSnapshot;
	public final TimingMetricSnapshot ttfbSnapshot;
	public final long concurrencyLast;
	public final double concurrencyMean;

	public final long countLimit;
	public final long timeLimitSec;

	public final long elapsedTimeMillis;

	public final boolean distributed;
	public final int nodeCount;
	public final List<String> nodesPresent;
	public final List<String> contributorsPresent;
	public final boolean partial;
	/** Optional detailed DELETE metrics retained for terminal API responses. */
	public final DeleteMetricsSnapshot deleteMetrics;
	/** Whether this step opted in to the standalone detailed DELETE contract. */
	public final boolean deleteDetailsExpected;

	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						null,
						null,
						null,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						false,
						0,
						List.of(),
						false);
	}

	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					TimingMetricSnapshot latencySnapshot,
					TimingMetricSnapshot durationSnapshot,
					TimingMetricSnapshot ttfbSnapshot,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					boolean partial) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						0,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						latencySnapshot,
						durationSnapshot,
						ttfbSnapshot,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						partial,
						null);
	}

	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long corruptCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					TimingMetricSnapshot latencySnapshot,
					TimingMetricSnapshot durationSnapshot,
					TimingMetricSnapshot ttfbSnapshot,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					boolean partial) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						corruptCount,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						latencySnapshot,
						durationSnapshot,
						ttfbSnapshot,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						partial,
						null);
	}

	/**
	 * Creates a terminal metrics entry with optional detailed DELETE measurements.
	 *
	 * @param stepId load-step identifier
	 * @param opType operation type
	 * @param runId run identifier
	 * @param recordedAtMillis terminal snapshot wall-clock timestamp
	 * @param successCount successful logical operations
	 * @param failedCount failed logical operations
	 * @param corruptCount corrupt logical operations
	 * @param bytesTotal transferred bytes
	 * @param latencyMeanUs mean request latency in microseconds
	 * @param durationMeanUs mean request duration in microseconds
	 * @param latencySnapshot request-latency distribution
	 * @param durationSnapshot request-duration distribution
	 * @param ttfbSnapshot time-to-first-byte distribution
	 * @param concurrencyLast last observed concurrency
	 * @param concurrencyMean mean observed concurrency
	 * @param countLimit configured operation-count limit
	 * @param timeLimitSec configured duration limit in seconds
	 * @param elapsedTimeMillis elapsed step time in milliseconds
	 * @param distributed whether the entry represents distributed execution
	 * @param nodeCount participating node count
	 * @param nodesPresent participating node identifiers
	 * @param partial whether the distributed result is incomplete
	 * @param deleteMetrics optional detailed DELETE metrics
	 */
	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long corruptCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					TimingMetricSnapshot latencySnapshot,
					TimingMetricSnapshot durationSnapshot,
					TimingMetricSnapshot ttfbSnapshot,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					boolean partial,
					DeleteMetricsSnapshot deleteMetrics) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						corruptCount,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						latencySnapshot,
						durationSnapshot,
						ttfbSnapshot,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						partial,
						deleteMetrics,
						deleteMetrics != null);
	}

	/** Creates a terminal entry while retaining an explicit standalone-detail expectation. */
	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long corruptCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					TimingMetricSnapshot latencySnapshot,
					TimingMetricSnapshot durationSnapshot,
					TimingMetricSnapshot ttfbSnapshot,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					boolean partial,
					DeleteMetricsSnapshot deleteMetrics,
					boolean deleteDetailsExpected) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						corruptCount,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						latencySnapshot,
						durationSnapshot,
						ttfbSnapshot,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						List.of(),
						partial,
						deleteMetrics,
						deleteDetailsExpected);
	}

	/** Creates a terminal entry with separate legacy node and contributor identity presentations. */
	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long corruptCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					TimingMetricSnapshot latencySnapshot,
					TimingMetricSnapshot durationSnapshot,
					TimingMetricSnapshot ttfbSnapshot,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					List<String> contributorsPresent,
					boolean partial,
					DeleteMetricsSnapshot deleteMetrics,
					boolean deleteDetailsExpected) {
		this.stepId = stepId;
		this.opType = opType;
		this.runId = runId;
		this.recordedAtMillis = recordedAtMillis;
		this.successCount = successCount;
		this.failedCount = failedCount;
		this.corruptCount = corruptCount;
		this.bytesTotal = bytesTotal;
		this.latencyMeanUs = latencyMeanUs;
		this.durationMeanUs = durationMeanUs;
		this.latencySnapshot = latencySnapshot;
		this.durationSnapshot = durationSnapshot;
		this.ttfbSnapshot = ttfbSnapshot;
		this.concurrencyLast = concurrencyLast;
		this.concurrencyMean = concurrencyMean;
		this.countLimit = countLimit;
		this.timeLimitSec = timeLimitSec;
		this.elapsedTimeMillis = elapsedTimeMillis;
		this.distributed = distributed;
		this.nodeCount = nodeCount;
		this.nodesPresent = nodesPresent == null ? List.of() : List.copyOf(nodesPresent);
		this.contributorsPresent = contributorsPresent == null ? List.of() : List.copyOf(contributorsPresent);
		this.partial = partial;
		this.deleteMetrics = deleteMetrics;
		this.deleteDetailsExpected = deleteDetailsExpected;
	}

	TerminalStepEntry withDeleteFailureOutcome(final String outcome) {
		if (deleteMetrics == null) {
			return this;
		}
		return new TerminalStepEntry(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						corruptCount,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						latencySnapshot,
						durationSnapshot,
						ttfbSnapshot,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						contributorsPresent,
						partial,
						deleteMetrics.toBuilder().failureOutcome(outcome).build(),
						deleteDetailsExpected);
	}

	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					boolean partial) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						null,
						null,
						null,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						partial);
	}

	/** Creates a compatibility terminal entry with additive contributor identity presentation. */
	public TerminalStepEntry(
					String stepId,
					OpType opType,
					long runId,
					long recordedAtMillis,
					long successCount,
					long failedCount,
					long bytesTotal,
					double latencyMeanUs,
					double durationMeanUs,
					long concurrencyLast,
					double concurrencyMean,
					long countLimit,
					long timeLimitSec,
					long elapsedTimeMillis,
					boolean distributed,
					int nodeCount,
					List<String> nodesPresent,
					List<String> contributorsPresent,
					boolean partial) {
		this(
						stepId,
						opType,
						runId,
						recordedAtMillis,
						successCount,
						failedCount,
						0,
						bytesTotal,
						latencyMeanUs,
						durationMeanUs,
						null,
						null,
						null,
						concurrencyLast,
						concurrencyMean,
						countLimit,
						timeLimitSec,
						elapsedTimeMillis,
						distributed,
						nodeCount,
						nodesPresent,
						contributorsPresent,
						partial,
						null,
						false);
	}
}
