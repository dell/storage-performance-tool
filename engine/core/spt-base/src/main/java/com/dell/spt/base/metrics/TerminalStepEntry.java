package com.dell.spt.base.metrics;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsTotal;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import java.util.List;

/** Cached snapshot of a finished step used to keep /metrics/json non-empty at idle. */
public final class TerminalStepEntry {
	public final String stepId;
	public final OpType opType;
	public final long runId;
	public final String clusterId;
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
	/** The storage driver's own statistics at the end of the step; null when it reports none. */
	public final DriverStatsSnapshot driverStats;
	/** For a distributed step, the total of its contributors' driver statistics; null otherwise. */
	public final DriverStatsTotal driverStatsTotal;

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

	/**
	 * Compatibility constructor for extensions compiled before run cluster identity was retained.
	 * Delegates to the full constructor; a missing cluster ID uses the API startup fallback.
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
					List<String> contributorsPresent,
					boolean partial,
					DeleteMetricsSnapshot deleteMetrics,
					boolean deleteDetailsExpected) {
		this(stepId, opType, runId, recordedAtMillis, successCount, failedCount, corruptCount, bytesTotal, latencyMeanUs, durationMeanUs, latencySnapshot, durationSnapshot, ttfbSnapshot, concurrencyLast, concurrencyMean, countLimit, timeLimitSec, elapsedTimeMillis, distributed, nodeCount, nodesPresent, contributorsPresent, partial, deleteMetrics, deleteDetailsExpected, null);
	}

	/**
	 * Compatibility constructor for extensions compiled before storage driver statistics were
	 * retained. Delegates to the full constructor without statistics.
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
					List<String> contributorsPresent,
					boolean partial,
					DeleteMetricsSnapshot deleteMetrics,
					boolean deleteDetailsExpected,
					String clusterId) {
		this(stepId, opType, runId, recordedAtMillis, successCount, failedCount, corruptCount, bytesTotal, latencyMeanUs, durationMeanUs, latencySnapshot, durationSnapshot, ttfbSnapshot, concurrencyLast, concurrencyMean, countLimit, timeLimitSec, elapsedTimeMillis, distributed, nodeCount, nodesPresent, contributorsPresent, partial, deleteMetrics, deleteDetailsExpected, clusterId, null, null);
	}

	/**
	 * Creates a terminal entry retaining storage driver statistics: a node's own in {@code
	 * driverStats}, or the total of a distributed step in {@code driverStatsTotal}.
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
					List<String> contributorsPresent,
					boolean partial,
					DeleteMetricsSnapshot deleteMetrics,
					boolean deleteDetailsExpected,
					String clusterId,
					DriverStatsSnapshot driverStats,
					DriverStatsTotal driverStatsTotal) {
		this.stepId = stepId;
		this.opType = opType;
		this.runId = runId;
		this.clusterId = clusterId;
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
		this.driverStats = driverStats;
		this.driverStatsTotal = driverStatsTotal;
	}

	TerminalStepEntry withDeleteFailureOutcome(final String outcome) {
		if (deleteMetrics == null) {
			return this;
		}
		return with(deleteMetrics.toBuilder().failureOutcome(outcome).build(), driverStats, driverStatsTotal);
	}

	/**
	 * Returns this entry with the storage driver statistics of an earlier entry of the same step
	 * and run, when this entry has none of its own. A step with several operation types has a
	 * metrics context for each; they end one after another and each leaves the entry of the step.
	 * One of them carries the statistics of the step's driver, and it need not be the last.
	 */
	TerminalStepEntry withDriverStatsOf(final TerminalStepEntry earlier) {
		if (driverStats != null || driverStatsTotal != null) {
			return this;
		}
		if (earlier == null || earlier.runId != runId
						|| (earlier.driverStats == null && earlier.driverStatsTotal == null)) {
			return this;
		}
		return with(deleteMetrics, earlier.driverStats, earlier.driverStatsTotal);
	}

	/**
	 * Returns this entry with the given storage driver statistics in place of its own, or this
	 * entry when there are none to give.
	 */
	TerminalStepEntry withDriverStats(final DriverStatsSnapshot stats, final DriverStatsTotal total) {
		if (stats == null && total == null) {
			return this;
		}
		return with(deleteMetrics, stats, total);
	}

	private TerminalStepEntry with(
					final DeleteMetricsSnapshot deleteMetrics,
					final DriverStatsSnapshot driverStats,
					final DriverStatsTotal driverStatsTotal) {
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
						deleteMetrics,
						deleteDetailsExpected, clusterId, driverStats, driverStatsTotal);
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
