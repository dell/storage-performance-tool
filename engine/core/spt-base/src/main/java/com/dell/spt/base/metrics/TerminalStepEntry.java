package com.dell.spt.base.metrics;

import com.dell.spt.base.item.op.OpType;
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
	public final boolean partial;

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
		this.stepId = stepId;
		this.opType = opType;
		this.runId = runId;
		this.recordedAtMillis = recordedAtMillis;
		this.successCount = successCount;
		this.failedCount = failedCount;
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
		this.partial = partial;
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
}
