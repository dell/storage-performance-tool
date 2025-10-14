package com.dell.spt.base.metrics;

import com.dell.spt.base.item.op.OpType;

/** Cached snapshot of a finished step used to keep /metrics/json non-empty at idle. */
public final class TerminalStepEntry {
	public final String stepId;
	public final OpType opType;
	public final long recordedAtMillis;

	public final long successCount;
	public final long failedCount;
	public final long bytesTotal;
	public final double latencyMeanUs;
	public final double durationMeanUs;
	public final long concurrencyLast;
	public final double concurrencyMean;

	public final long countLimit;
	public final long timeLimitSec;

	public final long elapsedTimeMillis;

	public TerminalStepEntry(
					String stepId,
					OpType opType,
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
		this.stepId = stepId;
		this.opType = opType;
		this.recordedAtMillis = recordedAtMillis;
		this.successCount = successCount;
		this.failedCount = failedCount;
		this.bytesTotal = bytesTotal;
		this.latencyMeanUs = latencyMeanUs;
		this.durationMeanUs = durationMeanUs;
		this.concurrencyLast = concurrencyLast;
		this.concurrencyMean = concurrencyMean;
		this.countLimit = countLimit;
		this.timeLimitSec = timeLimitSec;
		this.elapsedTimeMillis = elapsedTimeMillis;
	}
}
