package com.dell.spt.base.metrics.snapshot;

import com.dell.spt.base.metrics.MetricsConstants;
import java.io.Serializable;

/** Aggregated view of rate, timing, concurrency, and byte metrics. */
public interface AllMetricsSnapshot extends Serializable {

	TimingMetricSnapshot durationSnapshot();

	TimingMetricSnapshot latencySnapshot();

	default TimingMetricSnapshot ttfbSnapshot() {
		return new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, "ttfb");
	}

	ConcurrencyMetricSnapshot concurrencySnapshot();

	RateMetricSnapshot byteSnapshot();

	RateMetricSnapshot successSnapshot();

	RateMetricSnapshot failsSnapshot();

	/** Corruption is a strict subset of failed operations. */
	default RateMetricSnapshot corruptSnapshot() {
		return new RateMetricSnapshotImpl(
						0.0, 0.0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedTimeMillis());
	}

	/** Optional additive DELETE contract; null for older snapshots and non-DELETE operations. */
	default DeleteMetricsSnapshot deleteMetrics() {
		return null;
	}

	/** Returns the duration of the measurement window in milliseconds. */
	long elapsedTimeMillis();
}
