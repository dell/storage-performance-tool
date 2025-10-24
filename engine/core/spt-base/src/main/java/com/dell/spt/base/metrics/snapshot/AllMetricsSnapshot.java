package com.dell.spt.base.metrics.snapshot;

import java.io.Serializable;

/** Aggregated view of rate, timing, concurrency, and byte metrics. */
public interface AllMetricsSnapshot extends Serializable {

	TimingMetricSnapshot durationSnapshot();

	TimingMetricSnapshot latencySnapshot();

	ConcurrencyMetricSnapshot concurrencySnapshot();

	RateMetricSnapshot byteSnapshot();

	RateMetricSnapshot successSnapshot();

	RateMetricSnapshot failsSnapshot();

	/** Returns the duration of the measurement window in milliseconds. */
	long elapsedTimeMillis();
}
