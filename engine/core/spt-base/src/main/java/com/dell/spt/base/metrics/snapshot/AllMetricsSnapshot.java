package com.dell.spt.base.metrics.snapshot;

import java.io.Serializable;

public interface AllMetricsSnapshot extends Serializable {

	TimingMetricSnapshot durationSnapshot();

	TimingMetricSnapshot latencySnapshot();

	ConcurrencyMetricSnapshot concurrencySnapshot();

	RateMetricSnapshot byteSnapshot();

	RateMetricSnapshot successSnapshot();

	RateMetricSnapshot failsSnapshot();

	/** @return value in milliseconds */
	long elapsedTimeMillis();
}
