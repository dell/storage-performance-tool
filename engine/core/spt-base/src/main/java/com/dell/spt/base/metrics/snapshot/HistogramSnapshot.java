package com.dell.spt.base.metrics.snapshot;

/** Snapshot summarizing a histogram distribution of metric samples. */
public interface HistogramSnapshot extends LongLastMetricSnapshot {

	long quantile(final double quantile);

	long[] values();

	@Override
	long last();
}
