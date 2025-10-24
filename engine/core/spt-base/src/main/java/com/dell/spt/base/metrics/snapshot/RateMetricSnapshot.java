package com.dell.spt.base.metrics.snapshot;

/** Snapshot describing a rate-based metric such as throughput. */
public interface RateMetricSnapshot
				extends CountMetricSnapshot,
				DoubleLastMetricSnapshot,
				ElapsedTimeMetricSnapshot,
				MeanMetricSnapshot,
				NamedMetricSnapshot {}
