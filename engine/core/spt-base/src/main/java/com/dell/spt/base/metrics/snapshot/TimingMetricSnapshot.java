package com.dell.spt.base.metrics.snapshot;

/** Snapshot containing aggregate timing statistics. */
public interface TimingMetricSnapshot
				extends CountMetricSnapshot, NamedMetricSnapshot, MeanMetricSnapshot {

	long sum();

	long min();

	long max();

}
