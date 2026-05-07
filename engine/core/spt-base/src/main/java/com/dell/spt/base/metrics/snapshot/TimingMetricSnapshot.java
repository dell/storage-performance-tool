package com.dell.spt.base.metrics.snapshot;

/** Snapshot containing aggregate timing statistics. */
public interface TimingMetricSnapshot
				extends CountMetricSnapshot, NamedMetricSnapshot, MeanMetricSnapshot {

	long sum();

	long min();

	long max();

	default long percentile(final double quantile) {
		return 0L;
	}

	default long overflowCount() {
		return 0L;
	}

	default byte[] compressedHistogram() {
		return new byte[0];
	}

}
