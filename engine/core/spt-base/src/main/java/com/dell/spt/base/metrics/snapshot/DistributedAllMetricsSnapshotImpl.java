package com.dell.spt.base.metrics.snapshot;

/** Snapshot aggregating metrics from all nodes in distributed mode. */
public final class DistributedAllMetricsSnapshotImpl extends AllMetricsSnapshotImpl
				implements DistributedAllMetricsSnapshot {

	private final int nodeCount;

	public DistributedAllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final int nodeCount,
					final long elapsedTimeMillis) {
		super(
						durSnapshot,
						latSnapshot,
						actualConcurrencySnapshot,
						failsSnapshot,
						successSnapshot,
						bytesSnapshot,
						elapsedTimeMillis);
		this.nodeCount = nodeCount;
	}

	public DistributedAllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final TimingMetricSnapshot ttfbSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final int nodeCount,
					final long elapsedTimeMillis) {
		super(
						durSnapshot,
						latSnapshot,
						ttfbSnapshot,
						actualConcurrencySnapshot,
						failsSnapshot,
						successSnapshot,
						bytesSnapshot,
						elapsedTimeMillis);
		this.nodeCount = nodeCount;
	}

	public DistributedAllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final TimingMetricSnapshot ttfbSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot corruptSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final int nodeCount,
					final long elapsedTimeMillis) {
		this(
						durSnapshot,
						latSnapshot,
						ttfbSnapshot,
						actualConcurrencySnapshot,
						failsSnapshot,
						corruptSnapshot,
						successSnapshot,
						bytesSnapshot,
						nodeCount,
						elapsedTimeMillis,
						null);
	}

	/**
	 * Creates a distributed snapshot including optional detailed DELETE measurements.
	 *
	 * @param durSnapshot request-duration distribution
	 * @param latSnapshot request-latency distribution
	 * @param ttfbSnapshot time-to-first-byte distribution
	 * @param actualConcurrencySnapshot observed concurrency
	 * @param failsSnapshot failed-operation rate
	 * @param corruptSnapshot corrupt-operation rate
	 * @param successSnapshot successful-operation rate
	 * @param bytesSnapshot transferred-byte rate
	 * @param nodeCount participating node count
	 * @param elapsedTimeMillis elapsed step time in milliseconds
	 * @param deleteMetrics optional detailed DELETE metrics
	 */
	public DistributedAllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final TimingMetricSnapshot ttfbSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot corruptSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final int nodeCount,
					final long elapsedTimeMillis,
					final DeleteMetricsSnapshot deleteMetrics) {
		super(
						durSnapshot,
						latSnapshot,
						ttfbSnapshot,
						actualConcurrencySnapshot,
						failsSnapshot,
						corruptSnapshot,
						successSnapshot,
						bytesSnapshot,
						elapsedTimeMillis,
						deleteMetrics);
		this.nodeCount = nodeCount;
	}

	@Override
	public int nodeCount() {
		return nodeCount;
	}

}
