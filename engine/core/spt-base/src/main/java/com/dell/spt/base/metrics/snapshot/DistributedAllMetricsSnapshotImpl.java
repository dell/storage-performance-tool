package com.dell.spt.base.metrics.snapshot;

import com.dell.spt.base.metrics.steady.SteadyResult;

/** Snapshot aggregating metrics from all nodes in distributed mode. */
public final class DistributedAllMetricsSnapshotImpl extends AllMetricsSnapshotImpl
				implements DistributedAllMetricsSnapshot {

	private final int nodeCount;
	private final DriverStatsTotal driverStatsTotal;

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
		this.driverStatsTotal = null;
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
		this.driverStatsTotal = null;
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
						deleteMetrics,
						null,
						null);
	}

	/**
	 * Creates a distributed snapshot that also carries the aggregate Steady-State Window.
	 *
	 * @param steadyResult the slices' aggregate result once any window is closed, otherwise null
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
					final DeleteMetricsSnapshot deleteMetrics,
					final SteadyResult steadyResult) {
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
						deleteMetrics,
						steadyResult,
						null);
	}

	/**
	 * Creates a distributed snapshot including the total of the storage driver statistics.
	 *
	 * @param driverStatsTotal optional total of the contributors' storage driver statistics
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
					final DeleteMetricsSnapshot deleteMetrics,
					final DriverStatsTotal driverStatsTotal) {
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
						deleteMetrics,
						null,
						driverStatsTotal);
	}

	/**
	 * Creates a distributed snapshot with the aggregate Steady-State Window and the total of the
	 * storage driver statistics.
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
	 * @param steadyResult the slices' aggregate result once any window is closed, otherwise null
	 * @param driverStatsTotal optional total of the contributors' storage driver statistics
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
					final DeleteMetricsSnapshot deleteMetrics,
					final SteadyResult steadyResult,
					final DriverStatsTotal driverStatsTotal) {
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
						deleteMetrics,
						steadyResult,
						null);
		this.nodeCount = nodeCount;
		this.driverStatsTotal = driverStatsTotal;
	}

	@Override
	public int nodeCount() {
		return nodeCount;
	}

	@Override
	public DriverStatsTotal driverStatsTotal() {
		return driverStatsTotal;
	}

}
