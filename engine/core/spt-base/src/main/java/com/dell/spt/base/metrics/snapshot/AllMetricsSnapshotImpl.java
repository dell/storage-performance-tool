package com.dell.spt.base.metrics.snapshot;

import com.dell.spt.base.metrics.MetricsConstants;

public class AllMetricsSnapshotImpl implements AllMetricsSnapshot {

	private final TimingMetricSnapshot durSnapshot;
	private final TimingMetricSnapshot latSnapshot;
	private final TimingMetricSnapshot ttfbSnapshot;
	private final ConcurrencyMetricSnapshot actualConcurrencySnapshot;
	private final RateMetricSnapshot failsSnapshot;
	private final RateMetricSnapshot corruptSnapshot;
	private final RateMetricSnapshot successSnapshot;
	private final RateMetricSnapshot bytesSnapshot;
	private final DeleteMetricsSnapshot deleteMetrics;
	protected final long elapsedTimeMillis;

	public AllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final long elapsedTimeMillis) {
		this(
						durSnapshot,
						latSnapshot,
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, "ttfb"),
						actualConcurrencySnapshot,
						failsSnapshot,
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedTimeMillis),
						successSnapshot,
						bytesSnapshot,
						elapsedTimeMillis);
	}

	public AllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final TimingMetricSnapshot ttfbSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final long elapsedTimeMillis) {
		this(
						durSnapshot,
						latSnapshot,
						ttfbSnapshot,
						actualConcurrencySnapshot,
						failsSnapshot,
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedTimeMillis),
						successSnapshot,
						bytesSnapshot,
						elapsedTimeMillis);
	}

	public AllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final TimingMetricSnapshot ttfbSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot corruptSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
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
						elapsedTimeMillis,
						null);
	}

	/**
	 * Creates a complete snapshot including optional detailed DELETE measurements.
	 *
	 * @param durSnapshot request-duration distribution
	 * @param latSnapshot request-latency distribution
	 * @param ttfbSnapshot time-to-first-byte distribution
	 * @param actualConcurrencySnapshot observed concurrency
	 * @param failsSnapshot failed-operation rate
	 * @param corruptSnapshot corrupt-operation rate
	 * @param successSnapshot successful-operation rate
	 * @param bytesSnapshot transferred-byte rate
	 * @param elapsedTimeMillis elapsed step time in milliseconds
	 * @param deleteMetrics optional detailed DELETE metrics
	 */
	public AllMetricsSnapshotImpl(
					final TimingMetricSnapshot durSnapshot,
					final TimingMetricSnapshot latSnapshot,
					final TimingMetricSnapshot ttfbSnapshot,
					final ConcurrencyMetricSnapshot actualConcurrencySnapshot,
					final RateMetricSnapshot failsSnapshot,
					final RateMetricSnapshot corruptSnapshot,
					final RateMetricSnapshot successSnapshot,
					final RateMetricSnapshot bytesSnapshot,
					final long elapsedTimeMillis,
					final DeleteMetricsSnapshot deleteMetrics) {
		this.durSnapshot = durSnapshot;
		this.latSnapshot = latSnapshot;
		this.ttfbSnapshot = ttfbSnapshot;
		this.actualConcurrencySnapshot = actualConcurrencySnapshot;
		this.failsSnapshot = failsSnapshot;
		this.corruptSnapshot = corruptSnapshot;
		this.successSnapshot = successSnapshot;
		this.bytesSnapshot = bytesSnapshot;
		this.elapsedTimeMillis = elapsedTimeMillis;
		this.deleteMetrics = deleteMetrics;
	}

	@Override
	public TimingMetricSnapshot durationSnapshot() {
		return durSnapshot;
	}

	@Override
	public TimingMetricSnapshot latencySnapshot() {
		return latSnapshot;
	}

	@Override
	public TimingMetricSnapshot ttfbSnapshot() {
		return ttfbSnapshot;
	}

	@Override
	public ConcurrencyMetricSnapshot concurrencySnapshot() {
		return actualConcurrencySnapshot;
	}

	@Override
	public RateMetricSnapshot byteSnapshot() {
		return bytesSnapshot;
	}

	@Override
	public RateMetricSnapshot successSnapshot() {
		return successSnapshot;
	}

	@Override
	public RateMetricSnapshot failsSnapshot() {
		return failsSnapshot;
	}

	@Override
	public RateMetricSnapshot corruptSnapshot() {
		return corruptSnapshot;
	}

	@Override
	public DeleteMetricsSnapshot deleteMetrics() {
		return deleteMetrics;
	}

	@Override
	public long elapsedTimeMillis() {
		return elapsedTimeMillis;
	}
}
