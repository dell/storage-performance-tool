package com.dell.spt.base.metrics.steady;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;

/** Computes Steady-Window Statistics from a start cut and an end cut. */
final class SteadyComputation {

	private static final double NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

	private SteadyComputation() {}

	static SteadyResult compute(
					final SteadySettings settings,
					final long stepStartNanos,
					final SteadyCut start,
					final SteadyCut end,
					final String cause) {
		if (start == null) {
			return SteadyResult.notReached(cause, end.acquisitionNanos());
		}
		final SteadyCut.Readings a = start.readings();
		final SteadyCut.Readings b = end.readings();
		final long durationNanos = Math.max(0L, end.timeNanos() - start.timeNanos());
		final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
		final double seconds = durationNanos / NANOS_PER_SECOND;
		final long succeeded = b.succeeded() - a.succeeded();
		final long failed = b.failed() - a.failed();
		final long corrupt = b.corrupt() - a.corrupt();
		final long bytes = b.bytes() - a.bytes();
		final long samples = b.concurrencySampleCount() - a.concurrencySampleCount();
		// A window shorter than the gauge's refresh period can have no samples; fall back to the
		// two cuts' fresh gauges.
		final double meanInFlight = samples > 0
						? (double) (b.concurrencySampleSum() - a.concurrencySampleSum()) / samples
						: (a.inFlight() + b.inFlight()) / 2.0;
		final long completions = succeeded + failed;
		final double turnovers = completions / Math.max(1.0, meanInFlight);
		final boolean valid = durationNanos > 0
						&& completions > 0
						&& turnovers >= settings.minTurnovers()
						&& durationNanos >= TimeUnit.MILLISECONDS.toNanos(settings.minDurationMillis());
		final var statistics = new AllMetricsSnapshotImpl(
						difference(MetricsConstants.METRIC_NAME_DUR, a.duration(), b.duration()),
						difference(MetricsConstants.METRIC_NAME_LAT, a.latency(), b.latency()),
						difference(MetricsConstants.METRIC_NAME_TTFB, a.ttfb(), b.ttfb()),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, b.inFlight(), meanInFlight),
						rate(MetricsConstants.METRIC_NAME_FAIL, failed, b.failedLastRate(), seconds, elapsedMillis),
						rate(MetricsConstants.METRIC_NAME_CORRUPT, corrupt, b.corruptLastRate(), seconds, elapsedMillis),
						rate(MetricsConstants.METRIC_NAME_SUCC, succeeded, b.succeededLastRate(), seconds, elapsedMillis),
						rate(MetricsConstants.METRIC_NAME_BYTE, bytes, b.bytesLastRate(), seconds, elapsedMillis),
						elapsedMillis);
		return new SteadyResult(
						valid ? SteadyStatus.VALID : SteadyStatus.SHORT,
						cause,
						TimeUnit.NANOSECONDS.toMillis(start.timeNanos() - stepStartNanos),
						durationNanos,
						turnovers,
						null,
						null,
						start.acquisitionNanos(),
						end.acquisitionNanos(),
						statistics);
	}

	private static RateMetricSnapshotImpl rate(
					final String name, final long count, final double lastRate, final double seconds, final long elapsedMillis) {
		return new RateMetricSnapshotImpl(lastRate, seconds > 0 ? count / seconds : 0.0, name, count, elapsedMillis);
	}

	/**
	 * Every timing statistic of the window from the end histogram minus the start histogram. The
	 * end copy starts after the start copy ends and bucket counts only grow, so the difference has
	 * no negative bucket and describes one population.
	 */
	static TimingMetricSnapshot difference(final String name, final Histogram start, final Histogram end) {
		final Histogram window = end.copy();
		try {
			window.subtract(start);
		} catch (final RuntimeException e) {
			Loggers.MSG.warn("Steady-state window: the {} histograms could not be differenced: {}", name, e.toString());
			return new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, name);
		}
		final long count = window.getTotalCount();
		if (count == 0) {
			return new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, name);
		}
		final double mean = window.getMean();
		return new TimingMetricSnapshotImpl(
						Math.round(mean * count),
						count,
						window.getMinValue(),
						window.getMaxValue(),
						mean,
						name,
						window.getCountAtValue(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS),
						TimingMetricSnapshotImpl.encodeHistogram(window));
	}
}
