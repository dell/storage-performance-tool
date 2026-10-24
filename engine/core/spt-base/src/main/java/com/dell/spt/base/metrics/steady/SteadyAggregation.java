package com.dell.spt.base.metrics.steady;

import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * The Entry Node's steady result: a sum of independently measured slice windows (D3), not one
 * cluster-wide interval. Counts and rates are summed and timing histograms merged; the start and
 * duration are coverage metadata, so count ÷ duration need not equal the summed rate.
 */
public final class SteadyAggregation {

	private SteadyAggregation() {}

	/**
	 * Aggregates the slices' results as contract item 10 states.
	 *
	 * @param results each expected contributor's result in contributor order; null where the
	 *     contributor is missing or its window is still open
	 * @param contributorsAuthoritative whether the contributor set is complete and unique
	 * @param nodeCount the step's node count, for the aggregate snapshot
	 * @return the aggregate, or null while no contributor has closed its window
	 */
	public static SteadyResult aggregate(
					final List<SteadyResult> results, final boolean contributorsAuthoritative, final int nodeCount) {
		final List<SteadyResult> closed = results.stream().filter(Objects::nonNull).toList();
		if (closed.isEmpty()) {
			return null;
		}
		final String cause = causes(closed);
		final long longestStartAcquisition = closed.stream()
						.mapToLong(SteadyResult::startAcquisitionNanos)
						.max()
						.orElse(SteadyResult.NONE);
		final long longestEndAcquisition = closed.stream()
						.mapToLong(SteadyResult::endAcquisitionNanos)
						.max()
						.orElse(SteadyResult.NONE);
		if (!contributorsAuthoritative || closed.size() != results.size()) {
			return new SteadyResult(
							SteadyStatus.INCOMPLETE, cause, SteadyResult.NONE, 0L, 0.0, null, null,
							longestStartAcquisition, longestEndAcquisition, null);
		}
		final SteadyStatus status = worst(closed);
		if (!status.hasStatistics()) {
			return new SteadyResult(
							status, cause, SteadyResult.NONE, 0L, 0.0, null, null,
							longestStartAcquisition, longestEndAcquisition, null);
		}
		final long durationNanos = closed.stream().mapToLong(SteadyResult::durationNanos).min().orElse(0L);
		return new SteadyResult(
						status,
						cause,
						closed.stream().mapToLong(SteadyResult::startOffsetMillis).max().orElse(SteadyResult.NONE),
						durationNanos,
						closed.stream().mapToDouble(SteadyResult::turnovers).min().orElse(0.0),
						largestMagnitude(closed, SteadyResult::trendPercent),
						largestMagnitude(closed, SteadyResult::cv),
						longestStartAcquisition,
						longestEndAcquisition,
						statistics(closed, nodeCount, TimeUnit.NANOSECONDS.toMillis(durationNanos)));
	}

	/** The worst slice status, ordered not-reached, then short, then valid. */
	private static SteadyStatus worst(final List<SteadyResult> closed) {
		SteadyStatus worst = SteadyStatus.VALID;
		for (final SteadyResult result : closed) {
			if (result.status() == SteadyStatus.NOT_REACHED) {
				return SteadyStatus.NOT_REACHED;
			}
			if (result.status() != SteadyStatus.VALID) {
				worst = SteadyStatus.SHORT;
			}
		}
		return worst;
	}

	/** The slice value with the largest magnitude, sign kept; null if any slice's value is null. */
	private static Double largestMagnitude(
					final List<SteadyResult> closed, final Function<SteadyResult, Double> indicator) {
		Double largest = null;
		for (final SteadyResult result : closed) {
			final Double value = indicator.apply(result);
			if (value == null) {
				return null;
			}
			if (largest == null || Math.abs(value) > Math.abs(largest)) {
				largest = value;
			}
		}
		return largest;
	}

	private static String causes(final List<SteadyResult> closed) {
		final Set<String> causes = new LinkedHashSet<>();
		for (final SteadyResult result : closed) {
			causes.add(result.cause());
		}
		return String.join(", ", causes);
	}

	private static DistributedAllMetricsSnapshotImpl statistics(
					final List<SteadyResult> closed, final int nodeCount, final long elapsedMillis) {
		final List<AllMetricsSnapshot> slices = closed.stream().map(SteadyResult::statistics).toList();
		return new DistributedAllMetricsSnapshotImpl(
						timing(slices, AllMetricsSnapshot::durationSnapshot, MetricsConstants.METRIC_NAME_DUR),
						timing(slices, AllMetricsSnapshot::latencySnapshot, MetricsConstants.METRIC_NAME_LAT),
						timing(slices, AllMetricsSnapshot::ttfbSnapshot, MetricsConstants.METRIC_NAME_TTFB),
						concurrency(slices),
						rate(slices, AllMetricsSnapshot::failsSnapshot, elapsedMillis),
						rate(slices, AllMetricsSnapshot::corruptSnapshot, elapsedMillis),
						rate(slices, AllMetricsSnapshot::successSnapshot, elapsedMillis),
						rate(slices, AllMetricsSnapshot::byteSnapshot, elapsedMillis),
						nodeCount,
						elapsedMillis,
						null);
	}

	private static TimingMetricSnapshot timing(
					final List<AllMetricsSnapshot> slices,
					final Function<AllMetricsSnapshot, TimingMetricSnapshot> meter,
					final String name) {
		final List<TimingMetricSnapshot> timings = new ArrayList<>(slices.size());
		for (final AllMetricsSnapshot slice : slices) {
			final TimingMetricSnapshot timing = meter.apply(slice);
			if (timing != null) {
				timings.add(timing);
			}
		}
		return TimingMetricSnapshotImpl.aggregate(timings, name);
	}

	private static ConcurrencyMetricSnapshot concurrency(final List<AllMetricsSnapshot> slices) {
		long last = 0;
		double mean = 0;
		for (final AllMetricsSnapshot slice : slices) {
			last += slice.concurrencySnapshot().last();
			mean += slice.concurrencySnapshot().mean();
		}
		return new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, last, mean);
	}

	/** Counts, Avg and Last rates summed, like the whole-step aggregate; elapsed is the shortest window. */
	private static RateMetricSnapshot rate(
					final List<AllMetricsSnapshot> slices,
					final Function<AllMetricsSnapshot, RateMetricSnapshot> meter,
					final long elapsedMillis) {
		long count = 0;
		double mean = 0;
		double last = 0;
		String name = null;
		for (final AllMetricsSnapshot slice : slices) {
			final RateMetricSnapshot rate = meter.apply(slice);
			if (rate != null) {
				count += rate.count();
				mean += rate.mean();
				last += rate.last();
				name = rate.name();
			}
		}
		return new RateMetricSnapshotImpl(last, mean, name, count, elapsedMillis);
	}
}
