package com.dell.spt.base.metrics.steady;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.junit.jupiter.api.Test;

/** The Entry Node's sum of independently measured slice windows (contract item 10, D3). */
class SteadyAggregationTest {

	private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

	@Test
	void noClosedWindowYetIsNoResult() {
		assertNull(SteadyAggregation.aggregate(Arrays.asList(null, null), true, 2));
	}

	@Test
	void countsAndRatesAreSummedAndHistogramsMerged() {
		final SteadyResult a = slice(SteadyStatus.VALID, 2_000, 30 * SECOND, 1_000, 100.0, 50, 0.5, 0.02, 1_000);
		final SteadyResult b = slice(SteadyStatus.VALID, 3_500, 25 * SECOND, 500, 80.0, 40, -2.0, 0.05, 3_000);
		final SteadyResult sum = SteadyAggregation.aggregate(List.of(a, b), true, 2);

		assertEquals(SteadyStatus.VALID, sum.status());
		final var stats = sum.statistics();
		assertInstanceOf(DistributedAllMetricsSnapshot.class, stats, "the total CSV writer needs the node count");
		assertEquals(2, ((DistributedAllMetricsSnapshot) stats).nodeCount());
		assertEquals(1_500, stats.successSnapshot().count());
		assertEquals(180.0, stats.successSnapshot().mean(), 1e-9, "rates are summed, like the Avg rates");
		assertEquals(90.0, stats.concurrencySnapshot().mean(), 1e-9);
		assertEquals(1_500, stats.durationSnapshot().count(), "the window populations are merged");
		assertEquals(1_000.0, (double) stats.durationSnapshot().min(), 1_000 * 0.01);
		assertEquals(3_000.0, (double) stats.durationSnapshot().max(), 3_000 * 0.01);
	}

	@Test
	void coverageMetadataIsTheLatestStartAndTheShortestWindow() {
		final SteadyResult early = slice(SteadyStatus.VALID, 1_000, 40 * SECOND, 400, 10.0, 4, 0.1, 0.01, 500);
		final SteadyResult late = slice(SteadyStatus.VALID, 9_000, 20 * SECOND, 400, 20.0, 4, 0.1, 0.01, 500);
		final SteadyResult sum = SteadyAggregation.aggregate(List.of(early, late), true, 2);
		assertEquals(9_000, sum.startOffsetMillis());
		assertEquals(20 * SECOND, sum.durationNanos());
		assertEquals(20_000, sum.statistics().elapsedTimeMillis(), "StepDuration is the shortest slice window");
		assertEquals(30.0, sum.statistics().successSnapshot().mean(), 1e-9, "count / duration need not equal the rate");
	}

	@Test
	void turnoversAreTheLowestAndStabilityTheWorstSlice() {
		final SteadyResult a = slice(SteadyStatus.VALID, 0, 30 * SECOND, 1_000, 10.0, 10, 1.5, 0.08, 500);
		final SteadyResult b = slice(SteadyStatus.VALID, 0, 30 * SECOND, 300, 10.0, 10, -4.0, 0.03, 500);
		final SteadyResult sum = SteadyAggregation.aggregate(List.of(a, b), true, 2);
		assertEquals(30.0, sum.turnovers(), 1e-9);
		assertEquals(-4.0, sum.trendPercent(), 1e-12, "largest magnitude, sign kept");
		assertEquals(0.08, sum.cv(), 1e-12);

		final SteadyResult unjudged = slice(SteadyStatus.VALID, 0, 30 * SECOND, 300, 10.0, 10, null, null, 500);
		final SteadyResult withEmpty = SteadyAggregation.aggregate(List.of(a, unjudged), true, 2);
		assertNull(withEmpty.trendPercent(), "empty if any contributor's value is empty");
		assertNull(withEmpty.cv());
	}

	@Test
	void theWorstSliceStatusWins() {
		final SteadyResult valid = slice(SteadyStatus.VALID, 0, 30 * SECOND, 100, 1.0, 1, 0.0, 0.0, 500);
		final SteadyResult shortWindow = slice(SteadyStatus.SHORT, 0, 5 * SECOND, 10, 1.0, 1, null, null, 500);
		final SteadyResult notReached = SteadyResult.notReached("t", 10);
		assertEquals(SteadyStatus.SHORT, SteadyAggregation.aggregate(List.of(valid, shortWindow), true, 2).status());
		final SteadyResult anyNotReached = SteadyAggregation.aggregate(List.of(valid, shortWindow, notReached), true, 3);
		assertEquals(SteadyStatus.NOT_REACHED, anyNotReached.status());
		assertNull(anyNotReached.statistics());
	}

	@Test
	void aMissingDuplicateOrStillOpenContributorIsIncomplete() {
		final SteadyResult valid = slice(SteadyStatus.VALID, 0, 30 * SECOND, 100, 1.0, 1, 0.0, 0.0, 500);
		final SteadyResult stillOpen = SteadyAggregation.aggregate(Arrays.asList(valid, null), true, 2);
		assertEquals(SteadyStatus.INCOMPLETE, stillOpen.status());
		assertNull(stillOpen.statistics());
		final SteadyResult missingOrDuplicate = SteadyAggregation.aggregate(List.of(valid, valid), false, 2);
		assertEquals(SteadyStatus.INCOMPLETE, missingOrDuplicate.status());
		assertNull(missingOrDuplicate.statistics());
		assertEquals(10, missingOrDuplicate.endAcquisitionNanos(), "acquisition durations are the longest slice values");
	}

	private static SteadyResult slice(
					final SteadyStatus status,
					final long startOffsetMillis,
					final long durationNanos,
					final long succeeded,
					final double rate,
					final double meanInFlight,
					final Double trend,
					final Double cv,
					final long durationMicros) {
		final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(durationNanos);
		final TimingMetricSnapshot duration = TimingMetricSnapshotImpl.fromSamples(
						MetricsConstants.METRIC_NAME_DUR,
						LongStream.range(0, succeeded).map(i -> durationMicros).boxed().toList());
		final var statistics = new AllMetricsSnapshotImpl(
						duration,
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_LAT),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, (long) meanInFlight, meanInFlight),
						new RateMetricSnapshotImpl(0, 0, MetricsConstants.METRIC_NAME_FAIL, 0, elapsedMillis),
						new RateMetricSnapshotImpl(0, 0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedMillis),
						new RateMetricSnapshotImpl(rate, rate, MetricsConstants.METRIC_NAME_SUCC, succeeded, elapsedMillis),
						new RateMetricSnapshotImpl(0, 0, MetricsConstants.METRIC_NAME_BYTE, 0, elapsedMillis),
						elapsedMillis);
		return new SteadyResult(
						status, "t", startOffsetMillis, durationNanos, succeeded / meanInFlight, trend, cv, 5, 10, statistics);
	}
}
