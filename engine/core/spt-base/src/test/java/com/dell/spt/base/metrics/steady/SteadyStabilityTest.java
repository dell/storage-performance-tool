package com.dell.spt.base.metrics.steady;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** {@code SteadyTrend[%]} and {@code SteadyCV}, and the bounded series they come from. */
class SteadyStabilityTest {

	private static final long QUARTER_SECOND = TimeUnit.MILLISECONDS.toNanos(250);

	/**
	 * Expected values come from the calibration replay's own {@code binned_rates}, {@code cv} and
	 * {@code thirds_trend} on the same series, {@code steady_replay.py} SHA-256
	 * 91fa1ca47e97366dfd0ca36e501bd1cdd1fd81f8c019fe118f27aca24fdd162b, with a constant
	 * {@code ConcurrencyMean} of 16.
	 */
	@Test
	void matchesTheReplayOnASeriesWithOneIntervalBins() {
		final int n = 201;
		final long[] time = new long[n], succeeded = new long[n], failed = new long[n];
		for (int i = 0; i < n; i++) {
			time[i] = i * QUARTER_SECOND;
			succeeded[i] = i == 0 ? 0 : succeeded[i - 1] + 250 + ((i - 1) * 37) % 50 + (i - 1);
			failed[i] = i / 7;
		}
		final double[] rates = SteadyStability.binRates(time, succeeded, failed, n, 16);
		assertEquals(200, rates.length);
		assertEquals(1000.0, rates[0]);
		assertEquals(1152.0, rates[1]);
		assertEquals(1848.0, rates[199]);
		final var indicators = SteadyStability.compute(time, succeeded, failed, n, 16);
		assertEquals(0.1587704046984334, indicators.cv(), 1e-12);
		assertEquals(43.54392892398813, indicators.trendPercent(), 1e-10);
	}

	@Test
	void matchesTheReplayOnASeriesWithNineIntervalBins() {
		final int n = 201;
		final long[] time = new long[n], succeeded = new long[n], failed = new long[n];
		for (int i = 0; i < n; i++) {
			time[i] = i * QUARTER_SECOND;
			succeeded[i] = i == 0 ? 0 : succeeded[i - 1] + 30 + ((i - 1) * 13) % 20;
			failed[i] = i / 9;
		}
		final double[] expected = {
				159.11111111111111, 156.0, 161.77777777777777, 158.66666666666666, 155.55555555555554,
				161.33333333333334, 149.33333333333334, 164.0, 152.0, 166.66666666666666, 154.66666666666666,
				160.44444444444446, 157.33333333333334, 154.22222222222223, 160.0, 156.88888888888889,
				162.66666666666666, 150.66666666666666, 165.33333333333334, 153.33333333333334,
				159.11111111111111, 156.0
		};
		assertArrayEquals(expected, SteadyStability.binRates(time, succeeded, failed, n, 16), 1e-9);
		final var indicators = SteadyStability.compute(time, succeeded, failed, n, 16);
		assertEquals(0.028714603773476385, indicators.cv(), 1e-12);
		assertEquals(0.20169423154498614, indicators.trendPercent(), 1e-10);
	}

	@Test
	void constantRateGivesZeroTrendAndCv() {
		final var indicators = indicatorsOfRates(i -> 500);
		assertEquals(0.0, indicators.trendPercent(), 1e-12);
		assertEquals(0.0, indicators.cv(), 1e-12);
	}

	@Test
	void linearRiseGivesTheExpectedTrend() {
		// 1,000 + 12 i ops/s in second i: the first third (0-19 s) averages 1,114, the last 1,594.
		final var indicators = indicatorsOfRates(i -> 1_000 + 12 * i);
		assertEquals(100.0 * (1_594.0 - 1_114.0) / 1_114.0, indicators.trendPercent(), 1e-9);
		assertTrue(indicators.cv() > 0);
	}

	@Test
	void dipInTheMiddleRaisesCvButNotTrend() {
		final var indicators = indicatorsOfRates(i -> i >= 25 && i < 35 ? 200 : 1_000);
		assertEquals(0.0, indicators.trendPercent(), 1e-12);
		assertTrue(indicators.cv() > 0.2, "cv " + indicators.cv());
	}

	@Test
	void fewerThanSixBinsLeavesBothEmpty() {
		final int n = 201;
		final long[] time = new long[n], succeeded = new long[n], failed = new long[n];
		for (int i = 0; i < n; i++) {
			time[i] = i * QUARTER_SECOND;
			succeeded[i] = 3L * i; // 600 completions: one bin of 100 at 16 in flight needs 20 turnovers
		}
		final var indicators = SteadyStability.compute(time, succeeded, failed, n, 16);
		assertNull(indicators.trendPercent());
		assertNull(indicators.cv());
		assertSame(SteadyStability.EMPTY, SteadyStability.compute(time, succeeded, failed, 1, 16));
	}

	@Test
	void seriesStaysBoundedAndEvenlySpacedThroughRepeatedHalving() {
		final var clock = new AtomicLong();
		final var source = new SteadyWindowTest.FakeSource();
		final var window = new SteadyWindow(new SteadySettings(1, 0, 0), source, clock::get);
		window.markStepStart();
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		final long tick = TimeUnit.MILLISECONDS.toNanos(10);
		for (int i = 0; i < 3_000; i++) {
			clock.addAndGet(tick);
			source.succeeded += 5;
			window.observe();
		}
		final long[] times = window.sampleTimes();
		assertTrue(times.length <= SteadyWindow.SERIES_CAPACITY, "size " + times.length);
		assertTrue(times.length > SteadyWindow.SERIES_CAPACITY / 4);
		assertEquals(0, times[0], "the start cut stays the first sample");
		final long spacing = times[1] - times[0];
		for (int i = 2; i < times.length; i++) {
			assertEquals(spacing, times[i] - times[i - 1], "sample " + i);
		}
		final SteadyResult result = window.close("t");
		assertEquals(0, window.sampleTimes().length, "the series is dropped at close");
		window.observe();
		assertEquals(0, window.sampleTimes().length, "no sample after the end cut");
		assertNotNull(result.cv());
		assertEquals(0.0, result.trendPercent(), 1e-9, "a constant rate");
	}

	@Test
	void notReachedAndShortWindowsWithoutBinsHaveNoIndicators() {
		final var source = new SteadyWindowTest.FakeSource();
		final var clock = new AtomicLong();
		final var window = new SteadyWindow(SteadySettings.DEFAULTS, source, clock::get);
		window.markStepStart();
		final SteadyResult notReached = window.close("t");
		assertNull(notReached.trendPercent());
		assertNull(notReached.cv());
	}

	private interface RateOfBin {
		long rate(int bin);
	}

	/** 60 s at the given rate in each second, sampled every 250 ms, 1 operation in flight; rates divisible by 4. */
	private static SteadyStability.Indicators indicatorsOfRates(final RateOfBin rateOfBin) {
		final int n = 241;
		final long[] time = new long[n], succeeded = new long[n], failed = new long[n];
		for (int i = 1; i < n; i++) {
			time[i] = i * QUARTER_SECOND;
			succeeded[i] = succeeded[i - 1] + rateOfBin.rate((i - 1) / 4) / 4;
		}
		// width: 100 x 240 / completions <= 4 intervals for rates of at least 100 ops/s per bin.
		final double[] rates = SteadyStability.binRates(time, succeeded, failed, n, 1);
		assertTrue(rates.length >= 6);
		return SteadyStability.compute(time, succeeded, failed, n, 1);
	}

	/** The end cut is the series' last sample: here it completes the sixth bin. */
	@Test
	void theEndCutCompletesTheLastBin() {
		final var clock = new AtomicLong();
		final var source = new SteadyWindowTest.FakeSource();
		final var window = new SteadyWindow(new SteadySettings(1, 0, 0), source, clock::get);
		window.markStepStart();
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		for (int i = 0; i < 5; i++) {
			clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
			source.succeeded += 1_000;
			window.observe();
		}
		clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
		source.succeeded += 1_000;
		final SteadyResult result = window.close("t");
		assertNotNull(result.cv(), "six one-second bins need the end cut as their last sample");
		assertEquals(0.0, result.cv(), 1e-12);
	}
}
