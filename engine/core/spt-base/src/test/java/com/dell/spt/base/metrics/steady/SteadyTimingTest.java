package com.dell.spt.base.metrics.steady;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** Window timing statistics from real timing meters: precision and acquisition skew (D5). */
class SteadyTimingTest {

	private final ExecutorService threads = Executors.newCachedThreadPool();

	@AfterEach
	void stopThreads() {
		threads.shutdownNow();
	}

	@Test
	void quiescentDifferenceEqualsAHistogramOfOnlyTheWindowsValues() {
		final var meter = new TimingMeterImpl(MetricsConstants.METRIC_NAME_DUR);
		for (long v = 1; v <= 5_000; v += 7) {
			meter.update(v * 3); // before the window, including values the window never sees
		}
		final Histogram start = meter.histogramCopy();
		final Histogram windowOnly = SteadyWindowTest.FakeSource.histogram();
		for (long v = 1; v <= 20_000; v++) {
			final long value = 200 + (v * 37) % 9_000;
			meter.update(value);
			windowOnly.recordValue(value);
		}
		final TimingMetricSnapshot window = SteadyComputation.difference(
						MetricsConstants.METRIC_NAME_DUR, start, meter.histogramCopy());
		assertEquals(windowOnly.getTotalCount(), window.count());
		assertEquals(windowOnly.getMean(), window.mean(), 1e-9);
		assertEquals(windowOnly.getMinValue(), window.min());
		assertEquals(windowOnly.getMaxValue(), window.max());
		for (final double q : new double[]{0.25, 0.5, 0.9, 0.99, 0.999
		}) {
			assertEquals(
							Math.min(windowOnly.getMaxValue(), windowOnly.getValueAtPercentile(q * 100)),
							window.percentile(q),
							"quantile " + q);
		}
	}

	@Test
	void histogramMeanIsWithinPrecisionOfTheScalarMean() {
		final var meter = new TimingMeterImpl(MetricsConstants.METRIC_NAME_LAT);
		final Histogram start = meter.histogramCopy();
		long sum = 0;
		for (long v = 1; v <= 10_000; v++) {
			final long value = 1_000 + (v * 7_919) % 250_000;
			meter.update(value);
			sum += value;
		}
		final TimingMetricSnapshot window = SteadyComputation.difference(
						MetricsConstants.METRIC_NAME_LAT, start, meter.histogramCopy());
		final double scalarMean = sum / 10_000.0;
		assertTrue(Math.abs(window.mean() / scalarMean - 1) <= 0.004, "2 significant digits: within about 0.4%");
		assertEquals(Math.round(window.mean() * window.count()), window.sum());
	}

	/**
	 * A cut paused between its meter reads while operations keep completing: the counters and the
	 * histogram copies describe different populations, but each timing population stays consistent
	 * with itself, the difference has no negative bucket, and the acquisition shows the pause.
	 */
	@Test
	void readerPausedBetweenMeterReadsKeepsEachTimingPopulationConsistent() throws Exception {
		final var duration = new TimingMeterImpl(MetricsConstants.METRIC_NAME_DUR);
		final var latency = new TimingMeterImpl(MetricsConstants.METRIC_NAME_LAT);
		final var ttfb = new TimingMeterImpl(MetricsConstants.METRIC_NAME_TTFB);
		final var succeeded = new LongAdder();
		final var paused = new CountDownLatch(1);
		final var resume = new CountDownLatch(1);
		final var pauseNextRead = new boolean[]{false
		};
		final SteadyWindow.Source source = new SteadyWindow.Source() {
			@Override
			public long succeeded() {
				return succeeded.sum();
			}

			@Override
			public long failed() {
				return 0;
			}

			@Override
			public int inFlight() {
				return 1;
			}

			@Override
			public SteadyCut.Readings read() {
				final long count = succeeded.sum();
				if (pauseNextRead[0]) {
					paused.countDown();
					try {
						resume.await(5, TimeUnit.SECONDS);
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
				return new SteadyCut.Readings(
								count, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
								duration.histogramCopy(), latency.histogramCopy(), ttfb.histogramCopy());
			}
		};
		final var window = new SteadyWindow(new SteadySettings(1, 0, 0), source, System::nanoTime);
		window.markStepStart();
		record(duration, latency, succeeded, 50);
		window.observe();
		assertTrue(window.isStarted());
		record(duration, latency, succeeded, 50);

		pauseNextRead[0] = true;
		final var closing = CompletableFuture.supplyAsync(() -> window.close("deadline"), threads);
		assertTrue(paused.await(5, TimeUnit.SECONDS));
		record(duration, latency, succeeded, 100); // completes while the reader is paused
		Thread.sleep(50);
		resume.countDown();
		final SteadyResult result = closing.get(5, TimeUnit.SECONDS);

		final var stats = result.statistics();
		assertEquals(50, stats.successSnapshot().count(), "the counter was read before the pause");
		assertEquals(150, stats.durationSnapshot().count(), "the histograms were copied after it");
		assertEquals(150, stats.latencySnapshot().count());
		assertEquals(0, stats.ttfbSnapshot().count());
		assertTrue(stats.durationSnapshot().min() > 0, "a negative bucket would have failed the difference");
		assertTrue(result.endAcquisitionNanos() >= TimeUnit.MILLISECONDS.toNanos(50), "the acquisition shows the pause");
		assertTrue(result.startAcquisitionNanos() < result.endAcquisitionNanos());
	}

	private static void record(
					final TimingMeterImpl duration, final TimingMeterImpl latency, final LongAdder succeeded, final int n) {
		for (int i = 0; i < n; i++) {
			duration.update(1_000 + i);
			latency.update(500 + i);
			succeeded.increment();
		}
	}
}
