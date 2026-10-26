package com.dell.spt.base.metrics.steady;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * {@code SteadyTrend[%]} and {@code SteadyCV} from a window's stability series, with the
 * calibration replay's rules ({@code .scratch/steady-state-metrics/steady_replay.py},
 * {@code binned_rates}, {@code thirds_trend} and {@code cv}).
 *
 * <p>Bins are consecutive groups of sample intervals, each wide enough to average at least
 * {@value #BIN_COMPLETIONS} completions and {@value #BIN_TURNOVERS} turnovers, so a bin's edge
 * error of about one in-flight set stays near 1 / {@value #BIN_TURNOVERS} of its count. The engine
 * applies no threshold to either value.
 */
final class SteadyStability {

	static final int BIN_COMPLETIONS = 100;
	static final int BIN_TURNOVERS = 20;
	static final int MIN_BINS = 6;

	/** Both values are null when the window has fewer than {@value #MIN_BINS} bins. */
	record Indicators(Double trendPercent, Double cv) {}

	static final Indicators EMPTY = new Indicators(null, null);

	private SteadyStability() {}

	/**
	 * Both indicators from one window's series.
	 *
	 * @param time monotonic sample times, nanoseconds
	 * @param succeeded cumulative succeeded operations at each sample
	 * @param failed cumulative failed operations at each sample
	 * @param size the number of samples, the start cut first and the end cut last
	 * @param meanInFlight the window's mean operations in flight
	 */
	static Indicators compute(
					final long[] time, final long[] succeeded, final long[] failed, final int size, final double meanInFlight) {
		final double[] rates = binRates(time, succeeded, failed, size, meanInFlight);
		if (rates.length < MIN_BINS) {
			return EMPTY;
		}
		final double mean = Arrays.stream(rates).average().orElse(0);
		Double cv = null;
		if (mean > 0) {
			double squares = 0;
			for (final double rate : rates) {
				squares += (rate - mean) * (rate - mean);
			}
			cv = Math.sqrt(squares / rates.length) / mean;
		}
		final int k = rates.length / 3;
		final double first = Arrays.stream(rates, 0, k).average().orElse(0);
		final double last = Arrays.stream(rates, rates.length - k, rates.length).average().orElse(0);
		return new Indicators(first > 0 ? 100.0 * (last - first) / first : null, cv);
	}

	/** The succeeded-operation rate of each bin, per second. */
	static double[] binRates(
					final long[] time, final long[] succeeded, final long[] failed, final int size, final double meanInFlight) {
		final int intervals = size - 1;
		if (intervals <= 0) {
			return new double[0];
		}
		final long done = succeeded[intervals] + failed[intervals] - succeeded[0] - failed[0];
		if (done <= 0) {
			return new double[0];
		}
		final double turnovers = done / Math.max(1.0, meanInFlight);
		final int width = (int) Math.max(
						1,
						Math.ceil(Math.max(
										BIN_COMPLETIONS * (double) intervals / done,
										BIN_TURNOVERS * (double) intervals / turnovers)));
		final double[] rates = new double[intervals / width];
		int bins = 0;
		for (int i = 0; i + width <= intervals; i += width) {
			final long nanos = time[i + width] - time[i];
			if (nanos > 0) {
				rates[bins++] = (succeeded[i + width] - succeeded[i]) / (nanos / (double) TimeUnit.SECONDS.toNanos(1));
			}
		}
		return Arrays.copyOf(rates, bins);
	}
}
