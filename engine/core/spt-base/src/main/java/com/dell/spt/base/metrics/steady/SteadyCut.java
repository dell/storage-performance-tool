package com.dell.spt.base.metrics.steady;

import org.HdrHistogram.Histogram;

/**
 * A Steady-State Cut: the engine's own reading of one metrics context at a window edge.
 *
 * <p>The meters are read one after another without blocking operation writers, so an operation
 * that completes during the reading can fall on either side of the edge, and differently in
 * each meter. Each histogram copy is consistent with itself.
 *
 * @param timeNanos monotonic time before the first meter was read
 * @param acquisitionNanos monotonic time the reading took
 */
public record SteadyCut(long timeNanos, long acquisitionNanos, Readings readings) {

	/** The meter values a cut reads, in the order the context reads them. */
	public record Readings(
					long succeeded,
					long failed,
					long corrupt,
					long bytes,
					double succeededLastRate,
					double failedLastRate,
					double corruptLastRate,
					double bytesLastRate,
					long concurrencySampleSum,
					long concurrencySampleCount,
					int inFlight,
					Histogram duration,
					Histogram latency,
					Histogram ttfb) {}
}
