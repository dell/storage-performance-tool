package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.util.Locale;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

/**
 * The setup check of bucket metrics: before any meter is allocated, compares the sustained footprint
 * of a step's meter sets, the live meters and the retained start cut, with the maximum heap.
 */
public final class BucketMetricsBudget {

	/** Above this share of the maximum heap a step refuses to start. */
	static final double MAX_HEAP_SHARE = 0.40;

	private static final int TIMINGS_PER_SET = 3;
	/** Four counters, three timing meters and their object headers, rounded up. */
	private static final long SET_OVERHEAD_BYTES = 4096;
	private static final long MIB = 1L << 20;

	private BucketMetricsBudget() {}

	/**
	 * The histograms' sizes are held by a lazily loaded class, so a run without bucket metrics does
	 * not build them.
	 */
	private static final class Footprint {
		static final long PER_SET = TIMINGS_PER_SET * (live() + cut()) + SET_OVERHEAD_BYTES;

		private Footprint() {}

		private static long live() {
			return new ConcurrentHistogram(
							1L,
							TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS,
							TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS).getEstimatedFootprintInBytes();
		}

		private static long cut() {
			return new Histogram(
							1L,
							TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS,
							TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS).getEstimatedFootprintInBytes();
		}
	}

	/** Returns the sustained footprint of the given number of meter sets. */
	public static long sustainedBytes(final long sets) {
		return sets * Footprint.PER_SET;
	}

	/**
	 * @param sets the step's meter sets: for each metrics context, its members plus the catch-all
	 * @param maxHeapBytes the JVM's maximum heap
	 * @throws RunTerminalException when the footprint is above {@value #MAX_HEAP_SHARE} of the heap
	 */
	public static void check(final long sets, final long maxHeapBytes) {
		final long sustained = sustainedBytes(sets);
		if (sustained > maxHeapBytes * MAX_HEAP_SHARE) {
			throw new RunTerminalException(Category.CONFIGURATION, String.format(
							Locale.ROOT,
							"Bucket metrics for this step need about %d MiB of heap for %d meter sets, above %.0f%% of"
											+ " the %d MiB maximum heap. Rerun with --bucket-metrics=false, or raise the"
											+ " engine's maximum heap.",
							sustained / MIB,
							sets,
							MAX_HEAP_SHARE * 100,
							maxHeapBytes / MIB), null);
		}
	}
}
