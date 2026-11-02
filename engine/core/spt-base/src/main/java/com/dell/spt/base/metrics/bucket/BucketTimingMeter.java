package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.type.LongMeter;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntFunction;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

/**
 * A timing meter of the aggregate's range and precision on a histogram with one count array, half
 * the size of a {@link TimingMeterImpl} and quicker to copy, because a step can hold thousands of
 * them and copies them inside its stop path. The only type to change to use another histogram.
 */
public final class BucketTimingMeter implements LongMeter<TimingMetricSnapshot> {

	private final LongAdder count = new LongAdder();
	private final LongAdder sum = new LongAdder();
	private final LongAdder overflowCount = new LongAdder();
	private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
	private final AtomicHistogram histogram = new AtomicHistogram(
					1L, TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
	private final String metricName;

	public BucketTimingMeter(final String metricName) {
		this.metricName = metricName;
	}

	@Override
	public void update(final long value) {
		final long recordedValue;
		if (value > TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS) {
			recordedValue = TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS;
			overflowCount.increment();
		} else {
			recordedValue = Math.max(0L, value);
		}
		sum.add(recordedValue);
		min.accumulateAndGet(recordedValue, Math::min);
		max.accumulateAndGet(recordedValue, Math::max);
		histogram.recordValue(recordedValue);
		count.increment();
	}

	/**
	 * A plain copy of the histogram, consistent with itself: its total is the sum of the buckets it
	 * read. Nothing is lost once writers have stopped.
	 */
	public Histogram histogramCopy() {
		final Histogram copy = new Histogram(
						1L, TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
		copy.add(histogram);
		return copy;
	}

	@Override
	public TimingMetricSnapshot snapshot() {
		return snapshot(capacity -> ByteBuffer.allocate(capacity));
	}

	/** A snapshot encoded with a buffer the caller reuses, because a step reads thousands of them. */
	TimingMetricSnapshot snapshot(final IntFunction<ByteBuffer> encodeBuffer) {
		final long countSum = count.sum();
		if (countSum == 0) {
			return new TimingMetricSnapshotImpl(0, 0, 0, 0, 0, metricName);
		}
		final long sumSum = sum.sum();
		return new TimingMetricSnapshotImpl(
						sumSum,
						countSum,
						min.get(),
						max.get(),
						((double) sumSum) / countSum,
						metricName,
						overflowCount.sum(),
						TimingMetricSnapshotImpl.encodeHistogram(histogramCopy(), encodeBuffer));
	}
}
