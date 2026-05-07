package com.dell.spt.base.metrics.type;

import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.ConcurrentHistogram;

/** Meter tracking timing samples with min/max/sum aggregation. */
public class TimingMeterImpl implements LongMeter<TimingMetricSnapshot> {

	public static final long HIGHEST_TRACKABLE_VALUE_MICROS = 3_600_000_000L;
	public static final int SIGNIFICANT_VALUE_DIGITS = 2;

	private final LongAdder count = new LongAdder();
	private final LongAdder sum = new LongAdder();
	private final LongAdder overflowCount = new LongAdder();
	private final AtomicLong min = new AtomicLong(Long.MAX_VALUE);
	private final AtomicLong max = new AtomicLong(Long.MIN_VALUE);
	private final ConcurrentHistogram histogram = new ConcurrentHistogram(
					1L,
					HIGHEST_TRACKABLE_VALUE_MICROS,
					SIGNIFICANT_VALUE_DIGITS);
	private final String metricName;

	public TimingMeterImpl(final String metricName) {
		this.metricName = metricName;
	}

	@Override
	public void update(final long value) {
		final long recordedValue;
		if (value > HIGHEST_TRACKABLE_VALUE_MICROS) {
			recordedValue = HIGHEST_TRACKABLE_VALUE_MICROS;
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

	@Override
	public TimingMetricSnapshotImpl snapshot() {
		final long countSum = count.sum();
		if (countSum == 0) {
			return new TimingMetricSnapshotImpl(0, 0, 0, 0, 0, metricName);
		}
		final long sumSum = sum.sum();
		final var histogramCopy = histogram.copy();
		return new TimingMetricSnapshotImpl(
						sumSum,
						countSum,
						min.get(),
						max.get(),
						((double) sumSum) / countSum,
						metricName,
						overflowCount.sum(),
						TimingMetricSnapshotImpl.encodeHistogram(histogramCopy));
	}
}
