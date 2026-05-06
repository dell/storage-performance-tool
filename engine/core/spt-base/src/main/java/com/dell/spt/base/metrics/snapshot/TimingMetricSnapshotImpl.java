package com.dell.spt.base.metrics.snapshot;

import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DataFormatException;
import org.HdrHistogram.Histogram;

/** Immutable summary of timing statistics for a metric. */
public class TimingMetricSnapshotImpl extends NamedCountMetricSnapshotImpl
				implements TimingMetricSnapshot {

	private final long sum;
	private final long min;
	private final long max;
	private final double mean;
	private final long overflowCount;
	private final byte[] compressedHistogram;
	private transient volatile Histogram decodedHistogram;

	public TimingMetricSnapshotImpl(
					final long sum,
					final long count,
					final long min,
					final long max,
					final double mean,
					final String metricName) {
		this(sum, count, min, max, mean, metricName, 0L, new byte[0]);
	}

	public TimingMetricSnapshotImpl(
					final long sum,
					final long count,
					final long min,
					final long max,
					final double mean,
					final String metricName,
					final long overflowCount,
					final byte[] compressedHistogram) {
		super(metricName, count);
		this.sum = sum;
		this.min = min;
		this.max = max;
		this.mean = mean;
		this.overflowCount = overflowCount;
		this.compressedHistogram = compressedHistogram == null
						? new byte[0]
						: Arrays.copyOf(compressedHistogram, compressedHistogram.length);
	}

	public static TimingMetricSnapshot aggregate(final List<TimingMetricSnapshot> snapshots) {
		return aggregate(snapshots, null);
	}

	public static TimingMetricSnapshot aggregate(final List<TimingMetricSnapshot> snapshots, final String emptyMetricName) {
		final int snapshotCount = snapshots.size();
		if (snapshotCount == 0) {
			return new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, emptyMetricName == null ? "" : emptyMetricName);
		}
		if (snapshotCount == 1) {
			return snapshots.get(0);
		}
		long countSum = 0;
		long sumOfSums = 0;
		long newMax = Long.MIN_VALUE;
		long newMin = Long.MAX_VALUE;
		long overflowCount = 0;
		Histogram aggregateHistogram = null;
		TimingMetricSnapshot nextSnapshot;
		for (int i = 0; i < snapshotCount; ++i) {
			nextSnapshot = snapshots.get(i);
			countSum += nextSnapshot.count();
			sumOfSums += nextSnapshot.sum();
			if (nextSnapshot.count() > 0) {
				newMax = Math.max(newMax, nextSnapshot.max());
				newMin = Math.min(newMin, nextSnapshot.min());
			}
			overflowCount += nextSnapshot.overflowCount();
			final byte[] histogramBytes = nextSnapshot.compressedHistogram();
			if (histogramBytes.length > 0) {
				final Histogram decoded = decodeHistogram(histogramBytes);
				if (decoded != null) {
					if (aggregateHistogram == null) {
						aggregateHistogram = new Histogram(
										1L,
										TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS,
										TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
					}
					aggregateHistogram.add(decoded);
				}
			}
		}
		if (countSum == 0) {
			newMin = 0;
			newMax = 0;
		}
		final double newMean = countSum > 0 ? ((double) sumOfSums) / countSum : 0;
		return new TimingMetricSnapshotImpl(
						sumOfSums,
						countSum,
						newMin,
						newMax,
						newMean,
						snapshots.get(0).name(),
						overflowCount,
						encodeHistogram(aggregateHistogram));
	}

	public static TimingMetricSnapshot fromSamples(final String metricName, final List<Long> samples) {
		final Histogram histogram = new Histogram(
						1L,
						TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS,
						TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
		long sum = 0L;
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		long overflowCount = 0L;
		for (final long sample : samples) {
			final long value;
			if (sample > TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS) {
				value = TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS;
				overflowCount++;
			} else {
				value = Math.max(0L, sample);
			}
			histogram.recordValue(value);
			sum += value;
			min = Math.min(min, value);
			max = Math.max(max, value);
		}
		final long count = samples.size();
		if (count == 0) {
			min = 0;
			max = 0;
		}
		return new TimingMetricSnapshotImpl(
						sum,
						count,
						min,
						max,
						count > 0 ? ((double) sum) / count : 0.0,
						metricName,
						overflowCount,
						encodeHistogram(histogram));
	}

	public static TimingMetricSnapshot fromCompressedHistogram(
					final String metricName,
					final long sum,
					final long count,
					final long min,
					final long max,
					final double mean,
					final long overflowCount,
					final byte[] compressedHistogram) {
		return new TimingMetricSnapshotImpl(
						sum,
						count,
						min,
						max,
						mean,
						metricName,
						overflowCount,
						compressedHistogram);
	}

	@Override
	public final long sum() {
		return sum;
	}

	@Override
	public final long min() {
		return min;
	}

	@Override
	public final long max() {
		return max;
	}

	@Override
	public final double mean() {
		return mean;
	}

	@Override
	public final long percentile(final double quantile) {
		if (quantile <= 0.0 || count() == 0) {
			return 0L;
		}
		final Histogram histogram = decodedHistogram();
		if (histogram == null) {
			return 0L;
		}
		return Math.min(max, histogram.getValueAtPercentile(Math.min(100.0, quantile * 100.0)));
	}

	@Override
	public final long overflowCount() {
		return overflowCount;
	}

	@Override
	public final byte[] compressedHistogram() {
		return Arrays.copyOf(compressedHistogram, compressedHistogram.length);
	}

	private Histogram decodedHistogram() {
		Histogram histogram = decodedHistogram;
		if (histogram == null && compressedHistogram.length > 0) {
			histogram = decodeHistogram(compressedHistogram);
			decodedHistogram = histogram;
		}
		return histogram;
	}

	public static byte[] encodeHistogram(final Histogram histogram) {
		if (histogram == null || histogram.getTotalCount() == 0) {
			return new byte[0];
		}
		final ByteBuffer buffer = ByteBuffer.allocate(histogram.getNeededByteBufferCapacity());
		final int length = histogram.encodeIntoCompressedByteBuffer(buffer);
		return Arrays.copyOf(buffer.array(), length);
	}

	private static Histogram decodeHistogram(final byte[] compressedHistogram) {
		try {
			return Histogram.decodeFromCompressedByteBuffer(
							ByteBuffer.wrap(compressedHistogram),
							TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS);
		} catch (final DataFormatException | RuntimeException e) {
			return null;
		}
	}
}
