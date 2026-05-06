package com.dell.spt.base.metrics.type;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;

/** @author veronika K. on 17.10.18 */
public class TimingMeterTest {

	private static final int INTERVALS = 100;

	@Test
	public void test() {
		final LongMeter<TimingMetricSnapshot> meter = new TimingMeterImpl("SOME_METRIC");
		int sum = 0;
		for (int i = 0; i < INTERVALS; ++i) {
			meter.update(i);
			sum += i;
		}
		//
		final TimingMetricSnapshot snapshot = meter.snapshot();
		assertEquals(snapshot.count(), INTERVALS);
		assertEquals(snapshot.sum(), sum);
		assertEquals(snapshot.min(), 0);
		final double mean = ((double) sum) / INTERVALS;
		assertEquals(snapshot.mean(), mean, mean * 0.001);
		assertEquals(snapshot.max(), INTERVALS - 1);
	}

	@Test
	void snapshotReportsStandardPercentilesFromHistogram() {
		final LongMeter<TimingMetricSnapshot> meter = new TimingMeterImpl("SOME_METRIC");
		for (int i = 1; i <= 100; ++i) {
			meter.update(i);
		}

		final TimingMetricSnapshot snapshot = meter.snapshot();

		assertTrue(snapshot.percentile(0.5) >= 49 && snapshot.percentile(0.5) <= 51);
		assertTrue(snapshot.percentile(0.9) >= 89 && snapshot.percentile(0.9) <= 91);
		assertTrue(snapshot.percentile(0.99) >= 98 && snapshot.percentile(0.99) <= 100);
		assertTrue(snapshot.compressedHistogram().length > 0);
	}

	@Test
	void valuesAboveHighestTrackableValueAreClampedAndCounted() {
		final LongMeter<TimingMetricSnapshot> meter = new TimingMeterImpl("SOME_METRIC");

		meter.update(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS + 1);

		final TimingMetricSnapshot snapshot = meter.snapshot();
		assertEquals(1, snapshot.count());
		assertEquals(1, snapshot.overflowCount());
		assertEquals(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, snapshot.max());
		assertEquals(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, snapshot.percentile(0.999));
	}

	@Test
	void highestTrackableValueIsRecordedWithoutOverflow() {
		final LongMeter<TimingMetricSnapshot> meter = new TimingMeterImpl("SOME_METRIC");

		meter.update(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS);

		final TimingMetricSnapshot snapshot = meter.snapshot();
		assertEquals(1, snapshot.count());
		assertEquals(0, snapshot.overflowCount());
		assertEquals(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, snapshot.max());
		assertEquals(TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, snapshot.percentile(0.5));
	}

	@Test
	void oneMicrosecondValueIsRecordedAtLowerBound() {
		final LongMeter<TimingMetricSnapshot> meter = new TimingMeterImpl("SOME_METRIC");

		meter.update(1L);

		final TimingMetricSnapshot snapshot = meter.snapshot();
		assertEquals(1, snapshot.count());
		assertEquals(1L, snapshot.min());
		assertEquals(1L, snapshot.max());
		assertEquals(1L, snapshot.percentile(0.5));
	}

	@Test
	void zeroIsAllowedAndRecorded() {
		final LongMeter<TimingMetricSnapshot> meter = new TimingMeterImpl("SOME_METRIC");
		meter.update(0L);
		final TimingMetricSnapshot snapshot = meter.snapshot();
		assertEquals(1, snapshot.count());
		assertEquals(0L, snapshot.max());
		assertEquals(0L, snapshot.percentile(0.5));
	}
}
