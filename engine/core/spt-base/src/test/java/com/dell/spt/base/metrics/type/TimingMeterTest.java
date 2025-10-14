package com.dell.spt.base.metrics.type;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
