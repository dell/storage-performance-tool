package com.dell.spt.base.metrics.type;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

/** @author veronika K. on 16.10.18 */
public class RateMeterTest {

	private static final int SLEEP_MILLISEC = 1000;
	private static final int COUNT_BYTES_1 = 1234;
	private static final int COUNT_BYTES_2 = 567;
	private static final double ACCURACY = 0.1;

	@Test
	public void test() throws InterruptedException {
		final RateMeter<RateMetricSnapshot> meter = new RateMeterImpl(Clock.systemUTC(), "SOME_RATE");
		final var t0 = System.currentTimeMillis();
		meter.update(COUNT_BYTES_1);
		TimeUnit.MILLISECONDS.sleep(SLEEP_MILLISEC);
		final var t1 = System.currentTimeMillis();
		final var snapshot1 = meter.snapshot();
		final var expected1 = 1000.0 * COUNT_BYTES_1 / (t1 - t0);
		assertEquals(expected1, snapshot1.mean(), expected1 * ACCURACY);
		assertEquals(expected1, snapshot1.last(), expected1 * ACCURACY);
		meter.update(COUNT_BYTES_2);
		TimeUnit.MILLISECONDS.sleep(SLEEP_MILLISEC);
		final var t2 = System.currentTimeMillis();
		final var snapshot2 = meter.snapshot();
		final var expectedRate = 1000.0 * (COUNT_BYTES_1 + COUNT_BYTES_2) / (t2 - t0);
		assertEquals(expectedRate, snapshot2.mean(), expectedRate * ACCURACY);
		assertEquals(expectedRate, snapshot2.last(), expectedRate * ACCURACY);
	}
}
