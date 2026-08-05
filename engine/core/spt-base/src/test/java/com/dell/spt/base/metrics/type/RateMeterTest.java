package com.dell.spt.base.metrics.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class RateMeterTest {

	private static final long BYTES_PER_MIB = 1024L * 1024L;

	@Test
	void meanRateUsesFractionalSecondsForShortRuns() {
		final var clock = new MutableClock();
		final RateMeter<RateMetricSnapshot> operationMeter = new RateMeterImpl(clock, "OPERATIONS");
		final RateMeter<RateMetricSnapshot> byteMeter = new RateMeterImpl(clock, "BYTES");

		operationMeter.update(32);
		byteMeter.update(32 * BYTES_PER_MIB);
		clock.advanceMillis(179);

		assertEquals(32.0 / 0.179, operationMeter.snapshot().mean(), 1.0e-9);
		assertEquals(32.0 * BYTES_PER_MIB / 0.179, byteMeter.snapshot().mean(), 1.0e-6);
	}

	@Test
	void meanRateRetainsFractionalPrecisionForLongerRuns() {
		final var clock = new MutableClock();
		final RateMeter<RateMetricSnapshot> meter = new RateMeterImpl(clock, "SOME_RATE");

		meter.update(1801);
		clock.advanceMillis(1500);

		assertEquals(1801.0 / 1.5, meter.snapshot().mean(), 1.0e-9);
	}

	@Test
	void lastRateUsesTheCompletedSamplingWindow() {
		final var clock = new MutableClock();
		final RateMeter<RateMetricSnapshot> meter = new RateMeterImpl(clock, "SOME_RATE");

		meter.update(1234);
		clock.advanceMillis(1000);

		assertEquals(1234.0, meter.snapshot().last(), 1.0e-9);
	}

	@Test
	void meanRateIsZeroUntilTimeAdvances() {
		final var clock = new MutableClock();
		final RateMeter<RateMetricSnapshot> meter = new RateMeterImpl(clock, "SOME_RATE");

		meter.update(32);

		assertEquals(0.0, meter.snapshot().mean());
	}

	private static final class MutableClock extends Clock {

		private long epochMillis;

		void advanceMillis(final long millis) {
			epochMillis += millis;
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(final ZoneId zone) {
			if (!ZoneOffset.UTC.equals(zone)) {
				throw new UnsupportedOperationException("test clock only supports UTC");
			}
			return this;
		}

		@Override
		public Instant instant() {
			return Instant.ofEpochMilli(epochMillis);
		}

		@Override
		public long millis() {
			return epochMillis;
		}
	}
}
