package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.type.TimingMeterImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BucketMeterSetTest {

	private static final long[] TIMING_VALUES = {
			-5, 0, 1, 2, 99, 100, 1_000, 123_456, TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS,
			TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS + 1, Long.MAX_VALUE / 2
	};

	private static MetricsContext<AllMetricsSnapshot> newAggregate() throws Exception {
		final MetricsContext<AllMetricsSnapshot> ctx = MetricsContextImpl.builder()
						.loadStepId("bucket-differential")
						.runId(1L)
						.opType(OpType.CREATE)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1024))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
		ctx.start();
		return ctx;
	}

	private static long timing(final Random random) {
		return random.nextBoolean()
						? TIMING_VALUES[random.nextInt(TIMING_VALUES.length)]
						: random.nextInt(5_000_000);
	}

	/** Item 6: one random sequence of calls gives the aggregate and the set the same numbers. */
	@ParameterizedTest
	@ValueSource(longs = {1L, 2L, 42L, 1234567L
	})
	void recordsWhatTheAggregateRecords(final long seed) throws Exception {
		final Random random = new Random(seed);
		final MetricsContext<AllMetricsSnapshot> aggregate = newAggregate();
		final BucketMeterSet set = new BucketMeterSet();
		for (int i = 0; i < 20_000; i++) {
			final long bytes = random.nextInt(1 << 20);
			final long duration = timing(random);
			final long latency = timing(random);
			final long ttfb = timing(random);
			switch (random.nextInt(6)) {
			case 0, 1 -> {
				aggregate.markSucc(bytes, duration, latency, ttfb);
				set.markSucc(bytes, duration, latency, ttfb);
			}
			case 2 -> {
				aggregate.markPartSucc(bytes, duration, latency, ttfb);
				set.markPartSucc(bytes, duration, latency, ttfb);
			}
			case 3 -> {
				aggregate.markFail();
				set.markFail();
			}
			case 4 -> {
				aggregate.markFail(duration, latency);
				set.markFail(duration, latency);
			}
			default -> {
				aggregate.markCorrupt();
				set.markCorrupt();
			}
			}
			if (i % 1000 == 0) {
				final long[] durations = {duration, timing(random)
				};
				final long[] latencies = {latency, timing(random)
				};
				final long[] ttfbs = {ttfb
				};
				aggregate.markSucc(7, bytes, durations, latencies, ttfbs);
				set.markSucc(7, bytes, durations, latencies, ttfbs);
			}
		}
		aggregate.refreshLastSnapshot();
		final AllMetricsSnapshot expected = aggregate.lastSnapshot();
		final BucketTerminalRecord actual = set.terminalRecord(OpType.CREATE, BucketKind.MEMBER, "b");
		assertEquals(expected.successSnapshot().count(), actual.succeeded());
		assertEquals(expected.failsSnapshot().count(), actual.failed());
		assertEquals(expected.corruptSnapshot().count(), actual.corrupt());
		assertEquals(expected.byteSnapshot().count(), actual.bytes());
		assertSameTiming(expected.durationSnapshot(), actual.duration());
		assertSameTiming(expected.latencySnapshot(), actual.latency());
		assertSameTiming(expected.ttfbSnapshot(), actual.timeToFirstByte());
		aggregate.close();
	}

	private static void assertSameTiming(final TimingMetricSnapshot expected, final TimingMetricSnapshot actual) {
		assertEquals(expected.count(), actual.count());
		assertEquals(expected.sum(), actual.sum());
		assertEquals(expected.min(), actual.min());
		assertEquals(expected.max(), actual.max());
		assertEquals(expected.overflowCount(), actual.overflowCount());
		for (final double quantile : new double[]{0.5, 0.9, 0.99, 0.999
		}) {
			assertEquals(expected.percentile(quantile), actual.percentile(quantile), "quantile " + quantile);
		}
		assertArrayEquals(expected.compressedHistogram(), actual.compressedHistogram());
	}

	@Test
	void anUntouchedSetReadsAsZeros() {
		final var record = new BucketMeterSet().terminalRecord(OpType.READ, BucketKind.CATCH_ALL, null);
		assertEquals(0, record.succeeded() + record.failed() + record.corrupt() + record.bytes());
		assertEquals(0, record.duration().count());
		assertEquals(OpType.READ, record.opType());
		assertEquals(BucketKind.CATCH_ALL, record.kind());
	}

	@Test
	void metersListMembersInOrderAndTheCatchAllLast() {
		final var meters = new BucketMeters(OpType.CREATE, List.of("b", "other", "a"));
		final var records = meters.terminalRecords();
		assertEquals(List.of("b", "other", "a"), records.subList(0, 3).stream().map(BucketTerminalRecord::bucket).toList());
		assertEquals(BucketKind.MEMBER, records.get(1).kind());
		assertEquals(BucketKind.CATCH_ALL, records.get(3).kind());
		assertEquals(null, records.get(3).bucket());
	}
}
