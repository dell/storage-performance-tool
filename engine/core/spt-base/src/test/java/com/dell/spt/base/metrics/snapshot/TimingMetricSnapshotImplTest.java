package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.metrics.MetricsConstants;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

final class TimingMetricSnapshotImplTest {

	@Test
	void gettersReturnProvidedValues() {
		final var s = new TimingMetricSnapshotImpl(100, 4, 10, 50, 25.0, "latency");
		assertEquals(100, s.sum());
		assertEquals(4, s.count());
		assertEquals(10, s.min());
		assertEquals(50, s.max());
		assertEquals(25.0, s.mean());
		assertEquals("latency", s.name());
	}

	@Test
	void aggregateSumsAndComputesMinMaxMean() {
		final TimingMetricSnapshot s1 = TimingMetricSnapshotImpl.fromSamples("dur", List.of(5L, 10L, 15L));
		final TimingMetricSnapshot s2 = new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, "dur");
		final var agg = TimingMetricSnapshotImpl.aggregate(List.of(s1, s2));
		assertEquals(30, agg.sum());
		assertEquals(3, agg.count());
		assertEquals(15, agg.max());
		assertEquals(10.0, agg.mean());
		assertEquals("dur", agg.name());
	}

	@Test
	void aggregateMergesCompressedHistogramsForPercentiles() {
		final TimingMetricSnapshot low = TimingMetricSnapshotImpl.fromSamples("lat", List.of(1L, 2L, 3L));
		final TimingMetricSnapshot high = TimingMetricSnapshotImpl.fromSamples("lat", List.of(100L, 200L, 300L));

		final TimingMetricSnapshot aggregate = TimingMetricSnapshotImpl.aggregate(List.of(low, high));

		assertEquals(6, aggregate.count());
		assertTrue(aggregate.percentile(0.5) >= 3);
		assertTrue(aggregate.percentile(0.9) >= 200);
		assertTrue(aggregate.compressedHistogram().length > 0);
	}

	@Test
	void aggregateSumsOverflowCounts() {
		final TimingMetricSnapshot first = TimingMetricSnapshotImpl.fromSamples(
						"lat",
						List.of(com.dell.spt.base.metrics.type.TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS + 1));
		final TimingMetricSnapshot second = TimingMetricSnapshotImpl.fromSamples(
						"lat",
						List.of(com.dell.spt.base.metrics.type.TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS + 2));

		final TimingMetricSnapshot aggregate = TimingMetricSnapshotImpl.aggregate(List.of(first, second));

		assertEquals(2, aggregate.count());
		assertEquals(2, aggregate.overflowCount());
	}

	@Test
	void aggregateEmptyTtfbSnapshotsUsesTtfbMetricName() {
		final TimingMetricSnapshot aggregate = TimingMetricSnapshotImpl.aggregate(
						List.of(),
						MetricsConstants.METRIC_NAME_TTFB);

		assertEquals(0, aggregate.count());
		assertEquals(MetricsConstants.METRIC_NAME_TTFB, aggregate.name());
	}

	@Test
	void compressedHistogramRoundTripsWithoutDefaultObjectSerialization() {
		final TimingMetricSnapshot original = TimingMetricSnapshotImpl.fromSamples("lat", List.of(10L, 20L, 30L));

		final TimingMetricSnapshot decoded = TimingMetricSnapshotImpl.fromCompressedHistogram(
						original.name(),
						original.sum(),
						original.count(),
						original.min(),
						original.max(),
						original.mean(),
						original.overflowCount(),
						original.compressedHistogram());

		assertEquals(original.count(), decoded.count());
		assertEquals(original.percentile(0.5), decoded.percentile(0.5));
		assertEquals(original.percentile(0.99), decoded.percentile(0.99));
	}

	@Test
	void snapshotJavaSerializationPreservesCompressedHistogramPercentiles() throws Exception {
		final TimingMetricSnapshot original = TimingMetricSnapshotImpl.fromSamples("lat", List.of(10L, 20L, 30L));
		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
			out.writeObject(original);
		}

		final TimingMetricSnapshot decoded;
		try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			decoded = (TimingMetricSnapshot) in.readObject();
		}

		assertEquals(original.count(), decoded.count());
		assertEquals(original.percentile(0.5), decoded.percentile(0.5));
		assertEquals(original.percentile(0.99), decoded.percentile(0.99));
	}

	@Test
	void aggregateZeroSumForcesZeroMinMax() {
		final TimingMetricSnapshot s1 = new TimingMetricSnapshotImpl(0, 0, 7, 9, 0.0, "lat");
		final var singleAggregate = TimingMetricSnapshotImpl.aggregate(List.of(s1));
		assertSame(s1, singleAggregate, "Single-element aggregation should return the original snapshot");
		// Single snapshot path returns same object; guard zero behavior in code via second snapshot
		final var agg2 = TimingMetricSnapshotImpl.aggregate(List.of(s1, new TimingMetricSnapshotImpl(0, 0, 1, 2, 0.0, "lat")));
		assertEquals(0, agg2.sum());
		assertEquals(0, agg2.min());
		assertEquals(0, agg2.max());
	}
}
