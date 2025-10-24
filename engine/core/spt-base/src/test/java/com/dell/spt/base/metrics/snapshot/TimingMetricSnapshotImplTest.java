package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.*;

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
		final TimingMetricSnapshot s1 = new TimingMetricSnapshotImpl(30, 3, 5, 20, 10.0, "dur");
		final TimingMetricSnapshot s2 = new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, "dur");
		final var agg = TimingMetricSnapshotImpl.aggregate(List.of(s1, s2));
		assertEquals(30, agg.sum());
		assertEquals(3, agg.count());
		assertEquals(20, agg.max());
		assertEquals(10.0, agg.mean());
		assertEquals("dur", agg.name());
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
