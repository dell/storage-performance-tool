package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

final class RateMetricSnapshotImplTest {

	@Test
	void gettersReturnProvidedValues() {
		final var s = new RateMetricSnapshotImpl(1.5, 2.5, "rate", 10, 1234);
		assertEquals(1.5, s.last());
		assertEquals(2.5, s.mean());
		assertEquals(10, s.count());
		assertEquals(1234, s.elapsedTimeMillis());
		assertEquals("rate", s.name());
	}

	@Test
	void aggregateSumsRatesAndCountsAndKeepsMaxElapsed() {
		final RateMetricSnapshot s1 = new RateMetricSnapshotImpl(1, 2, "x", 5, 100);
		final RateMetricSnapshot s2 = new RateMetricSnapshotImpl(3, 4, "x", 7, 250);
		final var agg = RateMetricSnapshotImpl.aggregate(List.of(s1, s2));
		assertEquals(1 + 3, agg.last());
		assertEquals(2 + 4, agg.mean());
		assertEquals(5 + 7, agg.count());
		assertEquals(250, agg.elapsedTimeMillis());
		assertEquals("x", agg.name());
	}
}
