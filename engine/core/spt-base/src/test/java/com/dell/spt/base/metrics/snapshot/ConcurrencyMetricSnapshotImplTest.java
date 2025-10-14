package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

final class ConcurrencyMetricSnapshotImplTest {

	@Test
	void gettersReturnProvidedValues() {
		final var s = new ConcurrencyMetricSnapshotImpl("con", 5, 2.5);
		assertEquals("con", s.name());
		assertEquals(5, s.last());
		assertEquals(2.5, s.mean());
	}

	@Test
	void aggregateSumsLastAndMean() {
		final ConcurrencyMetricSnapshot s1 = new ConcurrencyMetricSnapshotImpl("c", 1, 1.5);
		final ConcurrencyMetricSnapshot s2 = new ConcurrencyMetricSnapshotImpl("c", 2, 2.0);
		final var agg = ConcurrencyMetricSnapshotImpl.aggregate(List.of(s1, s2));
		assertEquals("c", agg.name());
		assertEquals(3, agg.last());
		assertEquals(3.5, agg.mean());
	}
}
