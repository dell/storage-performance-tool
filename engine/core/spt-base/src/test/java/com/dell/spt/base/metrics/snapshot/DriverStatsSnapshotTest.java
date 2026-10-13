package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.junit.jupiter.api.Test;

class DriverStatsSnapshotTest {

	@Test
	void carriesTheDriverAndItsNamedValuesInNameOrder() {
		final DriverStatsSnapshot stats = DriverStatsSnapshot.builder("some-driver")
						.counter("zebras", 3)
						.counter("apples", Long.MAX_VALUE)
						.gauge("shelf_load", 0)
						.build();

		assertEquals("some-driver", stats.driver());
		assertEquals(List.of("apples", "zebras"), List.copyOf(stats.counters().keySet()));
		assertEquals(Long.MAX_VALUE, stats.counters().get("apples"));
		assertEquals(3L, stats.counters().get("zebras"));
		assertEquals(0L, stats.gauges().get("shelf_load"));
	}

	@Test
	void doesNotChangeAfterItIsBuilt() {
		final DriverStatsSnapshot.Builder builder = DriverStatsSnapshot.builder("some-driver").counter("apples", 1);
		final DriverStatsSnapshot stats = builder.build();

		builder.counter("apples", 2).gauge("shelf_load", 9);

		assertEquals(1L, stats.counters().get("apples"));
		assertEquals(0, stats.gauges().size());
		assertThrows(UnsupportedOperationException.class, () -> stats.counters().put("apples", 5L));
		assertThrows(UnsupportedOperationException.class, () -> stats.gauges().put("shelf_load", 5L));
	}

	@Test
	void isEqualByDriverAndValues() {
		final DriverStatsSnapshot stats = DriverStatsSnapshot.builder("some-driver").counter("apples", 1).build();

		assertEquals(stats, DriverStatsSnapshot.builder("some-driver").counter("apples", 1).build());
		assertEquals(
						stats.hashCode(),
						DriverStatsSnapshot.builder("some-driver").counter("apples", 1).build().hashCode());
		assertNotEquals(stats, DriverStatsSnapshot.builder("other-driver").counter("apples", 1).build());
		assertNotEquals(stats, DriverStatsSnapshot.builder("some-driver").counter("apples", 2).build());
		assertNotEquals(stats, DriverStatsSnapshot.builder("some-driver").gauge("apples", 1).build());
	}

	@Test
	void requiresADriverAndNames() {
		assertThrows(NullPointerException.class, () -> DriverStatsSnapshot.builder(null));
		assertThrows(NullPointerException.class, () -> DriverStatsSnapshot.builder("some-driver").counter(null, 1));
		assertThrows(NullPointerException.class, () -> DriverStatsSnapshot.builder("some-driver").gauge(null, 1));
	}

	@Test
	void survivesSerialization() throws Exception {
		final DriverStatsSnapshot stats = DriverStatsSnapshot.builder("some-driver")
						.counter("apples", 7)
						.gauge("shelf_load", 11)
						.build();
		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
			out.writeObject(stats);
		}

		final Object copy;
		try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			copy = in.readObject();
		}

		assertEquals(stats, copy);
	}
}
