package com.dell.spt.base.metrics.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The rules by which the driver statistics of a distributed step's contributors are totalled. */
class DriverStatsTotalTest {

	private static final String DRIVER = "some-driver";

	@Test
	void sumsCountersAndGaugesByNameWhenEveryContributorReports() {
		final DriverStatsTotal total = DriverStatsTotal.of(
						List.of(stats(DRIVER, 3, 10), stats(DRIVER, 4, 20), stats(DRIVER, 0, 0)), true);

		assertEquals(stats(DRIVER, 7, 30), total.sum());
		assertEquals(3, total.reporting());
		assertFalse(total.partial());
		assertEquals(List.of(DRIVER), List.copyOf(total.drivers()));
	}

	@Test
	void keepsValuesBeyondTheIntegerRange() {
		final DriverStatsTotal total = DriverStatsTotal.of(
						List.of(stats(DRIVER, 1L << 40, 1L << 41), stats(DRIVER, 1L << 40, 1L << 41)), true);

		assertEquals(stats(DRIVER, 1L << 41, 1L << 42), total.sum());
	}

	@Test
	void leavesOutAPresentContributorWithoutStatisticsAndIsPartial() {
		final DriverStatsTotal total = DriverStatsTotal.of(
						Arrays.asList(stats(DRIVER, 3, 10), null, stats(DRIVER, 4, 20)), true);

		assertEquals(stats(DRIVER, 7, 30), total.sum());
		assertEquals(2, total.reporting());
		assertTrue(total.partial());
	}

	@Test
	void isPartialWhenAnExpectedContributorIsNotPresent() {
		final DriverStatsTotal total = DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10), stats(DRIVER, 4, 20)), false);

		assertEquals(stats(DRIVER, 7, 30), total.sum());
		assertEquals(2, total.reporting());
		assertTrue(total.partial());
	}

	@Test
	void hasNoSumWhenContributorsReportDifferentDrivers() {
		final DriverStatsTotal total = DriverStatsTotal.of(
						List.of(stats("zebra-driver", 3, 10), stats("apple-driver", 4, 20), stats("zebra-driver", 1, 1)), true);

		assertNull(total.sum());
		assertEquals(0, total.reporting());
		assertTrue(total.partial());
		assertEquals(List.of("apple-driver", "zebra-driver"), List.copyOf(total.drivers()));
	}

	@Test
	void sumsOverTheUnionOfNamesAndIsPartialWhenTheSetsOfNamesDiffer() {
		final DriverStatsSnapshot older = DriverStatsSnapshot.builder(DRIVER)
						.counter("apples", 3)
						.gauge("shelf_load", 10)
						.build();
		final DriverStatsSnapshot newer = DriverStatsSnapshot.builder(DRIVER)
						.counter("apples", 4)
						.counter("pears", 5)
						.gauge("shelf_load", 20)
						.gauge("shelf_peak", 25)
						.build();

		final DriverStatsTotal total = DriverStatsTotal.of(List.of(older, newer), true);

		assertEquals(Map.of("apples", 7L, "pears", 5L), total.sum().counters());
		assertEquals(Map.of("shelf_load", 30L, "shelf_peak", 25L), total.sum().gauges());
		assertEquals(2, total.reporting());
		assertTrue(total.partial());
	}

	@Test
	void isPartialWhenOnlyTheGaugeNamesDiffer() {
		final DriverStatsSnapshot withPeak = DriverStatsSnapshot.builder(DRIVER)
						.counter("apples", 4)
						.gauge("shelf_load", 20)
						.gauge("shelf_peak", 25)
						.build();

		assertTrue(DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10), withPeak), true).partial());
	}

	@Test
	void isAbsentWhenNoContributorReports() {
		assertNull(DriverStatsTotal.of(Arrays.asList(null, null), true));
		assertNull(DriverStatsTotal.of(List.of(), true));
	}

	@Test
	void totalOfOneContributorIsWhatItReported() {
		final DriverStatsTotal total = DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10)), true);

		assertEquals(stats(DRIVER, 3, 10), total.sum());
		assertEquals(1, total.reporting());
		assertFalse(total.partial());
	}

	@Test
	void doesNotChangeAfterItIsBuilt() {
		final DriverStatsTotal total = DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10)), true);

		assertThrows(UnsupportedOperationException.class, () -> total.drivers().add("other-driver"));
	}

	@Test
	void isEqualByItsValues() {
		final DriverStatsTotal total = DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10), stats(DRIVER, 4, 20)), true);

		assertEquals(total, DriverStatsTotal.of(List.of(stats(DRIVER, 4, 20), stats(DRIVER, 3, 10)), true));
		assertEquals(
						total.hashCode(),
						DriverStatsTotal.of(List.of(stats(DRIVER, 4, 20), stats(DRIVER, 3, 10)), true).hashCode());
		assertNotEquals(total, DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10), stats(DRIVER, 4, 20)), false));
		assertNotEquals(total, DriverStatsTotal.of(List.of(stats(DRIVER, 7, 30)), true));
		assertNotEquals(total, DriverStatsTotal.of(List.of(stats(DRIVER, 3, 10), stats(DRIVER, 4, 21)), true));
	}

	@Test
	void survivesSerialization() throws Exception {
		final DriverStatsTotal summed = DriverStatsTotal.of(Arrays.asList(stats(DRIVER, 3, 10), null), true);
		final DriverStatsTotal mismatch = DriverStatsTotal.of(List.of(stats("a", 1, 1), stats("b", 1, 1)), true);

		assertEquals(summed, serializedCopy(summed));
		assertEquals(mismatch, serializedCopy(mismatch));
	}

	private static DriverStatsSnapshot stats(final String driver, final long apples, final long shelfLoad) {
		return DriverStatsSnapshot.builder(driver)
						.counter("apples", apples)
						.gauge("shelf_load", shelfLoad)
						.build();
	}

	private static Object serializedCopy(final Object value) throws Exception {
		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
			out.writeObject(value);
		}
		try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			return in.readObject();
		}
	}
}
