package com.dell.spt.base.metrics.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsTotal;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * How the Entry Node totals the storage driver statistics of a distributed step.
 *
 * <p>Each contributor is a real node metrics context. The Entry Node's own contributor hands over
 * its snapshot directly; a remote one hands it over serialized, as the node-to-node transfer does.
 */
class DistributedMetricsContextImplDriverStatsTest {

	private static final String STEP_ID = "driver-stats-fleet";

	private final List<MetricsContext<?>> contexts = new ArrayList<>();

	@AfterEach
	void closeContexts() {
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void totalIsTheSumOfTheEntryNodesOwnContributorAndARemoteOne() throws Exception {
		final AllMetricsSnapshot local = node(() -> StatsReportingDriverMock.stats(8, 3, 5));
		final AllMetricsSnapshot remote = transferred(node(() -> StatsReportingDriverMock.stats(6, 2, 4)));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> List.of(local, remote));
		final DriverStatsTotal total = totalOf(fleet);

		assertEquals(StatsReportingDriverMock.stats(14, 5, 9), total.sum());
		assertEquals(2, total.reporting());
		assertFalse(total.partial());
		assertFalse(fleet.partial());
		// A fleet row has a total, not the statistics of one driver.
		assertNull(fleet.lastSnapshot().driverStats());
	}

	@Test
	void statisticsOfADriverTheCoreDoesNotKnowCrossTheTransferUnchanged() throws Exception {
		final DriverStatsSnapshot reported = StatsReportingDriverMock.stats(Long.MAX_VALUE, 0, 1L << 40);

		final AllMetricsSnapshot remote = transferred(node(() -> reported));

		assertEquals(reported, remote.driverStats());
		assertEquals(1, remote.successSnapshot().count());
		assertEquals(DriverStatsFleet.ITEM_SIZE, remote.byteSnapshot().count());
	}

	@Test
	void snapshotOfADriverWithoutStatisticsCrossesTheTransferWithoutThem() throws Exception {
		assertNull(transferred(node(null)).driverStats());
	}

	@Test
	void presentContributorWithoutStatisticsIsLeftOutOfTheTotal() throws Exception {
		final AllMetricsSnapshot local = node(() -> StatsReportingDriverMock.stats(8, 3, 5));
		final AllMetricsSnapshot remote = transferred(node(() -> null));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> List.of(local, remote));
		final DriverStatsTotal total = totalOf(fleet);

		assertEquals(StatsReportingDriverMock.stats(8, 3, 5), total.sum());
		assertEquals(1, total.reporting());
		assertTrue(total.partial());
		assertFalse(fleet.partial(), "the row itself is complete: both contributors are present");
		assertEquals(2, fleet.lastSnapshot().successSnapshot().count());
	}

	@Test
	void contributorsReportingDifferentDriversGiveNoSum() throws Exception {
		final AllMetricsSnapshot local = node(() -> StatsReportingDriverMock.stats(8, 3, 5));
		final AllMetricsSnapshot remote = transferred(
						node(() -> DriverStatsSnapshot.builder("other-mock").counter("gears_cut", 6).build()));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> List.of(local, remote));
		final DriverStatsTotal total = totalOf(fleet);

		assertNull(total.sum());
		assertEquals(0, total.reporting());
		assertTrue(total.partial());
		assertEquals(List.of("other-mock", StatsReportingDriverMock.DRIVER), List.copyOf(total.drivers()));
		assertFalse(fleet.partial());
	}

	@Test
	void contributorsReportingDifferentNamesAreSummedOverTheUnion() throws Exception {
		final AllMetricsSnapshot local = node(() -> StatsReportingDriverMock.stats(8, 3, 5));
		final AllMetricsSnapshot remote = transferred(node(() -> DriverStatsSnapshot.builder(StatsReportingDriverMock.DRIVER)
						.counter(StatsReportingDriverMock.COUNTER_WIDGETS, 6)
						.counter("gears_cut", 1)
						.gauge(StatsReportingDriverMock.GAUGE_SHELF, 4)
						.build()));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> List.of(local, remote));
		final DriverStatsTotal total = totalOf(fleet);

		assertEquals(
						Map.of(
										StatsReportingDriverMock.COUNTER_WIDGETS, 14L,
										StatsReportingDriverMock.COUNTER_SPROCKETS, 3L,
										"gears_cut", 1L),
						total.sum().counters());
		assertEquals(Map.of(StatsReportingDriverMock.GAUGE_SHELF, 9L), total.sum().gauges());
		assertEquals(2, total.reporting());
		assertTrue(total.partial());
		assertFalse(fleet.partial());
	}

	@Test
	void fleetHasNoTotalWhenNoContributorReports() throws Exception {
		final AllMetricsSnapshot local = node(null);
		final AllMetricsSnapshot remote = transferred(node(() -> null));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> List.of(local, remote));
		fleet.refreshLastSnapshot(true);

		final DistributedAllMetricsSnapshot snapshot = (DistributedAllMetricsSnapshot) fleet.lastSnapshot();
		assertNull(snapshot.driverStatsTotal());
		assertFalse(fleet.partial());
		assertEquals(2, snapshot.successSnapshot().count());
	}

	@Test
	void totalIsPartialWhenAnExpectedContributorIsNotPresent() {
		final AllMetricsSnapshot local = node(() -> StatsReportingDriverMock.stats(8, 3, 5));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> Arrays.asList(local, null));
		final DriverStatsTotal total = totalOf(fleet);

		assertEquals(StatsReportingDriverMock.stats(8, 3, 5), total.sum());
		assertEquals(1, total.reporting());
		assertTrue(total.partial());
		assertTrue(fleet.partial());
	}

	@Test
	void totalOfAFleetOfOneIsWhatItsContributorReported() {
		final AllMetricsSnapshot local = node(() -> StatsReportingDriverMock.stats(8, 3, 5));

		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 1, () -> List.of(local));
		final DriverStatsTotal total = totalOf(fleet);

		assertEquals(StatsReportingDriverMock.stats(8, 3, 5), total.sum());
		assertEquals(1, total.reporting());
		assertFalse(total.partial());
	}

	@Test
	void totalFollowsWhatTheContributorsReportAtEachRefresh() {
		final List<AllMetricsSnapshot> supplied = new ArrayList<>(List.of(
						node(() -> StatsReportingDriverMock.stats(1, 0, 1)), node(() -> StatsReportingDriverMock.stats(2, 0, 2))));
		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, 2, () -> supplied);
		assertEquals(StatsReportingDriverMock.stats(3, 0, 3), totalOf(fleet).sum());

		supplied.set(1, node(() -> StatsReportingDriverMock.stats(7, 4, 0)));

		assertEquals(StatsReportingDriverMock.stats(8, 4, 1), totalOf(fleet).sum());
	}

	@Test
	void rowPartialFlagOfADeleteStepDoesNotDependOnDriverStatistics() {
		final DriverStatsSnapshot stats = StatsReportingDriverMock.stats(8, 3, 5);
		final DeleteMetricsSnapshot details = DeleteMetricsSnapshot.builder(1).build();

		// Every contributor has DELETE details; one has no driver statistics.
		final DistributedMetricsContext<?> detailed = fleet(
						OpType.DELETE,
						2,
						() -> List.of(deleteSnapshot(details, stats), deleteSnapshot(details, null)));
		final DriverStatsTotal totalOfOne = totalOf(detailed);
		assertFalse(detailed.partial(), "the row is complete");
		assertTrue(totalOfOne.partial());
		assertEquals(1, totalOfOne.reporting());
		assertNotNull(detailed.lastSnapshot().deleteMetrics());

		// One contributor has no DELETE details; every contributor has driver statistics.
		final DistributedMetricsContext<?> undetailed = fleet(
						OpType.DELETE,
						2,
						() -> List.of(deleteSnapshot(details, stats), deleteSnapshot(null, stats)));
		final DriverStatsTotal totalOfBoth = totalOf(undetailed);
		assertTrue(undetailed.partial(), "the row lacks the DELETE details of a contributor");
		assertFalse(totalOfBoth.partial());
		assertEquals(2, totalOfBoth.reporting());
		assertEquals(StatsReportingDriverMock.stats(16, 6, 10), totalOfBoth.sum());
	}

	private static DriverStatsTotal totalOf(final DistributedMetricsContext<?> fleet) {
		fleet.refreshLastSnapshot(true);
		final DriverStatsTotal total = ((DistributedAllMetricsSnapshot) fleet.lastSnapshot()).driverStatsTotal();
		assertNotNull(total);
		return total;
	}

	/** The snapshot of a node on which one operation succeeded and whose driver reports the given. */
	private AllMetricsSnapshot node(final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<?> ctx = DriverStatsFleet.node(STEP_ID, stats);
		contexts.add(ctx);
		ctx.markSucc(DriverStatsFleet.ITEM_SIZE, 1_000, 100);
		return DriverStatsFleet.snapshotOf(ctx);
	}

	/** A node snapshot that also carries DELETE details, as a DELETE step's contributor reports. */
	private AllMetricsSnapshot deleteSnapshot(final DeleteMetricsSnapshot details, final DriverStatsSnapshot stats) {
		final AllMetricsSnapshot node = node(null);
		return new AllMetricsSnapshotImpl(
						node.durationSnapshot(),
						node.latencySnapshot(),
						node.ttfbSnapshot(),
						node.concurrencySnapshot(),
						node.failsSnapshot(),
						node.corruptSnapshot(),
						node.successSnapshot(),
						node.byteSnapshot(),
						node.elapsedTimeMillis(),
						details,
						stats);
	}

	/** The copy of a snapshot that the Entry Node receives from a remote Engine Participant. */
	private static AllMetricsSnapshot transferred(final AllMetricsSnapshot snapshot) throws Exception {
		final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
			out.writeObject(snapshot);
		}
		try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
			return (AllMetricsSnapshot) in.readObject();
		}
	}

	private DistributedMetricsContext<?> fleet(
					final OpType opType, final int expected, final Supplier<List<AllMetricsSnapshot>> contributors) {
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(STEP_ID, opType, expected, contributors);
		contexts.add(fleet);
		return fleet;
	}
}
