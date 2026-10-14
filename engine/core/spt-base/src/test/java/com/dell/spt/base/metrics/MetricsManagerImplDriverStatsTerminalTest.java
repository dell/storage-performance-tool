package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DriverStatsFleet;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsTotal;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Which storage driver statistics a finished step keeps, and when it keeps an entry at all.
 *
 * <p>The value kept is the one of the forced snapshot taken when the metrics context is
 * unregistered. A driver that counted something keeps an entry even when no operation completed;
 * in every other case the choice of entry is what it was before drivers reported statistics.
 */
class MetricsManagerImplDriverStatsTerminalTest {

	private static final String STEP_ID = "driver-stats-terminal";

	private final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final List<MetricsContext<?>> contexts = new ArrayList<>();

	@BeforeEach
	void retainTerminalEntries() {
		mgr.setTerminalRetentionMillis(30_000);
	}

	@AfterEach
	void closeContexts() {
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void entryKeepsTheValuesTheDriverReportedWhenTheContextWasUnregistered() {
		final AtomicLong widgets = new AtomicLong(5);
		final MetricsContext<?> ctx = context(() -> StatsReportingDriverMock.stats(widgets.get(), 1, 3));
		ctx.markSucc(1024, 1_000, 100);
		mgr.register(ctx);
		// A periodic refresh has just seen 5, so the 100 ms guard is closed.
		ctx.refreshLastSnapshot(true);
		// The last request ends before the step stops.
		widgets.set(7);

		mgr.unregister(ctx);
		// What the driver changes afterwards, when it closes.
		widgets.set(9);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(StatsReportingDriverMock.stats(7, 1, 3), entry.driverStats);
		assertEquals(1, entry.successCount);
	}

	@Test
	void stepInWhichNothingCompletedKeepsWhatItsDriverCounted() {
		final MetricsContext<?> ctx = context(() -> StatsReportingDriverMock.stats(2, 0, 4));
		mgr.register(ctx);

		mgr.unregister(ctx);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(StatsReportingDriverMock.stats(2, 0, 4), entry.driverStats);
		assertEquals(0, entry.successCount);
		assertEquals(0, entry.failedCount);
		assertEquals(0, entry.bytesTotal);
		assertEquals(ctx.loadStepId(), entry.stepId);
		assertEquals(OpType.CREATE, entry.opType);
	}

	@Test
	void stepWhoseDriverCountedNothingKeepsNoEntryWithoutProgress() {
		// A gauge is a level, not work: on its own it keeps no entry.
		final MetricsContext<?> ctx = context(() -> StatsReportingDriverMock.stats(0, 0, 4));
		mgr.register(ctx);

		mgr.unregister(ctx);

		assertTrue(mgr.getTerminalSteps().isEmpty());
	}

	@Test
	void driverWithoutStatisticsKeepsNoEntryWithoutProgress() {
		final MetricsContext<?> withoutSupplier = context(null);
		final MetricsContext<?> reportingNothing = context(() -> null);
		mgr.register(withoutSupplier);
		mgr.register(reportingNothing);

		mgr.unregister(withoutSupplier);
		mgr.unregister(reportingNothing);

		assertTrue(mgr.getTerminalSteps().isEmpty());
	}

	@Test
	void driverWithoutStatisticsKeepsItsEntryWithoutStatistics() {
		final MetricsContext<?> ctx = context(null);
		ctx.markSucc(1024, 1_000, 100);
		mgr.register(ctx);

		mgr.unregister(ctx);

		final TerminalStepEntry entry = onlyEntry();
		assertNull(entry.driverStats);
		assertEquals(1, entry.successCount);
		assertEquals(1024, entry.bytesTotal);
	}

	@Test
	void cachedProgressIsStillPublishedWhenTheFinalSnapshotShowsNone() throws Exception {
		// The fallback for a driver without statistics: unchanged.
		final AllMetricsSnapshot progress = snapshot(6_000, null);
		final MetricsContext<?> ctx = mockContext(snapshot(0, null));
		mgr.register(ctx);
		recordProgress(ctx, progress);

		mgr.unregister(ctx);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(6_000, entry.successCount);
		assertNull(entry.driverStats);
	}

	@Test
	void cachedProgressWinsOverAFinalSnapshotThatOnlyShowsDriverActivity() throws Exception {
		// Driver activity adds an entry where there would be none; it never replaces one.
		final DriverStatsSnapshot earlier = StatsReportingDriverMock.stats(3, 0, 1);
		final MetricsContext<?> ctx = mockContext(snapshot(0, StatsReportingDriverMock.stats(4, 0, 1)));
		mgr.register(ctx);
		recordProgress(ctx, snapshot(6_000, earlier));

		mgr.unregister(ctx);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(6_000, entry.successCount);
		assertSame(earlier, entry.driverStats, "the statistics of the snapshot the row was built from");
	}

	@Test
	void entryKeepsItsStatisticsWhenTheDeleteOutcomeIsUpdated() {
		final DriverStatsSnapshot stats = StatsReportingDriverMock.stats(8, 0, 2);
		final DriverStatsTotal total = DriverStatsTotal.of(List.of(stats, stats), true);
		final TerminalStepEntry entry = new TerminalStepEntry(
						STEP_ID, OpType.DELETE, 1, 0, 1, 0, 0, 0, 0, 0, null, null, null, 0, 0, 0, 0, 0, true, 1,
						List.of(), List.of(), false, DeleteMetricsSnapshot.builder(1).build(), true, "cluster", stats, total);

		final TerminalStepEntry updated = entry.withDeleteFailureOutcome("failed");

		assertSame(stats, updated.driverStats);
		assertSame(total, updated.driverStatsTotal);
	}

	@Test
	void entryBuiltWithoutStatisticsHasNone() {
		final TerminalStepEntry entry = new TerminalStepEntry(
						STEP_ID, OpType.DELETE, 1, 0, 1, 0, 0, 0, 0, 0, null, null, null, 0, 0, 0, 0, 0, true, 1,
						List.of(), List.of(), false, DeleteMetricsSnapshot.builder(1).build(), true, "cluster");

		assertNull(entry.driverStats);
		assertNull(entry.driverStatsTotal);
	}

	@Test
	void fleetEntryKeepsTheTotalOfWhatItsContributorsReportedWhenTheContextWasUnregistered() {
		final AtomicLong localWidgets = new AtomicLong(5);
		final AtomicLong remoteWidgets = new AtomicLong(2);
		final MetricsContext<?> local = context(() -> StatsReportingDriverMock.stats(localWidgets.get(), 1, 3));
		final MetricsContext<?> remote = context(() -> StatsReportingDriverMock.stats(remoteWidgets.get(), 0, 4));
		local.markSucc(1024, 1_000, 100);
		final DistributedMetricsContext<?> fleet = fleet(local, remote);
		mgr.register(fleet);
		// A periodic refresh has seen 5 and 2.
		fleet.refreshLastSnapshot();
		// The last requests end before the step stops.
		localWidgets.set(7);
		remoteWidgets.set(6);

		mgr.unregister(fleet);
		// What the drivers change afterwards, when they close.
		localWidgets.set(9);
		remoteWidgets.set(9);

		final TerminalStepEntry entry = onlyEntry();
		assertTrue(entry.distributed);
		assertEquals(StatsReportingDriverMock.stats(13, 1, 7), entry.driverStatsTotal.sum());
		assertEquals(2, entry.driverStatsTotal.reporting());
		assertFalse(entry.driverStatsTotal.partial());
		assertNull(entry.driverStats, "a fleet entry has a total, not the statistics of one driver");
		assertEquals(1, entry.successCount);
	}

	@Test
	void nodeEntryHasNoTotal() {
		final MetricsContext<?> ctx = context(() -> StatsReportingDriverMock.stats(2, 0, 4));
		mgr.register(ctx);

		mgr.unregister(ctx);

		assertNull(onlyEntry().driverStatsTotal);
	}

	@Test
	void fleetStepInWhichNothingCompletedKeepsWhatItsDriversCounted() {
		final MetricsContext<?> local = context(() -> StatsReportingDriverMock.stats(2, 0, 4));
		final MetricsContext<?> remote = context(() -> StatsReportingDriverMock.stats(0, 0, 1));
		final DistributedMetricsContext<?> fleet = fleet(local, remote);
		mgr.register(fleet);

		mgr.unregister(fleet);

		final TerminalStepEntry entry = onlyEntry();
		assertTrue(entry.distributed);
		assertEquals(StatsReportingDriverMock.stats(2, 0, 5), entry.driverStatsTotal.sum());
		assertEquals(2, entry.driverStatsTotal.reporting());
		assertEquals(0, entry.successCount);
		assertEquals(0, entry.failedCount);
		assertEquals(2, entry.nodeCount);
		assertFalse(entry.partial);
	}

	@Test
	void fleetStepWhoseDriversCountedNothingKeepsNoEntryWithoutProgress() {
		final MetricsContext<?> local = context(() -> StatsReportingDriverMock.stats(0, 0, 4));
		final MetricsContext<?> remote = context(null);
		final DistributedMetricsContext<?> fleet = fleet(local, remote);
		mgr.register(fleet);

		mgr.unregister(fleet);

		assertTrue(mgr.getTerminalSteps().isEmpty());
	}

	@Test
	void fleetStepWhoseDriversDifferKeepsNoEntryWithoutProgress() {
		// Without a sum there is no count that shows work.
		final MetricsContext<?> local = context(() -> StatsReportingDriverMock.stats(2, 0, 4));
		final MetricsContext<?> remote = context(
						() -> DriverStatsSnapshot.builder("other-mock").counter("gears_cut", 6).build());
		final DistributedMetricsContext<?> fleet = fleet(local, remote);
		mgr.register(fleet);

		mgr.unregister(fleet);

		assertTrue(mgr.getTerminalSteps().isEmpty());
	}

	@Test
	void fleetStepWithoutAllItsContributorsKeepsNoEntryWithoutProgress() {
		// The rule for a fleet that could not be read at its end: unchanged by driver activity.
		final MetricsContext<?> local = context(() -> StatsReportingDriverMock.stats(2, 0, 4));
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(
						STEP_ID, OpType.CREATE, 2, () -> Arrays.asList(DriverStatsFleet.snapshotOf(local), null));
		contexts.add(fleet);
		mgr.register(fleet);

		mgr.unregister(fleet);

		assertTrue(fleet.partial());
		assertTrue(mgr.getTerminalSteps().isEmpty());
	}

	@Test
	void stepWithSeveralOperationTypesKeepsTheStatisticsOfItsDriverWhicheverContextEndsLast() {
		// A mixed step has a metrics context per operation type and one driver. The first context
		// carries the driver's statistics; the step ends its contexts in order, the carrier first.
		final AtomicLong widgets = new AtomicLong(5);
		final MetricsContext<?> read = context(
						STEP_ID, OpType.READ, 7, () -> StatsReportingDriverMock.stats(widgets.get(), 1, 3));
		final MetricsContext<?> create = context(STEP_ID, OpType.CREATE, 7, null);
		final MetricsContext<?> stat = context(STEP_ID, OpType.STAT, 7, null);
		read.markSucc(1024, 1_000, 100);
		create.markSucc(2048, 1_000, 100);
		create.markSucc(2048, 1_000, 100);
		stat.markSucc(0, 1_000, 100);
		mgr.register(read);
		mgr.register(create);
		mgr.register(stat);
		widgets.set(7);

		mgr.unregister(read);
		// What the driver changes afterwards, when it closes.
		widgets.set(9);
		mgr.unregister(create);
		mgr.unregister(stat);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(OpType.STAT, entry.opType, "the entry of the context that ended last");
		assertEquals(1, entry.successCount);
		assertEquals(StatsReportingDriverMock.stats(7, 1, 3), entry.driverStats);
	}

	@Test
	void stepWithSeveralOperationTypesKeepsTheStatisticsOfACarrierThatEndsLast() {
		final MetricsContext<?> create = context(STEP_ID, OpType.CREATE, 7, null);
		final MetricsContext<?> read = context(STEP_ID, OpType.READ, 7, () -> StatsReportingDriverMock.stats(7, 1, 3));
		create.markSucc(2048, 1_000, 100);
		read.markSucc(1024, 1_000, 100);
		mgr.register(create);
		mgr.register(read);

		mgr.unregister(create);
		mgr.unregister(read);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(OpType.READ, entry.opType);
		assertEquals(StatsReportingDriverMock.stats(7, 1, 3), entry.driverStats);
	}

	@Test
	void contextInWhichNothingCompletedLeavesTheEntryOfItsStepAsItWas() {
		final MetricsContext<?> read = context(STEP_ID, OpType.READ, 7, () -> StatsReportingDriverMock.stats(7, 1, 3));
		final MetricsContext<?> idle = context(STEP_ID, OpType.DELETE, 7, null);
		read.markSucc(1024, 1_000, 100);
		mgr.register(read);
		mgr.register(idle);

		mgr.unregister(read);
		mgr.unregister(idle);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(OpType.READ, entry.opType);
		assertEquals(StatsReportingDriverMock.stats(7, 1, 3), entry.driverStats);
	}

	@Test
	void entryWithStatisticsOfItsOwnKeepsThem() {
		// A weighted or pipeline step has a driver per operation and so a carrier per context.
		// The step keeps one entry: that of the context that ended last, with its own statistics.
		final MetricsContext<?> first = context(STEP_ID, OpType.CREATE, 7, () -> StatsReportingDriverMock.stats(7, 1, 3));
		final MetricsContext<?> second = context(STEP_ID, OpType.READ, 7, () -> StatsReportingDriverMock.stats(2, 0, 4));
		first.markSucc(1024, 1_000, 100);
		second.markSucc(1024, 1_000, 100);
		mgr.register(first);
		mgr.register(second);

		mgr.unregister(first);
		mgr.unregister(second);

		assertEquals(StatsReportingDriverMock.stats(2, 0, 4), onlyEntry().driverStats);
	}

	@Test
	void statisticsOfAnEarlierRunDoNotReachAStepOfTheSameIdentifier() {
		// A custom scenario can name its step, and an engine can run it again.
		final MetricsContext<?> earlierRun = context(
						STEP_ID, OpType.CREATE, 7, () -> StatsReportingDriverMock.stats(7, 1, 3));
		earlierRun.markSucc(1024, 1_000, 100);
		mgr.register(earlierRun);
		mgr.unregister(earlierRun);
		final MetricsContext<?> laterRun = context(STEP_ID, OpType.CREATE, 8, null);
		laterRun.markSucc(1024, 1_000, 100);
		mgr.register(laterRun);

		mgr.unregister(laterRun);

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(8, entry.runId);
		assertNull(entry.driverStats);
	}

	@Test
	void fleetStepWithSeveralOperationTypesKeepsTheTotalOfItsDriversWhicheverContextEndsLast() {
		// On the nodes only the contexts of the first operation type carry statistics, so only
		// the fleet context of that type has a total.
		final MetricsContext<?> localRead = context(() -> StatsReportingDriverMock.stats(7, 1, 3));
		final MetricsContext<?> remoteRead = context(() -> StatsReportingDriverMock.stats(6, 0, 4));
		final MetricsContext<?> localCreate = context(null);
		final MetricsContext<?> remoteCreate = context(null);
		localRead.markSucc(1024, 1_000, 100);
		localCreate.markSucc(2048, 1_000, 100);
		remoteCreate.markSucc(2048, 1_000, 100);
		final DistributedMetricsContext<?> read = fleet(OpType.READ, localRead, remoteRead);
		final DistributedMetricsContext<?> create = fleet(OpType.CREATE, localCreate, remoteCreate);
		mgr.register(read);
		mgr.register(create);

		mgr.unregister(read);
		mgr.unregister(create);

		final TerminalStepEntry entry = onlyEntry();
		assertTrue(entry.distributed);
		assertEquals(OpType.CREATE, entry.opType, "the entry of the context that ended last");
		assertEquals(2, entry.successCount);
		assertEquals(StatsReportingDriverMock.stats(13, 1, 7), entry.driverStatsTotal.sum());
		assertEquals(2, entry.driverStatsTotal.reporting());
		assertFalse(entry.driverStatsTotal.partial());
		assertNull(entry.driverStats);
	}

	@Test
	void nodeEntryAndFleetEntryOfAStepDoNotShareStatistics() {
		// On the Entry Node a step has node contexts and fleet contexts under one identifier.
		final MetricsContext<?> node = context(STEP_ID, OpType.READ, 7, () -> StatsReportingDriverMock.stats(7, 1, 3));
		node.markSucc(1024, 1_000, 100);
		final MetricsContext<?> localCreate = context(null);
		final MetricsContext<?> remoteCreate = context(null);
		localCreate.markSucc(2048, 1_000, 100);
		final DistributedMetricsContext<?> fleet = fleet(OpType.CREATE, localCreate, remoteCreate);
		mgr.register(node);
		mgr.register(fleet);

		mgr.unregister(node);
		mgr.unregister(fleet);

		final List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(2, entries.size(), entries.toString());
		for (final TerminalStepEntry entry : entries) {
			if (entry.distributed) {
				assertNull(entry.driverStats);
				assertNull(entry.driverStatsTotal);
			} else {
				assertEquals(StatsReportingDriverMock.stats(7, 1, 3), entry.driverStats);
			}
		}
	}

	private TerminalStepEntry onlyEntry() {
		final List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size(), entries.toString());
		return entries.get(0);
	}

	/** A fleet of the given contributors, each of which answers with its current values. */
	private DistributedMetricsContext<?> fleet(final MetricsContext<?> local, final MetricsContext<?> remote) {
		return fleet(OpType.CREATE, local, remote);
	}

	private DistributedMetricsContext<?> fleet(
					final OpType opType, final MetricsContext<?> local, final MetricsContext<?> remote) {
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(
						STEP_ID,
						opType,
						2,
						() -> List.of(DriverStatsFleet.snapshotOf(local), DriverStatsFleet.snapshotOf(remote)));
		contexts.add(fleet);
		return fleet;
	}

	/** A context of a step of its own. */
	private MetricsContext<?> context(final Supplier<DriverStatsSnapshot> stats) {
		return context(STEP_ID + "-" + contexts.size(), OpType.CREATE, 7, stats);
	}

	private MetricsContext<?> context(
					final String stepId, final OpType opType, final long runId, final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<?> ctx = MetricsContextImpl.builder()
						.loadStepId(stepId)
						.runId(runId)
						.opType(opType)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1024))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
		if (stats != null) {
			ctx.metadata().put(MetricsConstants.METADATA_DRIVER_STATS, stats);
		}
		ctx.start();
		contexts.add(ctx);
		return ctx;
	}

	/** A snapshot with the given count of successes and nothing else. */
	private static AllMetricsSnapshot snapshot(final long successCount, final DriverStatsSnapshot stats) {
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class, RETURNS_DEEP_STUBS);
		when(snapshot.successSnapshot().count()).thenReturn(successCount);
		when(snapshot.failsSnapshot().count()).thenReturn(0L);
		when(snapshot.byteSnapshot().count()).thenReturn(0L);
		when(snapshot.deleteMetrics()).thenReturn(null);
		when(snapshot.driverStats()).thenReturn(stats);
		return snapshot;
	}

	/** A context whose snapshot stays the given one, before and after the final refresh. */
	private static MetricsContext<?> mockContext(final AllMetricsSnapshot lastSnapshot) {
		final MetricsContext<?> ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn(STEP_ID);
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.runId()).thenReturn(7L);
		when(ctx.metadata()).thenReturn(Map.of());
		when(ctx.itemDataSize()).thenReturn(new SizeInBytes(1024));
		when(ctx.lastSnapshot()).thenAnswer(invocation -> lastSnapshot);
		return ctx;
	}

	private void recordProgress(final MetricsContext<?> ctx, final AllMetricsSnapshot snapshot) throws Exception {
		final var recordProgress = MetricsManagerImpl.class.getDeclaredMethod(
						"recordProgressSnapshot", MetricsContext.class, AllMetricsSnapshot.class);
		recordProgress.setAccessible(true);
		recordProgress.invoke(mgr, ctx, snapshot);
	}
}
