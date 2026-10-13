package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
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
		final TerminalStepEntry entry = new TerminalStepEntry(
						STEP_ID, OpType.DELETE, 1, 0, 1, 0, 0, 0, 0, 0, null, null, null, 0, 0, 0, 0, 0, true, 1,
						List.of(), List.of(), false,
						com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot.builder(1).build(), true, "cluster", stats);

		assertSame(stats, entry.withDeleteFailureOutcome("failed").driverStats);
	}

	private TerminalStepEntry onlyEntry() {
		final List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size(), entries.toString());
		return entries.get(0);
	}

	private MetricsContext<?> context(final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<?> ctx = MetricsContextImpl.builder()
						.loadStepId(STEP_ID + "-" + contexts.size())
						.runId(7)
						.opType(OpType.CREATE)
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
