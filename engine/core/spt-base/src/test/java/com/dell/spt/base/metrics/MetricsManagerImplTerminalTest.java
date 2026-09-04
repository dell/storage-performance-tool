package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MetricsManagerImplTerminalTest {

	@Test
	void cachesDeleteOnlyUnresolvedProgressAtUnregister() {
		final var mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class, RETURNS_DEEP_STUBS);
		when(snapshot.successSnapshot().count()).thenReturn(0L);
		when(snapshot.failsSnapshot().count()).thenReturn(0L);
		when(snapshot.byteSnapshot().count()).thenReturn(0L);
		when(snapshot.deleteMetrics()).thenReturn(DeleteMetricsSnapshot.builder(100)
						.requests(1, 0, 0, 0, 1, 0)
						.objects(3, 3, 0, 0, 0, 3, 0)
						.build());

		final MetricsContext ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn("delete-unresolved-only");
		when(ctx.opType()).thenReturn(OpType.DELETE);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.metadata()).thenReturn(Map.of());
		when(ctx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(0));
		when(ctx.sumPersistEnabled()).thenReturn(false);
		when(ctx.timingPersistEnabled()).thenReturn(false);

		mgr.register(ctx);
		mgr.unregister(ctx);

		final var entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size());
		assertEquals(1, entries.get(0).deleteMetrics.requestUnresolved());
		assertEquals(3, entries.get(0).deleteMetrics.objectUnresolved());
	}

	@Test
	void cachesTerminalEntryOnUnregisterAndHonorsRetention() {
		MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(5_000);

		// Snapshot mocks
		DistributedAllMetricsSnapshot snap = mock(DistributedAllMetricsSnapshot.class);
		RateMetricSnapshot succ = mock(RateMetricSnapshot.class);
		RateMetricSnapshot fail = mock(RateMetricSnapshot.class);
		RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		TimingMetricSnapshot lat = mock(TimingMetricSnapshot.class);
		TimingMetricSnapshot dur = mock(TimingMetricSnapshot.class);
		ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);

		when(succ.count()).thenReturn(10L);
		when(fail.count()).thenReturn(1L);
		when(bytes.count()).thenReturn(1024L);
		when(lat.mean()).thenReturn(100.0);
		when(dur.mean()).thenReturn(200.0);
		when(conc.last()).thenReturn(0L);
		when(conc.mean()).thenReturn(0.0);

		when(snap.successSnapshot()).thenReturn(succ);
		when(snap.failsSnapshot()).thenReturn(fail);
		when(snap.byteSnapshot()).thenReturn(bytes);
		when(snap.latencySnapshot()).thenReturn(lat);
		when(snap.durationSnapshot()).thenReturn(dur);
		when(snap.concurrencySnapshot()).thenReturn(conc);
		when(snap.elapsedTimeMillis()).thenReturn(5000L);

		// Context mock
		DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn("step-t-1");
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.lastSnapshot()).thenReturn(snap);
		when(ctx.quantileValues()).thenReturn(List.of());
		when(ctx.nodeCount()).thenReturn(1);
		when(ctx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 100L));
		// Provide non-null values required by Prometheus exporter registration in MetricsManagerImpl.register
		when(ctx.concurrencyLimit()).thenReturn(1);
		when(ctx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(0));
		when(ctx.startTimeStamp()).thenReturn(System.currentTimeMillis());
		when(ctx.nodeAddrs()).thenReturn(java.util.List.of("127.0.0.1:1099"));
		when(ctx.comment()).thenReturn("");
		when(ctx.runId()).thenReturn(1L);

		// Also satisfy MetricsContext interface used by MetricsManagerImpl
		when(ctx.sumPersistEnabled()).thenReturn(false);
		when(ctx.timingPersistEnabled()).thenReturn(false);
		when(ctx.outputPeriodMillis()).thenReturn(0L);
		when(ctx.thresholdStateEntered()).thenReturn(false);
		when(ctx.thresholdStateExited()).thenReturn(false);

		// Register then unregister to trigger terminal caching
		mgr.register(ctx);
		mgr.unregister(ctx);

		List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size());
		TerminalStepEntry e = entries.get(0);
		assertEquals("step-t-1", e.stepId);
		assertEquals(10L, e.successCount);
		assertEquals(1024L, e.bytesTotal);
		assertEquals(100.0, e.latencyMeanUs, 0.0001);
		assertEquals(1L, e.runId);

		// Expire retention and verify eviction
		mgr.setTerminalRetentionMillis(0);
		try {
			Thread.sleep(2);
		} catch (final InterruptedException ie) {
			Thread.currentThread().interrupt();
			fail("Interrupted while waiting for terminal retention eviction", ie);
		}
		assertTrue(mgr.getTerminalSteps().isEmpty());
	}

	@Test
	void terminalFleetPreservesDistributedDetailPartialState() {
		final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);
		final DistributedAllMetricsSnapshot snapshot = mock(DistributedAllMetricsSnapshot.class);
		final RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		final TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot ttfb = mock(TimingMetricSnapshot.class);
		final ConcurrencyMetricSnapshot concurrency = mock(ConcurrencyMetricSnapshot.class);
		when(success.count()).thenReturn(1L);
		when(snapshot.successSnapshot()).thenReturn(success);
		when(snapshot.failsSnapshot()).thenReturn(fails);
		when(snapshot.byteSnapshot()).thenReturn(bytes);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.ttfbSnapshot()).thenReturn(ttfb);
		when(snapshot.concurrencySnapshot()).thenReturn(concurrency);
		when(snapshot.elapsedTimeMillis()).thenReturn(1_000L);

		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn("delete-detail-partial");
		when(ctx.opType()).thenReturn(OpType.DELETE);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.metadata()).thenReturn(Map.of());
		when(ctx.quantileValues()).thenReturn(List.of());
		when(ctx.nodeCount()).thenReturn(2);
		when(ctx.nodeAddrs()).thenReturn(List.of("node-a"));
		when(ctx.nodesPresent()).thenReturn(List.of("node-a"));
		when(ctx.contributorsPresent()).thenReturn(List.of("local", "node-a"));
		when(ctx.partial()).thenReturn(true);
		when(ctx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(0));
		when(ctx.concurrencyLimit()).thenReturn(1);
		when(ctx.comment()).thenReturn("");
		when(ctx.sumPersistEnabled()).thenReturn(false);
		when(ctx.timingPersistEnabled()).thenReturn(false);

		mgr.register(ctx);
		mgr.unregister(ctx);

		assertEquals(1, mgr.getTerminalSteps().size());
		final TerminalStepEntry terminal = mgr.getTerminalSteps().get(0);
		assertEquals(List.of("node-a"), terminal.nodesPresent);
		assertEquals(List.of("local", "node-a"), terminal.contributorsPresent);
		assertTrue(terminal.partial,
						"terminal fleet metadata must retain missing DELETE-detail completeness");
	}

	@Test
	void failedFinalFleetRefreshCannotRepublishEarlierAuthoritativeDeleteProgress()
					throws Exception {
		final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);
		final DistributedAllMetricsSnapshot snapshot = mock(DistributedAllMetricsSnapshot.class);
		final RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		final TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot ttfb = mock(TimingMetricSnapshot.class);
		final ConcurrencyMetricSnapshot concurrency = mock(ConcurrencyMetricSnapshot.class);
		when(success.count()).thenReturn(1L);
		when(snapshot.successSnapshot()).thenReturn(success);
		when(snapshot.failsSnapshot()).thenReturn(fails);
		when(snapshot.byteSnapshot()).thenReturn(bytes);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.ttfbSnapshot()).thenReturn(ttfb);
		when(snapshot.concurrencySnapshot()).thenReturn(concurrency);
		when(snapshot.elapsedTimeMillis()).thenReturn(1_000L);
		when(snapshot.deleteMetrics()).thenReturn(DeleteMetricsSnapshot.builder(1)
						.requests(1, 1, 0, 0, 0, 1)
						.objects(1, 1, 1, 0, 0, 0, 0)
						.build());

		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn("delete-final-refresh-failed");
		when(ctx.opType()).thenReturn(OpType.DELETE);
		when(ctx.lastSnapshot()).thenReturn(null);
		when(ctx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_DELETE_METRICS, true));
		when(ctx.quantileValues()).thenReturn(List.of());
		when(ctx.nodeCount()).thenReturn(2);
		when(ctx.nodeAddrs()).thenReturn(List.of("node-a"));
		when(ctx.nodesPresent()).thenReturn(List.of("node-a"));
		when(ctx.contributorsPresent()).thenReturn(List.of());
		when(ctx.partial()).thenReturn(false, true);
		when(ctx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(0));
		when(ctx.concurrencyLimit()).thenReturn(1);
		when(ctx.comment()).thenReturn("");
		when(ctx.sumPersistEnabled()).thenReturn(false);
		when(ctx.timingPersistEnabled()).thenReturn(false);

		mgr.register(ctx);
		final var recordProgress = MetricsManagerImpl.class.getDeclaredMethod(
						"recordProgressSnapshot", MetricsContext.class, AllMetricsSnapshot.class);
		recordProgress.setAccessible(true);
		recordProgress.invoke(mgr, ctx, snapshot);
		mgr.unregister(ctx);

		assertTrue(mgr.getTerminalSteps().isEmpty(),
						"a failed final fleet refresh must not republish stale authoritative DELETE detail");
	}

	@Test
	void retainsLastNonZeroSnapshotWhenRefreshClearsCounts() {
		MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);

		AllMetricsSnapshot nonZero = mock(AllMetricsSnapshot.class, RETURNS_DEEP_STUBS);
		when(nonZero.successSnapshot().count()).thenReturn(6_000L);
		when(nonZero.failsSnapshot().count()).thenReturn(0L);
		when(nonZero.byteSnapshot().count()).thenReturn(1_024L);
		when(nonZero.latencySnapshot().mean()).thenReturn(150.0);
		when(nonZero.durationSnapshot().mean()).thenReturn(200.0);
		when(nonZero.concurrencySnapshot().last()).thenReturn(0L);
		when(nonZero.concurrencySnapshot().mean()).thenReturn(0.0);
		when(nonZero.elapsedTimeMillis()).thenReturn(12_000L);

		AllMetricsSnapshot zero = mock(AllMetricsSnapshot.class, RETURNS_DEEP_STUBS);
		when(zero.successSnapshot().count()).thenReturn(0L);
		when(zero.failsSnapshot().count()).thenReturn(0L);
		when(zero.byteSnapshot().count()).thenReturn(0L);
		when(zero.latencySnapshot().mean()).thenReturn(0.0);
		when(zero.durationSnapshot().mean()).thenReturn(0.0);
		when(zero.concurrencySnapshot().last()).thenReturn(0L);
		when(zero.concurrencySnapshot().mean()).thenReturn(0.0);
		when(zero.elapsedTimeMillis()).thenReturn(12_100L);

		MetricsContext ctx = mock(MetricsContext.class);
		when(ctx.loadStepId()).thenReturn("step-retain");
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.runId()).thenReturn(42L);
		when(ctx.concurrencyLimit()).thenReturn(8);
		when(ctx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(1_024));
		when(ctx.startTimeStamp()).thenReturn(System.currentTimeMillis() - 15_000L);
		when(ctx.metadata()).thenReturn(new java.util.HashMap<>(
						Map.of(
										MetricsConstants.METADATA_LIMIT_OP_COUNT, 6_000L,
										MetricsConstants.METADATA_LIMIT_TIME_SEC, 0L)));
		when(ctx.sumPersistEnabled()).thenReturn(false);
		when(ctx.avgPersistEnabled()).thenReturn(false);
		when(ctx.timingPersistEnabled()).thenReturn(false);
		when(ctx.thresholdStateEntered()).thenReturn(false);
		when(ctx.thresholdStateExited()).thenReturn(false);
		when(ctx.lastSnapshot()).thenReturn(nonZero, nonZero, zero, zero);

		mgr.register(ctx);
		mgr.unregister(ctx);

		List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size());
		TerminalStepEntry e = entries.get(0);
		assertEquals("step-retain", e.stepId);
		assertEquals(6_000L, e.successCount);
		assertEquals(1_024L, e.bytesTotal);
		assertEquals(150.0, e.latencyMeanUs, 0.0001);
		assertEquals(42L, e.runId, "runId=" + e.runId);
	}

	@Test
	void cachesTerminalEntriesSeparatelyForNodeAndAggregate() {
		MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000);

		AllMetricsSnapshot localSnapshot = mock(AllMetricsSnapshot.class, RETURNS_DEEP_STUBS);
		when(localSnapshot.successSnapshot().count()).thenReturn(1_200L);
		when(localSnapshot.failsSnapshot().count()).thenReturn(0L);
		when(localSnapshot.byteSnapshot().count()).thenReturn(12_288_000L);
		when(localSnapshot.latencySnapshot().mean()).thenReturn(75_000.0);
		when(localSnapshot.durationSnapshot().mean()).thenReturn(76_000.0);
		when(localSnapshot.concurrencySnapshot().last()).thenReturn(0L);
		when(localSnapshot.concurrencySnapshot().mean()).thenReturn(7.5);
		when(localSnapshot.elapsedTimeMillis()).thenReturn(12_000L);

		MetricsContext localCtx = mock(MetricsContext.class);
		when(localCtx.loadStepId()).thenReturn("step-shared");
		when(localCtx.opType()).thenReturn(OpType.CREATE);
		when(localCtx.lastSnapshot()).thenReturn(localSnapshot);
		when(localCtx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 1_200L));
		when(localCtx.concurrencyLimit()).thenReturn(8);
		when(localCtx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(10_240));
		when(localCtx.startTimeStamp()).thenReturn(System.currentTimeMillis());
		when(localCtx.comment()).thenReturn("local");
		when(localCtx.runId()).thenReturn(111L);
		when(localCtx.sumPersistEnabled()).thenReturn(false);
		when(localCtx.timingPersistEnabled()).thenReturn(false);
		when(localCtx.outputPeriodMillis()).thenReturn(0L);
		when(localCtx.thresholdStateEntered()).thenReturn(false);
		when(localCtx.thresholdStateExited()).thenReturn(false);
		when(localCtx.compareTo(any())).thenAnswer(invocation -> {
			MetricsContext other = invocation.getArgument(0);
			return other == localCtx ? 0 : -1;
		});

		DistributedAllMetricsSnapshot distSnapshot = mock(DistributedAllMetricsSnapshot.class);
		RateMetricSnapshot distSucc = mock(RateMetricSnapshot.class);
		RateMetricSnapshot distFail = mock(RateMetricSnapshot.class);
		RateMetricSnapshot distBytes = mock(RateMetricSnapshot.class);
		TimingMetricSnapshot distLat = mock(TimingMetricSnapshot.class);
		TimingMetricSnapshot distDur = mock(TimingMetricSnapshot.class);
		ConcurrencyMetricSnapshot distConc = mock(ConcurrencyMetricSnapshot.class);
		when(distSucc.count()).thenReturn(6_000L);
		when(distFail.count()).thenReturn(0L);
		when(distBytes.count()).thenReturn(61_440_000L);
		when(distLat.mean()).thenReturn(80_000.0);
		when(distDur.mean()).thenReturn(81_000.0);
		when(distConc.last()).thenReturn(0L);
		when(distConc.mean()).thenReturn(36.0);
		when(distSnapshot.successSnapshot()).thenReturn(distSucc);
		when(distSnapshot.failsSnapshot()).thenReturn(distFail);
		when(distSnapshot.byteSnapshot()).thenReturn(distBytes);
		when(distSnapshot.latencySnapshot()).thenReturn(distLat);
		when(distSnapshot.durationSnapshot()).thenReturn(distDur);
		when(distSnapshot.concurrencySnapshot()).thenReturn(distConc);
		when(distSnapshot.elapsedTimeMillis()).thenReturn(12_000L);

		DistributedMetricsContext distCtx = mock(DistributedMetricsContext.class);
		when(distCtx.loadStepId()).thenReturn("step-shared");
		when(distCtx.opType()).thenReturn(OpType.CREATE);
		when(distCtx.lastSnapshot()).thenReturn(distSnapshot);
		when(distCtx.quantileValues()).thenReturn(List.of());
		when(distCtx.nodeCount()).thenReturn(5);
		when(distCtx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 6_000L));
		when(distCtx.concurrencyLimit()).thenReturn(40);
		when(distCtx.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(10_240));
		when(distCtx.startTimeStamp()).thenReturn(System.currentTimeMillis());
		when(distCtx.nodeAddrs()).thenReturn(List.of("node-1", "node-2"));
		when(distCtx.comment()).thenReturn("aggregate");
		when(distCtx.runId()).thenReturn(222L);
		when(distCtx.sumPersistEnabled()).thenReturn(false);
		when(distCtx.timingPersistEnabled()).thenReturn(false);
		when(distCtx.outputPeriodMillis()).thenReturn(0L);
		when(distCtx.thresholdStateEntered()).thenReturn(false);
		when(distCtx.thresholdStateExited()).thenReturn(false);
		when(distCtx.compareTo(any())).thenAnswer(invocation -> {
			MetricsContext other = invocation.getArgument(0);
			return other == distCtx ? 0 : 1;
		});

		mgr.register(localCtx);
		mgr.register(distCtx);

		mgr.unregister(localCtx);
		List<TerminalStepEntry> afterLocal = mgr.getTerminalSteps();
		assertEquals(1, afterLocal.size(), "afterLocal=" + afterLocal);
		assertFalse(afterLocal.get(0).distributed);
		Map<?, ?> rawAfterLocal;
		TerminalStepEntry rawLocalEntry;
		try {
			java.lang.reflect.Field field = MetricsManagerImpl.class.getDeclaredField("terminalByStepId");
			field.setAccessible(true);
			rawAfterLocal = (Map<?, ?>) field.get(mgr);
			assertEquals(1, rawAfterLocal.size(), "rawAfterLocal=" + rawAfterLocal);
			rawLocalEntry = (TerminalStepEntry) rawAfterLocal.values().iterator().next();
		} catch (ReflectiveOperationException reflectionFailure) {
			fail(reflectionFailure);
			return;
		}
		assertFalse(rawLocalEntry.distributed);
		mgr.unregister(distCtx);
		Map<?, ?> raw;
		TerminalStepEntry rawAggregateEntry;
		try {
			java.lang.reflect.Field field = MetricsManagerImpl.class.getDeclaredField("terminalByStepId");
			field.setAccessible(true);
			raw = (Map<?, ?>) field.get(mgr);
			assertEquals(2, raw.size(), "terminalMap=" + raw);
			rawAggregateEntry = (TerminalStepEntry) raw.entrySet().stream()
							.map(e -> (TerminalStepEntry) e.getValue())
							.filter(e -> e.distributed)
							.findFirst()
							.orElse(null);
		} catch (ReflectiveOperationException reflectionFailure) {
			fail(reflectionFailure);
			return;
		}
		assertNotNull(rawAggregateEntry, "rawAggregateEntry missing: " + raw);
		assertTrue(rawAggregateEntry.distributed);

		List<TerminalStepEntry> terminals = mgr.getTerminalSteps();
		assertEquals(2, terminals.size(), "terminals=" + terminals);
		TerminalStepEntry nodeEntry = terminals.stream().filter(e -> !e.distributed).findFirst().orElseThrow();
		TerminalStepEntry aggregateEntry = terminals.stream().filter(e -> e.distributed).findFirst().orElseThrow();

		assertEquals("step-shared", nodeEntry.stepId);
		assertEquals(111L, nodeEntry.runId);
		assertFalse(nodeEntry.distributed);
		assertEquals(1_200L, nodeEntry.successCount);

		assertEquals("step-shared", aggregateEntry.stepId);
		assertEquals(222L, aggregateEntry.runId);
		assertTrue(aggregateEntry.distributed);
		assertEquals(6_000L, aggregateEntry.successCount, "aggregateSuccess=" + aggregateEntry.successCount);
	}
}
