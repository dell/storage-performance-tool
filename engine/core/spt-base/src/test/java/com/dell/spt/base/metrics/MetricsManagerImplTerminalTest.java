package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MetricsManagerImplTerminalTest {

	@Test
	void cachesTerminalEntryOnUnregisterAndHonorsRetention() {
		MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
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

		// Expire retention and verify eviction
		mgr.setTerminalRetentionMillis(0);
		try {
			Thread.sleep(2);
		} catch (InterruptedException ignore) {}
		assertTrue(mgr.getTerminalSteps().isEmpty());
	}
}
