package com.dell.spt.base.metrics.util;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.*;
import io.prometheus.client.Collector.MetricFamilySamples;
import static com.dell.spt.base.metrics.MetricsConstants.METRIC_NAME_STEP_COMPLETION;
import java.util.*;
import org.junit.jupiter.api.Test;

class CombinedCompletionCollectorTest {

	@Test
	void testAggregatedCountBasedPercent() {
		// Two contexts for the same step
		DistributedMetricsContext c1 = mockCtx("step-A", 111L, 100L, 40L, 10L, 0L, 10_000L);
		DistributedMetricsContext c2 = mockCtx("step-A", 111L, 200L, 60L, 20L, 0L, 12_000L);

		CombinedCompletionCollector collector = new CombinedCompletionCollector(() -> Set.of(c1, c2));
		List<MetricFamilySamples> out = collector.collect();

		MetricFamilySamples m = out.stream().filter(f -> f.name.contains(METRIC_NAME_STEP_COMPLETION)).findFirst().orElse(null);
		assertNotNull(m);
		assertEquals(1, m.samples.size());
		// (40+10+60+20) / (100+200) = 130/300 =~ 43%
		assertEquals(43.0, m.samples.get(0).value, 0.0001);
		assertEquals("step-A", m.samples.get(0).labelValues.get(0));
		assertEquals("111", m.samples.get(0).labelValues.get(1));
	}

	@Test
	void testAggregatedTimeBasedFallback() {
		// One context unlimited count, fallback to time
		DistributedMetricsContext c1 = mockCtx("step-B", 222L, 0L, 40L, 10L, 100L, 40_000L);
		DistributedMetricsContext c2 = mockCtx("step-B", 222L, 200L, 60L, 20L, 100L, 30_000L);

		CombinedCompletionCollector collector = new CombinedCompletionCollector(() -> Set.of(c1, c2));
		List<MetricFamilySamples> out = collector.collect();

		MetricFamilySamples m = out.stream().filter(f -> f.name.contains(METRIC_NAME_STEP_COMPLETION)).findFirst().orElse(null);
		assertNotNull(m);
		assertEquals(40.0, m.samples.get(0).value, 0.0001); // max elapsed 40s of 100s
	}

	private static DistributedMetricsContext mockCtx(
					String stepId, long runId, long countLimit, long succ, long fail, long timeLimitSec, long elapsedMillis) {
		DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.runId()).thenReturn(runId);
		Map<String, Object> md = new HashMap<>();
		md.put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_OP_COUNT, countLimit);
		md.put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_TIME_SEC, timeLimitSec);
		when(ctx.metadata()).thenReturn(md);

		RateMetricSnapshot succSnap = mock(RateMetricSnapshot.class);
		when(succSnap.count()).thenReturn(succ);
		RateMetricSnapshot failSnap = mock(RateMetricSnapshot.class);
		when(failSnap.count()).thenReturn(fail);

		ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);
		when(conc.last()).thenReturn(0L);
		TimingMetricSnapshot lat = mock(TimingMetricSnapshot.class);
		TimingMetricSnapshot dur = mock(TimingMetricSnapshot.class);

		DistributedAllMetricsSnapshot snap = mock(DistributedAllMetricsSnapshot.class);
		when(snap.successSnapshot()).thenReturn(succSnap);
		when(snap.failsSnapshot()).thenReturn(failSnap);
		when(snap.concurrencySnapshot()).thenReturn(conc);
		when(snap.latencySnapshot()).thenReturn(lat);
		when(snap.durationSnapshot()).thenReturn(dur);
		when(snap.elapsedTimeMillis()).thenReturn(elapsedMillis);

		when(ctx.lastSnapshot()).thenReturn(snap);
		return ctx;
	}
}
