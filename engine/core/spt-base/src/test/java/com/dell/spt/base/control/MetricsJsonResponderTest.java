package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

public class MetricsJsonResponderTest {

	@Test
	void clusterMetricsIncludesAggregatedFields() throws Exception {
		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot();
		final DistributedMetricsContext ctx = mockDistributedContext("step-agg", OpType.READ, snapshot,
						Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 200));

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctx));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final ArrayNode payload = responder.buildClusterMetrics(false);
		assertEquals(1, payload.size());
		final JsonNode obj = payload.get(0);
		assertEquals(3, obj.get("metrics_schema").asInt());
		assertEquals("fleet", obj.get("scope").asText());
		assertEquals("aggregate", obj.get("role").asText());
		assertEquals("step-agg", obj.get("step_id").asText());
		assertEquals("READ", obj.get("op_type").asText());
		assertEquals(200, obj.get("limit").get("op_count").asInt(0));
		assertTrue(obj.get("operations").get("success_count").asLong() > 0L);
		assertTrue(obj.get("bandwidth").get("bytes_total").asLong() > 0L);
		assertEquals(1, obj.get("nodes_count").asInt());
		assertEquals(1, obj.get("nodes_present").size());
		assertFalse(obj.get("partial").asBoolean());
		assertEquals("123", obj.get("run_id").asText());
	}

	@Test
	void timingBlockIncludesSchema3PercentilesAndNullableTtfb() {
		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot();
		when(snapshot.latencySnapshot()).thenReturn(TimingMetricSnapshotImpl.fromSamples("latency", List.of(10L, 20L, 30L)));
		when(snapshot.durationSnapshot()).thenReturn(TimingMetricSnapshotImpl.fromSamples("duration", List.of(100L, 200L, 300L)));
		when(snapshot.ttfbSnapshot()).thenReturn(TimingMetricSnapshotImpl.fromSamples("ttfb", List.of(15L, 25L)));
		final DistributedMetricsContext ctx = mockDistributedContext("step-timing", OpType.READ, snapshot, Map.of());

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctx));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode timing = responder.buildClusterMetrics(false).get(0).get("timing");

		assertEquals(3, timing.get("latency").get("count").asInt());
		assertTrue(timing.get("latency").get("p50_us").asLong() > 0);
		assertTrue(timing.get("duration").get("p99_us").asLong() > 0);
		assertEquals(2, timing.get("ttfb").get("count").asInt());
		assertTrue(timing.get("ttfb").get("p999_us").asLong() > 0);
	}

	@Test
	void clusterMetricsReflectOpCountLimit() throws Exception {
		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot();
		final DistributedMetricsContext ctx = mockDistributedContext("step-count", OpType.CREATE, snapshot,
						Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 1000L,
										MetricsConstants.METADATA_LIMIT_TIME_SEC, 0L));

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctx));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode obj = responder.buildClusterMetrics(false).get(0);
		assertEquals("op_count", obj.get("limit").get("type").asText());
		assertEquals(1000L, obj.get("limit").get("op_count").asLong());
		assertFalse(obj.get("unbounded").asBoolean());
	}

	@Test
	void clusterMetricsReflectTimeLimitAndUnbounded() throws Exception {
		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot();
		final DistributedMetricsContext ctxTime = mockDistributedContext("step-time", OpType.CREATE, snapshot,
						Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 0L,
										MetricsConstants.METADATA_LIMIT_TIME_SEC, 60L));
		final DistributedMetricsContext ctxUnbounded = mockDistributedContext("step-unbounded", OpType.CREATE, snapshot, Map.of());

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctxTime, ctxUnbounded));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final ArrayNode payload = responder.buildClusterMetrics(false);

		JsonNode objTime = null;
		JsonNode objUnbounded = null;
		for (JsonNode node : payload) {
			if ("step-time".equals(node.get("step_id").asText())) {
				objTime = node;
			} else if ("step-unbounded".equals(node.get("step_id").asText())) {
				objUnbounded = node;
			}
		}
		assertNotNull(objTime);
		assertNotNull(objUnbounded);
		assertEquals("time", objTime.get("limit").get("type").asText());
		assertEquals(60L, objTime.get("limit").get("time_sec").asLong());
		assertFalse(objTime.get("unbounded").asBoolean());
		assertEquals("none", objUnbounded.get("limit").get("type").asText());
		assertTrue(objUnbounded.get("unbounded").asBoolean());
	}

	@Test
	void nodeMetricsIncludeTerminalEntries() {
		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of());
		final TerminalStepEntry entry = new TerminalStepEntry(
						"step-terminal",
						OpType.UPDATE,
						777L,
						System.currentTimeMillis(),
						10L,
						2L,
						1024L,
						100.0,
						200.0,
						0L,
						0.0,
						0L,
						0L,
						5000L);
		when(mgr.getTerminalSteps()).thenReturn(List.of(entry));

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final ArrayNode nodePayload = responder.buildNodeMetrics(false);
		assertEquals(1, nodePayload.size());
		final JsonNode first = nodePayload.get(0);
		assertEquals("step-terminal", first.get("step_id").asText());
		assertEquals("responder-test-node", first.get("node_id").asText());
		assertEquals("777", first.get("run_id").asText());
		assertTrue(first.get("terminal").asBoolean());
		assertEquals("node", nodePayload.get(0).get("scope").asText());
		assertEquals(3, nodePayload.get(0).get("metrics_schema").asInt());
		assertEquals("entry", nodePayload.get(0).get("role").asText());
	}

	@Test
	void terminalNodeMetricsPreserveSchema3TimingDistributions() {
		final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(30_000L);
		final MetricsContext<?> ctx = MetricsContextImpl.builder()
						.loadStepId("terminal-timing")
						.runId(444L)
						.opType(OpType.READ)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1024))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
		ctx.start();
		ctx.markSucc(1024L, 1_000L, 100L, 250L);
		ctx.markSucc(1024L, 2_000L, 200L, 350L);
		ctx.refreshLastSnapshot();

		mgr.register(ctx);
		mgr.unregister(ctx);

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode timing = responder.buildNodeMetrics(false).get(0).get("timing");
		assertEquals(2, timing.get("latency").get("count").asInt());
		assertTrue(timing.get("latency").get("p50_us").asLong() > 0);
		assertEquals(2, timing.get("duration").get("count").asInt());
		assertTrue(timing.get("duration").get("p99_us").asLong() > 0);
		assertEquals(2, timing.get("ttfb").get("count").asInt());
		assertTrue(timing.get("ttfb").get("p90_us").asLong() > 0);
	}

	@Test
	void clusterMetricsIncludeDistributedTerminalEntries() {
		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of());
		final TerminalStepEntry terminal = new TerminalStepEntry(
						"step-dist",
						OpType.CREATE,
						321L,
						System.currentTimeMillis(),
						100L,
						10L,
						4096L,
						120.0,
						180.0,
						0L,
						0.0,
						1000L,
						0L,
						9_000L,
						true,
						3,
						List.of("node-a", "node-b"),
						true);
		when(mgr.getTerminalSteps()).thenReturn(List.of(terminal));

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final ArrayNode nodePayload = responder.buildNodeMetrics(false);
		assertEquals(1, nodePayload.size(), "Node metrics should not surface distributed terminal entries");
		final ArrayNode clusterPayload = responder.buildClusterMetrics(false);
		assertEquals(1, clusterPayload.size());
		final JsonNode agg = clusterPayload.get(0);
		assertEquals("aggregate", agg.get("role").asText());
		assertEquals(3, agg.get("nodes_count").asInt());
		assertEquals(2, agg.get("nodes_present").size());
		assertTrue(agg.get("partial").asBoolean());
		assertEquals("321", agg.get("run_id").asText());
		assertEquals(110L, agg.get("operations").get("success_count").asLong() + agg.get("operations").get("failed_count").asLong());
	}

	@Test
	void distributedTerminalMetricsPreserveSchema3TimingDistributions() {
		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of());
		final TerminalStepEntry terminal = new TerminalStepEntry(
						"step-dist-timing",
						OpType.READ,
						321L,
						System.currentTimeMillis(),
						100L,
						0L,
						4096L,
						120.0,
						180.0,
						TimingMetricSnapshotImpl.fromSamples("latency", List.of(100L, 120L, 140L)),
						TimingMetricSnapshotImpl.fromSamples("duration", List.of(150L, 180L, 210L)),
						TimingMetricSnapshotImpl.fromSamples("ttfb", List.of(80L, 90L, 100L)),
						0L,
						0.0,
						1000L,
						0L,
						9_000L,
						true,
						3,
						List.of("node-a", "node-b", "node-c"),
						false);
		when(mgr.getTerminalSteps()).thenReturn(List.of(terminal));

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode timing = responder.buildClusterMetrics(false).get(0).get("timing");
		assertEquals(3, timing.get("latency").get("count").asInt());
		assertTrue(timing.get("latency").get("p99_us").asLong() > 0);
		assertEquals(3, timing.get("duration").get("count").asInt());
		assertTrue(timing.get("duration").get("p999_us").asLong() > 0);
		assertEquals(3, timing.get("ttfb").get("count").asInt());
		assertTrue(timing.get("ttfb").get("p50_us").asLong() > 0);
	}

	@Test
	void clusterCompletionUsesDistributedSnapshots() {
		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot(80L, 20L, 4000L);
		final DistributedMetricsContext ctx = mockDistributedContext(
						"step-complete",
						OpType.CREATE,
						snapshot,
						Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 400L));

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctx));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode obj = responder.buildClusterMetrics(false).get(0);
		assertEquals(25, obj.get("overall_completion_percent").asInt());
		assertFalse(obj.get("overall_unbounded").asBoolean());
		assertEquals(400L, obj.get("limit").get("op_count").asLong());
	}

	@Test
	void clusterPartialFlagSetWhenNodesMissing() {
		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot(10L, 0L, 1000L);
		final DistributedMetricsContext ctx = mockDistributedContext(
						"step-partial",
						OpType.READ,
						snapshot,
						Map.of(MetricsConstants.METADATA_LIMIT_TIME_SEC, 30L),
						3,
						List.of("node-1", "node-2"));

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctx));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode obj = responder.buildClusterMetrics(false).get(0);
		assertEquals(3, obj.get("nodes_count").asInt());
		assertEquals(2, obj.get("nodes_present").size());
		assertTrue(obj.get("partial").asBoolean());
	}

	@Test
	void nodeMetricsExposeLimitAndCompletion() {
		final MetricsManager mgr = mock(MetricsManager.class);
		final MetricsContext ctx = mock(MetricsContext.class);
		final AllMetricsSnapshot snapshot = mockNodeSnapshot(70L, 0L, 5000L);
		final Map<String, Object> metadata = Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 100L);

		when(ctx.loadStepId()).thenReturn("node-step");
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.metadata()).thenReturn(metadata);
		when(ctx.runId()).thenReturn(1234L);

		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of(ctx));
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final Config workerConfig = defaultConfig();
		workerConfig.val("run-node", false);
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, workerConfig);
		final JsonNode obj = responder.buildNodeMetrics(false).get(0);
		assertEquals("node", obj.get("scope").asText());
		assertEquals("worker", obj.get("role").asText());
		assertEquals(70, obj.get(MetricsConstants.METRIC_NAME_COMPLETION).asInt());
		assertEquals(70, obj.get("overall_completion_percent").asInt());
		assertFalse(obj.get("unbounded").asBoolean());
		assertFalse(obj.get("overall_unbounded").asBoolean());
		assertEquals("op_count", obj.get("limit").get("type").asText());
		assertEquals(100L, obj.get("limit").get("op_count").asLong());
	}

	@Test
	void nodeMetricsIncludeShardInstrumentation() {
		final MetricsManager mgr = mock(MetricsManager.class);
		final MetricsContext ctx = mock(MetricsContext.class);
		final AllMetricsSnapshot snapshot = mockNodeSnapshot(10L, 0L, 1000L);
		final Map<String, Object> metadata = new HashMap<>();
		metadata.put(MetricsConstants.METADATA_LIST_SHARD_METRICS, testRecorder());

		when(ctx.loadStepId()).thenReturn("list-node");
		when(ctx.opType()).thenReturn(OpType.LIST);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.metadata()).thenReturn(metadata);
		when(ctx.runId()).thenReturn(777L);
		when(ctx.concurrencyLimit()).thenReturn(1);
		when(ctx.concurrencyThreshold()).thenReturn(Integer.MAX_VALUE);
		when(ctx.itemDataSize()).thenReturn(new SizeInBytes(1));
		when(ctx.comment()).thenReturn("");

		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of(ctx));
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode obj = responder.buildNodeMetrics(false).get(0);
		assertTrue(obj.has("list_shards"));
		final JsonNode listShards = obj.get("list_shards");
		assertEquals(1, listShards.get("shards").size());
		final JsonNode shard = listShards.get("shards").get(0);
		assertEquals("prefix-0", shard.get("prefix").asText());
		assertEquals(5, shard.get("objects").asInt());
		assertTrue(shard.get("active").asBoolean());
		assertEquals(12, shard.get("stall_warnings").asInt());
		assertEquals(2, shard.get("consecutive_full_pages").asInt());
		assertEquals(900, shard.get("last_page_objects").asInt());
		assertEquals(1_000, shard.get("last_page_max_keys").asInt());
		assertEquals(300L, shard.get("last_full_page_millis").asLong());
	}

	@Test
	void nodeMetricsReuseCachedProgressWhenSnapshotRegresses() {
		final MetricsManager mgr = mock(MetricsManager.class);
		final MetricsContext ctx = mock(MetricsContext.class);
		final AllMetricsSnapshot zeroSnapshot = mockNodeSnapshot(0L, 0L, 0L);
		final Map<String, Object> metadata = Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 1000L);

		when(ctx.loadStepId()).thenReturn("step-retain");
		when(ctx.opType()).thenReturn(OpType.CREATE);
		when(ctx.lastSnapshot()).thenReturn(zeroSnapshot);
		when(ctx.metadata()).thenReturn(metadata);
		when(ctx.runId()).thenReturn(42L);

		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of(ctx));
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final TerminalStepEntry progress = new TerminalStepEntry(
						"step-retain",
						OpType.CREATE,
						1760907495374L,
						System.currentTimeMillis() - 1_000L,
						1000L,
						0L,
						1_024L,
						150.0,
						200.0,
						0L,
						0.0,
						1000L,
						0L,
						12_000L);
		when(mgr.getLastProgressSnapshot("step-retain", false)).thenReturn(java.util.Optional.of(progress));
		when(mgr.getLastProgressSnapshot("step-retain")).thenReturn(java.util.Optional.of(progress));

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode nodeVerbose = responder.buildNodeMetrics(true).get(0);
		assertTrue(nodeVerbose.get("diag_cached_progress").asBoolean());

		final JsonNode node = responder.buildNodeMetrics(false).get(0);

		assertEquals(1000L, node.get("operations").get("success_count").asLong());
		assertEquals(1_024L, node.get("bandwidth").get("bytes_total").asLong());
		assertEquals(150.0, node.get("timing").get("latency_mean_us").asDouble(), 0.0001);
		assertEquals(200.0, node.get("timing").get("duration_mean_us").asDouble(), 0.0001);
		assertEquals(12.0, node.get("elapsed_time_seconds").asDouble(), 0.0001);
		assertEquals(100, node.get(MetricsConstants.METRIC_NAME_COMPLETION).asInt());
		assertEquals(2, node.get("test_state").asInt());
	}

	@Test
	void verboseNodeMetricsIncludeDiagnostics() {
		final MetricsManager mgr = mock(MetricsManager.class);
		final MetricsContext ctx = mock(MetricsContext.class);
		final AllMetricsSnapshot snapshot = mockNodeSnapshot(10L, 0L, 1000L);

		when(ctx.loadStepId()).thenReturn("node-verbose");
		when(ctx.opType()).thenReturn(OpType.UPDATE);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.metadata()).thenReturn(Map.of());
		when(ctx.runId()).thenReturn(55L);

		final DistributedMetricsContext distCtx = mock(DistributedMetricsContext.class);
		final DistributedAllMetricsSnapshot distSnapshot = mockDistributedSnapshot();
		when(distCtx.loadStepId()).thenReturn("other");
		when(distCtx.lastSnapshot()).thenReturn(distSnapshot);
		when(distCtx.metadata()).thenReturn(Map.of());
		when(distCtx.nodeCount()).thenReturn(1);
		when(distCtx.nodeAddrs()).thenReturn(List.of("node-1"));
		when(distCtx.runId()).thenReturn(999L);

		when(mgr.getDistributedContexts()).thenReturn(Set.of(distCtx));
		when(mgr.getAllContexts()).thenReturn(Set.of(ctx, distCtx));
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, defaultConfig());
		final JsonNode obj = responder.buildNodeMetrics(true).get(0);
		assertEquals("entry", obj.get("role").asText());
		assertTrue(obj.has("diag_distributed_contexts"));
		assertTrue(obj.has("diag_local_contexts"));
		assertEquals(1, obj.get("diag_distributed_contexts").asInt());
		assertEquals(1, obj.get("diag_local_contexts").asInt());
	}

	@Test
	void idleSampleEmittedWhenNoContextsOrTerminals() {
		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of());
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final Config config = TestConfigBuilder.config();
		config.val("run-comment", "idle-node");
		config.val("run-port", 19090);

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, config);
		final ArrayNode payload = responder.buildNodeMetrics(false);
		assertEquals(1, payload.size(), "Expected a single idle element");
		final JsonNode obj = payload.get(0);
		assertEquals(3, obj.get("metrics_schema").asInt());
		assertEquals("node", obj.get("scope").asText());
		assertEquals("worker", obj.get("role").asText());
		assertEquals("idle-node", obj.get("node_id").asText());
		assertEquals("", obj.get("run_id").asText());
		assertEquals("", obj.get("step_id").asText());
		assertEquals("none", obj.get("op_type").asText());
		assertEquals(0, obj.get("elapsed_time_seconds").asInt());
		assertEquals(0, obj.get("test_state").asInt());
		assertEquals("none", obj.get("limit").get("type").asText());
		assertEquals(0, obj.get("operations").get("success_count").asInt());
		assertEquals(0, obj.get("operations").get("failed_count").asInt());
		// parse sample_ts for RFC3339
		java.time.Instant.parse(obj.get("sample_ts").asText());
	}

	@Test
	void nodeIdDerivedFromRunComment() {
		final Config config = TestConfigBuilder.config();
		config.val("run-comment", "comment-node");

		final DistributedAllMetricsSnapshot snapshot = mockDistributedSnapshot(5L, 0L, 1000L);
		final DistributedMetricsContext ctx = mockDistributedContext(
						"step-id",
						OpType.CREATE,
						snapshot,
						Map.of());

		final MetricsManager mgr = mock(MetricsManager.class);
		when(mgr.getDistributedContexts()).thenReturn(Set.of(ctx));
		when(mgr.getAllContexts()).thenReturn(Set.of());
		when(mgr.getTerminalSteps()).thenReturn(List.of());

		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, config);
		final JsonNode obj = responder.buildClusterMetrics(false).get(0);
		assertEquals("comment-node", obj.get("node_id").asText());
		assertFalse(obj.has("cluster_id"));
		assertEquals("aggregate", obj.get("role").asText());
	}

	private static ListShardMetricsRecorder testRecorder() {
		final var shard = new ListShardMetricsRecorder.ShardSnapshot(
						"prefix-0",
						3L,
						5L,
						1.5d,
						100L,
						200L,
						true,
						12,
						2,
						900,
						1_000,
						300L,
						4,
						2,
						250L,
						"FULL_PAGE_STREAK",
						3,
						1,
						275L,
						2,
						"RESP_FAIL_SVC");
		final var snapshot = new ListShardMetricsRecorder.Snapshot(
						List.of(shard),
						3L,
						5L,
						1.5d,
						500L,
						4L,
						2,
						Map.of("FULL_PAGE_STREAK", 4),
						1L);
		return new ListShardMetricsRecorder() {
			@Override
			public ListShardMetricsRecorder.Snapshot snapshot() {
				return snapshot;
			}
		};
	}

	private static DistributedAllMetricsSnapshot mockDistributedSnapshot() {
		final DistributedAllMetricsSnapshot snapshot = mock(DistributedAllMetricsSnapshot.class);
		final RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		final TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		final ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);

		when(success.count()).thenReturn(60L);
		when(fails.count()).thenReturn(40L);
		when(success.last()).thenReturn(6.0);
		when(fails.last()).thenReturn(4.0);
		when(bytes.count()).thenReturn(1024L);
		when(bytes.last()).thenReturn(128.0);
		when(latency.mean()).thenReturn(100.0);
		when(duration.mean()).thenReturn(200.0);
		when(conc.last()).thenReturn(3L);
		when(conc.mean()).thenReturn(2.0);

		when(snapshot.successSnapshot()).thenReturn(success);
		when(snapshot.failsSnapshot()).thenReturn(fails);
		when(snapshot.byteSnapshot()).thenReturn(bytes);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.concurrencySnapshot()).thenReturn(conc);
		when(snapshot.elapsedTimeMillis()).thenReturn(5000L);
		return snapshot;
	}

	private static DistributedAllMetricsSnapshot mockDistributedSnapshot(
					final long succCount,
					final long failCount,
					final long elapsedMillis) {
		final DistributedAllMetricsSnapshot snapshot = mock(DistributedAllMetricsSnapshot.class);
		final RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		final TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		final ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);

		when(success.count()).thenReturn(succCount);
		when(fails.count()).thenReturn(failCount);
		when(success.last()).thenReturn(0.0);
		when(fails.last()).thenReturn(0.0);
		when(bytes.count()).thenReturn(0L);
		when(bytes.last()).thenReturn(0.0);
		when(latency.mean()).thenReturn(0.0);
		when(duration.mean()).thenReturn(0.0);
		when(conc.last()).thenReturn(0L);
		when(conc.mean()).thenReturn(0.0);

		when(snapshot.successSnapshot()).thenReturn(success);
		when(snapshot.failsSnapshot()).thenReturn(fails);
		when(snapshot.byteSnapshot()).thenReturn(bytes);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.concurrencySnapshot()).thenReturn(conc);
		when(snapshot.elapsedTimeMillis()).thenReturn(elapsedMillis);
		return snapshot;
	}

	private static AllMetricsSnapshot mockNodeSnapshot(
					final long succCount,
					final long failCount,
					final long elapsedMillis) {
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class);
		final RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		final TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		final ConcurrencyMetricSnapshot conc = mock(ConcurrencyMetricSnapshot.class);

		when(success.count()).thenReturn(succCount);
		when(fails.count()).thenReturn(failCount);
		when(success.last()).thenReturn(0.0);
		when(fails.last()).thenReturn(0.0);
		when(bytes.count()).thenReturn(0L);
		when(bytes.last()).thenReturn(0.0);
		when(latency.mean()).thenReturn(0.0);
		when(duration.mean()).thenReturn(0.0);
		when(conc.last()).thenReturn(0L);
		when(conc.mean()).thenReturn(0.0);

		when(snapshot.successSnapshot()).thenReturn(success);
		when(snapshot.failsSnapshot()).thenReturn(fails);
		when(snapshot.byteSnapshot()).thenReturn(bytes);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.concurrencySnapshot()).thenReturn(conc);
		when(snapshot.elapsedTimeMillis()).thenReturn(elapsedMillis);
		return snapshot;
	}

	private static DistributedMetricsContext mockDistributedContext(
					final String stepId,
					final OpType opType,
					final DistributedAllMetricsSnapshot snapshot,
					final Map<String, Object> metadata) {
		return mockDistributedContext(stepId, opType, snapshot, metadata, 1, List.of("node-1"));
	}

	private static DistributedMetricsContext mockDistributedContext(
					final String stepId,
					final OpType opType,
					final DistributedAllMetricsSnapshot snapshot,
					final Map<String, Object> metadata,
					final int nodeCount,
					final List<String> nodeAddrs) {
		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(opType);
		when(ctx.lastSnapshot()).thenReturn(snapshot);
		when(ctx.metadata()).thenReturn(metadata);
		when(ctx.runId()).thenReturn(123L);
		when(ctx.nodeCount()).thenReturn(nodeCount);
		when(ctx.nodeAddrs()).thenReturn(nodeAddrs);
		return ctx;
	}

	private static Config defaultConfig() {
		Config config = TestConfigBuilder.config();
		config.val("run-comment", "responder-test-node");
		config.val("run-port", 18080);
		config.val("run-id", 123L);
		config.val("run-node", true);
		config.val("server-metrics-expose_fleet", true);
		return config;
	}
}
