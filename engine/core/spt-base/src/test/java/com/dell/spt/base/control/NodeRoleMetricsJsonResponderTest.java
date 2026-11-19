package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The {@code role} of {@code /metrics/json} node rows on packaged-engine configuration ({@code run-node} true on
 * every node): {@code entry} for the rows of a run this node accepted through {@code POST /run}, {@code worker}
 * for every row of a slice an entry started here, constant from a row's first appearance to its expiry.
 */
class NodeRoleMetricsJsonResponderTest {

	private static final SizeInBytes SIZE = new SizeInBytes(1024);

	private MetricsManagerImpl mgr;

	@BeforeEach
	void setUp() {
		mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(60_000L);
	}

	@AfterEach
	void tearDown() throws Exception {
		// Registration exports to the shared Prometheus registry; leave nothing for later tests.
		for (final MetricsContext<?> ctx : List.copyOf(mgr.getAllContexts())) {
			mgr.unregister(ctx);
		}
		mgr.close();
	}

	@Test
	void sliceRowsOnAWorkerReadWorkerLiveAndTerminal() {
		// A worker whose /status stays idle: the entry starts its slice over RMI.
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, productionConfig("worker-1"), new ApiStatus());
		final MetricsContext<?> slice = localContext("step-a", 77L);
		mgr.register(slice);

		final JsonNode live = row(responder.buildNodeMetrics(false), "step-a");
		assertFalse(live.has("terminal"));
		assertEquals("worker", live.get("role").asText());

		mgr.unregister(slice);
		final JsonNode terminal = row(responder.buildNodeMetrics(false), "step-a");
		assertTrue(terminal.get("terminal").asBoolean());
		assertEquals("worker", terminal.get("role").asText());
	}

	@Test
	void entryOwnRowReadsEntryFromBeforeItsDistributedContextUntilAfterItCloses() {
		final ApiStatus apiStatus = new ApiStatus();
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, productionConfig("entry-1"), apiStatus);
		// POST /run records the run before its first step starts.
		apiStatus.setRunning("step-e", 55L);

		final MetricsContext<?> local = localContext("step-e", 55L);
		mgr.register(local);
		assertEquals(0, mgr.getDistributedContexts().size(), "the local slice registers first");
		assertEquals("entry", row(responder.buildNodeMetrics(false), "step-e").get("role").asText());

		final DistributedMetricsContext distributed = distributedContext("step-e", 55L, local);
		mgr.register(distributed);
		assertEquals("entry", row(responder.buildNodeMetrics(false), "step-e").get("role").asText());

		mgr.unregister(local);
		final JsonNode terminalWhileAggregating = row(responder.buildNodeMetrics(false), "step-e");
		assertTrue(terminalWhileAggregating.get("terminal").asBoolean());
		assertEquals("entry", terminalWhileAggregating.get("role").asText());

		mgr.unregister(distributed);
		apiStatus.completeIfNotStopped();
		final JsonNode terminalAfterRun = row(responder.buildNodeMetrics(false), "step-e");
		assertEquals("entry", terminalAfterRun.get("role").asText());

		// The next run starts while this run's rows are still retained: they keep their role.
		apiStatus.setRunning("step-next", 56L);
		assertEquals("entry", row(responder.buildNodeMetrics(false), "step-e").get("role").asText());
	}

	@Test
	void nodeWithItsOwnStepAndAnotherEntrysSliceGivesEachRowItsRole() {
		final ApiStatus apiStatus = new ApiStatus();
		apiStatus.setRunning("step-own", 55L);
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, productionConfig("node-1"), apiStatus);
		final MetricsContext<?> own = localContext("step-own", 55L);
		final MetricsContext<?> slice = localContext("step-slice", 66L);
		mgr.register(own);
		mgr.register(distributedContext("step-own", 55L, own));
		mgr.register(slice);

		final ArrayNode live = responder.buildNodeMetrics(false);
		assertEquals("entry", row(live, "step-own").get("role").asText());
		assertEquals("worker", row(live, "step-slice").get("role").asText());

		mgr.unregister(slice);
		mgr.unregister(own);
		final ArrayNode terminal = responder.buildNodeMetrics(false);
		assertEquals("entry", row(terminal, "step-own").get("role").asText());
		assertEquals("worker", row(terminal, "step-slice").get("role").asText());
	}

	@Test
	void idleSampleReadsWorkerWithoutADistributedTerminalAndEntryWithOne() {
		final ApiStatus apiStatus = new ApiStatus();
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, productionConfig("entry-1"), apiStatus);
		final JsonNode fresh = only(responder.buildNodeMetrics(false));
		assertEquals("", fresh.get("step_id").asText());
		assertEquals("worker", fresh.get("role").asText());

		// Only the distributed context lives here (no local slice): the idle sample reads entry, also once
		// the distributed context has closed and its terminal entry is retained.
		apiStatus.setRunning("step-d", 88L);
		final MetricsContext<?> remoteOnly = localContext("step-d", 88L);
		final DistributedMetricsContext distributed = distributedContext("step-d", 88L, remoteOnly);
		mgr.register(distributed);
		assertEquals("entry", only(responder.buildNodeMetrics(false)).get("role").asText());
		mgr.unregister(distributed);
		final JsonNode retained = only(responder.buildNodeMetrics(false));
		assertEquals("", retained.get("step_id").asText());
		assertEquals("entry", retained.get("role").asText());
		remoteOnly.close();

		// Once the distributed terminal has expired the idle sample reads worker again.
		mgr.setTerminalRetentionMillis(0L);
		sleepPastRetention();
		assertEquals("worker", only(responder.buildNodeMetrics(false)).get("role").asText());
	}

	@Test
	void fleetRowsStayAggregate() {
		final ApiStatus apiStatus = new ApiStatus();
		apiStatus.setRunning("step-f", 99L);
		final MetricsJsonResponder responder = new MetricsJsonResponder(mgr, productionConfig("entry-1"), apiStatus);
		final MetricsContext<?> local = localContext("step-f", 99L);
		final DistributedMetricsContext distributed = distributedContext("step-f", 99L, local);
		mgr.register(local);
		mgr.register(distributed);
		assertEquals("aggregate", only(responder.buildClusterMetrics(false)).get("role").asText());
		mgr.unregister(local);
		mgr.unregister(distributed);
		assertEquals("aggregate", only(responder.buildClusterMetrics(false)).get("role").asText());
	}

	private static Config productionConfig(final String nodeId) {
		final Config config = TestConfigBuilder.config();
		config.val("run-comment", nodeId);
		config.val("run-port", 18081);
		config.val("run-node", true);
		config.val("server-metrics-expose_fleet", true);
		return config;
	}

	private static MetricsContext<?> localContext(final String stepId, final long runId) {
		final MetricsContext<?> ctx = MetricsContextImpl.builder()
						.loadStepId(stepId)
						.runId(runId)
						.opType(OpType.CREATE)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(SIZE)
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
		ctx.start();
		ctx.markSucc(SIZE.get(), 1_000L, 100L, 250L);
		ctx.refreshLastSnapshot();
		return ctx;
	}

	private static DistributedMetricsContext distributedContext(
					final String stepId, final long runId, final MetricsContext<?> contributor) {
		final DistributedMetricsContextImpl ctx = DistributedMetricsContextImpl.builder()
						.loadStepId(stepId)
						.opType(OpType.CREATE)
						.nodeCountSupplier(() -> 1)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(SIZE)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(contributor.lastSnapshot()))
						.quantileValues(List.of(0.5))
						.nodeAddrs(List.of("127.0.0.1:1099"))
						.comment("entry")
						.runId(runId)
						.opCountLimit(0L)
						.timeLimitSec(0L)
						.build();
		ctx.start();
		ctx.refreshLastSnapshot();
		return ctx;
	}

	private static JsonNode row(final ArrayNode payload, final String stepId) {
		for (final JsonNode node : payload) {
			if (stepId.equals(node.path("step_id").asText())) {
				return node;
			}
		}
		throw new AssertionError("expected a row for " + stepId + " in " + payload);
	}

	private static JsonNode only(final ArrayNode payload) {
		assertEquals(1, payload.size(), payload::toString);
		return payload.get(0);
	}

	private static void sleepPastRetention() {
		try {
			Thread.sleep(5L);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
	}
}
