package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.FleetMetricsHandler;
import com.dell.spt.base.control.NodeMetricsHandler;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.params.ItemSize;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Simulates 1 entry + 1 worker: worker has only local metrics; entry has a distributed
 * context aggregating the worker snapshot supplier. Verifies both endpoints are non-empty.
 */
import com.dell.spt.testing.tags.IntegrationTest;

@IntegrationTest
public class EntryWorkerJsonMetricsIntegrationTest {

	private static final int WORKER_JSON_PORT = 2115;
	private static final int ENTRY_JSON_PORT = 2116;
	private static final String WORKER_JSON_URL = "http://localhost:" + WORKER_JSON_PORT + "/metrics/json";
	private static final String ENTRY_JSON_URL = "http://localhost:" + ENTRY_JSON_PORT + "/metrics/json";
	private static final String ENTRY_FLEET_URL = "http://localhost:" + ENTRY_JSON_PORT + "/metrics/fleet/json";
	private static final String WORKER_FLEET_URL = "http://localhost:" + WORKER_JSON_PORT + "/metrics/fleet/json";
	private static final String ENTRY_CLUSTER_URL = "http://localhost:" + ENTRY_JSON_PORT + "/metrics/cluster/json";
	private static final String WORKER_CLUSTER_URL = "http://localhost:" + WORKER_JSON_PORT + "/metrics/cluster/json";

	private Server workerJsonServer;
	private Server entryJsonServer;
	private MetricsManager workerMgr;
	private MetricsManager entryMgr;
	private MetricsContext workerLocalCtx;
	private DistributedMetricsContextImpl entryDistCtx;
	private MetricsContext entryLocalCtx;
	private ObjectMapper om;
	private Config workerConfig;
	private Config entryConfig;

	@BeforeEach
	public void setUp() throws Exception {
		om = new ObjectMapper();
		workerMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
		entryMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
		workerConfig = TestConfigBuilder.config();
		workerConfig.val("run-comment", "worker-node");
		workerConfig.val("run-port", WORKER_JSON_PORT);
		workerConfig.val("run-id", 77L);
		workerConfig.val("server-metrics-expose_fleet", true);
		entryConfig = TestConfigBuilder.config();
		entryConfig.val("run-comment", "entry-node");
		entryConfig.val("run-port", ENTRY_JSON_PORT);
		entryConfig.val("run-id", 77L);
		entryConfig.val("run-node", true);
		entryConfig.val("server-metrics-expose_fleet", true);

		// Worker server
		workerJsonServer = new Server(WORKER_JSON_PORT);
		final var workerContext = new ServletContextHandler();
		workerContext.setContextPath("/");
		workerJsonServer.setHandler(workerContext);
		workerContext.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		workerContext.addServlet(new ServletHolder(new NodeMetricsHandler(workerMgr, workerConfig)), "/metrics/json");
		workerContext.addServlet(new ServletHolder(new FleetMetricsHandler(workerMgr, workerConfig)), "/metrics/fleet/json");
		workerContext.addServlet(new ServletHolder(new FleetMetricsHandler(workerMgr, workerConfig)), "/metrics/cluster/json");
		workerJsonServer.start();

		// Worker local context
		final SizeInBytes size = ItemSize.SMALL.getValue();
		workerLocalCtx = MetricsContextImpl.builder()
						.loadStepId("step-agg-1")
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 2)
						.concurrencyLimit(8)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("worker")
						.runId(77)
						.build();
		workerLocalCtx.metadata().put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_OP_COUNT, 200L);
		workerLocalCtx.start();
		workerMgr.register(workerLocalCtx);
		for (int i = 0; i < 20; i++) {
			workerLocalCtx.markSucc(size.get(), 1000 + i, 500 + i);
		}
		workerLocalCtx.refreshLastSnapshot();

		// Entry server
		entryJsonServer = new Server(ENTRY_JSON_PORT);
		final var entryContext = new ServletContextHandler();
		entryContext.setContextPath("/");
		entryJsonServer.setHandler(entryContext);
		entryContext.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		entryContext.addServlet(new ServletHolder(new NodeMetricsHandler(entryMgr, entryConfig)), "/metrics/json");
		entryContext.addServlet(new ServletHolder(new FleetMetricsHandler(entryMgr, entryConfig)), "/metrics/fleet/json");
		entryContext.addServlet(new ServletHolder(new FleetMetricsHandler(entryMgr, entryConfig)), "/metrics/cluster/json");
		entryJsonServer.start();

		// Entry distributed context that aggregates the worker snapshot
		entryDistCtx = DistributedMetricsContextImpl.builder()
						.loadStepId("step-agg-1")
						.opType(OpType.CREATE)
						.nodeCountSupplier(() -> 1)
						.concurrencyLimit(8)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.timingPersistFlag(false)
						.snapshotsSupplier(() -> List.of(workerLocalCtx.lastSnapshot()))
						.quantileValues(java.util.Arrays.asList(0.25, 0.5, 0.75))
						.nodeAddrs(java.util.List.of("127.0.0.1:1099"))
						.comment("entry")
						.runId(77)
						.opCountLimit(200L)
						.timeLimitSec(0L)
						.build();
		entryDistCtx.start();
		entryMgr.register(entryDistCtx);
		entryDistCtx.refreshLastSnapshot();
	}

	@AfterEach
	public void tearDown() throws Exception {
		if (entryDistCtx != null) {
			entryMgr.unregister(entryDistCtx);
			entryDistCtx.close();
		}
		if (entryLocalCtx != null) {
			entryMgr.unregister(entryLocalCtx);
			entryLocalCtx.close();
			entryLocalCtx = null;
		}
		if (workerLocalCtx != null) {
			workerMgr.unregister(workerLocalCtx);
			workerLocalCtx.close();
		}
		if (workerJsonServer != null)
			workerJsonServer.stop();
		if (entryJsonServer != null)
			entryJsonServer.stop();
	}

	@Test
	public void entryAndWorkerJsonNonEmpty() throws Exception {
		final JsonNode arrWorker = om.readTree(fetch(WORKER_JSON_URL));
		final JsonNode arrEntry = om.readTree(fetch(ENTRY_JSON_URL));
		final JsonNode arrCluster = om.readTree(fetch(ENTRY_CLUSTER_URL));
		final JsonNode arrFleet = om.readTree(fetch(ENTRY_FLEET_URL));
		assertTrue(arrWorker.size() > 0, "Worker /metrics/json should be non-empty");
		final JsonNode workerObj = arrWorker.get(0);
		assertEquals(2, workerObj.get("metrics_schema").asInt());
		assertEquals("node", workerObj.get("scope").asText());
		assertEquals("worker", workerObj.get("role").asText());
		assertEquals("worker-node", workerObj.get("node_id").asText());
		assertEquals("77", workerObj.get("run_id").asText());
		assertTrue(workerObj.hasNonNull("sample_ts"));
		assertEquals(1, arrEntry.size(), "Entry /metrics/json should return a single idle sample when no local contexts exist");
		assertTrue(arrCluster.size() > 0, "Entry /metrics/cluster/json should contain aggregated metrics");
		final JsonNode clusterObj = arrCluster.get(0);
		assertEquals(2, clusterObj.get("metrics_schema").asInt());
		assertEquals("fleet", clusterObj.get("scope").asText());
		assertEquals("aggregate", clusterObj.get("role").asText());
		assertEquals("entry-node", clusterObj.get("node_id").asText());
		assertEquals("77", clusterObj.get("run_id").asText());
		assertEquals(1, clusterObj.get("nodes_count").asInt());
		assertEquals(1, clusterObj.get("nodes_present").size());
		assertFalse(clusterObj.get("partial").asBoolean());
		assertTrue(clusterObj.hasNonNull("sample_ts"));
		assertTrue(arrFleet.size() > 0, "Legacy /metrics/fleet/json should remain available");
		assertEquals(arrCluster.size(), arrFleet.size(), "Cluster and fleet endpoints should return same number of samples");
		assertEquals(404, fetchStatus(WORKER_CLUSTER_URL), "Worker /metrics/cluster/json should return 404");
		assertEquals(404, fetchStatus(WORKER_FLEET_URL), "Worker /metrics/fleet/json should return 404");
	}

	@Test
	public void entryNodeReportsLocalContextWithEntryRole() throws Exception {
		entryLocalCtx = MetricsContextImpl.builder()
						.loadStepId("step-entry-local")
						.opType(OpType.UPDATE)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(ItemSize.SMALL.getValue())
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("entry-local")
						.runId(77)
						.build();
		entryLocalCtx
						.metadata()
						.put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_OP_COUNT, 50L);
		entryLocalCtx.start();
		entryMgr.register(entryLocalCtx);
		for (int i = 0; i < 5; i++) {
			entryLocalCtx.markSucc(ItemSize.SMALL.getValue().get(), 900 + i, 400 + i);
		}
		entryLocalCtx.refreshLastSnapshot();

		final JsonNode arrEntry = om.readTree(fetch(ENTRY_JSON_URL));
		assertEquals(1, arrEntry.size(), "Entry /metrics/json should expose local context once registered");
		final JsonNode nodeObj = arrEntry.get(0);
		assertEquals("node", nodeObj.get("scope").asText());
		assertEquals("entry", nodeObj.get("role").asText());
		assertEquals("entry-node", nodeObj.get("node_id").asText());
		assertEquals("op_count", nodeObj.get("limit").get("type").asText());
		assertEquals(50L, nodeObj.get("limit").get("op_count").asLong());
		assertFalse(nodeObj.get("unbounded").asBoolean());
		assertFalse(nodeObj.get("overall_unbounded").asBoolean());
		assertEquals(10, nodeObj.get("completion_percent").asInt());
		assertEquals(10, nodeObj.get("overall_completion_percent").asInt());

		final JsonNode verboseObj = om.readTree(fetch(ENTRY_JSON_URL + "?verbose=1")).get(0);
		assertEquals(1, verboseObj.get("diag_distributed_contexts").asInt());
		assertEquals(1, verboseObj.get("diag_local_contexts").asInt());
	}

	@Test
	public void clusterEndpointRetainsTerminalAggregateWhenDistributedStops() throws Exception {
		entryMgr.unregister(entryDistCtx);
		entryDistCtx.close();
		entryDistCtx = null;

		final JsonNode arrEntry = om.readTree(fetch(ENTRY_JSON_URL));
		assertEquals(1, arrEntry.size(), "Entry /metrics/json should fall back to idle sample");
		final JsonNode nodeObj = arrEntry.get(0);
		assertEquals("entry", nodeObj.get("role").asText(), "Entry node should keep entry role after distributed teardown");
		assertEquals(0L, nodeObj.get("operations").get("success_count").asLong(), "Node metrics should not expose fleet totals");

		final JsonNode arrCluster = om.readTree(fetch(ENTRY_CLUSTER_URL));
		assertTrue(arrCluster.size() > 0, "Cluster endpoint should retain terminal aggregate");
		final JsonNode clusterObj = arrCluster.get(0);
		assertEquals("aggregate", clusterObj.get("role").asText());
		assertEquals(20L, clusterObj.get("operations").get("success_count").asLong());
	}

	private static String fetch(String url) throws Exception {
		final var u = new URL(url);
		final var conn = (HttpURLConnection) u.openConnection();
		conn.setRequestMethod("GET");
		final var sb = new StringBuilder();
		try (var r = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
			String line;
			while ((line = r.readLine()) != null)
				sb.append(line).append('\n');
		}
		conn.disconnect();
		return sb.toString();
	}

	private static int fetchStatus(String url) throws Exception {
		final var u = new URL(url);
		final var conn = (HttpURLConnection) u.openConnection();
		conn.setRequestMethod("GET");
		final int status = conn.getResponseCode();
		conn.disconnect();
		return status;
	}
}
