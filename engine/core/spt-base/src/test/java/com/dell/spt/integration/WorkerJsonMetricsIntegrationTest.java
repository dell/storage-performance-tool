package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.FleetMetricsHandler;
import com.dell.spt.base.control.NodeMetricsHandler;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
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

/**
 * Verifies that /metrics/json returns non-empty data when only local (non-distributed) contexts
 * are registered on a node (worker behavior in RMI runs).
 */
import com.dell.spt.testing.tags.IntegrationTest;

@IntegrationTest
public class WorkerJsonMetricsIntegrationTest {

	private static final int JSON_PORT = 2114;
	private static final String JSON_URL = "http://localhost:" + JSON_PORT + "/metrics/json";
	private static final String FLEET_URL = "http://localhost:" + JSON_PORT + "/metrics/fleet/json";
	private static final String CLUSTER_URL = "http://localhost:" + JSON_PORT + "/metrics/cluster/json";

	private Server jsonServer;
	private MetricsManager metricsManager;
	private MetricsContext metricsContext;
	private ObjectMapper objectMapper;
	private Config config;

	@BeforeEach
	public void setUp() throws Exception {
		objectMapper = new ObjectMapper();
		metricsManager = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
		config = TestConfigBuilder.config();
		config.val("run-comment", "worker-node");
		config.val("run-port", JSON_PORT);
		config.val("run-id", 42L);

		// Start JSON servlet bound to this metrics manager
		jsonServer = new Server(JSON_PORT);
		final var jsonContext = new ServletContextHandler();
		jsonContext.setContextPath("/");
		jsonServer.setHandler(jsonContext);
		jsonContext.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		jsonContext.addServlet(new ServletHolder(new NodeMetricsHandler(metricsManager, config)), "/metrics/json");
		jsonContext.addServlet(new ServletHolder(new FleetMetricsHandler(metricsManager, config)), "/metrics/fleet/json");
		jsonContext.addServlet(new ServletHolder(new FleetMetricsHandler(metricsManager, config)), "/metrics/cluster/json");
		jsonServer.start();

		// Create and register a local metrics context (no distributed contexts)
		final SizeInBytes size = ItemSize.SMALL.getValue();
		metricsContext = MetricsContextImpl.builder()
						.loadStepId("worker-step-1")
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(size)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("integration-worker")
						.runId(42)
						.build();

		// Set limits so completion can be calculated
		metricsContext.metadata().put(com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_OP_COUNT, 100L);
		metricsContext.start();
		metricsManager.register(metricsContext);

		// Simulate some operations
		for (int i = 0; i < 10; i++) {
			metricsContext.markSucc(size.get(), 1000 + i, 500 + i);
		}
		metricsContext.refreshLastSnapshot();
	}

	@AfterEach
	public void tearDown() throws Exception {
		if (metricsContext != null) {
			metricsManager.unregister(metricsContext);
			metricsContext.close();
		}
		if (jsonServer != null) {
			jsonServer.stop();
		}
	}

	@Test
	public void workerJsonReturnsLocalMetrics() throws Exception {
		final String json = fetch(JSON_URL);
		assertNotNull(json);
		final JsonNode arr = objectMapper.readTree(json);
		assertTrue(arr.size() > 0, "/metrics/json should return at least one entry on worker");
		final JsonNode first = arr.get(0);
		assertEquals(2, first.get("metrics_schema").asInt());
		assertEquals("node", first.get("scope").asText());
		assertEquals("worker", first.get("role").asText());
		assertEquals("worker-node", first.get("node_id").asText());
		assertEquals("42", first.get("run_id").asText());
		assertTrue(first.hasNonNull("sample_ts"));
		assertEquals("worker-step-1", first.get("step_id").asText());
		assertTrue(first.get("operations").get("success_count").asLong() > 0);
		assertEquals("op_count", first.get("limit").get("type").asText());
		assertEquals(100L, first.get("limit").get("op_count").asLong());
		assertFalse(first.get("unbounded").asBoolean());
		assertFalse(first.get("overall_unbounded").asBoolean());
		assertEquals(10, first.get("completion_percent").asInt());
		assertEquals(10, first.get("overall_completion_percent").asInt());
		assertEquals(404, fetchStatus(FLEET_URL), "/metrics/fleet/json should not be available on worker");
		assertEquals(404, fetchStatus(CLUSTER_URL), "/metrics/cluster/json should not be available on worker");
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
