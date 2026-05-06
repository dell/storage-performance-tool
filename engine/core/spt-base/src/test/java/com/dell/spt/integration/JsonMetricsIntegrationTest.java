package com.dell.spt.integration;

import static com.dell.spt.base.metrics.MetricsConstants.*;
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
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.params.ItemSize;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.prometheus.client.exporter.MetricsServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.dell.spt.testing.tags.IntegrationTest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Locale;
import java.net.InetSocketAddress;
import java.io.OutputStream;
import com.sun.net.httpserver.HttpServer;

/**
 * Integration test comparing JSON vs Prometheus output to ensure consistency.
 * Tests the complete JSON endpoint implementation against real metrics data.
 */
@IntegrationTest
public class JsonMetricsIntegrationTest {

	private static final int METRICS_PORT = 2112;
	private static final int JSON_PORT = 2113;
	private static final String NODE_JSON_URL = "http://localhost:" + JSON_PORT + "/metrics/json";
	private static final String FLEET_JSON_URL = "http://localhost:" + JSON_PORT + "/metrics/fleet/json";
	private static final String CLUSTER_JSON_URL = "http://localhost:" + JSON_PORT + "/metrics/cluster/json";

	private static final String STEP_ID = "integration-test-step";
	private static final int RUN_ID = 12345;
	private static final OpType OP_TYPE = OpType.CREATE;
	private static final List<String> NODE_LIST = Arrays.asList("127.0.0.1:1099");

	private static int nodeCount() {
		return 1;
	}

	private static final int CONCURRENCY_LIMIT = 10;
	private static final SizeInBytes ITEM_DATA_SIZE = ItemSize.SMALL.getValue();

	private Server prometheusServer;
	private Server jsonServer;
	private MetricsManager metricsManager;
	private DistributedMetricsContextImpl distributedMetricsContext;
	private MetricsContext metricsContext;
	private ObjectMapper objectMapper;
	private Supplier<List<AllMetricsSnapshot>> snapshotsSupplier;
	private Config jsonConfig;

	@BeforeEach
	public void setUp() throws Exception {
		objectMapper = new ObjectMapper();
		metricsManager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		jsonConfig = TestConfigBuilder.config();
		jsonConfig.val("run-comment", "integration-entry");
		jsonConfig.val("run-port", JSON_PORT);
		jsonConfig.val("run-id", RUN_ID);
		jsonConfig.val("run-node", true);

		// Setup Prometheus metrics server
		prometheusServer = new Server(METRICS_PORT);
		final var prometheusContext = new ServletContextHandler();
		prometheusContext.setContextPath("/");
		prometheusServer.setHandler(prometheusContext);
		prometheusContext.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		prometheusServer.start();

		// Setup JSON metrics server
		jsonServer = new Server(JSON_PORT);
		final var jsonContext = new ServletContextHandler();
		jsonContext.setContextPath("/");
		jsonServer.setHandler(jsonContext);
		jsonContext.addServlet(new ServletHolder(new NodeMetricsHandler(metricsManager, jsonConfig)), "/metrics/json");
		jsonContext.addServlet(new ServletHolder(new FleetMetricsHandler(metricsManager, jsonConfig)), "/metrics/fleet/json");
		jsonContext.addServlet(new ServletHolder(new FleetMetricsHandler(metricsManager, jsonConfig)), "/metrics/cluster/json");
		jsonServer.start();

		// Create basic metrics context (needed for snapshots supplier)
		metricsContext = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.opType(OP_TYPE)
						.actualConcurrencyGauge(() -> 1)
						.concurrencyLimit(CONCURRENCY_LIMIT)
						.concurrencyThreshold(0)
						.itemDataSize(ITEM_DATA_SIZE)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("integration-test")
						.runId(RUN_ID)
						.build();

		// Create snapshots supplier
		snapshotsSupplier = () -> Arrays.asList(metricsContext.lastSnapshot());
		metricsContext.start();

		// Create distributed metrics context
		distributedMetricsContext = DistributedMetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.nodeAddrs(NODE_LIST)
						.nodeCountSupplier(JsonMetricsIntegrationTest::nodeCount)
						.concurrencyLimit(CONCURRENCY_LIMIT)
						.concurrencyThreshold(0)
						.itemDataSize(ITEM_DATA_SIZE)
						.opType(OP_TYPE)
						.quantileValues(Arrays.asList(0.25, 0.5, 0.75))
						.outputPeriodSec(0)
						.avgPersistFlag(false)
						.sumPersistFlag(false)
						.stdOutColorFlag(false)
						.snapshotsSupplier(snapshotsSupplier)
						.comment("integration-test")
						.runId(RUN_ID)
						.build();

		// Register metrics context
		metricsManager.register(distributedMetricsContext);

		// Generate some test metrics data
		generateTestMetricsData();
	}

	@AfterEach
	public void tearDown() throws Exception {
		if (distributedMetricsContext != null) {
			metricsManager.unregister(distributedMetricsContext);
			distributedMetricsContext.close();
		}
		if (metricsContext != null) {
			metricsContext.close();
		}
		if (metricsManager != null) {
			metricsManager.close();
		}
		if (prometheusServer != null) {
			prometheusServer.stop();
		}
		if (jsonServer != null) {
			jsonServer.stop();
		}
	}

	@Test
	public void testJsonEndpointResponseFormat() throws Exception {
		// Fetch JSON metrics
		String jsonResponse = fetchJsonMetrics();
		JsonNode jsonArray = objectMapper.readTree(jsonResponse);

		// Validate JSON structure
		assertTrue(jsonArray.isArray());

		if (jsonArray.size() > 0) {
			JsonNode jsonObj = jsonArray.get(0);
			assertEquals(3, jsonObj.get("metrics_schema").asInt());
			assertEquals("fleet", jsonObj.get("scope").asText());
			assertEquals("aggregate", jsonObj.get("role").asText());
			assertEquals("integration-entry", jsonObj.get("node_id").asText());
			assertEquals(String.valueOf(RUN_ID), jsonObj.get("run_id").asText());
			assertTrue(jsonObj.hasNonNull("sample_ts"));
			assertTrue(jsonObj.has("nodes_count"));
			assertTrue(jsonObj.get("nodes_present").isArray());
			assertTrue(jsonObj.has("partial"));

			// Required top-level fields
			String[] requiredFields = {"step_id", "op_type", "timestamp", "elapsed_time_seconds", "test_state"
			};
			for (String field : requiredFields) {
				assertTrue(jsonObj.has(field), "Missing required field: " + field);
			}

			// Required nested objects
			String[] requiredObjects = {"operations", "bandwidth", "timing", "concurrency"
			};
			for (String obj : requiredObjects) {
				assertTrue(jsonObj.has(obj), "Missing required object: " + obj);
				assertTrue(jsonObj.get(obj).isObject(), "Field should be object: " + obj);
			}
		}
	}

	@Test
	public void testNodeMetricsRetainTerminalTotalsAfterUnregister() throws Exception {
		final String localStepId = "pause-window-step";
		final MetricsContextImpl localCtx = (MetricsContextImpl) MetricsContextImpl.builder()
						.loadStepId(localStepId)
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 8)
						.concurrencyLimit(8)
						.concurrencyThreshold(0)
						.itemDataSize(ITEM_DATA_SIZE)
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("integration-local")
						.runId(RUN_ID)
						.build();

		localCtx.start();
		metricsManager.register(localCtx);

		for (int i = 0; i < 1_200; i++) {
			localCtx.markSucc(ITEM_DATA_SIZE.get(), 1_000_000L, 500_000L);
		}
		localCtx.refreshLastSnapshot();

		metricsManager.unregister(localCtx);

		String nodeVerboseResponse = fetchFromUrl(NODE_JSON_URL + "?verbose=true");
		JsonNode nodeArray = objectMapper.readTree(nodeVerboseResponse);
		assertTrue(nodeArray.isArray());
		JsonNode terminalEntry = null;
		for (JsonNode node : nodeArray) {
			if (node.path("terminal").asBoolean(false)) {
				terminalEntry = node;
				break;
			}
		}
		assertNotNull(terminalEntry, "expected terminal entry in node metrics payload");
		assertEquals(localStepId, terminalEntry.get("step_id").asText());
		assertEquals("CREATE", terminalEntry.get("op_type").asText());
		assertEquals(1_200L, terminalEntry.get("operations").get("success_count").asLong());
		assertEquals(String.valueOf(RUN_ID), terminalEntry.get("run_id").asText());
		assertTrue(terminalEntry.get("diag_cached_progress").asBoolean(), "cached progress flag not set");

		localCtx.close();
	}

	@Test
	public void testClusterEndpointMatchesFleet() throws Exception {
		final ArrayNode cluster = (ArrayNode) objectMapper.readTree(fetchFromUrl(CLUSTER_JSON_URL));
		final ArrayNode fleet = (ArrayNode) objectMapper.readTree(fetchJsonMetrics());
		stripVolatileFields(cluster);
		stripVolatileFields(fleet);
		assertEquals(fleet, cluster, "Cluster endpoint should mirror fleet aggregates");
	}

	@Test
	public void testNodeEndpointIdleSampleWhenOnlyDistributedContextsPresent() throws Exception {
		JsonNode arr = objectMapper.readTree(fetchFromUrl(NODE_JSON_URL));
		assertEquals(1, arr.size(), "/metrics/json should emit a single idle sample when no local contexts are active");
		JsonNode obj = arr.get(0);
		assertEquals(3, obj.get("metrics_schema").asInt());
		assertEquals("node", obj.get("scope").asText());
		assertEquals("entry", obj.get("role").asText(), "Role should be entry when distributed context exists");
		assertTrue(obj.hasNonNull("sample_ts"));
		// RFC3339 parseable timestamp
		java.time.Instant.parse(obj.get("sample_ts").asText());
		assertEquals("", obj.get("run_id").asText());
		assertEquals("", obj.get("step_id").asText());
		assertEquals("none", obj.get("op_type").asText());
		assertEquals(0, obj.get("test_state").asInt());
	}

	@Test
	public void testUnboundedAndLimitFields() throws Exception {
		// Configure limits on the distributed context and verify JSON fields
		distributedMetricsContext.metadata().put(METADATA_LIMIT_OP_COUNT, 500L);
		distributedMetricsContext.metadata().put(METADATA_LIMIT_TIME_SEC, 0L);

		String json = fetchJsonMetrics();
		JsonNode arr = objectMapper.readTree(json);
		assertTrue(arr.isArray());
		JsonNode obj = arr.get(0);
		assertFalse(obj.get("unbounded").asBoolean());
		assertEquals("op_count", obj.get("limit").get("type").asText());
		assertEquals(500L, obj.get("limit").get("op_count").asLong());

		// Switch to time-based limit
		distributedMetricsContext.metadata().put(METADATA_LIMIT_OP_COUNT, 0L);
		distributedMetricsContext.metadata().put(METADATA_LIMIT_TIME_SEC, 30L);
		json = fetchJsonMetrics();
		arr = objectMapper.readTree(json);
		obj = arr.get(0);
		assertFalse(obj.get("unbounded").asBoolean());
		assertEquals("time", obj.get("limit").get("type").asText());
		assertEquals(30L, obj.get("limit").get("time_sec").asLong());

		// No limits
		distributedMetricsContext.metadata().remove(METADATA_LIMIT_OP_COUNT);
		distributedMetricsContext.metadata().remove(METADATA_LIMIT_TIME_SEC);
		json = fetchJsonMetrics();
		arr = objectMapper.readTree(json);
		obj = arr.get(0);
		assertTrue(obj.get("unbounded").asBoolean());
		assertEquals("none", obj.get("limit").get("type").asText());
	}

	@Test
	public void testJsonEndpointCorsHeaders() throws Exception {
		URL url = new URL(FLEET_JSON_URL);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		connection.connect();

		// Check CORS headers
		assertEquals("*", connection.getHeaderField("Access-Control-Allow-Origin"));
		assertEquals("GET", connection.getHeaderField("Access-Control-Allow-Methods"));
		assertEquals("application/json", connection.getContentType());

		connection.disconnect();
	}

	@Test
	public void fetchFromUrlReadsUtf8AsExpected() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/ascii", exchange -> {
			byte[] body = "plain-ascii".getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream os = exchange.getResponseBody()) {
				os.write(body);
			}
		});
		server.start();
		try {
			String response = fetchFromUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/ascii");
			assertEquals("plain-ascii\n", response);
		} finally {
			server.stop(0);
		}
	}

	@Test
	public void fetchFromUrlHonorsUtf16Charset() throws Exception {
		HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
		server.createContext("/latin", exchange -> {
			byte[] body = "føø".getBytes(StandardCharsets.UTF_16LE);
			exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-16LE");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream os = exchange.getResponseBody()) {
				os.write(body);
			}
		});
		server.start();
		try {
			String response = fetchFromUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/latin");
			assertEquals("føø\n", response);
		} finally {
			server.stop(0);
		}
	}

	/**
	 * Generate test metrics data by simulating operations
	 */
	private void generateTestMetricsData() {
		// Start the distributed context
		distributedMetricsContext.start();

		// Simulate successful operations on the basic metrics context
		for (int i = 0; i < 100; i++) {
			metricsContext.markSucc(ITEM_DATA_SIZE.get(), 1000000 + i * 1000, 500000 + i * 100); // bytes, duration, latency in nanos
		}

		// Simulate a few failures
		for (int i = 0; i < 5; i++) {
			metricsContext.markFail();
		}

		// Force snapshot refresh on both contexts
		metricsContext.refreshLastSnapshot();
		distributedMetricsContext.refreshLastSnapshot();
	}

	/**
	 * Fetch JSON metrics via HTTP
	 */
	private String fetchJsonMetrics() throws Exception {
		return fetchFromUrl(FLEET_JSON_URL);
	}

	private String fetchFromUrl(final String urlString) throws Exception {
		final URL url = new URL(urlString);
		final HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestMethod("GET");
		final StringBuilder response = new StringBuilder();
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(),
						resolveCharset(connection)))) {
			String line;
			while ((line = reader.readLine()) != null) {
				response.append(line).append('\n');
			}
		} finally {
			connection.disconnect();
		}
		return response.toString();
	}

	/**
	 * Parse Prometheus metrics text format into key-value map
	 */
	@SuppressWarnings("unused")
	private Map<String, Double> parsePrometheusMetrics(String prometheusText) {
		Map<String, Double> metrics = new HashMap<>();

		// Pattern to match Prometheus metric lines (ignoring HELP and TYPE lines)
		Pattern metricPattern = Pattern.compile("^spt_(\\w+)\\{.*\\}\\s+([\\d\\.\\-eE]+)");

		try (BufferedReader reader = new BufferedReader(new java.io.StringReader(prometheusText))) {
			String line;
			while ((line = reader.readLine()) != null) {
				if (line.startsWith("spt_") && !line.startsWith("# ")) {
					Matcher matcher = metricPattern.matcher(line);
					if (matcher.matches()) {
						String metricName = matcher.group(1);
						double value = Double.parseDouble(matcher.group(2));
						metrics.put(metricName, value);
					}
				}
			}
		} catch (java.io.IOException e) {
			// StringReader#read does not throw, but keep catch for completeness
		}

		return metrics;
	}

	private static void stripVolatileFields(final ArrayNode array) {
		for (JsonNode node : array) {
			if (node instanceof ObjectNode obj) {
				obj.remove("sample_ts");
				obj.remove("timestamp");
				obj.remove("elapsed_time_seconds");
			}
		}
	}

	private static Charset resolveCharset(final HttpURLConnection connection) {
		final String contentType = connection.getContentType();
		if (contentType != null) {
			int start = 0;
			while (start <= contentType.length()) {
				final int separator = contentType.indexOf(';', start);
				final int end = separator >= 0 ? separator : contentType.length();
				final String part = contentType.substring(start, end).trim();
				if (part.toLowerCase(Locale.ROOT).startsWith("charset=")) {
					final String charsetName = part.substring("charset=".length()).trim();
					try {
						return Charset.forName(charsetName);
					} catch (Exception ignored) {
						// fall back to UTF-8
					}
				}
				if (separator < 0) {
					break;
				}
				start = separator + 1;
			}
		}
		return StandardCharsets.UTF_8;
	}

	/**
	 * Compare metric values with tolerance for floating point precision
	 */
	@SuppressWarnings("unused")
	private void compareMetric(String metricName, double jsonValue, Double prometheusValue) {
		assertNotNull(prometheusValue, "Prometheus value should not be null for: " + metricName);
		assertEquals(prometheusValue, jsonValue, 0.001,
						String.format("Metric %s mismatch: JSON=%.3f, Prometheus=%.3f",
										metricName, jsonValue, prometheusValue));
	}
}
