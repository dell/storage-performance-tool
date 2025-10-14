package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.akurilov.confuse.Config;

import io.prometheus.client.exporter.MetricsServlet;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ServletAPITest {
	private static final int PORT = 9999;
	private static final String HOST = "http://localhost:" + PORT;
	private static final Server server = new Server(PORT);
	private Config testConfig;

	private ReadinessGate readinessGate;

	@BeforeEach
	public void setUp() throws Exception {
		testConfig = TestConfigBuilder.config();
		final ServletContextHandler context = new ServletContextHandler();
		final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
		context.setContextPath("/");
		server.setHandler(context);

		// Add all servlets here
		context.addServlet(new ServletHolder(new ConfigServlet(testConfig)), "/config/*");
		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		context.addServlet(new ServletHolder(new HealthServlet(metricsMgr, testConfig)), "/health");
		readinessGate = new ReadinessGate();
		context.addServlet(new ServletHolder(new ReadinessServlet(readinessGate, metricsMgr, testConfig)), "/ready");
		context.addServlet(new ServletHolder(new NodeMetricsHandler(metricsMgr, testConfig)), "/metrics/json");
		context.addServlet(new ServletHolder(new FleetMetricsHandler(metricsMgr, testConfig)), "/metrics/fleet/json");
		server.start();
	}

	@Test
	void getConfigServletTest() throws Exception {
		final String expectedValue = "einon";

		testConfig.val("item-data-input-file", expectedValue);

		final String configString = resultFromServer(HOST + "/config");
		final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
		final JsonNode rootNode = yamlMapper.readTree(configString);

		final String actualValue = rootNode.path("item").path("data").path("input").path("file").asText();

		assertEquals(expectedValue, actualValue);
	}

	@Test
	void getLogServletTest() throws IOException, InterruptedException {
		final String expectedValue = "Base config";
		final String logString = resultFromServer(HOST + "/logs");
		final ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
		final JsonNode rootNode = yamlMapper.readTree(logString);

		final String actualValue = rootNode.path("Config").asText();

		assertEquals(expectedValue, actualValue);
	}

	@Test
	void getMetricsServletTest() throws IOException, InterruptedException {
		final String metricsString = resultFromServer(HOST + "/metrics");
		assertEquals("", metricsString); // if its an empty string, that means we did not get a 404 error, and it exists
	}

	@Test
	void getMetricsJsonServletTest() throws IOException, InterruptedException {
		final String metricsJsonString = resultFromServer(HOST + "/metrics/json");
		final ObjectMapper mapper = new ObjectMapper();
		final JsonNode arr = mapper.readTree(metricsJsonString);
		assertTrue(arr.isArray());
		assertEquals(1, arr.size());
		final JsonNode obj = arr.get(0);
		assertEquals(2, obj.get("metrics_schema").asInt());
		assertEquals("node", obj.get("scope").asText());
		assertTrue(obj.hasNonNull("sample_ts"));
		assertEquals("", obj.get("run_id").asText());
	}

	@Test
	void getHealthServletTest() throws IOException, InterruptedException {
		final String body = resultFromServer(HOST + "/health");
		final ObjectMapper mapper = new ObjectMapper();
		final JsonNode obj = mapper.readTree(body);
		assertEquals("ok", obj.get("status").asText());
		assertEquals("node", obj.get("scope").asText());
		assertTrue(obj.hasNonNull("node_id"));
		assertEquals("worker", obj.get("role").asText());
	}

	@Test
	void getReadyServletTest_beforeAndAfter() throws IOException, InterruptedException {
		// Initially not ready
		var resp = responseFromServer(HOST + "/ready");
		assertEquals(503, resp.statusCode());
		// Flip readiness
		readinessGate.setReady(true);
		resp = responseFromServer(HOST + "/ready");
		assertEquals(200, resp.statusCode());
		final ObjectMapper mapper = new ObjectMapper();
		final JsonNode obj = mapper.readTree(resp.body());
		assertTrue(obj.get("ready").asBoolean());
	}

	@Test
	void fleetEndpointAvailableByDefault() throws IOException, InterruptedException {
		final HttpResponse<String> response = responseFromServer(HOST + "/metrics/fleet/json");
		assertEquals(404, response.statusCode());
		assertTrue(response.body().contains("Fleet metrics unavailable"));
	}

	@Test
	void fleetEndpointDisabledWhenConfigured() throws Exception {
		final Config localConfig = TestConfigBuilder.config();
		localConfig.val("server-metrics-expose_fleet", false);
		final MetricsManager localMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
		final Server localServer = new Server(0);
		try {
			final var context = new ServletContextHandler();
			context.setContextPath("/");
			localServer.setHandler(context);
			context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
			context.addServlet(new ServletHolder(new NodeMetricsHandler(localMgr, localConfig)), "/metrics/json");
			if (localConfig.boolVal("server-metrics-expose_fleet")) {
				context.addServlet(new ServletHolder(new FleetMetricsHandler(localMgr, localConfig)), "/metrics/fleet/json");
			}
			localServer.start();
			final int port = ((ServerConnector) localServer.getConnectors()[0]).getLocalPort();
			final HttpResponse<String> response = responseFromServer("http://127.0.0.1:" + port + "/metrics/fleet/json");
			assertEquals(404, response.statusCode());
			assertFalse(response.body().contains("Fleet metrics unavailable"));
		} finally {
			localServer.stop();
			localMgr.close();
		}
	}

	@AfterEach
	public void tearDown() throws Exception {
		server.stop();
	}

	/* Utility Methods */
	private String resultFromServer(final String urlPath) throws IOException, InterruptedException {
		final HttpClient client = HttpClient.newHttpClient();
		final HttpRequest request = HttpRequest.newBuilder(URI.create(urlPath)).build();
		final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		return response.body();
	}

	private HttpResponse<String> responseFromServer(final String urlPath) throws IOException, InterruptedException {
		final HttpClient client = HttpClient.newHttpClient();
		final HttpRequest request = HttpRequest.newBuilder(URI.create(urlPath)).build();
		return client.send(request, HttpResponse.BodyHandlers.ofString());
	}
}
