package com.dell.spt.integration;

import static com.dell.spt.base.Constants.DIR_EXT;
import static com.dell.spt.base.Constants.MIB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.AddCorsHeadersRule;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.ConfigServlet;
import com.dell.spt.base.control.FleetMetricsHandler;
import com.dell.spt.base.control.NodeMetricsHandler;
import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.control.run.RunServlet;
import com.dell.spt.base.env.CoreResourcesToInstall;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.svc.Service;

import io.prometheus.client.exporter.MetricsServlet;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

import javax.servlet.MultipartConfigElement;

import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.dell.spt.testing.tags.IntegrationTest;

@DisplayName("Run servlet readiness and early-POST behavior")
@IntegrationTest
public class RunReadinessIntegrationTest {

	private final CoreResourcesToInstall coreResources = new CoreResourcesToInstall();
	private final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
	private Path appHomePath;
	private List<Extension> extensions;
	private ClassLoader extClsLoader;

	@BeforeEach
	void setUp() {
		appHomePath = coreResources.appHomePath();
		coreResources.install(appHomePath);
		extClsLoader = Extension.extClassLoader(Paths.get(appHomePath.toString(), DIR_EXT).toFile());
		extensions = Extension.load(extClsLoader);
	}

	@AfterEach
	void tearDown() throws Exception {
		// nothing to clean explicitly here
	}

	@Test
	@DisplayName("HEAD /run returns 204 immediately; POST works once services start")
	void runEndpointReadyBeforeServiceStart() throws Exception {
		final var config = TestConfigBuilder.config();

		// Pick an ephemeral HTTP port for Jetty
		final int httpPort = 0; // Jetty will select a free port

		// Prepare Jetty with all servlets
		final var server = new Server(httpPort);
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		final var addCorsHeaderHandler = new RewriteHandler();
		addCorsHeaderHandler.addRule(new AddCorsHeadersRule());
		server.insertHandler(addCorsHeaderHandler);
		context.addServlet(new ServletHolder(new ConfigServlet(config)), "/config/*");
		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		context.addServlet(new ServletHolder(new NodeMetricsHandler(metricsMgr, config)), "/metrics/json");
		context.addServlet(new ServletHolder(new FleetMetricsHandler(metricsMgr, config)), "/metrics/fleet/json");

		// Create services but DO NOT start yet
		final int listenPort = config.intVal("load-step-node-port");
		try (final Service fileMgrSvc = new FileManagerServiceImpl(listenPort);
						final LoadStepManagerService scenarioStepSvc = new LoadStepManagerServiceImpl(listenPort, extensions, metricsMgr)) {

			// Register /run BEFORE server.start()
			final var runServletHolder = new ServletHolder(
							new RunServlet(extClsLoader, extensions, metricsMgr, config, appHomePath, scenarioStepSvc, new ApiStatus()));
			context.addServlet(runServletHolder, "/run");
			runServletHolder.getRegistration()
							.setMultipartConfig(new MultipartConfigElement("", 16 * MIB, 16 * MIB, 16 * MIB));

			server.start();
			final int actualPort = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
			final String base = "http://127.0.0.1:" + actualPort;

			final var client = HttpClient.newBuilder()
							.connectTimeout(Duration.ofSeconds(3))
							.build();

			// Immediately check readiness of /run
			final var headReq = HttpRequest.newBuilder(URI.create(base + "/run"))
							.method("HEAD", HttpRequest.BodyPublishers.noBody())
							.timeout(Duration.ofSeconds(3))
							.build();
			final var headResp = client.send(headReq, HttpResponse.BodyHandlers.discarding());
			assertEquals(204, headResp.statusCode(), "HEAD /run should be 204 (idle) immediately after start");

			// Metrics should be available already
			final var metricsReq = HttpRequest.newBuilder(URI.create(base + "/metrics"))
							.GET().timeout(Duration.ofSeconds(3)).build();
			final var metricsResp = client.send(metricsReq, HttpResponse.BodyHandlers.ofString());
			assertEquals(200, metricsResp.statusCode(), "/metrics should be 200 after server start");

			// Start services now; POST should work immediately without 404/405/409
			fileMgrSvc.start();
			scenarioStepSvc.start();

			final var postReq = HttpRequest.newBuilder(URI.create(base + "/run"))
							.POST(HttpRequest.BodyPublishers.noBody())
							.timeout(Duration.ofSeconds(10))
							.build();
			final var postResp = client.send(postReq, HttpResponse.BodyHandlers.discarding());
			assertEquals(202, postResp.statusCode(), "POST /run should be accepted once services start");

			// Stop the run promptly using DELETE with If-Match from ETag
			final var runId = postResp.headers().firstValue("ETag").orElse(null);
			if (runId != null) {
				final var delReq = HttpRequest.newBuilder(URI.create(base + "/run"))
								.header("If-Match", runId)
								.DELETE()
								.timeout(Duration.ofSeconds(5))
								.build();
				final var delResp = client.send(delReq, HttpResponse.BodyHandlers.discarding());
				assertTrue(delResp.statusCode() == 200 || delResp.statusCode() == 204,
								"DELETE /run should succeed (200/204), was: " + delResp.statusCode());
			}

			server.stop();
		} finally {
			server.destroy();
		}
	}
}
