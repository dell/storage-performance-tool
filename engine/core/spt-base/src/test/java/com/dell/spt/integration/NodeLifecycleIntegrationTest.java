package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.AddCorsHeadersRule;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.FleetMetricsHandler;
import com.dell.spt.base.control.NodeMetricsHandler;
import com.dell.spt.base.control.StatusServlet;
import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.control.run.RunServlet;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.svc.Service;
import com.github.akurilov.confuse.Config;
import io.prometheus.client.exporter.MetricsServlet;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
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

@DisplayName("Node lifecycle: /status + /run transitions")
@IntegrationTest
public class NodeLifecycleIntegrationTest {

	private Server server;
	private Service fileMgrSvc;
	private LoadStepManagerService scenarioStepSvc;
	private MetricsManager metricsMgr;
	private ApiStatus apiStatus;

	@BeforeEach
	void setUp() throws Exception {
		metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final Config config = TestConfigBuilder.config();

		server = new Server(0); // ephemeral HTTP port
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		final var addCorsHeaderHandler = new RewriteHandler();
		addCorsHeaderHandler.addRule(new AddCorsHeadersRule());
		server.insertHandler(addCorsHeaderHandler);

		// Basic endpoints
		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		context.addServlet(new ServletHolder(new NodeMetricsHandler(metricsMgr, config)), "/metrics/json");
		context.addServlet(new ServletHolder(new FleetMetricsHandler(metricsMgr, config)), "/metrics/fleet/json");

		// Services
		final int listenPort = config.intVal("load-step-node-port");
		fileMgrSvc = new FileManagerServiceImpl(listenPort);
		scenarioStepSvc = new LoadStepManagerServiceImpl(listenPort, List.<Extension> of(), metricsMgr);

		// /status + /run
		apiStatus = new ApiStatus();
		apiStatus.setIdle();
		context.addServlet(new ServletHolder(new StatusServlet(apiStatus)), "/status");
		final var runServletHolder = new ServletHolder(new RunServlet(
						null, List.of(), metricsMgr, config, Path.of("."), scenarioStepSvc, apiStatus));
		context.addServlet(runServletHolder, "/run");
		runServletHolder.getRegistration().setMultipartConfig(new MultipartConfigElement(""));

		server.start();
		fileMgrSvc.start();
		scenarioStepSvc.start();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (scenarioStepSvc != null)
			scenarioStepSvc.close();
		if (fileMgrSvc != null)
			fileMgrSvc.close();
		if (server != null)
			server.stop();
	}

	@Test
	void statusTransitionsWithRunLifecycle() throws Exception {
		int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
		final String base = "http://127.0.0.1:" + port;
		final var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

		// Initially IDLE
		var getStatus = HttpRequest.newBuilder(URI.create(base + "/status")).GET().build();
		var statusResp = client.send(getStatus, HttpResponse.BodyHandlers.ofString());
		assertEquals(200, statusResp.statusCode());
		assertTrue(statusResp.body().contains("\"IDLE\""));

		// Start run
		var postRun = HttpRequest.newBuilder(URI.create(base + "/run")).POST(HttpRequest.BodyPublishers.noBody()).build();
		var postResp = client.send(postRun, HttpResponse.BodyHandlers.discarding());
		assertEquals(202, postResp.statusCode());
		var runId = postResp.headers().firstValue("ETag").orElse(null);
		assertNotNull(runId);

		// Status should be RUNNING and include run_id
		statusResp = client.send(getStatus, HttpResponse.BodyHandlers.ofString());
		assertEquals(200, statusResp.statusCode());
		assertTrue(statusResp.body().contains("\"RUNNING\""));
		assertTrue(statusResp.body().contains("run_id"));

		// Stop run
		var delRun = HttpRequest.newBuilder(URI.create(base + "/run"))
						.header("If-Match", runId).DELETE().build();
		var delResp = client.send(delRun, HttpResponse.BodyHandlers.discarding());
		assertEquals(200, delResp.statusCode());

		// STOPPED is published only after the interrupted run finishes cleanup.
		awaitStatus(client, getStatus, "STOPPED", Duration.ofSeconds(2));
	}

	private static void awaitStatus(
					final HttpClient client,
					final HttpRequest request,
					final String expectedState,
					final Duration timeout)
					throws Exception {
		final long deadlineNanos = System.nanoTime() + timeout.toNanos();
		HttpResponse<String> response;
		do {
			response = client.send(request, HttpResponse.BodyHandlers.ofString());
			if (response.statusCode() == 200
							&& response.body().contains("\"" + expectedState + "\"")) {
				return;
			}
			Thread.sleep(50);
		} while (System.nanoTime() < deadlineNanos);
		fail("Status did not become " + expectedState + "; last response=" + response);
	}
}
