package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.control.AddCorsHeadersRule;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.ShutdownServlet;
import com.dell.spt.base.control.StatusServlet;
import com.dell.spt.base.svc.ServiceBase;
import com.dell.spt.testing.tags.IntegrationTest;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("/shutdown: returns 202 and closes services; status lingers briefly")
@IntegrationTest
public class NodeShutdownIntegrationTest {

	static class LatchingService extends ServiceBase {
		private final CountDownLatch closed = new CountDownLatch(1);

		LatchingService() {
			super(0);
		}

		@Override
		public String name() {
			return "test";
		}

		@Override
		protected void doStop() {
			super.doStop();
			closed.countDown();
		}

		boolean awaitClosed(long t, TimeUnit u) throws InterruptedException {
			return closed.await(t, u);
		}
	}

	private Server server;
	private ApiStatus apiStatus;
	private LatchingService svc1, svc2;

	@BeforeEach
	void setUp() throws Exception {
		server = new Server(0);
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		final var addCorsHeaderHandler = new RewriteHandler();
		addCorsHeaderHandler.addRule(new AddCorsHeadersRule());
		server.insertHandler(addCorsHeaderHandler);

		apiStatus = new ApiStatus();
		apiStatus.setIdle();
		context.addServlet(new ServletHolder(new StatusServlet(apiStatus)), "/status");

		svc1 = new LatchingService();
		svc2 = new LatchingService();
		context.addServlet(new ServletHolder(new ShutdownServlet(List.of(svc1, svc2))), "/shutdown");

		server.start();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (server != null)
			server.stop();
	}

	@Test
	void shutdownClosesServicesAndStatusLingersBriefly() throws Exception {
		int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
		final String base = "http://127.0.0.1:" + port;
		final var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

		// Status available
		var statusResp = client.send(HttpRequest.newBuilder(URI.create(base + "/status")).GET().build(), HttpResponse.BodyHandlers.ofString());
		assertEquals(200, statusResp.statusCode());

		// Request shutdown
		var shutResp = client.send(HttpRequest.newBuilder(URI.create(base + "/shutdown")).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
		assertEquals(202, shutResp.statusCode());

		// Status should still be available briefly
		statusResp = client.send(HttpRequest.newBuilder(URI.create(base + "/status")).GET().build(), HttpResponse.BodyHandlers.ofString());
		assertEquals(200, statusResp.statusCode());

		// Services should close
		assertTrue(svc1.awaitClosed(2, TimeUnit.SECONDS));
		assertTrue(svc2.awaitClosed(2, TimeUnit.SECONDS));
	}
}
