package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.control.AddCorsHeadersRule;
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

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@DisplayName("/logs/<stepId>/index.json returns 200 and JSON body")
@IntegrationTest
public class LogsIndexIntegrationTest {

	private Server server;

	@BeforeEach
	void setUp() throws Exception {
		server = new Server(0);
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		final var addCorsHeaderHandler = new RewriteHandler();
		addCorsHeaderHandler.addRule(new AddCorsHeadersRule());
		server.insertHandler(addCorsHeaderHandler);
		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		server.start();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (server != null)
			server.stop();
	}

	@Test
	void indexJsonBasic() throws Exception {
		int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
		final var client = HttpClient.newHttpClient();
		final var req = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/logs/any/index.json"))
						.GET().build();
		final var resp = client.send(req, HttpResponse.BodyHandlers.ofString());
		assertEquals(200, resp.statusCode());
		assertTrue(resp.body().contains("\"step_id\""));
		assertTrue(resp.body().contains("\"items\""));
	}
}
