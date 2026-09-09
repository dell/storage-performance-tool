package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.buildinfo.EngineBuildInfo;
import com.dell.spt.base.buildinfo.EngineBuildInfoJson;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.Test;

class VersionServletTest {

	@Test
	void getReturnsTheSharedSafeSchemaOneSnapshotAsJson() throws Exception {
		final var buildInfo = new EngineBuildInfo(
						1,
						"spt-engine",
						"5.14.2",
						"0123456789abcdef0123456789abcdef01234567",
						"2026-08-26T12:34:56Z",
						true,
						false);
		final var server = new Server(0);
		try {
			final var context = new ServletContextHandler();
			context.setContextPath("/");
			server.setHandler(context);
			context.addServlet(new ServletHolder(new VersionServlet(buildInfo)), "/version");
			server.start();

			final int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
			final var response = HttpClient.newHttpClient().send(
							HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/version")).GET().build(),
							HttpResponse.BodyHandlers.ofString());

			assertEquals(200, response.statusCode());
			assertEquals("application/json;charset=utf-8", response.headers().firstValue("content-type").orElseThrow());
			assertEquals(EngineBuildInfoJson.serialize(buildInfo), response.body());
		} finally {
			server.stop();
		}
	}
}
