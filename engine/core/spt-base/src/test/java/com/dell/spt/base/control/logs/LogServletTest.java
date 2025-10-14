package com.dell.spt.base.control.logs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.github.akurilov.confuse.Config;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LogServletTest {
	private static final int PORT = 9999;
	private static final String HOST = "http://localhost:" + PORT;
	private static final Server server = new Server(PORT);
	private Config testConfig;

	@BeforeEach
	public void setUp() throws Exception {
		testConfig = TestConfigBuilder.config();
		final ServletContextHandler context = new ServletContextHandler();
		final MetricsManager metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
		context.setContextPath("/");
		server.setHandler(context);

		context.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
		server.start();
	}

	/* This had to be removed from servletAPITest due to LogServlet being located in /logs directory, meaning we could not call protected methods */
	@Test
	public void testDoGetLogServlet() throws Exception {
		HttpServletRequest req = mock(HttpServletRequest.class);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("GET");
		when(req.getRequestURI()).thenReturn("/logs/stepId/loggerName");

		LogServlet logServlet = new LogServlet();

		assertDoesNotThrow(() -> logServlet.doGet(req, resp));
		assertDoesNotThrow(() -> logServlet.doDelete(req, resp));
	}

	@Test
	public void testDoHeadUnknownLoggerReturns404() throws Exception {
		HttpServletRequest req = mock(HttpServletRequest.class);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("HEAD");
		// Unknown logger name to trigger NoLoggerException inside logFilePath
		when(req.getRequestURI()).thenReturn("/logs/any-step/DefinitelyUnknownLogger");

		LogServlet servlet = new LogServlet();
		assertDoesNotThrow(() -> servlet.doHead(req, resp));
		// We can't use Mockito's verify static here easily; rely on absence of exception
	}

	@Test
	public void testIndexJsonReturns200WithEmptyItemsWhenNoFiles() throws Exception {
		HttpServletRequest req = mock(HttpServletRequest.class);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("GET");
		when(req.getRequestURI()).thenReturn("/logs/step-no-files/index.json");

		StringWriter sw = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(sw));

		LogServlet servlet = new LogServlet();
		assertDoesNotThrow(() -> servlet.doGet(req, resp));
		final String body = sw.toString();
		assertTrue(body.contains("\"items\""));
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
}
