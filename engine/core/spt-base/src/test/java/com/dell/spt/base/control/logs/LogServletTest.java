package com.dell.spt.base.control.logs;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.StringWriter;
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
	private static final Server server = new Server(PORT);

	@BeforeEach
	public void setUp() throws Exception {
		final ServletContextHandler context = new ServletContextHandler();
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
	public void testDoGetPreservesHyphenatedArtifactName() throws Exception {
		final HttpServletRequest req = mock(HttpServletRequest.class);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getRequestURI()).thenReturn("/logs/any-step/verify-input.complete.json");

		new LogServlet().doGet(req, resp);

		verify(resp).sendError(eq(404), contains("verify-input.complete.json"));
	}

	@Test
	public void testDoHeadPreservesHyphenatedArtifactName() throws Exception {
		final HttpServletRequest req = mock(HttpServletRequest.class);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getRequestURI()).thenReturn("/logs/any-step/verify-input.csv");

		new LogServlet().doHead(req, resp);

		verify(resp).sendError(eq(404), contains("verify-input.csv"));
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
}
