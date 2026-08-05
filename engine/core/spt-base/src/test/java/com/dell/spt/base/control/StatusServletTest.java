package com.dell.spt.base.control;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StatusServletTest {

	@Test
	void returnsIdleJsonByDefault() throws Exception {
		ApiStatus status = new ApiStatus();
		StatusServlet servlet = new StatusServlet(status);

		HttpServletRequest req = mock(HttpServletRequest.class);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		StringWriter sw = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(sw));

		servlet.doGet(req, resp);

		verify(resp).setContentType("application/json");
		verify(resp).setStatus(HttpServletResponse.SC_OK);
		String json = sw.toString();
		assertTrue(json.contains("\"state\":\"IDLE\""), json);
		assertTrue(json.contains("\"since\":"), json);
		assertFalse(json.contains("run_id"), json);
		assertFalse(json.contains("step_id"), json);
	}

	@Test
	void includesRunAndStepWhenRunning() throws Exception {
		ApiStatus status = new ApiStatus();
		status.setRunning("abc-123", 9876L);
		StatusServlet servlet = new StatusServlet(status);

		HttpServletRequest req = mock(HttpServletRequest.class);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		StringWriter sw = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(sw));

		servlet.doGet(req, resp);
		String json = sw.toString();
		assertTrue(json.contains("\"state\":\"RUNNING\""), json);
		assertTrue(json.contains("\"run_id\":9876"), json);
		assertTrue(json.contains("\"step_id\":\"abc-123\""), json);
	}

	@Test
	void stoppedNotOverwrittenByCompleted() throws Exception {
		ApiStatus status = new ApiStatus();
		status.setRunning("s", 1L);
		status.setStopped();
		status.completeIfNotStopped();

		StatusServlet servlet = new StatusServlet(status);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		StringWriter sw = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(sw));

		servlet.doGet(mock(HttpServletRequest.class), resp);
		String json = sw.toString();
		assertTrue(json.contains("\"state\":\"STOPPED\""), json);
	}

	@Test
	void includesStructuredFailureFields() throws Exception {
		final var status = new ApiStatus();
		status.setRunning("read-verify", 42L);
		status.setFailed(
						"read-verify", IntegrityTerminalException.Category.AGGREGATION, "merge failed");
		final var servlet = new StatusServlet(status);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		final var output = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(output));

		servlet.doGet(mock(HttpServletRequest.class), resp);

		final String json = output.toString();
		assertTrue(json.contains("\"state\":\"FAILED\""), json);
		assertTrue(json.contains("\"run_id\":42"), json);
		assertTrue(json.contains("\"step_id\":\"read-verify\""), json);
		assertTrue(json.contains("\"category\":\"aggregation\""), json);
		assertTrue(json.contains("\"message\":\"merge failed\""), json);
	}
}
