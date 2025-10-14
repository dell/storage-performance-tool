package com.dell.spt.base.control.run;

import org.eclipse.jetty.http.HttpHeader;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for simple RunServlet static helpers to raise coverage
 * without instantiating the full servlet.
 */
public class RunServletHelpersTest {

	@Test
	void setRunTimestampHeaderSetsEtag() {
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		final Run run = mock(Run.class);
		when(run.runId()).thenReturn(12345L);

		RunServlet.setRunTimestampHeader(run, resp);
		verify(resp).setHeader(eq(HttpHeader.ETAG.name()), eq("12345"));
	}

	@Test
	void setRunMatchesResponseBranching() {
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		final Run run = mock(Run.class);
		when(run.runId()).thenReturn(10L);

		RunServlet.setRunMatchesResponse(run, resp, 10L);
		verify(resp).setStatus(eq(HttpServletResponse.SC_OK));

		RunServlet.setRunMatchesResponse(run, resp, 11L);
		verify(resp).setStatus(eq(HttpServletResponse.SC_NO_CONTENT));
	}
}
