package com.dell.spt.base.control.logs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.logging.Loggers;
import java.nio.file.Path;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.ThreadContext;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The servlet serves {@code event.loops.json} under the logger name the CLI's fetcher
 * requests, {@code EventLoops}.
 */
class LogServletEventLoopsArtifactTest {

	/** The name in the CLI's {@code ArtifactSpec} for the event-loop report (cli/internal/results/fetcher.go). */
	private static final String CLI_LOGGER_NAME = "EventLoops";
	private static final String PATTERN_STEP_ID = "${ctx:step_id}";

	@AfterEach
	void clearContext() {
		ThreadContext.clearAll();
	}

	@Test
	void theEngineLoggerIsTheNameTheCliRequests() {
		assertEquals(CLI_LOGGER_NAME, Loggers.EVENT_LOOPS.getName().substring(Loggers.BASE.length()));
	}

	@Test
	@SuppressWarnings("unchecked")
	void theServletMapsThatNameToTheReport() throws Exception {
		final var field = LogServlet.class.getDeclaredField("logFileNamePatternByName");
		field.setAccessible(true);
		final var patterns = (java.util.Map<String, String>) field.get(new LogServlet());
		final String pattern = patterns.get(CLI_LOGGER_NAME);
		assertTrue(pattern != null && pattern.endsWith("event.loops.json"), "pattern: " + pattern);
		assertTrue(pattern.contains(PATTERN_STEP_ID), "the file lives in the step's log directory: " + pattern);
	}

	@Test
	void anAbsentReportIsNotFound(@TempDir final Path home) throws Exception {
		ThreadContext.put(Constants.KEY_HOME_DIR, home.toString());
		final HttpServletRequest req = mock(HttpServletRequest.class);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("HEAD");
		when(req.getRequestURI()).thenReturn("/logs/no-such-step/" + CLI_LOGGER_NAME);
		new LogServlet().doHead(req, resp);

		verify(resp, never()).setStatus(HttpStatus.OK_200);
		verify(resp).sendError(HttpStatus.NOT_FOUND_404);
	}
}
