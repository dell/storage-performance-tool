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
 * The servlet serves {@code metrics.steady.total.csv} under the logger name the CLI's fetcher
 * requests, {@code metrics.steady.FileTotal}.
 */
class LogServletSteadyArtifactTest {

	/** The name in the CLI's {@code ArtifactSpec} for the steady file (cli/internal/results/fetcher.go). */
	private static final String CLI_LOGGER_NAME = "metrics.steady.FileTotal";
	private static final String PATTERN_STEP_ID = "${ctx:step_id}";

	@AfterEach
	void clearContext() {
		ThreadContext.clearAll();
	}

	@Test
	void theEngineLoggerIsTheNameTheCliRequests() {
		assertEquals(CLI_LOGGER_NAME, Loggers.METRICS_STEADY_FILE_TOTAL.getName().substring(Loggers.BASE.length()));
	}

	@Test
	@SuppressWarnings("unchecked")
	void theServletMapsThatNameToTheSteadyFile() throws Exception {
		final var field = LogServlet.class.getDeclaredField("logFileNamePatternByName");
		field.setAccessible(true);
		final var patterns = (java.util.Map<String, String>) field.get(new LogServlet());
		final String pattern = patterns.get(CLI_LOGGER_NAME);
		assertTrue(pattern != null && pattern.endsWith("metrics.steady.total.csv"), "pattern: " + pattern);
		assertTrue(pattern.contains(PATTERN_STEP_ID), "the file lives in the step's log directory: " + pattern);
	}

	@Test
	void anAbsentSteadyFileIsNotFound(@TempDir final Path home) throws Exception {
		ThreadContext.put(Constants.KEY_HOME_DIR, home.toString());
		final HttpServletRequest req = mock(HttpServletRequest.class);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("HEAD");
		when(req.getRequestURI()).thenReturn("/logs/no-such-step/" + CLI_LOGGER_NAME);
		new LogServlet().doHead(req, resp);

		verify(resp, never()).setStatus(HttpStatus.OK_200);
	}
}
