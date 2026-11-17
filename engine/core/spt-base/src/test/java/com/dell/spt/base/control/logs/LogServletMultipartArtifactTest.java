package com.dell.spt.base.control.logs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A step's multipart log, {@code parts.upload.csv}, is listed in the step's {@code index.json}
 * under the logger name the CLI's fetcher requests, {@code Multipart}, and not under its file name.
 */
class LogServletMultipartArtifactTest {

	/** The name in the CLI's {@code ArtifactSpec} for the multipart log (cli/internal/results/fetcher.go). */
	private static final String CLI_LOGGER_NAME = "Multipart";

	private Path logFile;

	@AfterEach
	void cleanUp() throws Exception {
		ThreadContext.clearAll();
		if (logFile != null && Files.isDirectory(logFile.getParent())) {
			// The step directory is unique to this test's step ID.
			try (final var files = Files.walk(logFile.getParent())) {
				for (final Path p : files.sorted(Comparator.reverseOrder()).toList()) {
					Files.deleteIfExists(p);
				}
			}
		}
	}

	@Test
	void theEngineLoggerIsTheNameTheCliRequests() {
		assertEquals(CLI_LOGGER_NAME, Loggers.MULTIPART.getName().substring(Loggers.BASE.length()));
	}

	@Test
	void theStepIndexListsTheMultipartLogUnderItsLoggerName() throws Exception {
		final String stepId = "multipart-index-" + System.nanoTime();
		logFile = Path.of(FileManager.INSTANCE.logFileName(Loggers.MULTIPART.getName(), stepId));
		assertEquals("parts.upload.csv", logFile.getFileName().toString());
		ThreadContext.put(Constants.KEY_STEP_ID, stepId);
		Loggers.MULTIPART.info("INIT,{},{},{},{},{},{},{}", "bucket/item", "upload-1", -1, 1L, 10L, 5L, 0);
		ThreadContext.remove(Constants.KEY_STEP_ID);
		awaitWritten(logFile);

		final HttpServletRequest req = mock(HttpServletRequest.class);
		final HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("GET");
		when(req.getRequestURI()).thenReturn("/logs/" + stepId + "/index.json");
		final StringWriter body = new StringWriter();
		when(resp.getWriter()).thenReturn(new PrintWriter(body));
		new LogServlet().doGet(req, resp);

		final JsonNode index = new ObjectMapper().readTree(body.toString());
		final List<String> loggers = new ArrayList<>();
		JsonNode multipart = null;
		for (final JsonNode item : index.get("items")) {
			loggers.add(item.get("logger").asText());
			if (CLI_LOGGER_NAME.equals(item.get("logger").asText())) {
				multipart = item;
			}
		}
		assertNotNull(multipart, "index lists " + CLI_LOGGER_NAME + ": " + loggers);
		assertEquals("/logs/" + stepId + "/" + CLI_LOGGER_NAME, multipart.get("href").asText());
		assertEquals(Files.size(logFile), multipart.get("size").asLong());
		assertFalse(loggers.contains("parts.upload.csv"), "not listed again by file name: " + loggers);
	}

	/** The logger is asynchronous and a flush does not wait for its thread to take the event, so poll. */
	private static void awaitWritten(final Path file) throws Exception {
		for (int attempt = 0; attempt < 250; attempt++) {
			LogUtil.flushAll();
			if (Files.exists(file) && Files.size(file) > 0) {
				return;
			}
			Thread.sleep(20);
		}
		assertTrue(Files.exists(file) && Files.size(file) > 0, "the Multipart logger wrote " + file);
	}
}
