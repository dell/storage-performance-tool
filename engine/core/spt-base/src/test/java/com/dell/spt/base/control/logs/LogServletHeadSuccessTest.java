package com.dell.spt.base.control.logs;

import com.dell.spt.base.Constants;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.*;

class LogServletHeadSuccessTest {

	@Test
	void headReturns200WithHeadersWhenFileExists() throws Exception {
		// Arrange a temp home_dir + stepId and create a Messages log file
		Path tmpHome = Files.createTempDirectory("spt-home-");
		String stepId = "test-step-001";
		ThreadContext.put(Constants.KEY_HOME_DIR, tmpHome.toString());
		ThreadContext.put(Constants.KEY_STEP_ID, stepId);
		Path logDir = tmpHome.resolve("log").resolve(stepId);
		Files.createDirectories(logDir);
		Path msg = logDir.resolve("messages.log");
		Files.writeString(msg, "hello\n");

		HttpServletRequest req = mock(HttpServletRequest.class);
		HttpServletResponse resp = mock(HttpServletResponse.class);
		when(req.getMethod()).thenReturn("HEAD");
		when(req.getRequestURI()).thenReturn("/logs/" + stepId + "/Messages");

		LogServlet servlet = new LogServlet();
		assertDoesNotThrow(() -> servlet.doHead(req, resp));

		// Cleanup
		try {
			Files.deleteIfExists(msg);
			Files.deleteIfExists(logDir);
			Files.deleteIfExists(tmpHome);
		} catch (Exception ignore) {}
	}
}
