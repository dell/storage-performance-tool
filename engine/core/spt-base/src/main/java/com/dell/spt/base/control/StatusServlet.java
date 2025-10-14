package com.dell.spt.base.control;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Serves GET /status as JSON with a small, stable schema for clients.
 */
public final class StatusServlet extends HttpServlet {

	private static final DateTimeFormatter ISO8601 = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);

	private final ApiStatus apiStatus;
	private final ObjectMapper om = new ObjectMapper();

	public StatusServlet(final ApiStatus apiStatus) {
		this.apiStatus = apiStatus;
	}

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
		final ObjectNode root = om.createObjectNode();
		root.put("state", apiStatus.state().name());
		final long sinceMillis = apiStatus.sinceMillis();
		root.put("since", ISO8601.format(Instant.ofEpochMilli(sinceMillis)));

		final long runId = apiStatus.runId();
		if (runId != 0L) {
			root.put("run_id", runId);
		}
		final String stepId = apiStatus.stepId();
		if (stepId != null && !stepId.isEmpty()) {
			root.put("step_id", stepId);
		}

		resp.setContentType("application/json");
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.getWriter().write(om.writeValueAsString(root));
	}
}
