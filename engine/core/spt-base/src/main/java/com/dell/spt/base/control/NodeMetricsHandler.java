package com.dell.spt.base.control;

import com.dell.spt.base.metrics.MetricsManager;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public final class NodeMetricsHandler extends HttpServlet {

	private final MetricsJsonResponder responder;

	public NodeMetricsHandler(final MetricsManager metricsManager, final Config config) {
		this.responder = new MetricsJsonResponder(metricsManager, config);
	}

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
					throws ServletException, IOException {
		final boolean verbose = isVerbose(req);
		final ArrayNode payload = responder.buildNodeMetrics(verbose);
		writeJson(resp, payload);
	}

	static boolean isVerbose(final HttpServletRequest req) {
		final String verboseParam = req.getParameter("verbose");
		return "1".equals(verboseParam) || (verboseParam != null && Boolean.parseBoolean(verboseParam));
	}

	static void writeJson(final HttpServletResponse resp, final ArrayNode payload)
					throws IOException {
		resp.setHeader("Access-Control-Allow-Origin", "*");
		resp.setHeader("Access-Control-Allow-Methods", "GET");
		resp.setContentType("application/json");
		resp.getWriter().write(payload.toString());
	}
}
