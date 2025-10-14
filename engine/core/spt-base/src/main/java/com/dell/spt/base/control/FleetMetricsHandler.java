package com.dell.spt.base.control;

import com.dell.spt.base.metrics.MetricsManager;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public final class FleetMetricsHandler extends HttpServlet {

	private final MetricsJsonResponder responder;
	private final MetricsManager metricsManager;

	public FleetMetricsHandler(final MetricsManager metricsManager, final Config config) {
		this.metricsManager = metricsManager;
		this.responder = new MetricsJsonResponder(metricsManager, config);
	}

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
		if (metricsManager.getDistributedContexts().isEmpty()) {
			resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Fleet metrics unavailable on this node");
			return;
		}
		final boolean verbose = NodeMetricsHandler.isVerbose(req);
		final ArrayNode payload = responder.buildFleetMetrics(verbose);
		NodeMetricsHandler.writeJson(resp, payload);
	}
}
