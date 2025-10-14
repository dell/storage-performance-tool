package com.dell.spt.base.control;

import com.dell.spt.base.metrics.MetricsManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Readiness endpoint for orchestrators: returns 200 only when the API is ready.
 * Readiness becomes true when Main sets the shared gate after services start.
 * As a fallback, becomes ready if any metrics context exists (useful for worker-only runs).
 */
public final class ReadinessServlet extends HttpServlet {

	private final ReadinessGate gate;
	private final MetricsManager metricsManager;
	private final Config config;
	private final ObjectMapper mapper = new ObjectMapper();

	public ReadinessServlet(final ReadinessGate gate, final MetricsManager metricsManager, final Config config) {
		this.gate = gate;
		this.metricsManager = metricsManager;
		this.config = config;
	}

	@Override
	protected void doGet(final HttpServletRequest req, final HttpServletResponse resp) throws IOException {
		boolean ready = gate.isReady() || hasAnyMetrics();
		final ObjectNode root = mapper.createObjectNode();
		root.put("ready", ready);
		root.put("status", ready ? "ready" : "starting");
		root.put("scope", "node");
		root.put("role", metricsManager.getDistributedContexts().isEmpty() ? "worker" : "entry");
		root.put("node_id", resolveNodeId());
		final String clusterId = resolveClusterId();
		if (clusterId != null && !clusterId.isBlank()) {
			root.put("cluster_id", clusterId);
		}

		resp.setHeader("Access-Control-Allow-Origin", "*");
		resp.setHeader("Access-Control-Allow-Methods", "GET");
		resp.setContentType("application/json");
		resp.setStatus(ready ? HttpServletResponse.SC_OK : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
		resp.getWriter().write(root.toString());
	}

	private boolean hasAnyMetrics() {
		return !(metricsManager.getAllContexts().isEmpty() && metricsManager.getDistributedContexts().isEmpty());
	}

	private String resolveNodeId() {
		for (String path : List.of("run-node-id", "output-metrics-node-id", "run-comment")) {
			try {
				final String value = config.stringVal(path);
				if (value != null && !value.isBlank()) {
					return value;
				}
			} catch (Exception ignore) {}
		}
		try {
			final String host = InetAddress.getLocalHost().getHostName();
			int port = 0;
			try {
				port = config.intVal("run-port");
			} catch (Exception ignore) {}
			return port > 0 ? host + ':' + port : host;
		} catch (Exception ignore) {
			return "spt-node";
		}
	}

	private String resolveClusterId() {
		for (String path : List.of("run-cluster-id", "run-cluster")) {
			try {
				final String value = config.stringVal(path);
				if (value != null && !value.isBlank()) {
					return value;
				}
			} catch (Exception ignore) {}
		}
		return null;
	}
}
