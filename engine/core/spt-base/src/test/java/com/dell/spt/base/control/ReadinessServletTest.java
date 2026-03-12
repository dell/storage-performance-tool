package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.params.ItemSize;
import com.github.akurilov.confuse.Config;
import io.prometheus.client.exporter.MetricsServlet;
import java.net.HttpURLConnection;
import java.net.URL;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ReadinessServletTest {

	private Server server;
	private MetricsManager metricsMgr;
	private ReadinessGate gate;
	private Config config;
	private int port;

	@BeforeEach
	public void setUp() throws Exception {
		metricsMgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		config = TestConfigBuilder.config();
		config.val("run-comment", "ready-node");
		config.val("run-id", 1L);
		gate = new ReadinessGate();
		server = new Server(0);
		final var ctx = new ServletContextHandler();
		ctx.setContextPath("/");
		server.setHandler(ctx);
		ctx.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
		ctx.addServlet(new ServletHolder(new ReadinessServlet(gate, metricsMgr, config)), "/ready");
		server.start();
		port = ((org.eclipse.jetty.server.ServerConnector) server.getConnectors()[0]).getLocalPort();
	}

	@AfterEach
	public void tearDown() throws Exception {
		if (server != null)
			server.stop();
	}

	@Test
	public void returns503UntilReadyThen200() throws Exception {
		int code = fetchStatus("http://127.0.0.1:" + port + "/ready");
		assertEquals(503, code);
		gate.setReady(true);
		code = fetchStatus("http://127.0.0.1:" + port + "/ready");
		assertEquals(200, code);
	}

	@Test
	public void fallbackToMetricsPresence() throws Exception {
		// With gate false, register a local metrics context and ensure /ready becomes 200
		final MetricsContext ctx = MetricsContextImpl.builder()
						.loadStepId("rdy")
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(ItemSize.SMALL.getValue())
						.outputPeriodSec(0)
						.stdOutColorFlag(false)
						.comment("rdy")
						.runId(1)
						.build();
		ctx.start();
		metricsMgr.register(ctx);
		int code = fetchStatus("http://127.0.0.1:" + port + "/ready");
		assertEquals(200, code);
	}

	private static int fetchStatus(final String url) throws Exception {
		final var u = new URL(url);
		final var conn = (HttpURLConnection) u.openConnection();
		conn.setRequestMethod("GET");
		final int status = conn.getResponseCode();
		conn.disconnect();
		return status;
	}
}
