package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.logs.LogServlet;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OperationLifecycleExportTest {
	@ParameterizedTest
	@ValueSource(strings = {"create", "read"
	})
	@SuppressWarnings({"unchecked", "rawtypes"
	})
	void realStopPublishesAndServletFetchesTerminalEvidence(final String operation) throws Exception {
		final Config config = TestConfigBuilder.config();
		final String stepId = "lifecycle-" + System.nanoTime() + "-" + operation;
		config.val("load-step-id", stepId);
		config.val("run-id", 123L);
		config.val("load-op-type", operation);
		config.val("load-op-retry", false);
		config.val("load-op-wait-finish", false);
		config.val("load-op-wait-limit", 0);
		final var tracker = new OperationLifecycleTracker<DataOperationImpl<DataItemImpl>>();
		final LoadGenerator generator = mock(LoadGenerator.class);
		when(generator.isNothingPendingRetry()).thenReturn(true);
		final StorageDriver driver = mock(StorageDriver.class);
		when(driver.operationLifecycle()).thenReturn(tracker);
		final var context = new LoadStepContextImpl(stepId, generator, driver,
						mock(MetricsContext.class), config.configVal("load"), false);
		final var step = new Probe(config, context);
		final Path path = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId));
		final Server server = new Server(0);
		try {
			assertNull(context.terminalOperationCounters());
			step.startContexts();
			final var success = new DataOperationImpl<>(0, OpType.valueOf(operation.toUpperCase(java.util.Locale.ROOT)),
							new DataItemImpl("one", 0, 1), null, null, null, null, 0);
			tracker.generatorBuffered(success);
			tracker.dispatched(success);
			tracker.completionStarted(success);
			success.status(Operation.Status.SUCC);
			tracker.terminal(success);
			final var queued = new DataOperationImpl<>(0, OpType.CREATE, new DataItemImpl("queued", 0, 1), null, null, null, null, 0);
			tracker.generatorBuffered(queued);
			when(generator.recoverBufferedOperations()).thenReturn(List.of(queued));
			step.stopContexts();
			step.stopContexts();
			LogUtil.flushAll();
			final var rows = CSVFormat.RFC4180.builder().setHeader().get().parse(Files.newBufferedReader(path)).getRecords();
			assertEquals(1, rows.size(), "stop must publish once");
			assertEquals("true", rows.get(0).get("terminal"));
			assertEquals("2", rows.get(0).get("selected"));
			assertEquals("1", rows.get(0).get("accepted"));
			assertEquals("1", rows.get(0).get("unattempted"));
			assertEquals(stepId, rows.get(0).get("step_id"));
			final String original = Files.readString(path);
			assertThrows(IllegalStateException.class, () -> com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.publish(
							123, stepId, "replacement", List.of(tracker.counters()), true));
			assertEquals(original, Files.readString(path));
			final var handler = new ServletContextHandler();
			handler.setContextPath("/");
			handler.addServlet(new ServletHolder(new LogServlet()), "/logs/*");
			server.setHandler(handler);
			server.start();
			final int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
			try (final var client = HttpClient.newHttpClient()) {
				for (final String suffix : List.of("OperationLifecycle", "index.json")) {
					final var response = client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/logs/" + stepId + "/" + suffix)).GET().build(), HttpResponse.BodyHandlers.ofString());
					assertEquals(200, response.statusCode());
					assertTrue(response.body().contains(suffix.equals("index.json") ? "OperationLifecycle" : "selected,accepted,failed"));
				}
			}
		} finally {
			server.stop();
			step.close();
			Files.deleteIfExists(path);
		}
	}

	private static class Probe extends LoadStepLocalBase {
		Probe(final Config config, final LoadStepContext context) {
			super(config, List.of(), List.of(), mock(MetricsManager.class));
			stepContexts.add(context);
		}

		public String getTypeName() {
			return "lifecycle-test";
		}

		protected void init() {}

		void startContexts() {
			doStartWrapped();
		}

		void stopContexts() {
			doStop();
		}
	}
}
