package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A PATH LIST never reads the DATA ranges threshold, so a malformed one runs as it did in 5.17.0:
 * the run completes, and every driver it builds, the entry's item input driver too, is closed.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
final class S3MalformedThresholdListTest {

	private final List<String> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(executor);
		server.start();
	}

	@AfterEach
	void stopServer() {
		server.stop(0);
		executor.shutdownNow();
	}

	@Test
	void aListWithAMalformedThresholdCompletesAndClosesEveryDriver(@TempDir final Path tempDir) throws Exception {
		final String stepId = tempDir.getFileName() + "-list";
		final String scenario = "Load.config({"
						+ "load: {step: {id: \"" + stepId + "\"}, op: {type: \"list\", limit: {count: 1}}},"
						+ "item: {type: \"path\", input: {path: \"/bucket\"}, data: {ranges: {threshold: \"bogus\"}}}"
						+ "}).run();";
		final Config driverConfig = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		final Config defaults = new BundledDefaultsProvider().config("-", driverConfig.schema());
		final Config config = ConfigUtil.merge("-", List.of(defaults, driverConfig));
		config.val("run-id", 901L);
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		config.val("output-color", false);

		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> loaded = Extension.load(classLoader);
		final StorageDriverFactory s3 = (StorageDriverFactory) loaded.stream()
						.filter(extension -> "s3".equals(extension.id()))
						.findFirst()
						.orElseThrow();
		final var counting = new CountingFactory(s3);
		final List<Extension> extensions = new ArrayList<>();
		extensions.add(counting);
		extensions.addAll(loaded);
		final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			metrics.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("malformed threshold LIST", scenario, engine, 901L).run();
			final var terminal = metrics.getTerminalSteps().stream()
							.filter(entry -> stepId.equals(entry.stepId))
							.findFirst()
							.orElseThrow();
			assertEquals(OpType.LIST, terminal.opType);
			assertEquals(0, terminal.failedCount);
		}
		assertTrue(requests.stream().anyMatch(request -> request.startsWith("GET /bucket?")), requests::toString);
		assertFalse(counting.drivers.isEmpty());
		for (final StorageDriver driver : counting.drivers) {
			assertTrue(driver.isClosed(), "every driver the run built is closed: " + driver);
		}
	}

	private void handle(final HttpExchange exchange) throws IOException {
		exchange.getRequestBody().readAllBytes();
		final var uri = exchange.getRequestURI();
		requests.add(exchange.getRequestMethod() + " " + uri.getRawPath()
						+ (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery()));
		if ("GET".equals(exchange.getRequestMethod()) && uri.getRawQuery() != null
						&& uri.getRawQuery().contains("list-type=2")) {
			final byte[] body = ("<ListBucketResult><IsTruncated>false</IsTruncated>"
							+ "<Contents><Key>a</Key><Size>1</Size></Contents></ListBucketResult>")
							.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, body.length);
			exchange.getResponseBody().write(body);
		} else {
			exchange.sendResponseHeaders(200, -1);
		}
		exchange.close();
	}

	/** The s3 factory, found before the loaded one, that keeps every driver it builds. */
	private static final class CountingFactory implements StorageDriverFactory {

		private final StorageDriverFactory target;
		private final List<StorageDriver> drivers = new CopyOnWriteArrayList<>();

		CountingFactory(final StorageDriverFactory target) {
			this.target = target;
		}

		@Override
		public String id() {
			return target.id();
		}

		@Override
		public com.github.akurilov.confuse.SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}

		@Override
		public StorageDriver create(
						final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
						final int batchSize) throws com.dell.spt.base.config.IllegalConfigurationException, InterruptedException {
			final StorageDriver driver = target.create(stepId, dataInput, storageConfig, verifyFlag, batchSize);
			drivers.add(driver);
			return driver;
		}
	}
}
