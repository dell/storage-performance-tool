package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.ITEM_SIZE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Mongoose synonym {@code storage.net.http.versioning} on the {@code s3} test classpath, which has the HTTP
 * protocol schema but no Swift jar. Resolved on the step configuration, it gives the Netty S3 driver, and so
 * {@code s3-rdma} and {@code s3-tables} which extend it, the versioned behaviour {@code storage.object.versioning}
 * gives: {@code GET ?versioning} and, when it is off, {@code PUT ?versioning} on first use of a bucket, then
 * {@code key~versionId} item names.
 */
@Timeout(60)
final class S3HttpVersioningSynonymTest {

	private static final String SYNONYM = "storage-net-http-versioning";
	private static final String TARGET = "storage-object-versioning";
	private static final String VERSION_ID = "v000001";

	private record Captured(String method, String path, String query) {}

	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final List<AutoCloseable> resources = new ArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private final AtomicInteger port = new AtomicInteger();
	private HttpServer server;

	@AfterEach
	void tearDown() throws Exception {
		for (final var resource : resources) {
			resource.close();
		}
		if (server != null) {
			server.stop(0);
		}
		serverThreads.shutdownNow();
	}

	@Test
	void theTestClasspathDeclaresTheSynonymWithoutTheSwiftJar() {
		final var ids = Extension.load(Thread.currentThread().getContextClassLoader())
						.stream().map(Extension::id).collect(Collectors.toSet());
		assertFalse(ids.contains("swift"), ids::toString);

		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");

		assertEquals(boolean.class, leaf(config.schema(), "storage", "net", "http", "versioning"));
		assertEquals(boolean.class, leaf(config.schema(), "storage", "object", "versioning"));
	}

	@Test
	void theShippedDefaultsHaveBothKeysFalse(@TempDir final Path appHome) throws Exception {
		final List<Config> defaults = Extension.load(Thread.currentThread().getContextClassLoader())
						.stream()
						.peek(extension -> extension.install(appHome))
						.map(extension -> extension.defaults(appHome))
						.filter(Objects::nonNull)
						.collect(Collectors.toList());
		final Config merged = ConfigUtil.merge("-", defaults);

		assertFalse(merged.boolVal(SYNONYM));
		assertFalse(merged.boolVal(TARGET));
	}

	@Test
	void theSynonymSendsTheVersioningCheckAndReconciliation() throws Exception {
		final var driver = driver(true, false);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));

		assertEquals(List.of(
						new Captured("HEAD", "/bench-0", null),
						new Captured("GET", "/bench-0", "versioning"),
						new Captured("PUT", "/bench-0", "versioning")), requests);
	}

	@Test
	void theSynonymNamesWrittenItemsWithTheirVersionId() throws Exception {
		final var driver = driver(true, false);

		final var written = write(driver);

		assertEquals(Operation.Status.SUCC, written.status());
		assertEquals("/bench-0/k0~" + VERSION_ID, written.item().name());
	}

	@Test
	void theSynonymFalseKeepsTheUnversionedBehaviour() throws Exception {
		final var driver = driver(false, false);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));
		final var written = write(driver);

		assertEquals(List.of(new Captured("HEAD", "/bench-0", null)), requests.subList(0, 1));
		assertEquals(0, requests.stream().filter(r -> "versioning".equals(r.query())).count(), requests::toString);
		assertEquals("/bench-0/k0", written.item().name(), "plain names, although the target returns a version");
	}

	@Test
	void theSynonymFalseLeavesTheSptSettingOn() throws Exception {
		final var driver = driver(false, true);

		assertEquals("/bench-0/p", driver.requestNewPath("/bench-0/p"));

		assertEquals(2, requests.stream().filter(r -> "versioning".equals(r.query())).count(), requests::toString);
	}

	@SuppressWarnings("rawtypes")
	private S3StorageDriver driver(final boolean synonym, final boolean target) throws Exception {
		listen();
		final Config config = S3StorageDriverTest.baseConfig(target, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", 5_000);
		config.val("storage-net-node-port", port.get());
		config.val(SYNONYM, synonym);
		// What the step's constructor does before any driver exists.
		HttpVersioningSynonym.resolve(config, null);
		assertEquals(synonym || target, config.boolVal(TARGET));
		final var driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"http-versioning-synonym", new SeedDataInput(1, ITEM_SIZE, 1, true), config.configVal("storage"), false, 16);
		resources.add(driver);
		driver.start();
		return driver;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private Operation<?> write(final S3StorageDriver driver) throws Exception {
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((com.github.akurilov.commons.io.Output) results);
		final Operation<DataItem> op = new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("k0", 0, ITEM_SIZE), null, "/bench-0", CREDENTIAL, null, 0);
		assertTrue(driver.put(op));
		final var result = results.await();
		assertNotNull(result);
		return result;
	}

	@SuppressWarnings("unchecked")
	private static Object leaf(final Map<String, Object> schema, final String... path) {
		Object node = schema;
		for (final var key : path) {
			node = ((Map<String, Object>) node).get(key);
		}
		return node;
	}

	private void listen() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
		port.set(server.getAddress().getPort());
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			final var uri = exchange.getRequestURI();
			final var method = exchange.getRequestMethod();
			requests.add(new Captured(method, uri.getRawPath(), uri.getRawQuery()));
			if ("HEAD".equals(method)) {
				exchange.sendResponseHeaders(200, -1);
				return;
			}
			if ("PUT".equals(method)) {
				exchange.getResponseHeaders().set("ETag", "\"etag\"");
				if (uri.getRawQuery() == null) {
					// A target that keeps versions returns one for every object write.
					exchange.getResponseHeaders().set("x-amz-version-id", VERSION_ID);
				}
			}
			final byte[] body = "GET".equals(method)
							? "<VersioningConfiguration/>".getBytes(StandardCharsets.UTF_8)
							: new byte[0];
			exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
			if (body.length > 0) {
				exchange.getResponseBody().write(body);
			}
		}
	}
}
