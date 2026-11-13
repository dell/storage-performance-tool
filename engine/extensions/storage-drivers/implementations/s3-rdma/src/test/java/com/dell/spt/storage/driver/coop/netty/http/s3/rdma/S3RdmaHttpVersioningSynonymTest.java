package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

/**
 * The Mongoose synonym {@code storage.net.http.versioning} with {@code s3-rdma}, which inherits the S3 driver's
 * versioning. With RDMA off the driver writes over HTTP, so this runs without RDMA hardware. The bucket check runs
 * when a path is first used, before the driver chooses RDMA or HTTP for the request.
 */
@Timeout(60)
final class S3RdmaHttpVersioningSynonymTest {

	private static final String SYNONYM = "storage-net-http-versioning";
	private static final String TARGET = "storage-object-versioning";
	private static final String VERSION_ID = "v000001";
	private static final Credential CREDENTIAL = Credential.getInstance("user1", "test-secret");
	private static final RdmaConfig RDMA_OFF = new RdmaConfig(false, 0, false, "auto", "", "WARN");

	private record Captured(String method, String path, String query) {}

	private final List<Captured> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService serverThreads = Executors.newCachedThreadPool();
	private HttpServer server;
	private S3RdmaStorageDriver<Item, Operation<Item>> driver;

	@AfterEach
	void tearDown() throws Exception {
		if (driver != null) {
			driver.close();
		}
		if (server != null) {
			server.stop(0);
		}
		serverThreads.shutdownNow();
	}

	@Test
	void theSynonymChecksTheBucketAndNamesItemsWithTheirVersionId() throws Exception {
		final Operation<Item> written = write(driver(true));

		assertEquals(List.of(
						new Captured("HEAD", "/bench-0", null),
						new Captured("GET", "/bench-0", "versioning"),
						new Captured("PUT", "/bench-0", "versioning"),
						new Captured("PUT", "/bench-0/k0", null)), requests);
		assertEquals(Operation.Status.SUCC, written.status());
		assertEquals("/bench-0/k0~" + VERSION_ID, written.item().name());
	}

	@Test
	void theSynonymFalseKeepsTheUnversionedBehaviour() throws Exception {
		final Operation<Item> written = write(driver(false));

		assertEquals(List.of(
						new Captured("HEAD", "/bench-0", null),
						new Captured("PUT", "/bench-0/k0", null)), requests);
		assertEquals("/bench-0/k0", written.item().name(), "plain names, although the target returns a version");
	}

	private S3RdmaStorageDriver<Item, Operation<Item>> driver(final boolean synonym) throws Exception {
		listen();
		final Config config = S3RdmaStorageDriverTestSupport.config(
						RDMA_OFF, "/bench-0", List.of("127.0.0.1"), server.getAddress().getPort());
		config.val("storage-driver-type", "s3-rdma");
		config.val(SYNONYM, synonym);
		// What the step's constructor does before any driver exists.
		HttpVersioningSynonym.resolve(config, null);
		assertEquals(synonym, config.boolVal(TARGET));
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
		driver = new S3RdmaStorageDriver<>(
						"http-versioning-synonym",
						dataInput,
						config.configVal("storage"),
						false,
						config.intVal("load-batch-size"),
						FakeRdmaTransport::new);
		return driver;
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> write(final S3RdmaStorageDriver<Item, Operation<Item>> driver) throws Exception {
		final Output<Operation<Item>> results = mock(Output.class);
		driver.operationResultOutput(results);
		driver.start();
		final Operation<Item> op = (Operation<Item>) (Operation<?>) new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("k0", 0, 1024), null, "/bench-0", CREDENTIAL, null, 0);
		assertTrue(driver.put(op));
		final ArgumentCaptor<Operation<Item>> result = ArgumentCaptor.forClass(Operation.class);
		verify(results, timeout(10_000)).put(result.capture());
		verify(results, timeout(1_000).times(1)).put(any(Operation.class));
		return result.getValue();
	}

	private void listen() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(serverThreads);
		server.start();
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
