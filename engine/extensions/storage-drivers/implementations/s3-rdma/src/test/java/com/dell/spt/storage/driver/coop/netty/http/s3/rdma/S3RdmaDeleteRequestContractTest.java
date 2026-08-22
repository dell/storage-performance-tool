package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Hardware-free contract coverage for standalone DELETE through the inherited HTTP path. */
final class S3RdmaDeleteRequestContractTest {

	private static final long RESULT_TIMEOUT_SECONDS = 5;
	private static final Credential CREDENTIAL = Credential.getInstance("access", "secret");

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;
	private volatile String multiDeleteResponse;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(
						new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(executor);
		server.start();
	}

	@AfterEach
	void stopServer() {
		if (server != null) {
			server.stop(0);
		}
		executor.shutdownNow();
	}

	@Test
	void unavailableRdmaUsesInheritedSingleDeleteForCurrentKey() throws Exception {
		final Config config = config();
		final var unavailableTransport = new FakeRdmaTransport(rdmaConfig(config)) {
			@Override
			public boolean init(
							final String endpoint, final String accessKey, final String secretKey) {
				setAvailable(false);
				return false;
			}
		};
		try (final var driver = newDriver(config, ignored -> unavailableTransport)) {
			assertTrue(driver.supportsStandaloneDeleteRequests());

			final DeleteRequestOperation result = execute(
							driver, operation(target("current key", null)));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyRequest("DELETE");
			assertEquals("/bucket/current%20key", request.rawPath());
			assertNull(request.rawQuery());
			assertNull(request.rdmaToken());
			assertEquals(0, unavailableTransport.getRegisterCount());
		}
	}

	@Test
	void availableRdmaUsesOneInheritedHttpBatchForOneOuterDeleteOperation() throws Exception {
		multiDeleteResponse = "<DeleteResult>"
						+ "<Deleted><Key>current</Key></Deleted>"
						+ "<Deleted><Key>exact</Key><VersionId>v+1/=</VersionId></Deleted>"
						+ "</DeleteResult>";
		final Config config = config();
		final var availableTransport = new FakeRdmaTransport(rdmaConfig(config));
		try (final var driver = newDriver(config, ignored -> availableTransport)) {
			final DeleteRequestOperation operation = operation(
							target("current", null), target("exact", "v+1/="));

			final DeleteRequestOperation result = execute(driver, operation);
			awaitCompletionAccounting(driver);

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			assertEquals(2, result.deleteResult().acceptedObjectCount());
			final CapturedRequest request = onlyRequest("POST");
			assertEquals("/bucket", request.rawPath());
			assertEquals("delete", request.rawQuery());
			assertTrue(request.body().contains("<Key>current</Key>"));
			assertTrue(request.body().contains("<Key>exact</Key><VersionId>v+1/=</VersionId>"));
			assertNull(request.rdmaToken());
			assertEquals(0, availableTransport.getRegisterCount());
			assertEquals(1, driver.scheduledOpCount());
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
			assertEquals(2, operation.deleteRequest().targets().size());
		}
	}

	@Test
	void availableRdmaUsesInheritedExactVersionSingleDelete() throws Exception {
		final Config config = config();
		final var availableTransport = new FakeRdmaTransport(rdmaConfig(config));
		try (final var driver = newDriver(config, ignored -> availableTransport)) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("exact/key", "v+1/=")));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyRequest("DELETE");
			assertEquals("/bucket/exact/key", request.rawPath());
			assertEquals("versionId=v%2B1%2F%3D", request.rawQuery());
			assertNull(request.rdmaToken());
			assertEquals(0, availableTransport.getRegisterCount());
		}
	}

	@Test
	void inheritedOrdinaryBuilderRejectsRepresentativeItemFallthrough() throws Exception {
		final Config config = config();
		final var availableTransport = new FakeRdmaTransport(rdmaConfig(config));
		try (final var driver = newDriver(config, ignored -> availableTransport)) {
			final DeleteRequestOperation operation = operation(
							target("first", null), target("second", null));

			assertThrows(IllegalArgumentException.class, () -> driver.buildOrdinary(operation));
			assertTrue(requests.isEmpty());
			assertEquals(0, availableTransport.getRegisterCount());
		}
	}

	private TestDriver newDriver(
					final Config config,
					final Function<RdmaConfig, RdmaTransport> transportFactory) throws Exception {
		return new TestDriver(config, transportFactory);
	}

	private static DeleteRequestOperation execute(
					final TestDriver driver, final DeleteRequestOperation operation) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final DeleteRequestOperation result = output.await();
		assertNotNull(result, "S3 RDMA inherited HTTP DELETE did not complete");
		return result;
	}

	private static void awaitCompletionAccounting(final TestDriver driver)
					throws InterruptedException {
		final long deadline = System.nanoTime()
						+ TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while ((driver.completedOpCount() != 1 || driver.activeOpCount() != 0)
						&& System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
	}

	private CapturedRequest onlyRequest(final String method) {
		final List<CapturedRequest> matching = requests.stream()
						.filter(request -> method.equals(request.method()))
						.toList();
		assertEquals(1, matching.size());
		assertEquals(
						1,
						requests.stream()
										.filter(request -> "DELETE".equals(request.method())
														|| "POST".equals(request.method()))
										.count());
		return matching.get(0);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		final byte[] requestBody = exchange.getRequestBody().readAllBytes();
		final CapturedRequest request = new CapturedRequest(
						exchange.getRequestMethod(),
						exchange.getRequestURI().getRawPath(),
						exchange.getRequestURI().getRawQuery(),
						exchange.getRequestHeaders().getFirst(S3RdmaStorageDriver.RDMA_TOKEN_HEADER),
						new String(requestBody, StandardCharsets.UTF_8));
		requests.add(request);
		if ("HEAD".equals(request.method())) {
			exchange.sendResponseHeaders(200, -1);
		} else if ("DELETE".equals(request.method())) {
			exchange.sendResponseHeaders(204, -1);
		} else if ("POST".equals(request.method()) && "delete".equals(request.rawQuery())) {
			final byte[] responseBody = multiDeleteResponse.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, responseBody.length);
			exchange.getResponseBody().write(responseBody);
		} else {
			exchange.sendResponseHeaders(500, -1);
		}
		exchange.close();
	}

	private static DeleteRequestOperation operation(final DeleteTarget... targets) {
		return new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", CREDENTIAL, List.of(targets)));
	}

	private static DeleteTarget target(final String key, final String versionId) {
		return new DeleteTarget(new IntegrityManifestDataItem("bucket", key, 0, versionId));
	}

	private static RdmaConfig rdmaConfig(final Config config) {
		return new RdmaConfig(config.configVal("storage").configVal("rdma"));
	}

	private Config config() {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(provider -> {
								try {
									return provider.schema();
								} catch (final Exception e) {
									throw new RuntimeException(e);
								}
							})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			configSchemas.add(0, InitialConfigSchemaProvider.provider().schema());
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			final Config config = new BasicConfig("-", TreeUtil.reduceForest(configSchemas));
			config.val("load-batch-size", 1024);
			config.val("storage-driver-limit-concurrency", 1);
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 8);
			config.val("storage-net-transport", "nio");
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl-enabled", false);
			config.val("storage-net-ssl-protocols", List.of());
			config.val("storage-net-ssl-provider", "OPENSSL");
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-writeSpinCount", 1);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 2_000);
			config.val("storage-net-ioRatio", 50);
			config.val("storage-net-node-addrs", List.of("127.0.0.1"));
			config.val("storage-net-node-port", server.getAddress().getPort());
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-net-http-headers", new HashMap<String, String>() {
				{
					put("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}");
				}
			});
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", Map.of());
			config.val("storage-object-fsAccess", true);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", false);
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
			config.val("storage-auth-version", 4);
			config.val("storage-checksum-enabled", false);
			config.val("storage-integrity-mode", "none");
			config.val("storage-integrity-algorithm", "sha256");
			config.val("storage-integrity-input-provenance", "none");
			config.val("storage-integrity-input-expectedProducerId", "");
			config.val("storage-integrity-selection-maxCount", 0L);
			config.val("storage-rdma-enabled", true);
			config.val("storage-rdma-thresholdBytes", 0L);
			config.val("storage-rdma-fallback", true);
			config.val("storage-rdma-device", "auto");
			config.val("storage-rdma-localIp", "");
			config.val("storage-rdma-logLevel", "WARN");
			config.val("storage-rdma-timeoutMs", 30_000L);
			return config;
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private record CapturedRequest(
					String method, String rawPath, String rawQuery, String rdmaToken, String body) {}

	private static final class TestDriver
					extends S3RdmaStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> {

		private TestDriver(
						final Config config,
						final Function<RdmaConfig, RdmaTransport> transportFactory) throws Exception {
			super(
							"s3-rdma-delete-contract",
							DataInput.instance(
											null, "7a42d9c483244167", new SizeInBytes("64KB"),
											16, false, 0.0, true),
							config.configVal("storage"),
							false,
							config.intVal("load-batch-size"),
							transportFactory);
		}

		private HttpRequest buildOrdinary(final DeleteRequestOperation operation) throws Exception {
			return ordinaryObjectRequest(operation, "127.0.0.1");
		}
	}

	private static final class ResultOutput implements Output<DeleteRequestOperation> {

		private final LinkedBlockingQueue<DeleteRequestOperation> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final DeleteRequestOperation value) {
			return results.offer(value);
		}

		@Override
		public int put(
						final List<DeleteRequestOperation> values, final int from, final int to) {
			int count = 0;
			for (int i = from; i < to; i++) {
				if (!results.offer(values.get(i))) {
					break;
				}
				count++;
			}
			return count;
		}

		@Override
		public int put(final List<DeleteRequestOperation> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			return null;
		}

		@Override
		public void close() {
			results.clear();
		}

		private DeleteRequestOperation await() throws InterruptedException {
			return results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		}
	}
}
