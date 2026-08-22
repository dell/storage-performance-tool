package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.operation;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.github.akurilov.commons.io.Input;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Real-driver canary backed only by a deterministic loopback S3 protocol endpoint. */
final class S3DeleteRequestIntegrationTest {

	private static final long RESULT_TIMEOUT_SECONDS = 5;

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;
	private volatile String multiDeleteResponse;
	private volatile int multiDeleteStatus = 200;

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
	void currentKeyDeleteRunsThroughRealNettyDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(driver, operation(target("current key", null)));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyDeleteRequest();
			assertEquals("DELETE", request.method());
			assertEquals("/bucket/current%20key", request.rawPath());
			assertNull(request.rawQuery());
			assertTrue(request.authorization().startsWith(S3Api.AUTH_V4_PREFIX));
		}
	}

	@Test
	void exactVersionDeleteRunsThroughRealNettyDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("exact/key", "v+1/=")));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyDeleteRequest();
			assertEquals("/bucket/exact/key", request.rawPath());
			assertEquals("versionId=v%2B1%2F%3D", request.rawQuery());
		}
	}

	@Test
	void batchDeleteOwnsOneRequestPermitAndTimingSample() throws Exception {
		multiDeleteResponse = "<DeleteResult>"
						+ "<Deleted><Key>one</Key></Deleted>"
						+ "<Deleted><Key>two</Key><VersionId>version-2</VersionId></Deleted>"
						+ "</DeleteResult>";
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", "version-2")));
			awaitCompletionAccounting(driver);

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method())).count());
			assertEquals(1, driver.scheduledOpCount());
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
			assertTrue(result.reqTimeStart() > 0);
			assertTrue(result.reqTimeDone() >= result.reqTimeStart());
			assertTrue(result.respTimeStart() >= result.reqTimeDone());
			assertTrue(result.duration() > 0);
		}
	}

	private static void awaitCompletionAccounting(
					final S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while ((driver.completedOpCount() != 1 || driver.activeOpCount() != 0)
						&& System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
	}

	@Test
	void partialBatchErrorRunsThroughRealNettyDriver() throws Exception {
		multiDeleteResponse = "<DeleteResult>"
						+ "<Deleted><Key>accepted</Key></Deleted>"
						+ "<Error><Key>denied</Key><VersionId>version-3</VersionId>"
						+ "<Code>AccessDenied</Code><Message>denied</Message></Error>"
						+ "</DeleteResult>";
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("accepted", null), target("denied", "version-3")));

			assertEquals(DeleteRequestOutcome.PARTIAL, result.deleteResult().outcome());
			assertEquals(1, result.deleteResult().acceptedObjectCount());
			assertEquals(1, result.deleteResult().failedObjectCount());
			assertEquals(1, requests.stream().filter(request -> "POST".equals(request.method())).count());
		}
	}

	@Test
	void serviceFailureRunsThroughSharedConservativePolicy() throws Exception {
		multiDeleteStatus = 503;
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", "version-2")));

			assertEquals(DeleteRequestOutcome.FAILED, result.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.OPERATIONAL,
							result.deleteResult().failureClassification());
			assertEquals(2, result.deleteResult().failedObjectCount());
		}
	}

	private S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> newDriver()
					throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-driver-limit-queue-input", 8);
		config.val("storage-net-node-port", server.getAddress().getPort());
		config.val("storage-net-timeoutMilliSec", 2_000);
		final var driver = new S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation>(
						"delete-integration-test",
						DataInput.instance(
										null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
						config.configVal("storage"),
						false,
						config.intVal("load-batch-size"));
		return driver;
	}

	private static DeleteRequestOperation execute(
					final S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver,
					final DeleteRequestOperation operation) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final DeleteRequestOperation result = output.await();
		assertNotNull(result, "real Netty DELETE did not complete before the canary timeout");
		return result;
	}

	private CapturedRequest onlyDeleteRequest() {
		final List<CapturedRequest> deletes = requests.stream()
						.filter(request -> "DELETE".equals(request.method()))
						.toList();
		assertEquals(1, deletes.size());
		return deletes.get(0);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		final byte[] requestBody = exchange.getRequestBody().readAllBytes();
		final CapturedRequest request = new CapturedRequest(
						exchange.getRequestMethod(),
						exchange.getRequestURI().getRawPath(),
						exchange.getRequestURI().getRawQuery(),
						exchange.getRequestHeaders().getFirst("Authorization"),
						new String(requestBody, StandardCharsets.UTF_8));
		requests.add(request);
		if ("HEAD".equals(request.method())) {
			exchange.sendResponseHeaders(200, -1);
		} else if ("DELETE".equals(request.method())) {
			exchange.sendResponseHeaders(204, -1);
		} else if ("POST".equals(request.method()) && "delete".equals(request.rawQuery())) {
			if (multiDeleteStatus != 200) {
				exchange.sendResponseHeaders(multiDeleteStatus, -1);
				exchange.close();
				return;
			}
			final byte[] responseBody = multiDeleteResponse.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, responseBody.length);
			exchange.getResponseBody().write(responseBody);
		} else {
			exchange.sendResponseHeaders(500, -1);
		}
		exchange.close();
	}

	private record CapturedRequest(
					String method, String rawPath, String rawQuery, String authorization, String body) {}

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
