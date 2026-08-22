package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.driver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.operation;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Real-driver and real-SDK canary backed by a deterministic loopback S3 endpoint. */
final class S3AwsDeleteRequestIntegrationTest {

	private static final long RESULT_TIMEOUT_SECONDS = 5;
	private static final String DELETE_RESULT_NAMESPACE = " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"";

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;
	private volatile String multiDeleteResponse;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
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
	void currentKeyDeleteRunsThroughRealAwsSdkDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(driver, operation(target("current-key", null)));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyRequest("DELETE");
			assertEquals("/bucket/current-key", request.rawPath());
			assertNull(request.rawQuery());
		}
	}

	@Test
	void exactVersionDeleteRunsThroughRealAwsSdkDriver() throws Exception {
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("exact-key", "v+1/=")));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyRequest("DELETE");
			assertEquals("/bucket/exact-key", request.rawPath());
			assertEquals("versionId=v%2B1%2F%3D", request.rawQuery());
		}
	}

	@Test
	void batchDeleteRunsAsOneRealNonQuietSdkRequest() throws Exception {
		multiDeleteResponse = "<DeleteResult" + DELETE_RESULT_NAMESPACE + ">"
						+ "<Deleted><Key>one</Key></Deleted>"
						+ "<Deleted><Key>two</Key><VersionId>version-2</VersionId></Deleted>"
						+ "</DeleteResult>";
		try (final var driver = newDriver()) {
			final DeleteRequestOperation result = execute(
							driver, operation(target("one", null), target("two", "version-2")));

			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, result.deleteResult().outcome());
			final CapturedRequest request = onlyRequest("POST");
			assertEquals("delete", request.rawQuery());
			assertTrue(request.body().contains("<Key>one</Key>"));
			assertTrue(request.body().contains("<VersionId>version-2</VersionId>"));
			assertTrue(request.body().contains("<Quiet>false</Quiet>"));
			assertEquals(1, driver.scheduledOpCount());
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
			assertTrue(result.duration() > 0);
		}
	}

	@Test
	void partialSdkResponsePreservesFailureAfterDriverTimingFinalization() throws Exception {
		multiDeleteResponse = "<DeleteResult" + DELETE_RESULT_NAMESPACE + ">"
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
			assertEquals(com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC, result.status());
		}
	}

	private S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> newDriver()
					throws Exception {
		return driver(newClient(), newClient());
	}

	private S3AsyncClient newClient() {
		final URI endpoint = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
		final var credentials = AwsBasicCredentials.create(CREDENTIAL.getUid(), CREDENTIAL.getSecret());
		return S3AsyncClient.builder()
						.credentialsProvider(StaticCredentialsProvider.create(credentials))
						.region(Region.US_EAST_1)
						.endpointOverride(endpoint)
						.forcePathStyle(true)
						.httpClientBuilder(AwsCrtAsyncHttpClient.builder().maxConcurrency(4))
						.overrideConfiguration(ClientOverrideConfiguration.builder()
										.apiCallAttemptTimeout(Duration.ofSeconds(2))
										.apiCallTimeout(Duration.ofSeconds(3))
										.build())
						.build();
	}

	private static DeleteRequestOperation execute(
					final S3AwsStorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver,
					final DeleteRequestOperation operation) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final DeleteRequestOperation result = output.await();
		assertNotNull(result, "real AWS SDK DELETE did not complete before the canary timeout");
		return result;
	}

	private CapturedRequest onlyRequest(final String method) {
		final List<CapturedRequest> matches = requests.stream()
						.filter(request -> method.equals(request.method()))
						.toList();
		assertEquals(1, matches.size());
		return matches.get(0);
	}

	private void handle(final HttpExchange exchange) throws IOException {
		final String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
		requests.add(new CapturedRequest(
						exchange.getRequestMethod(),
						exchange.getRequestURI().getRawPath(),
						exchange.getRequestURI().getRawQuery(),
						body));
		if ("HEAD".equals(exchange.getRequestMethod())) {
			exchange.sendResponseHeaders(200, -1);
		} else if ("DELETE".equals(exchange.getRequestMethod())) {
			exchange.sendResponseHeaders(204, -1);
		} else if ("POST".equals(exchange.getRequestMethod())
						&& "delete".equals(exchange.getRequestURI().getRawQuery())) {
			final byte[] responseBody = multiDeleteResponse.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(200, responseBody.length);
			exchange.getResponseBody().write(responseBody);
		} else {
			exchange.sendResponseHeaders(500, -1);
		}
		exchange.close();
	}

	private record CapturedRequest(String method, String rawPath, String rawQuery, String body) {}

	private static final class ResultOutput implements Output<DeleteRequestOperation> {

		private final LinkedBlockingQueue<DeleteRequestOperation> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final DeleteRequestOperation value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<DeleteRequestOperation> values, final int from, final int to) {
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
