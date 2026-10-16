package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Capability proof for the timeouts of the s3-aws clients. A loopback server accepts the
 * connection, reads the request and never answers. The driver is built by the real factory from
 * the merged configuration, so a timeout that does not reach a client leaves its request waiting
 * for the server until this test gives up.
 */
@Execution(ExecutionMode.CONCURRENT)
@Timeout(300)
final class S3AwsStallBoundIntegrationTest {

	private static final Credential CREDENTIAL = Credential.getInstance("access-key", "secret-key");
	private static final int STALL_TIMEOUT_MS = S3AwsClientTimeouts.MINIMUM_STALL_TIMEOUT_MS;
	private static final int OBJECT_BYTES = 10 * 1024;

	/**
	 * Attempts at one stalled request: the CRT client makes up to six, the SDK client four. Each
	 * costs the stall timeout and the monitor's sampling, plus the pause before a retry; about
	 * thirty seconds at the lowest stall timeout. The budget leaves room for a slow machine.
	 */
	private static final Duration FAILURE_BUDGET = Duration.ofSeconds(90);

	private static final Duration RESULT_WAIT = Duration.ofSeconds(120);

	private ScriptedS3Server server;

	@BeforeEach
	void startServer() throws Exception {
		server = new ScriptedS3Server();
	}

	@AfterEach
	void stopServer() throws Exception {
		server.close();
	}

	/**
	 * The three clients retry on their own, so the scenarios run side by side: the test lasts as
	 * long as the slowest one.
	 */
	@Test
	void aStalledRequestOfEveryClientFailsAsATimeoutInsteadOfStayingInFlight(@TempDir final Path home)
					throws Exception {
		final var scenarios = Executors.newFixedThreadPool(3);
		try {
			final var put = scenarios.submit(() -> stalledPut(home.resolve("put")));
			final var exactVersion = scenarios.submit(() -> stalledExactVersionRequest(home.resolve("exact-version")));
			final var standaloneDelete = scenarios.submit(() -> stalledStandaloneDelete(home.resolve("delete")));
			put.get();
			exactVersion.get();
			standaloneDelete.get();
		} catch (final ExecutionException failure) {
			throw failure.getCause() instanceof Exception cause ? cause : failure;
		} finally {
			scenarios.shutdownNow();
		}
	}

	private static DataOperationImpl<DataItem> putOp(final String key) throws IOException {
		final DataItem item = new DataItemImpl(key, 0, OBJECT_BYTES);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
		return new DataOperationImpl<DataItem>(0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0);
	}

	/** A request for one version of an object, which the exact-version client serves. */
	private static DataOperationImpl<IntegrityManifestDataItem> versionedStatOp(final String key) {
		return new DataOperationImpl<IntegrityManifestDataItem>(
						0, OpType.STAT, new IntegrityManifestDataItem("bucket", key, 0, "version-1"), null, "/bucket",
						CREDENTIAL, null, 0);
	}

	private static DeleteRequestOperation deleteOp(final String key) {
		return S3AwsDeleteRequestTestFixture.operation(S3AwsDeleteRequestTestFixture.target(key, null));
	}

	private Void stalledPut(final Path home) throws Exception {
		final var op = putOp("put-key");

		final Duration elapsed = runToCompletion("PUT", home, op);

		assertEquals(Operation.Status.FAIL_TIMEOUT, op.status());
		assertWithinBudget("PUT", elapsed);
		assertRequestSent("PUT /bucket/put-key");
		assertTrue(server.stalledClosedByClient() >= 1, "the client must have given up a stalled connection");
		return null;
	}

	private Void stalledExactVersionRequest(final Path home) throws Exception {
		final var op = versionedStatOp("exact-key");

		final Duration elapsed = runToCompletion("exact-version HEAD", home, op);

		assertEquals(Operation.Status.FAIL_TIMEOUT, op.status());
		assertWithinBudget("exact-version HEAD", elapsed);
		assertRequestSent("HEAD /bucket/exact-key?versionId=version-1");
		return null;
	}

	private Void stalledStandaloneDelete(final Path home) throws Exception {
		final var op = deleteOp("delete-key");

		final Duration elapsed = runToCompletion("standalone DELETE", home, op);

		assertNotNull(op.deleteResult());
		assertEquals(Operation.Status.FAIL_TIMEOUT, op.deleteResult().operationStatus());
		assertWithinBudget("standalone DELETE", elapsed);
		assertRequestSent("DELETE /bucket/delete-key");
		return null;
	}

	@Test
	void withTheMonitorDisabledNoClientGivesUpAStalledConnection(@TempDir final Path home) throws Exception {
		final var running = new ArrayList<Running>();
		try {
			running.add(start("PUT", home.resolve("put"), putOp("put-key")));
			running.add(start("exact-version HEAD", home.resolve("exact-version"), versionedStatOp("exact-key")));
			running.add(start("standalone DELETE", home.resolve("delete"), deleteOp("delete-key")));

			// Enabled, a client gives up its first stalled connection in about five seconds.
			Thread.sleep(STALL_TIMEOUT_MS * 5L);

			assertRequestSent("PUT /bucket/put-key");
			assertRequestSent("HEAD /bucket/exact-key?versionId=version-1");
			assertRequestSent("DELETE /bucket/delete-key");
			assertEquals(0, server.stalledClosedByClient(), "no client may give up a stalled connection");
			for (final Running request : running) {
				assertEquals(1, request.driver().activeOpCount(), "the stalled " + request.name() + " must stay in flight");
			}
		} finally {
			// Nothing but the end of the server can end a stalled request, and a driver waits for its requests on close.
			server.close();
			for (final Running request : running) {
				request.finish();
			}
		}
	}

	/**
	 * Pins what the SDK does with per-request timeouts on the CRT transfer client, which is why the
	 * driver does not use them: they arm a timer for every request, and the attempt timeout is not
	 * supported at all.
	 */
	@Test
	void theCrtClientRejectsARequestAttemptTimeoutButHonorsARequestCallTimeout() {
		try (final S3AsyncClient client = S3AsyncClient.crtBuilder()
						.credentialsProvider(StaticCredentialsProvider.create(
										AwsBasicCredentials.create(CREDENTIAL.getUid(), CREDENTIAL.getSecret())))
						.region(Region.US_EAST_1)
						.endpointOverride(URI.create("http://127.0.0.1:" + server.port()))
						.forcePathStyle(true)
						.maxConcurrency(4)
						.build()) {
			final var attempt = PutObjectRequest.builder().bucket("bucket").key("key")
							.overrideConfiguration(o -> o.apiCallAttemptTimeout(Duration.ofSeconds(1)))
							.build();
			final var unsupported = assertThrows(CompletionException.class,
							() -> client.putObject(attempt, AsyncRequestBody.fromBytes(new byte[OBJECT_BYTES])).join());
			assertInstanceOf(UnsupportedOperationException.class, unsupported.getCause());

			final var call = PutObjectRequest.builder().bucket("bucket").key("key")
							.overrideConfiguration(o -> o.apiCallTimeout(Duration.ofSeconds(1)))
							.build();
			final long start = System.nanoTime();
			final var timedOut = assertThrows(CompletionException.class,
							() -> client.putObject(call, AsyncRequestBody.fromBytes(new byte[OBJECT_BYTES])).join());
			assertInstanceOf(ApiCallTimeoutException.class, timedOut.getCause());
			assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(10)) < 0);
		}
	}

	@Test
	void theFactoryHandsTheTimeoutsToTheStandaloneDeleteResources(@TempDir final Path home) throws Exception {
		final var handed = new AtomicReference<S3AwsStandaloneDeleteResources.Configuration>();
		final var factory = new S3AwsStorageDriverFactory<IntegrityManifestDataItem, DeleteRequestOperation>(
						configuration -> {
							handed.set(configuration);
							return S3AwsStandaloneDeleteResources.create(configuration);
						});
		final Config storage = S3AwsMergedConfig.create(
						home, server.port(),
						Map.of("storage-crt-connectionTimeoutMs", 7000, "storage-crt-socketTimeoutMs", 9000))
						.configVal("storage");

		try (final var driver = factory.create(
						"timeouts-handoff", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage, false, 16)) {
			driver.prepareStandaloneDelete();
		}

		assertNotNull(handed.get(), "the standalone DELETE resources must have been requested");
		assertEquals(
						new S3AwsClientTimeouts(Duration.ofSeconds(7), Duration.ofSeconds(9)),
						handed.get().timeouts());
	}

	private <I extends Item, O extends Operation<I>> Duration runToCompletion(
					final String request, final Path home, final O op) throws Exception {
		final var results = new Results<O>();
		try (final S3AwsStorageDriver<I, O> driver = driver(home, Map.of("storage-crt-socketTimeoutMs", STALL_TIMEOUT_MS))) {
			driver.operationResultOutput(results);
			driver.start();
			final long start = System.nanoTime();
			assertTrue(driver.put(op));
			final O completed = results.queue.poll(RESULT_WAIT.toMillis(), TimeUnit.MILLISECONDS);
			final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
			assertNotNull(completed, "the stalled " + request + " was still in flight after " + RESULT_WAIT);
			assertEquals(0, driver.activeOpCount(), "a failed " + request + " must not stay active");
			assertTrue(driver.isIdle());
			return elapsed;
		}
	}

	/** Starts a driver without stall detection and hands it an operation. */
	private <I extends Item, O extends Operation<I>> Running start(final String name, final Path home, final O op)
					throws Exception {
		final S3AwsStorageDriver<I, O> driver = driver(home, Map.of("storage-crt-socketTimeoutMs", 0));
		final var results = new Results<O>();
		driver.operationResultOutput(results);
		driver.start();
		assertTrue(driver.put(op));
		return new Running(name, driver, results.queue);
	}

	/** A driver with an operation in flight, and the results it has output. */
	private record Running(String name, S3AwsStorageDriver<?, ?> driver, LinkedBlockingQueue<?> results) {

		/** Waits for the operation to end, which needs the server gone, and closes the driver. */
		void finish() throws Exception {
			results.poll(RESULT_WAIT.toMillis(), TimeUnit.MILLISECONDS);
			driver.close();
		}
	}

	@SuppressWarnings("unchecked")
	private <I extends Item, O extends Operation<I>> S3AwsStorageDriver<I, O> driver(
					final Path home, final Map<String, Object> overrides) throws Exception {
		final Config storage = S3AwsMergedConfig.create(home, server.port(), overrides).configVal("storage");
		return (S3AwsStorageDriver<I, O>) new S3AwsStorageDriverFactory<I, O>().create(
						"stall-bound",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage,
						false,
						16);
	}

	private static void assertWithinBudget(final String request, final Duration elapsed) {
		assertTrue(elapsed.compareTo(FAILURE_BUDGET) < 0,
						"a stalled " + request + " must fail within " + FAILURE_BUDGET + ", took " + elapsed);
	}

	private void assertRequestSent(final String requestLinePrefix) {
		assertTrue(server.requestLines().stream().anyMatch(line -> line.startsWith(requestLinePrefix)),
						"the server must have received the request it never answered: " + server.requestLines());
	}

	private static final class Results<O extends Operation<?>> implements Output<O> {

		private final LinkedBlockingQueue<O> queue = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final O value) {
			return queue.offer(value);
		}

		@Override
		public int put(final List<O> values, final int from, final int to) {
			int count = 0;
			for (int i = from; i < to; i++) {
				if (!queue.offer(values.get(i))) {
					break;
				}
				count++;
			}
			return count;
		}

		@Override
		public int put(final List<O> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<O> getInput() {
			return null;
		}

		@Override
		public void close() {
			queue.clear();
		}
	}
}
