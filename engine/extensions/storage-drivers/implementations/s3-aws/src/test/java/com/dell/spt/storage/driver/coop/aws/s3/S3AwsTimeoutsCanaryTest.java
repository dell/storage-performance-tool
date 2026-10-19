package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Proof on every build that the factory gives each of its three S3 clients the configured
 * timeouts. A client that was not given them waits forever, so each test would hit its budget.
 *
 * <p>A request that fails only fails after every retry of its client, which takes half a minute
 * or more. These tests instead let a client give up a stalled connection and retry on a new one,
 * which the server answers, or end a connection that cannot be made on a client that retries
 * quickly. The full failures are proven by the tests tagged {@code slow}, which the
 * {@code slowTest} task runs.
 */
@Timeout(60)
final class S3AwsTimeoutsCanaryTest {

	private static final Credential CREDENTIAL = Credential.getInstance("access-key", "secret-key");

	/** The stall is noticed after about four seconds, and the retry is answered at once. */
	private static final Duration STALL_BUDGET = Duration.ofSeconds(20);

	/** Four attempts of a hundred milliseconds take about a second; without the timeout, eight. */
	private static final Duration CONNECT_BUDGET = Duration.ofSeconds(5);

	@Test
	void everyClientGivesUpAStalledConnectionAndRetriesTheRequest(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final Map<String, Object> stallBound = Map.of(
							"storage-crt-socketTimeoutMs", S3AwsClientTimeouts.MINIMUM_STALL_TIMEOUT_MS);
			final long start = System.nanoTime();
			try (final var put = Started.<DataItem, DataOperationImpl<DataItem>> start(
							driver(home.resolve("put"), server, stallBound), putOp(ScriptedS3Server.STALL_ONCE_KEY + "-put"));
							final var head = Started.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> start(
											driver(home.resolve("exact-version"), server, stallBound),
											versionedStatOp(ScriptedS3Server.STALL_ONCE_KEY + "-head"));
							final var delete = Started.<IntegrityManifestDataItem, DeleteRequestOperation> start(
											driver(home.resolve("delete"), server, stallBound),
											S3AwsDeleteRequestTestFixture.operation(S3AwsDeleteRequestTestFixture.target(
															ScriptedS3Server.STALL_ONCE_KEY + "-delete", null)))) {
				assertEquals(Status.SUCC, put.await(STALL_BUDGET).status());
				assertEquals(Status.SUCC, head.await(STALL_BUDGET).status());
				final var deleted = delete.await(STALL_BUDGET).deleteResult();
				assertEquals(DeleteRequestOutcome.FULL_SUCCESS, deleted.outcome(), deleted + " " + server.requestLines());
				final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
				assertTrue(elapsed.compareTo(STALL_BUDGET) < 0, "took " + elapsed);
			}
			final List<String> givenUp = server.stalledRequestsGivenUp();
			assertGivenUp(givenUp, "PUT /bucket/" + ScriptedS3Server.STALL_ONCE_KEY + "-put");
			assertGivenUp(givenUp, "HEAD /bucket/" + ScriptedS3Server.STALL_ONCE_KEY + "-head?versionId=version-1");
			assertGivenUp(givenUp, "DELETE /bucket/" + ScriptedS3Server.STALL_ONCE_KEY + "-delete");
		}
	}

	/**
	 * The transfer client takes ten seconds or more to give up a connection that cannot be made,
	 * because of the pauses between its retries, so only the other two clients are tried here. The
	 * transfer client gets its connection timeout in the same configuration object as its stall
	 * monitor, which the test above proves.
	 */
	@Test
	void theConnectionTimeoutEndsRequestsOfTheExactVersionAndDeleteClients(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server(1)) {
			final S3AwsStorageDriver<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>> driver = driver(home, server, Map.of("storage-crt-connectionTimeoutMs", 100));
			try (final var started = Started.start(driver, noop())) {
				// The first request makes the connection of the transfer client, which the server keeps serving.
				started.await(STALL_BUDGET);
				server.refuseNewConnections();

				final var exactVersion = versionedStatOp("key");
				final long headStart = System.nanoTime();
				assertEquals(Status.FAIL_TIMEOUT, started.next(cast(exactVersion), CONNECT_BUDGET).status());
				assertWithin(CONNECT_BUDGET, headStart, "the HEAD");

				final DeleteRequestOperation delete = S3AwsDeleteRequestTestFixture.operation(
								S3AwsDeleteRequestTestFixture.target("key", null));
				final long deleteStart = System.nanoTime();
				started.next(cast(delete), CONNECT_BUDGET);
				assertEquals(Status.FAIL_TIMEOUT, delete.deleteResult().operationStatus());
				assertWithin(CONNECT_BUDGET, deleteStart, "the DELETE");
				assertEquals(0, driver.activeOpCount());
			}
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
						home, 9, Map.of("storage-crt-connectionTimeoutMs", 7000, "storage-crt-socketTimeoutMs", 9000))
						.configVal("storage");

		try (final var driver = factory.create(
						"timeouts-handoff", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage, false, 16)) {
			driver.prepareStandaloneDelete();
		}

		assertNotNull(handed.get(), "the standalone DELETE resources must have been requested");
		assertEquals(new S3AwsClientTimeouts(Duration.ofSeconds(7), Duration.ofSeconds(9)), handed.get().timeouts());
	}

	private static DataOperationImpl<DataItem> putOp(final String key) throws Exception {
		final DataItem item = new DataItemImpl(key, 0, 10 * 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
		return new DataOperationImpl<DataItem>(0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0);
	}

	/** A request for one version of an object, which the exact-version client serves. */
	private static DataOperationImpl<IntegrityManifestDataItem> versionedStatOp(final String key) {
		return new DataOperationImpl<IntegrityManifestDataItem>(
						0, OpType.STAT, new IntegrityManifestDataItem("bucket", key, 0, "version-1"), null, "/bucket",
						CREDENTIAL, null, 0);
	}

	private static Operation<IntegrityManifestDataItem> noop() {
		return new DataOperationImpl<IntegrityManifestDataItem>(
						0, OpType.NOOP, new IntegrityManifestDataItem("bucket", "key", 0, null), null, "/bucket", CREDENTIAL, null, 0);
	}

	@SuppressWarnings("unchecked")
	private static Operation<IntegrityManifestDataItem> cast(final Operation<?> operation) {
		return (Operation<IntegrityManifestDataItem>) operation;
	}

	@SuppressWarnings("unchecked")
	private static <I extends Item, O extends Operation<I>> S3AwsStorageDriver<I, O> driver(
					final Path home, final ScriptedS3Server server, final Map<String, Object> overrides) throws Exception {
		final Config storage = S3AwsMergedConfig.create(home, server.port(), overrides).configVal("storage");
		return (S3AwsStorageDriver<I, O>) new S3AwsStorageDriverFactory<I, O>().create(
						"timeouts-canary",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage,
						false,
						16);
	}

	private static void assertGivenUp(final List<String> givenUp, final String request) {
		assertTrue(givenUp.stream().anyMatch(line -> line.startsWith(request)),
						"the client must have given up the stalled " + request + ": " + givenUp);
	}

	private static void assertWithin(final Duration budget, final long startNanos, final String request) {
		final Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
		assertTrue(elapsed.compareTo(budget) < 0, request + " took " + elapsed + ", budget " + budget);
	}

	/** A started driver, the results it outputs, and its close. */
	private static final class Started<I extends Item, O extends Operation<I>> implements AutoCloseable {

		private final S3AwsStorageDriver<I, O> driver;
		private final LinkedBlockingQueue<O> results = new LinkedBlockingQueue<>();

		private Started(final S3AwsStorageDriver<I, O> driver) {
			this.driver = driver;
		}

		/** Starts the driver and hands it a first operation. */
		static <I extends Item, O extends Operation<I>> Started<I, O> start(final S3AwsStorageDriver<I, O> driver, final O op)
						throws Exception {
			final var started = new Started<I, O>(driver);
			driver.operationResultOutput(new ResultOutput<>(started.results));
			driver.start();
			assertTrue(driver.put(op));
			return started;
		}

		O await(final Duration budget) throws InterruptedException {
			final O result = results.poll(budget.toMillis(), TimeUnit.MILLISECONDS);
			assertNotNull(result, "the request was still in flight after " + budget);
			return result;
		}

		O next(final O op, final Duration budget) throws Exception {
			assertTrue(driver.put(op));
			return await(budget);
		}

		@Override
		public void close() throws Exception {
			driver.close();
		}
	}

	private record ResultOutput<O>(LinkedBlockingQueue<O> results)implements Output<O>
	{

	@Override
	public boolean put(final O value) {
		return results.offer(value);
	}

	@Override
	public int put(final List<O> values, final int from, final int to) {
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
	public int put(final List<O> values) {
		return put(values, 0, values.size());
	}

	@Override
	public Input<O> getInput() {
		return null;
	}

	@Override
	public void close() {
		results.clear();
	}
}}
