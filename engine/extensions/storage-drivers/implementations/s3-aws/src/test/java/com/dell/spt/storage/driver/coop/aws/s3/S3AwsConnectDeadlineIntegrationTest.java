package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * The connection timeout reaches the native client of every path. The server's queue of
 * connections waiting to be accepted is full, so a client that connects waits for a connection
 * that never comes, until its connection timeout ends the wait. Stall detection does not apply
 * to a connection that was never made.
 *
 * <p>Tagged {@code slow}: a request fails only after every retry of its client, so these tests
 * take minutes in all. The build runs {@link S3AwsTimeoutsCanaryTest} instead; run these with the
 * {@code slowTest} task after an upgrade of the AWS SDK or CRT.
 */
@Tag("slow")
@Execution(ExecutionMode.CONCURRENT)
@Timeout(180)
final class S3AwsConnectDeadlineIntegrationTest {

	private static final Credential CREDENTIAL = Credential.getInstance("access-key", "secret-key");
	private static final int CONNECTION_TIMEOUT_MS = 100;
	private static final S3AwsClientTimeouts TIMEOUTS = new S3AwsClientTimeouts(Duration.ofMillis(CONNECTION_TIMEOUT_MS), Duration.ofSeconds(30));
	private static final Duration RESULT_WAIT = Duration.ofSeconds(60);

	/**
	 * How long a request may take to fail, by kind of client. Each attempt is bounded by the
	 * connection timeout of a hundred milliseconds, and the clients make six attempts and four, with
	 * pauses between them; the transfer client takes ten to fifteen seconds, the pauses of its
	 * retries being random, and the others about two. Without a connection timeout of ours the same
	 * requests take about 28 and 8 seconds, so a client that was not given the timeout does not fit
	 * its budget.
	 */
	private static final Duration TRANSFER_BUDGET = Duration.ofSeconds(22);

	private static final Duration SDK_BUDGET = Duration.ofSeconds(5);

	private ScriptedS3Server server;

	@BeforeEach
	void startServer() throws Exception {
		server = new ScriptedS3Server(1);
	}

	@AfterEach
	void stopServer() throws Exception {
		server.close();
	}

	@Test
	void everyClientEndsAConnectionThatCannotBeMadeAsATimeout() throws Exception {
		server.refuseNewConnections();
		try (final var transfer = S3AwsRawClients.transfer(server.port(), TIMEOUTS);
						final var exactVersion = S3AwsRawClients.exactVersion(server.port(), TIMEOUTS);
						final var standaloneDelete = S3AwsRawClients.standaloneDelete(server.port(), TIMEOUTS);
						final var scenarios = Executors.newFixedThreadPool(3)) {
			final var put = scenarios.submit(() -> failure(() -> transfer.putObject(
							PutObjectRequest.builder().bucket("bucket").key("key").build(),
							AsyncRequestBody.fromBytes(new byte[100])).get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS)));
			final var head = scenarios.submit(() -> failure(() -> exactVersion.headObject(
							HeadObjectRequest.builder().bucket("bucket").key("key").versionId("version-1").build())
							.get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS)));
			final var delete = scenarios.submit(() -> failure(() -> standaloneDelete.s3Client().deleteObject(
							DeleteObjectRequest.builder().bucket("bucket").key("key").build())
							.get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS)));

			assertTimedOut("a PUT by the transfer client", put.get(), TRANSFER_BUDGET);
			assertTimedOut("a HEAD by the ordinary SDK client", head.get(), SDK_BUDGET);
			assertTimedOut("a DELETE by the standalone DELETE client", delete.get(), SDK_BUDGET);
			assertEquals(0, server.connections(), "no connection may have been accepted");
		}
	}

	@Test
	void aConnectionThatIsRefusedIsNotATimeout() throws Exception {
		final int closedPort;
		try (final var closed = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			closedPort = closed.getLocalPort();
		}
		try (final var transfer = S3AwsRawClients.transfer(closedPort, TIMEOUTS);
						final var exactVersion = S3AwsRawClients.exactVersion(closedPort, TIMEOUTS)) {
			final var viaTransfer = failure(() -> transfer.headObject(
							HeadObjectRequest.builder().bucket("bucket").key("key").build()).get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS));
			final var viaExactVersion = failure(() -> exactVersion.headObject(
							HeadObjectRequest.builder().bucket("bucket").key("key").build()).get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS));

			for (final Failure refused : List.of(viaTransfer, viaExactVersion)) {
				assertNotNull(refused.cause(), "a refused connection must fail the request");
				final Status status = S3AwsStorageDriver.classifyFailure(new CompletionException(refused.cause()));
				assertNotEquals(Status.FAIL_TIMEOUT, status, refused.cause().toString());
			}
		}
	}

	@Test
	void theDriverEndsItsFirstRequestToAnUnreachableServerAsAFailure(@TempDir final Path home) throws Exception {
		server.refuseNewConnections();
		final var op = new DataOperationImpl<IntegrityManifestDataItem>(
						0, OpType.STAT, new IntegrityManifestDataItem("bucket", "key", 0, null), null, "/bucket", CREDENTIAL, null, 0);

		try (final S3AwsStorageDriver<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> driver = driver(home)) {
			driver.start();
			final long start = System.nanoTime();
			final var failed = assertThrows(RuntimeException.class, () -> driver.put(op));
			final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

			assertTrue(S3AwsClientTimeouts.isSocketTimeout(failed), failed.toString());
			assertTrue(elapsed.compareTo(TRANSFER_BUDGET) < 0, "took " + elapsed);
		}
	}

	@SuppressWarnings("unchecked")
	private <I extends Item, O extends Operation<I>> S3AwsStorageDriver<I, O> driver(final Path home) throws Exception {
		final Config storage = S3AwsMergedConfig.create(
						home, server.port(), Map.of("storage-crt-connectionTimeoutMs", CONNECTION_TIMEOUT_MS)).configVal("storage");
		return (S3AwsStorageDriver<I, O>) new S3AwsStorageDriverFactory<I, O>().create(
						"connect-deadline",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						storage,
						false,
						16);
	}

	/** The failure a request ends with, and how long it took. */
	private record Failure(Throwable cause, Duration elapsed) {}

	private static Failure failure(final Callable<Object> request) {
		final long start = System.nanoTime();
		try {
			request.call();
		} catch (final ExecutionException failed) {
			return new Failure(failed.getCause(), Duration.ofNanos(System.nanoTime() - start));
		} catch (final TimeoutException stillInFlight) {
			throw new AssertionError("the request was still in flight after " + RESULT_WAIT, stillInFlight);
		} catch (final Exception failed) {
			return new Failure(failed, Duration.ofNanos(System.nanoTime() - start));
		}
		return new Failure(null, Duration.ofNanos(System.nanoTime() - start));
	}

	private static void assertTimedOut(final String request, final Failure failure, final Duration budget) {
		assertNotNull(failure.cause(), request + " must fail, but it succeeded");
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(failure.cause())),
						request + " ended with " + failure.cause());
		assertTrue(failure.elapsed().compareTo(budget) < 0, request + " took " + failure.elapsed() + ", budget " + budget);
	}
}
