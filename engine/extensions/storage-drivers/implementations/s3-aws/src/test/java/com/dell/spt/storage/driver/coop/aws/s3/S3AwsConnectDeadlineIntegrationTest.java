package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse.Kind;
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

	/**
	 * The first request an s3-aws run makes is the target-bucket check, which runs before any load
	 * step slice starts, through the control client the factory opens. It is an ordinary SDK client,
	 * so it has the SDK budget: without the connection timeout the check would take about eight
	 * seconds, and the driver's own wait for it ends only after the request timeout of minutes.
	 */
	@Test
	void theFirstRequestOfARunToAnUnreachableServerEndsAsATimeout(@TempDir final Path home) throws Exception {
		server.refuseNewConnections();
		final Config storage = S3AwsMergedConfig.create(
						home, server.port(), Map.of("storage-crt-connectionTimeoutMs", CONNECTION_TIMEOUT_MS)).configVal("storage");

		try (final TargetBucketControl control = new S3AwsStorageDriverFactory<>().openTargetBucketControl("connect-deadline", storage)) {
			final long start = System.nanoTime();
			final TargetBucketResponse checked = control.check("bucket", CREDENTIAL);
			final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

			assertEquals(Kind.FAILED, checked.kind(), checked.toString());
			// "timed out" is the client's own timeout; the driver's wait would end "timed out after ... ms".
			assertEquals("timed out", checked.detail(), checked.toString());
			assertTrue(elapsed.compareTo(SDK_BUDGET) < 0, "took " + elapsed + ", budget " + SDK_BUDGET);
			assertEquals(0, server.connections(), "no connection may have been accepted");
		}
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
