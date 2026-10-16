package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.Operation.Status;
import java.time.Duration;
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
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * The stall timeout ends a connection that moves no data, in any phase of a request, and leaves
 * alone a transfer that keeps moving data for longer than the timeout. The clients are built as
 * the driver builds them; the server is scripted by the object a request names.
 *
 * <p>Tagged {@code slow}: a request fails only after every retry of its client, so these tests
 * take minutes in all. The build runs {@link S3AwsTimeoutsCanaryTest} instead; run these with the
 * {@code slowTest} task after an upgrade of the AWS SDK or CRT.
 */
@Tag("slow")
@Execution(ExecutionMode.CONCURRENT)
@Timeout(180)
final class S3AwsTransferProgressIntegrationTest {

	private static final S3AwsClientTimeouts TIMEOUTS = new S3AwsClientTimeouts(
					Duration.ofSeconds(5), Duration.ofMillis(S3AwsClientTimeouts.MINIMUM_STALL_TIMEOUT_MS));
	private static final int BODY_BYTES = ScriptedS3Server.CHUNKS * ScriptedS3Server.CHUNK_BYTES;
	private static final int UPLOAD_BYTES = 64 * 1024 * 1024;
	private static final Duration RESULT_WAIT = Duration.ofSeconds(120);

	/** Six attempts at the lowest stall timeout last about thirty seconds. */
	private static final Duration FAILURE_BUDGET = Duration.ofSeconds(90);

	private ScriptedS3Server server;

	@BeforeEach
	void startServer() throws Exception {
		server = new ScriptedS3Server();
	}

	@AfterEach
	void stopServer() throws Exception {
		server.close();
	}

	@Test
	void aTransferThatKeepsMovingDataIsNotCutOffByTheStallTimeout() throws Exception {
		try (final var transfer = S3AwsRawClients.transfer(server.port(), TIMEOUTS);
						final var exactVersion = S3AwsRawClients.exactVersion(server.port(), TIMEOUTS);
						final var scenarios = Executors.newFixedThreadPool(2)) {
			final long start = System.nanoTime();
			final var viaTransfer = scenarios.submit(() -> download(transfer, ScriptedS3Server.PROGRESS_KEY + "-transfer"));
			final var viaExactVersion = scenarios.submit(
							() -> download(exactVersion, ScriptedS3Server.PROGRESS_KEY + "-sdk"));

			assertTransferred("the transfer client", viaTransfer.get());
			assertTransferred("the ordinary SDK client", viaExactVersion.get());
			final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
			assertTrue(elapsed.compareTo(TIMEOUTS.stallTimeout().multipliedBy(2)) > 0,
							"the transfers must outlast the stall timeout, but took " + elapsed);
			assertRequestSent("GET /bucket/" + ScriptedS3Server.PROGRESS_KEY + "-transfer");
			assertRequestSent("GET /bucket/" + ScriptedS3Server.PROGRESS_KEY + "-sdk");
		}
	}

	/**
	 * The clients retry on their own, so the scenarios run side by side: the test lasts as long
	 * as the slowest one.
	 */
	@Test
	void aTransferThatStopsInTheBodyOrAnUploadThePeerStopsReadingFailsAsATimeout() throws Exception {
		try (final var transfer = S3AwsRawClients.transfer(server.port(), TIMEOUTS);
						final var exactVersion = S3AwsRawClients.exactVersion(server.port(), TIMEOUTS);
						final var scenarios = Executors.newFixedThreadPool(3)) {
			final var downloadViaTransfer = scenarios.submit(
							() -> failure(() -> download(transfer, ScriptedS3Server.BODY_STALL_KEY + "-transfer")));
			final var downloadViaExactVersion = scenarios.submit(
							() -> failure(() -> download(exactVersion, ScriptedS3Server.BODY_STALL_KEY + "-sdk")));
			final var upload = scenarios.submit(() -> failure(() -> upload(exactVersion)));

			assertTimedOut("a download that stops in the body, by the transfer client", downloadViaTransfer.get());
			assertTimedOut("a download that stops in the body, by the ordinary SDK client", downloadViaExactVersion.get());
			assertTimedOut("an upload the peer stops reading", upload.get());
			assertRequestSent("GET /bucket/" + ScriptedS3Server.BODY_STALL_KEY + "-transfer");
			assertRequestSent("GET /bucket/" + ScriptedS3Server.BODY_STALL_KEY + "-sdk");
			final String uploadHead = server.requestHeads().stream()
							.filter(head -> head.startsWith("PUT /bucket/" + ScriptedS3Server.UPLOAD_STALL_KEY))
							.findFirst()
							.orElseThrow(() -> new AssertionError("the upload never reached the server: " + server.requestLines()));
			final String lowerHead = uploadHead.toLowerCase(java.util.Locale.ROOT);
			assertTrue(lowerHead.contains("content-length: " + UPLOAD_BYTES), uploadHead);
			// The client sends the body without waiting for a reply to its Expect header, so the stall is in the body.
			assertTrue(server.uploadBytesRead() >= ScriptedS3Server.UPLOAD_READ_LIMIT_BYTES,
							"the server read " + server.uploadBytesRead() + " bytes of the upload");
		}
	}

	private void assertRequestSent(final String requestLinePrefix) {
		assertTrue(server.requestLines().stream().anyMatch(line -> line.startsWith(requestLinePrefix)),
						"the server must have received " + requestLinePrefix + ": " + server.requestLines());
	}

	private static byte[] download(final S3AsyncClient client, final String key) throws Exception {
		return client.getObject(
						GetObjectRequest.builder().bucket("bucket").key(key).build(), AsyncResponseTransformer.toBytes())
						.get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS)
						.asByteArray();
	}

	private static Object upload(final S3AsyncClient client) throws Exception {
		return client.putObject(
						PutObjectRequest.builder().bucket("bucket").key(ScriptedS3Server.UPLOAD_STALL_KEY).build(),
						AsyncRequestBody.fromBytes(new byte[UPLOAD_BYTES]))
						.get(RESULT_WAIT.toSeconds(), TimeUnit.SECONDS);
	}

	private static void assertTransferred(final String client, final byte[] body) {
		assertEquals(BODY_BYTES, body.length, client + " must receive the whole body");
	}

	/** The failure a request ends with, and how long it took. */
	private record Failure(Throwable cause, Duration elapsed) {}

	private interface Request {
		Object run() throws Exception;
	}

	private static Failure failure(final Request request) {
		final long start = System.nanoTime();
		try {
			request.run();
		} catch (final ExecutionException failed) {
			return new Failure(failed.getCause(), Duration.ofNanos(System.nanoTime() - start));
		} catch (final TimeoutException stillInFlight) {
			throw new AssertionError("the request was still in flight after " + RESULT_WAIT, stillInFlight);
		} catch (final Exception failed) {
			return new Failure(failed, Duration.ofNanos(System.nanoTime() - start));
		}
		return new Failure(null, Duration.ofNanos(System.nanoTime() - start));
	}

	private static void assertTimedOut(final String request, final Failure failure) {
		assertTrue(failure.cause() != null, request + " must fail, but it succeeded after " + failure.elapsed());
		assertEquals(Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(new CompletionException(failure.cause())),
						request + " ended with " + failure.cause());
		assertTrue(failure.elapsed().compareTo(FAILURE_BUDGET) < 0, request + " took " + failure.elapsed());
	}
}
