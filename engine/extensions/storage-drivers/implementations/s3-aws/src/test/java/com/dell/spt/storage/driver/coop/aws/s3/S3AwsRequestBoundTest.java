package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.DriverLogCapture.count;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.driver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.op;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.Started;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * A request the CRT has lost, with no connection, no timer and no pending read, never completes or
 * fails on its own. The driver bounds its wait for every request, and once the load step has
 * stopped it cancels the requests still in flight after a grace, so that a lost request costs one
 * operation and never a worker, the permits queued behind it, or the stop of the step.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsRequestBoundTest {

	private static final Duration RESULT_BUDGET = Duration.ofSeconds(10);

	/** A client whose HEAD requests never complete, as a lost CRT request does. */
	private static S3AsyncClient neverAnsweringClient(final List<CompletableFuture<HeadObjectResponse>> sent) {
		final var client = mock(S3AsyncClient.class);
		when(client.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
			final var request = new CompletableFuture<HeadObjectResponse>();
			sent.add(request);
			return request;
		});
		return client;
	}

	@Test
	void aRequestThatNeverCompletesIsCancelledAndFailsAsATimeout() throws Exception {
		final var sent = new java.util.ArrayList<CompletableFuture<HeadObjectResponse>>();
		final var client = neverAnsweringClient(sent);
		final var driver = S3AwsDeleteRequestTestFixture
						.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(
										client, client, Map.of(S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 200));

		try (final var log = new DriverLogCapture()) {
			for (int request = 0; request < 2; request++) {
				final var op = op(OpType.STAT, "lost-" + request, null);
				op.startRequest();
				op.finishRequest();
				final long start = System.nanoTime();
				driver.invokeNio(op);
				final Duration waited = Duration.ofNanos(System.nanoTime() - start);

				assertEquals(Status.FAIL_TIMEOUT, op.status(), "the lost request must fail as a timeout");
				assertTrue(waited.compareTo(Duration.ofSeconds(5)) < 0, "the worker waited " + waited);
				assertFalse(Thread.currentThread().isInterrupted(), "a timeout is not a stop");
			}
			assertEquals(2, sent.size());
			assertTrue(sent.stream().allMatch(CompletableFuture::isCancelled), "each lost request must be cancelled");
			final var events = log.events();
			// The first timeout of a run is a warning that names the setting; the rest are DEBUG.
			assertEquals(1, count(events, Level.WARN), events.toString());
			assertEquals(0, count(events, Level.ERROR), events.toString());
			assertTrue(events.stream().anyMatch(event -> event.message()
							.contains("storage.crt." + S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY)), events.toString());
		} finally {
			driver.close();
		}
	}

	@Test
	void withoutABoundTheWaitIsOnlyEndedByAStop() throws Exception {
		final var sent = new java.util.ArrayList<CompletableFuture<HeadObjectResponse>>();
		final var client = neverAnsweringClient(sent);
		final var driver = S3AwsDeleteRequestTestFixture
						.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(
										client, client, Map.of(S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 0));
		final var op = op(OpType.STAT, "unbounded", null);
		op.startRequest();
		op.finishRequest();
		try {
			final Thread worker = Thread.ofVirtual().start(() -> driver.invokeNio(op));
			worker.join(Duration.ofMillis(500).toMillis());
			assertTrue(worker.isAlive(), "with no bound the worker must still be waiting");
			worker.interrupt();
			worker.join(RESULT_BUDGET.toMillis());
			assertFalse(worker.isAlive());
			assertEquals(Status.INTERRUPTED, op.status());
		} finally {
			driver.close();
		}
	}

	/**
	 * The step closes admission before it waits for the requests in flight. With the shipped
	 * driver that wait ended only when the request did, so a lost request held the stop for the
	 * whole of the step's wait and left the operation active.
	 */
	@Test
	void requestsStillInFlightAfterTheStopGraceAreCancelledAndCountedAsInterrupted(@TempDir final Path home)
					throws Exception {
		try (final var server = new ScriptedS3Server()) {
			// A HEAD the server never answers; nothing but the driver can end it.
			final Map<String, Object> overrides = Map.of(
							"storage-crt-socketTimeoutMs", 0,
							"storage-crt-" + S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 0,
							"storage-crt-" + S3AwsClientTimeouts.STOP_TIMEOUT_KEY, 500);
			final var op = op(OpType.STAT, "stop-grace", "version-1");
			final Started<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> started = Started.start(
							driver(home.resolve("grace"), server, overrides), op);
			try {
				awaitStalled(server, "HEAD /bucket/stop-grace?versionId=version-1");

				// What the load step does first when it stops. The workers keep running.
				final long closedAt = System.nanoTime();
				started.driver().closeAdmission();
				final var result = started.await(RESULT_BUDGET);
				final Duration afterClose = Duration.ofNanos(System.nanoTime() - closedAt);

				assertEquals(Status.INTERRUPTED, result.status(), "a request cancelled at stop ends as interrupted");
				assertTrue(afterClose.compareTo(Duration.ofMillis(400)) >= 0, "cancelled before the grace: " + afterClose);
				assertTrue(afterClose.compareTo(Duration.ofSeconds(5)) < 0, "cancelled only after " + afterClose);
				assertEquals(0, started.driver().activeOpCount(), "the cancelled operation must not stay active");
				// The driver stops within its budget afterwards: nothing is left for the workers to wait on.
				final long stopStart = System.nanoTime();
				started.driver().shutdown();
				assertTrue(Duration.ofNanos(System.nanoTime() - stopStart).compareTo(Duration.ofSeconds(2)) < 0);
			} finally {
				started.close();
			}
		}
	}

	@Test
	void withTheStopGraceOffARequestInFlightWaitsForTheStopOfTheWorkers(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final Map<String, Object> overrides = Map.of(
							"storage-crt-socketTimeoutMs", 0,
							"storage-crt-" + S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 0,
							"storage-crt-" + S3AwsClientTimeouts.STOP_TIMEOUT_KEY, 0);
			final var op = op(OpType.STAT, "no-grace", "version-1");
			final Started<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> started = Started.start(
							driver(home.resolve("no-grace"), server, overrides), op);
			try {
				awaitStalled(server, "HEAD /bucket/no-grace?versionId=version-1");
				started.driver().closeAdmission();
				Thread.sleep(700);
				assertEquals(1, started.driver().activeOpCount(), "with the grace off, closing admission cancels nothing");

				started.driver().shutdown();
				assertEquals(Status.INTERRUPTED, started.await(RESULT_BUDGET).status());
			} finally {
				started.close();
			}
		}
	}

	@Test
	void everyPermitGetsAWorkerUnlessTheWorkerCountIsSet(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			try (final var byPermits = driver(home.resolve("permits"), server,
							Map.of("storage-driver-limit-concurrency", 37))) {
				assertEquals(37, byPermits.ioWorkerCount(), "one worker per permit when storage.driver.threads is unset");
				assertTrue(byPermits.ioWorkerPerPermit());
			}
			try (final var configured = driver(home.resolve("configured"), server,
							Map.of("storage-driver-limit-concurrency", 37, "storage-driver-threads", 5))) {
				assertEquals(5, configured.ioWorkerCount(), "storage.driver.threads still decides when it is set");
				assertFalse(configured.ioWorkerPerPermit());
			}
		}
	}

	private static void awaitStalled(final ScriptedS3Server server, final String requestLine) throws InterruptedException {
		final long deadline = System.nanoTime() + RESULT_BUDGET.toNanos();
		while (server.stalledRequests().stream().noneMatch(line -> line.startsWith(requestLine))) {
			assertTrue(System.nanoTime() < deadline, "the request never stalled on the server: " + server.requestLines());
			Thread.sleep(10);
		}
	}
}
