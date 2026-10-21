package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.DriverLogCapture.count;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.driver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.op;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.Started;
import com.github.akurilov.commons.system.SizeInBytes;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
 * operation and never a worker, the permits queued behind it, or the stop of the step. Both cover
 * a GET until its body has been read, and both are the watchdog's: a request made after the grace
 * is ended at once, and a restart of the driver gets a grace of its own, once a sweep of the earlier
 * stop has finished.
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

	private static Map<String, Object> onlyTheDriverBounds(final int requestMs, final int stopMs) {
		// Without stall detection only the driver can end these requests.
		return Map.of(
						"storage-crt-socketTimeoutMs", 0,
						"storage-crt-" + S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, requestMs,
						"storage-crt-" + S3AwsClientTimeouts.STOP_TIMEOUT_KEY, stopMs);
	}

	/**
	 * A GET's response arrives with its headers; the body is read afterwards, on the worker. The
	 * exact-version client hands the body over as it arrives, so the worker is reading it when it
	 * stops coming; the transfer client buffers it, so the worker is still waiting for the response.
	 */
	@Test
	void theDeadlineCoversTheBodyOfAGet(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			for (final String versionId : new String[]{"version-1", null
			}) {
				final var op = op(OpType.READ, ScriptedS3Server.BODY_STALL_KEY + "-deadline", versionId);
				try (final var started = Started.start(
								driver(home.resolve("body-" + versionId), server, onlyTheDriverBounds(300, 0)), op)) {
					final long start = System.nanoTime();
					final var result = started.await(RESULT_BUDGET);
					final Duration waited = Duration.ofNanos(System.nanoTime() - start);
					assertEquals(Status.FAIL_TIMEOUT, result.status(), "a GET whose body never ends must fail as a timeout");
					assertTrue(waited.compareTo(Duration.ofSeconds(5)) < 0, "the GET was ended only after " + waited);
					assertEquals(0, started.driver().activeOpCount());
				}
			}
		}
	}

	@Test
	void theStopGraceEndsAGetStillReadingItsBody(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final var op = op(OpType.READ, ScriptedS3Server.BODY_STALL_KEY + "-stop", "version-1");
			try (final var started = Started.start(driver(home.resolve("body-stop"), server, onlyTheDriverBounds(0, 300)), op)) {
				final long deadline = System.nanoTime() + RESULT_BUDGET.toNanos();
				while (op.respTimeStart() == 0) {
					assertTrue(System.nanoTime() < deadline, "the worker never started reading the body");
					Thread.sleep(10);
				}
				started.driver().closeAdmission();
				final var result = started.await(RESULT_BUDGET);
				assertEquals(Status.INTERRUPTED, result.status(), "a GET cancelled at stop ends as interrupted");
				assertEquals(0, started.driver().activeOpCount());
			}
		}
	}

	/**
	 * The grace ends the requests in flight at that moment. A request a worker makes afterwards,
	 * such as the abort of a multipart upload whose completion was just cancelled, must not be
	 * waited for with a fresh deadline: the step has stopped.
	 */
	@Test
	void aRequestMadeAfterTheStopGraceIsCancelledAtOnce() throws Exception {
		final var client = mock(S3AsyncClient.class);
		final var making = new CountDownLatch(1);
		final var made = new CountDownLatch(1);
		final var response = new CompletableFuture<HeadObjectResponse>();
		when(client.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
			making.countDown();
			assertTrue(made.await(RESULT_BUDGET.toMillis(), TimeUnit.MILLISECONDS));
			return response;
		});
		final var driver = S3AwsDeleteRequestTestFixture
						.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(
										client, client,
										Map.of(S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 0, S3AwsClientTimeouts.STOP_TIMEOUT_KEY, 150));
		final var op = op(OpType.STAT, "late", null);
		op.startRequest();
		op.finishRequest();
		final Thread worker = Thread.ofVirtual().start(() -> driver.invokeNio(op));
		try {
			assertTrue(making.await(RESULT_BUDGET.toMillis(), TimeUnit.MILLISECONDS));
			driver.closeAdmission();
			Thread.sleep(500); // the grace is over before the client hands the request back
			made.countDown();
			worker.join(RESULT_BUDGET.toMillis());
			assertFalse(worker.isAlive(), "a request made after the grace was waited for");
			assertEquals(Status.INTERRUPTED, op.status());
			assertTrue(response.isCancelled());
		} finally {
			made.countDown();
			worker.interrupt();
			worker.join(Duration.ofSeconds(2).toMillis());
			driver.close();
		}
	}

	/** The driver may be stopped and started again; each stop gets a grace of its own. */
	@Test
	void everyStopGetsItsOwnGrace(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final var first = op(OpType.STAT, "restart-first", "version-1");
			try (final var started = Started.start(driver(home.resolve("restart"), server, onlyTheDriverBounds(0, 300)), first)) {
				awaitStalled(server, "HEAD /bucket/restart-first?versionId=version-1");
				started.driver().closeAdmission();
				assertEquals(Status.INTERRUPTED, started.await(RESULT_BUDGET).status());
				started.driver().stop();
				started.driver().start();
				assertTrue(started.driver().requestWatchdogRunning());

				final var second = op(OpType.STAT, "restart-second", "version-1");
				assertTrue(started.driver().put(second));
				awaitStalled(server, "HEAD /bucket/restart-second?versionId=version-1");
				started.driver().closeAdmission();
				assertEquals(Status.INTERRUPTED, started.await(RESULT_BUDGET).status(), "the second stop must end its request too");
				assertEquals(0, started.driver().activeOpCount());
			}
		}
	}

	/** A stop undone by a restart before its grace ran out must not act on the next cycle. */
	@Test
	void aGraceOfAnEarlierCycleDoesNotEndTheRequestsOfTheNext(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final var first = op(OpType.STAT, "cycle-first", "version-1");
			try (final var started = Started.start(driver(home.resolve("cycle"), server, onlyTheDriverBounds(0, 300)), first)) {
				awaitStalled(server, "HEAD /bucket/cycle-first?versionId=version-1");
				started.driver().closeAdmission();
				// Stopping the workers ends the first request well before the grace does.
				started.driver().stop();
				assertEquals(Status.INTERRUPTED, started.await(RESULT_BUDGET).status());
				started.driver().start();

				final var second = op(OpType.STAT, "cycle-second", "version-1");
				assertTrue(started.driver().put(second));
				awaitStalled(server, "HEAD /bucket/cycle-second?versionId=version-1");
				Thread.sleep(700);
				assertEquals(1, started.driver().activeOpCount(), "the grace of the first stop ended a request of the second cycle");
				assertNotEquals(Status.INTERRUPTED, second.status());

				started.driver().shutdown();
				assertEquals(Status.INTERRUPTED, started.await(RESULT_BUDGET).status());
			}
		}
	}

	/**
	 * The watchdog ends the requests in flight at stop under the lock a restart takes to reset the
	 * stop, so a sweep that has seen the grace over finishes before the next cycle admits anything:
	 * it can end neither a request of that cycle nor its grace. A driver whose sweep is held at
	 * that point shows the restart waiting and the next cycle's request surviving the sweep.
	 */
	@Test
	void aRestartWaitsForAStopSweepThatHasBegun() throws Exception {
		final var sent = new CopyOnWriteArrayList<CompletableFuture<HeadObjectResponse>>();
		final var client = neverAnsweringClient(sent);
		final var sweeping = new CountDownLatch(1);
		final var resumeSweep = new CountDownLatch(1);
		final var held = new AtomicBoolean();
		final var driver = new S3AwsStorageDriver<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>>(
						"aws-restart-test",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
						S3AwsDeleteRequestTestFixture.storageConfig(
										Map.of(S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 0, S3AwsClientTimeouts.STOP_TIMEOUT_KEY, 50)),
						false, 16, client, client, 100 * 1024L, 8 * 1024 * 1024L) {
			@Override
			int stopRequestsInFlight() {
				// The first sweep is held once it has seen the grace over, before it ends anything.
				if (held.compareAndSet(false, true)) {
					sweeping.countDown();
					try {
						resumeSweep.await(RESULT_BUDGET.toMillis(), TimeUnit.MILLISECONDS);
					} catch (final InterruptedException interrupted) {
						Thread.currentThread().interrupt();
					}
				}
				return super.stopRequestsInFlight();
			}
		};
		final var first = op(OpType.STAT, "sweep-first", null);
		try (final var started = Started.start(driver, first)) {
			awaitSent(sent, 1);
			driver.closeAdmission();
			assertTrue(sweeping.await(RESULT_BUDGET.toMillis(), TimeUnit.MILLISECONDS), "the watchdog never swept at the end of the grace");
			// The stop of the workers ends the first request while the sweep of its grace is held.
			driver.stop();
			assertEquals(Status.INTERRUPTED, started.await(RESULT_BUDGET).status());

			final var restartFailure = new AtomicReference<Throwable>();
			final Thread restarting = Thread.ofVirtual().start(() -> {
				try {
					driver.start();
				} catch (final Exception failure) {
					restartFailure.set(failure);
				}
			});
			restarting.join(3 * S3AwsStorageDriver.WATCHDOG_TICK_MILLIS);
			assertTrue(restarting.isAlive(), "the restart did not wait for the stop sweep of the earlier cycle");
			resumeSweep.countDown();
			restarting.join(RESULT_BUDGET.toMillis());
			assertFalse(restarting.isAlive(), "the restart did not go on once the sweep had ended");
			assertNull(restartFailure.get());

			final var second = op(OpType.STAT, "sweep-second", null);
			assertTrue(driver.put(second));
			awaitSent(sent, 2);
			Thread.sleep(3 * S3AwsStorageDriver.WATCHDOG_TICK_MILLIS);
			assertFalse(sent.get(1).isCancelled(), "the stop sweep of the earlier cycle ended a request of the next");
			assertNotEquals(Status.INTERRUPTED, second.status());
			assertEquals(1, driver.activeOpCount());
		} finally {
			resumeSweep.countDown();
		}
	}

	private static void awaitSent(final List<?> sent, final int count) throws InterruptedException {
		final long deadline = System.nanoTime() + RESULT_BUDGET.toNanos();
		while (sent.size() < count) {
			assertTrue(System.nanoTime() - deadline < 0, "request " + count + " was never made");
			Thread.sleep(1);
		}
	}

	/**
	 * The engine gives a run ten seconds to stop. With the shipped grace a lost request is cancelled
	 * at five, which leaves the other five for the stop itself: the worker publishes the result,
	 * the step's drain sees nothing in flight, and the workers and the clients are stopped.
	 */
	@Test
	void withTheShippedGraceAStopWithALostRequestFitsTheEngineBudget(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final Map<String, Object> onlyTheGrace = Map.of(
							"storage-crt-socketTimeoutMs", 0,
							"storage-crt-" + S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY, 0);
			final var op = op(OpType.STAT, "shipped-grace", "version-1");
			final var started = Started.start(driver(home.resolve("shipped"), server, onlyTheGrace), op);
			final Duration cancelledAfter;
			final Duration stopped;
			final Status status;
			try {
				awaitStalled(server, "HEAD /bucket/shipped-grace?versionId=version-1");
				final long stopStart = System.nanoTime();
				started.driver().closeAdmission();
				status = started.await(Duration.ofSeconds(9)).status();
				cancelledAfter = Duration.ofNanos(System.nanoTime() - stopStart);
				assertEquals(0, started.driver().activeOpCount());
				started.driver().shutdown();
				started.driver().close();
				stopped = Duration.ofNanos(System.nanoTime() - stopStart);
			} finally {
				started.close();
			}
			assertEquals(Status.INTERRUPTED, status);
			assertTrue(cancelledAfter.compareTo(Duration.ofMillis(4500)) >= 0, "cancelled before the shipped grace: " + cancelledAfter);
			assertTrue(stopped.compareTo(Duration.ofSeconds(8)) < 0, "the stop took " + stopped + " of the engine's ten seconds");
		}
	}

	@Test
	void theWatchdogEndsWithTheDriver(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final var driver = S3AwsDriverHarness.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> driver(
							home.resolve("watchdog"), server, Map.of());
			assertFalse(driver.requestWatchdogRunning(), "nothing to watch before the driver starts");
			driver.start();
			assertTrue(driver.requestWatchdogRunning());
			driver.close();
			final long deadline = System.nanoTime() + RESULT_BUDGET.toNanos();
			while (driver.requestWatchdogRunning()) {
				assertTrue(System.nanoTime() < deadline, "the watchdog outlived the driver");
				Thread.sleep(10);
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
