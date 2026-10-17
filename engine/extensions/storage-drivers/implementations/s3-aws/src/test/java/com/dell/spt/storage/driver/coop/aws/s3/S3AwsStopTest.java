package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.DriverLogCapture.count;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.driver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.op;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.putOp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.storage.driver.coop.aws.s3.S3AwsDriverHarness.Started;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * When the driver stops, after the load step has waited for the requests in flight, it cancels
 * the ones still in flight. Before, a worker waiting for a request that never ended kept the
 * driver from stopping, and closing the clients left the worker waiting. A response that arrives
 * as the driver stops is applied by the worker before it gives up the operation, never after.
 */
// A request the driver fails to cancel is waited for without end, which an interrupt does not stop.
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsStopTest {

	/** Stopping a driver allows its workers one second to end. */
	private static final Duration STOP_BUDGET = Duration.ofSeconds(2);

	/** How long a cancelled request may take to close its connection, and its result to arrive. */
	private static final Duration CANCEL_BUDGET = Duration.ofSeconds(5);

	private static final Duration STALL_BUDGET = Duration.ofSeconds(10);

	@Test
	void stoppingTheDriverCancelsTheRequestsStillInFlightOnEveryClient(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			// Without stall detection only the stop can end these requests.
			final Map<String, Object> noStallDetection = Map.of("storage-crt-socketTimeoutMs", 0);
			final String transferGet = ScriptedS3Server.BODY_STALL_KEY + "-transfer";
			final String exactGet = ScriptedS3Server.BODY_STALL_KEY + "-exact";
			final var exactGetOp = op(OpType.READ, exactGet, "version-1");
			final List<InFlight> requests = List.of(
							inFlight("a PUT by the transfer client",
											driver(home.resolve("put"), server, noStallDetection),
											putOp("stop-put"), "PUT /bucket/stop-put", true, Operation::status),
							inFlight("a GET by the transfer client, whose body stopped",
											driver(home.resolve("transfer-get"), server, noStallDetection),
											op(OpType.READ, transferGet, null), "GET /bucket/" + transferGet, true, Operation::status),
							inFlight("a HEAD by the exact-version client",
											driver(home.resolve("exact-head"), server, noStallDetection),
											op(OpType.STAT, "stop-head", "version-1"), "HEAD /bucket/stop-head?versionId=version-1", true,
											Operation::status),
							// The client leaves the connection of a body it handed out open when the read is
							// abandoned, even once the client is closed, until the stall monitor closes it.
							inFlight("a GET by the exact-version client, read up to where its body stopped",
											driver(home.resolve("exact-get"), server, noStallDetection),
											exactGetOp, "GET /bucket/" + exactGet + "?versionId=version-1", false,
											Operation::status),
							inFlight("a DELETE by the standalone DELETE client",
											driver(home.resolve("delete"), server, noStallDetection),
											S3AwsDeleteRequestTestFixture.operation(S3AwsDeleteRequestTestFixture.target("stop-delete", null)),
											"DELETE /bucket/stop-delete", true, deleted -> {
												// The server may or may not have carried out a cancelled DELETE.
												assertNull(deleted.deleteResult(), "a cancelled DELETE must have no result");
												return deleted.status();
											}));
			try {
				for (final InFlight request : requests) {
					awaitStalled(server, request);
				}
				// The exact-version client hands the body to the worker, which must be reading it when the
				// driver stops. The transfer client buffers the body, so its worker waits for the request.
				final long deadline = System.nanoTime() + STALL_BUDGET.toNanos();
				while (exactGetOp.respTimeStart() == 0) {
					assertTrue(System.nanoTime() < deadline, "the worker never started reading the body of the exact-version GET");
					Thread.sleep(10);
				}

				for (final InFlight request : requests) {
					final long start = System.nanoTime();
					request.started().driver().shutdown();
					final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
					assertTrue(elapsed.compareTo(STOP_BUDGET) < 0, "stopping with " + request.name() + " took " + elapsed);
				}

				for (final InFlight request : requests) {
					assertEquals(Status.INTERRUPTED, request.status(CANCEL_BUDGET), request.name());
					assertEquals(0, request.started().driver().activeOpCount(), request.name());
				}
				awaitConnectionsClosed(server, requests.stream().filter(InFlight::abortedAtStop).toList());
			} finally {
				for (final InFlight request : requests) {
					request.started().close();
				}
			}
		}
	}

	@Test
	void aCancelledRequestIsLoggedOnceAndNotAsAnError() throws Exception {
		final var sent = new ArrayList<CompletableFuture<HeadObjectResponse>>();
		final var primary = mock(S3AsyncClient.class);
		when(primary.headObject(any(HeadObjectRequest.class))).thenAnswer(invocation -> {
			final var request = new CompletableFuture<HeadObjectResponse>();
			sent.add(request);
			return request;
		});
		final var driver = S3AwsDeleteRequestTestFixture.<IntegrityManifestDataItem, DataOperationImpl<IntegrityManifestDataItem>> anyDriver(primary, mock(S3AsyncClient.class));

		try (final var log = new DriverLogCapture()) {
			for (int request = 0; request < 3; request++) {
				final var op = op(OpType.STAT, "key", null);
				op.startRequest();
				op.finishRequest();
				// What stopping the driver does to a worker.
				Thread.currentThread().interrupt();
				try {
					driver.invokeNio(op);
				} finally {
					assertTrue(Thread.interrupted(), "the interrupt must stay set, so that the worker stops");
				}
				assertEquals(Status.INTERRUPTED, op.status());
			}

			assertEquals(3, sent.size());
			assertTrue(sent.stream().allMatch(CompletableFuture::isCancelled), "each request must have been cancelled");
			final var events = log.events();
			assertEquals(1, count(events, Level.WARN), events.toString());
			assertEquals(0, count(events, Level.ERROR), events.toString());
		}
	}

	@Test
	void aResponseBeingAppliedWhenTheDriverStopsIsAppliedBeforeTheWorkerGivesUpItsOperation() throws Exception {
		final var sent = new CompletableFuture<DeleteObjectsResponse>();
		final var client = mock(S3AsyncClient.class);
		when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(sent);
		final var applier = new AtomicReference<Thread>();
		final var applying = new CountDownLatch(1);
		final var release = new CountDownLatch(1);
		final var response = mock(DeleteObjectsResponse.class);
		when(response.deleted()).thenAnswer(invocation -> {
			applier.set(Thread.currentThread());
			applying.countDown();
			awaitIgnoringInterrupts(release);
			return List.of(DeletedObject.builder().key("a").build(), DeletedObject.builder().key("b").build());
		});
		when(response.errors()).thenReturn(List.of());
		final var op = S3AwsDeleteRequestTestFixture.operation(
						S3AwsDeleteRequestTestFixture.target("a", null), S3AwsDeleteRequestTestFixture.target("b", null));
		op.startRequest();
		op.finishRequest();

		try (final var driver = S3AwsDeleteRequestTestFixture.driver(client, client)) {
			final var returned = new AtomicReference<Status>();
			final Thread worker = Thread.ofVirtual().start(() -> {
				driver.invokeNio(op);
				returned.set(op.status());
			});
			final long deadline = System.nanoTime() + STALL_BUDGET.toNanos();
			while (sent.getNumberOfDependents() == 0) {
				assertTrue(System.nanoTime() < deadline, "the worker never waited for the response");
				Thread.sleep(1);
			}
			final Thread responder = Thread.ofVirtual().start(() -> sent.complete(response));
			try {
				assertTrue(applying.await(STALL_BUDGET.toSeconds(), TimeUnit.SECONDS));
				worker.interrupt();
				// A worker that gave up the operation while another thread applies the response would
				// return now, and the response would then change the operation.
				if (applier.get() != worker) {
					worker.join(STALL_BUDGET.toMillis());
				}
			} finally {
				release.countDown();
				responder.join(STALL_BUDGET.toMillis());
				worker.join(STALL_BUDGET.toMillis());
			}

			assertEquals(op.status(), returned.get(), "the operation changed after the worker gave it up");
			assertEquals(Status.SUCC, op.status(), "the response arrived before the stop, so it completes the operation");
			assertEquals(2, op.deleteResult().acceptedObjectCount());
			assertSame(worker, applier.get(), "the worker, not a thread of the client, applies the response");
		}
	}

	/** Applying a response is ordinary code, which an interrupt does not cut short. */
	private static void awaitIgnoringInterrupts(final CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				assertTrue(latch.await(STALL_BUDGET.toSeconds(), TimeUnit.SECONDS));
				break;
			} catch (final InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static <I extends Item, O extends Operation<I>> InFlight inFlight(
					final String name,
					final S3AwsStorageDriver<I, O> driver,
					final O op,
					final String requestLine,
					final boolean abortedAtStop,
					final Function<O, Status> status) throws Exception {
		final Started<I, O> started = Started.start(driver, op);
		return new InFlight(name, started, requestLine, abortedAtStop, budget -> status.apply(started.await(budget)));
	}

	/** Waits until the server has answered the request as far as it will. */
	private static void awaitStalled(final ScriptedS3Server server, final InFlight request) throws InterruptedException {
		final long deadline = System.nanoTime() + STALL_BUDGET.toNanos();
		while (server.stalledRequests().stream().noneMatch(line -> line.startsWith(request.requestLine()))) {
			assertTrue(System.nanoTime() < deadline, request.name() + " never stalled on the server: " + server.requestLines());
			Thread.sleep(10);
		}
	}

	/** A request the driver cancelled is aborted, which closes its connection, and not left to run on. */
	private static void awaitConnectionsClosed(final ScriptedS3Server server, final List<InFlight> requests)
					throws InterruptedException {
		final long deadline = System.nanoTime() + CANCEL_BUDGET.toNanos();
		for (final InFlight request : requests) {
			while (server.stalledRequestsGivenUp().stream().noneMatch(line -> line.startsWith(request.requestLine()))) {
				assertTrue(System.nanoTime() < deadline,
								"the connection of " + request.name() + " stayed open: " + server.stalledRequestsGivenUp());
				Thread.sleep(10);
			}
		}
	}

	private interface ResultStatus {
		Status await(Duration budget) throws InterruptedException;
	}

	/** @param abortedAtStop whether stopping the driver closes the connection of the request */
	private record InFlight(String name, Started<?, ?> started, String requestLine, boolean abortedAtStop, ResultStatus result) {

		Status status(final Duration budget) throws InterruptedException {
			return result.await(budget);
		}
	}
}
