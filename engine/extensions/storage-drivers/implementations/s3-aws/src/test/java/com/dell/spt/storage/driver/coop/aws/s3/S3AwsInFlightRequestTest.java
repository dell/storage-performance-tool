package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.http.Abortable;

@Timeout(30)
final class S3AwsInFlightRequestTest {

	private final Set<S3AwsInFlightRequest> registry = ConcurrentHashMap.newKeySet();

	private static final class CountingBody implements Abortable {
		final AtomicInteger aborts = new AtomicInteger();

		@Override
		public void abort() {
			aborts.incrementAndGet();
		}
	}

	@Test
	void aRequestIsRegisteredUntilItIsClosed() {
		final var request = new S3AwsInFlightRequest(registry, 0);
		assertTrue(registry.contains(request));
		request.close();
		assertFalse(registry.contains(request));
		assertEquals(S3AwsInFlightRequest.End.NONE, request.end());
	}

	@Test
	void timingOutCancelsTheResponseAndTheWaitFailsAsATimeout() throws Exception {
		final var request = new S3AwsInFlightRequest(registry, TimeUnit.SECONDS.toNanos(20));
		final var response = new CompletableFuture<String>();
		final var thrown = new AtomicReference<Throwable>();
		final Thread worker = Thread.ofVirtual().start(() -> {
			try {
				request.await(response);
			} catch (final Throwable failure) {
				thrown.set(failure);
			}
		});
		Thread.sleep(50);
		assertTrue(request.timeOut(), "the first end counts");
		assertFalse(request.timeOut(), "a request is ended once");
		worker.join(TimeUnit.SECONDS.toMillis(10));

		assertFalse(worker.isAlive());
		assertTrue(response.isCancelled());
		assertEquals(S3AwsInFlightRequest.End.TIMED_OUT, request.end());
		assertInstanceOf(CompletionException.class, thrown.get());
		assertInstanceOf(S3AwsRequests.RequestTimeoutException.class, thrown.get().getCause());
		assertTrue(thrown.get().getCause().getMessage().contains("20000 ms"), thrown.get().getCause().getMessage());
	}

	@Test
	void stoppingAbortsTheBodyAndTheReadFailureBecomesAStop() {
		final var request = new S3AwsInFlightRequest(registry, 0);
		final var body = new CountingBody();
		request.body(body);
		assertEquals(0, body.aborts.get());

		assertTrue(request.stop());
		assertEquals(1, body.aborts.get(), "the body is aborted with the request");
		assertFalse(request.stop());
		assertEquals(1, body.aborts.get());

		final var read = new IOException("stream closed");
		final RuntimeException failure = request.failure(read, "Failed to read S3 object data");
		assertInstanceOf(S3AwsRequests.RequestStoppedException.class, failure);
		assertSame(read, failure.getCause());
	}

	@Test
	void aResponseOrABodyGivenAfterTheEndIsEndedAtOnce() {
		final var request = new S3AwsInFlightRequest(registry, 0);
		assertTrue(request.stop());

		final var response = new CompletableFuture<String>();
		assertThrows(S3AwsRequests.RequestStoppedException.class, () -> request.await(response));
		assertTrue(response.isCancelled());

		final var body = new CountingBody();
		request.body(body);
		assertEquals(1, body.aborts.get());
	}

	@Test
	void aFailureOfARequestNobodyEndedStands() {
		final var request = new S3AwsInFlightRequest(registry, 0);
		final var unchecked = new IllegalStateException("as is");
		assertSame(unchecked, request.failure(unchecked, "ignored"));

		final var checked = new IOException("connection reset");
		final RuntimeException wrapped = request.failure(checked, "Failed to read S3 object data");
		assertEquals("Failed to read S3 object data", wrapped.getMessage());
		assertSame(checked, wrapped.getCause());
	}

	@Test
	void theDeadlineRunsFromTheMomentTheRequestIsMade() throws Exception {
		final var request = new S3AwsInFlightRequest(registry, TimeUnit.MILLISECONDS.toNanos(100));
		final long made = System.nanoTime();
		assertFalse(request.expired(made));
		assertTrue(request.expired(made + TimeUnit.MILLISECONDS.toNanos(100)));

		final var never = new CompletableFuture<String>();
		final var failed = assertThrows(CompletionException.class, () -> request.await(never));
		assertInstanceOf(S3AwsRequests.RequestTimeoutException.class, failed.getCause());
		assertTrue(never.isCancelled());

		final var unbounded = new S3AwsInFlightRequest(null, 0);
		assertFalse(unbounded.expired(System.nanoTime() + TimeUnit.DAYS.toNanos(1)));
	}
}
