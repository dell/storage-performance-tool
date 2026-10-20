package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
final class S3AwsRequestsTest {

	@Test
	void aRequestThatEndsGivesWhatJoinGives() {
		assertEquals("done", S3AwsRequests.await(CompletableFuture.completedFuture("done")));

		final var failure = new IOException("connection reset");
		final var failed = assertThrows(
						CompletionException.class, () -> S3AwsRequests.await(CompletableFuture.failedFuture(failure)));
		assertSame(failure, failed.getCause());

		final var cancelled = new CompletableFuture<String>();
		cancelled.cancel(true);
		assertThrows(CancellationException.class, () -> S3AwsRequests.await(cancelled));
	}

	@Test
	void aBoundedWaitCancelsTheRequestWhenItRunsOut() {
		final var request = new CompletableFuture<String>();
		final long bound = TimeUnit.MILLISECONDS.toNanos(100);

		final long start = System.nanoTime();
		final var failed = assertThrows(CompletionException.class, () -> S3AwsRequests.await(request, bound));
		final long waited = System.nanoTime() - start;

		assertInstanceOf(S3AwsRequests.RequestTimeoutException.class, failed.getCause());
		assertTrue(failed.getCause().getMessage().contains("storage.crt.requestTimeoutMs"), failed.getCause().getMessage());
		assertTrue(failed.getCause().getMessage().contains("100 ms"), failed.getCause().getMessage());
		assertTrue(request.isCancelled(), "the request must be cancelled once its wait runs out");
		assertTrue(waited >= bound, "gave up after " + waited + " ns");
		assertFalse(Thread.currentThread().isInterrupted());
	}

	@Test
	void aBoundedWaitGivesWhatJoinGivesWhenTheRequestEndsInTime() {
		final long bound = TimeUnit.SECONDS.toNanos(10);
		assertEquals("done", S3AwsRequests.await(CompletableFuture.completedFuture("done"), bound));

		final var failure = new IOException("connection reset");
		final var failed = assertThrows(
						CompletionException.class, () -> S3AwsRequests.await(CompletableFuture.failedFuture(failure), bound));
		assertSame(failure, failed.getCause());

		final var later = new CompletableFuture<String>();
		Thread.ofVirtual().start(() -> later.complete("eventually"));
		assertEquals("eventually", S3AwsRequests.await(later, bound));
	}

	@Test
	void aZeroBoundWaitsWithoutBound() throws Exception {
		final var request = new CompletableFuture<String>();
		final var worker = Thread.ofVirtual().start(() -> S3AwsRequests.await(request, 0));
		worker.join(300);
		assertTrue(worker.isAlive(), "a zero bound must not give up");
		request.complete("done");
		worker.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse(worker.isAlive());
	}

	@Test
	void anInterruptCancelsTheRequestAndStaysSet() throws Exception {
		final var request = new CompletableFuture<String>();
		final var thrown = new AtomicReference<Throwable>();
		final var stillInterrupted = new AtomicReference<Boolean>();
		final Thread worker = Thread.ofVirtual().start(() -> {
			try {
				// A bounded wait, so that the interrupt is what ends it.
				S3AwsRequests.await(request, TimeUnit.SECONDS.toNanos(20));
			} catch (final Throwable failure) {
				thrown.set(failure);
			}
			stillInterrupted.set(Thread.currentThread().isInterrupted());
		});
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (worker.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
			Thread.sleep(1);
		}

		worker.interrupt();
		worker.join(TimeUnit.SECONDS.toMillis(10));

		assertFalse(worker.isAlive(), "the wait must end");
		assertInstanceOf(CancellationException.class, thrown.get());
		assertInstanceOf(InterruptedException.class, thrown.get().getCause());
		assertTrue(request.isCancelled());
		assertTrue(stillInterrupted.get(), "the interrupt must stay set, so that the worker stops");
	}
}
