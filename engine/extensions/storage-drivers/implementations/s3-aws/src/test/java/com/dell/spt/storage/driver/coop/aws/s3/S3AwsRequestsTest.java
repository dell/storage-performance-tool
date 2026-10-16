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
	void anInterruptCancelsTheRequestAndStaysSet() throws Exception {
		final var request = new CompletableFuture<String>();
		final var thrown = new AtomicReference<Throwable>();
		final var stillInterrupted = new AtomicReference<Boolean>();
		final Thread worker = Thread.ofVirtual().start(() -> {
			try {
				S3AwsRequests.await(request);
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

	@Test
	void cancellingAStageCancelsTheRequestItIsChainedOnto() {
		final var request = new CompletableFuture<String>();
		final var stage = S3AwsRequests.forwardCancellation(request.thenApply(String::length), request);

		stage.cancel(true);

		assertTrue(request.isCancelled());
	}

	@Test
	void aStageThatEndsOtherwiseLeavesTheRequestAlone() {
		final var completed = new CompletableFuture<String>();
		final var completedStage = S3AwsRequests.forwardCancellation(completed.thenApply(String::length), completed);
		completed.complete("done");

		final var failed = new CompletableFuture<String>();
		final var failedStage = S3AwsRequests.forwardCancellation(failed.thenApply(String::length), failed);
		failed.completeExceptionally(new IOException("connection reset"));

		assertEquals(4, completedStage.join());
		assertFalse(completed.isCancelled());
		assertTrue(failedStage.isCompletedExceptionally());
		assertFalse(failed.isCancelled());
	}
}
