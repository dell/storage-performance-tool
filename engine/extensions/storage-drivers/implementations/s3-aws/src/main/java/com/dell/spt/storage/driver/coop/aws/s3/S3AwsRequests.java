package com.dell.spt.storage.driver.coop.aws.s3;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Waiting for S3 requests on the driver's workers. The driver stops a worker by interrupting it,
 * after the load step has waited for the requests in flight, and a request the worker is still
 * waiting for is then cancelled. Cancelling the future an S3 client returned aborts the request in
 * the CRT. Closing the client does not end it: its future is left incomplete, and a worker waiting
 * for it would wait forever.
 */
final class S3AwsRequests {

	private S3AwsRequests() {}

	/**
	 * Waits for a request as {@link CompletableFuture#join()} does, except that an interrupt
	 * cancels the request and ends the wait.
	 *
	 * @throws CancellationException when the thread is interrupted, whose interrupt stays set
	 */
	static <T> T await(final CompletableFuture<T> request) {
		try {
			return request.get();
		} catch (final InterruptedException interrupted) {
			request.cancel(true);
			Thread.currentThread().interrupt();
			final var cancelled = new CancellationException("the request was cancelled because the driver stopped");
			cancelled.initCause(interrupted);
			throw cancelled;
		} catch (final ExecutionException failed) {
			throw new CompletionException(failed.getCause());
		}
	}

	/**
	 * Makes a stage chained onto a request cancel the request when the stage is cancelled, which a
	 * dependent stage does not do by itself.
	 *
	 * @return the stage
	 */
	static <T> CompletableFuture<T> forwardCancellation(final CompletableFuture<T> stage, final CompletableFuture<?> request) {
		stage.whenComplete((ignored, failure) -> {
			if (failure instanceof CancellationException) {
				request.cancel(true);
			}
		});
		return stage;
	}
}
