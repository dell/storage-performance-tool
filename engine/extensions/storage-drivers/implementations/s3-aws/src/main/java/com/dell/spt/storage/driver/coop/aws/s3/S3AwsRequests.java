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
 *
 * <p>A worker changes its operation only after the wait returns the response, never from a
 * callback on the client's threads, so a request cancelled at stop cannot change the operation
 * after the worker has given it up.
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
}
