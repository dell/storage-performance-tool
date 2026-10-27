package com.dell.spt.storage.driver.coop.aws.s3;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Waiting for S3 requests on the driver's workers. The driver stops a worker by interrupting it,
 * after the load step has waited for the requests in flight, and a request the worker is still
 * waiting for is then cancelled. Cancelling the future an S3 client returned aborts the request in
 * the CRT. Closing the client does not end it: its future is left incomplete, and a worker waiting
 * for it would wait forever.
 *
 * <p>A request can also be lost inside the CRT with nothing left to complete or fail it: no
 * connection, no timer and no pending read. The connection monitor cannot see such a request, so
 * the wait is bounded on this side too. A request that outlives its bound is cancelled and fails as
 * a timeout, which costs one failed operation instead of the worker and every permit queued
 * behind it.
 *
 * <p>A worker changes its operation only after the wait returns the response, never from a
 * callback on the client's threads, so a request cancelled at stop cannot change the operation
 * after the worker has given it up.
 */
final class S3AwsRequests {

	private S3AwsRequests() {}

	/** The failure of a request that was cancelled because its wait ran out. */
	static final class RequestTimeoutException extends TimeoutException {

		private static final long serialVersionUID = 1L;

		RequestTimeoutException(final long timeoutNanos) {
			super("the request did not complete within storage.crt." + S3AwsClientTimeouts.REQUEST_TIMEOUT_KEY
							+ " (" + TimeUnit.NANOSECONDS.toMillis(timeoutNanos) + " ms) and was cancelled");
		}
	}

	/** The failure of a request that was cancelled because the load step or the worker stopped. */
	static final class RequestStoppedException extends CancellationException {

		private static final long serialVersionUID = 1L;

		RequestStoppedException(final String message) {
			super(message);
		}
	}

	/**
	 * Waits for a request as {@link CompletableFuture#join()} does, except that an interrupt
	 * cancels the request and ends the wait.
	 *
	 * @throws RequestStoppedException when the thread is interrupted, whose interrupt stays set
	 */
	static <T> T await(final CompletableFuture<T> request) {
		return await(request, 0);
	}

	/**
	 * Waits for a request for at most the given time, as {@link CompletableFuture#join()} does
	 * otherwise, except that an interrupt cancels the request and ends the wait.
	 *
	 * @param timeoutNanos how long to wait, or 0 to wait without bound
	 * @throws RequestStoppedException when the thread is interrupted, whose interrupt stays set
	 * @throws CompletionException with a {@link RequestTimeoutException} cause when the wait runs
	 *     out, after the request has been cancelled
	 */
	static <T> T await(final CompletableFuture<T> request, final long timeoutNanos) {
		try {
			return timeoutNanos > 0 ? request.get(timeoutNanos, TimeUnit.NANOSECONDS) : request.get();
		} catch (final InterruptedException interrupted) {
			request.cancel(true);
			Thread.currentThread().interrupt();
			final var stopped = new RequestStoppedException("the request was cancelled because the driver stopped");
			stopped.initCause(interrupted);
			throw stopped;
		} catch (final TimeoutException expired) {
			request.cancel(true);
			throw new CompletionException(new RequestTimeoutException(timeoutNanos));
		} catch (final ExecutionException failed) {
			throw new CompletionException(failed.getCause());
		}
	}
}
