package com.dell.spt.storage.driver.coop.aws.s3;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import software.amazon.awssdk.http.Abortable;

/**
 * One request a worker is making, from the moment the driver makes it until its response has been
 * applied, a GET's body included. It is what the driver can end from outside the worker: the SDK
 * future while the worker waits for the response, and the response stream while the worker reads a
 * body. It records why it was ended, so that the worker fails its operation accordingly: as a
 * timeout when the request's deadline passed, as interrupted when the load step stopped.
 *
 * <p>A request is registered with the driver's set of requests in flight when it is made and
 * removed when it is {@linkplain #close() closed}, on the worker, whatever the outcome.
 */
final class S3AwsInFlightRequest implements AutoCloseable {

	/** Why the driver ended the request, if it did. */
	enum End {
		NONE, TIMED_OUT, STOPPED
	}

	private final Set<S3AwsInFlightRequest> registry;
	private final long timeoutNanos;
	private final long deadlineNanos;
	private CompletableFuture<?> response;
	private Abortable body;
	private volatile End end = End.NONE;

	/**
	 * @param registry the driver's requests in flight, which this request joins until it is
	 *     closed, or null for a request nothing watches
	 * @param timeoutNanos how long the request may take in all, or 0 for no deadline
	 */
	S3AwsInFlightRequest(final Set<S3AwsInFlightRequest> registry, final long timeoutNanos) {
		this.registry = registry;
		this.timeoutNanos = timeoutNanos;
		this.deadlineNanos = timeoutNanos > 0 ? System.nanoTime() + timeoutNanos : 0;
		if (registry != null) {
			registry.add(this);
		}
	}

	/** Whether the request has a deadline and it had passed at the given time. */
	boolean expired(final long nowNanos) {
		return deadlineNanos != 0 && nowNanos - deadlineNanos >= 0;
	}

	End end() {
		return end;
	}

	/**
	 * Waits on the worker for the response of this request, for what is left of the deadline.
	 *
	 * @throws CompletionException with a {@link S3AwsRequests.RequestTimeoutException} cause when
	 *     the deadline passes, after the request has been cancelled
	 * @throws S3AwsRequests.RequestStoppedException when the request was cancelled because the
	 *     step or the worker stopped
	 */
	<T> T await(final CompletableFuture<T> future) {
		synchronized (this) {
			response = future;
			if (end != End.NONE) {
				future.cancel(true);
			}
		}
		final long remainingNanos = deadlineNanos == 0 ? 0 : Math.max(1, deadlineNanos - System.nanoTime());
		try {
			return S3AwsRequests.await(future, remainingNanos);
		} catch (final CancellationException cancelled) {
			throw failure(cancelled, null);
		}
	}

	/** Hands the response stream the worker is about to read to this request, to be aborted with it. */
	synchronized void body(final Abortable stream) {
		body = stream;
		if (end != End.NONE) {
			stream.abort();
		}
	}

	/**
	 * Ends the request because its deadline passed.
	 *
	 * @return whether this call ended it
	 */
	boolean timeOut() {
		return end(End.TIMED_OUT);
	}

	/**
	 * Ends the request because the load step stopped.
	 *
	 * @return whether this call ended it
	 */
	boolean stop() {
		return end(End.STOPPED);
	}

	private synchronized boolean end(final End reason) {
		if (end != End.NONE) {
			return false;
		}
		end = reason;
		if (response != null) {
			response.cancel(true);
		}
		if (body != null) {
			body.abort();
		}
		return true;
	}

	/**
	 * The failure to throw for one the worker met while waiting for or reading the response. When
	 * this request was ended from outside, the failure is the consequence and the end is the cause;
	 * otherwise the failure stands, wrapped with the message when it is checked.
	 */
	RuntimeException failure(final Throwable failure, final String message) {
		switch (end) {
		case TIMED_OUT: {
			final var timedOut = new S3AwsRequests.RequestTimeoutException(timeoutNanos);
			timedOut.addSuppressed(failure);
			return new CompletionException(timedOut);
		}
		case STOPPED: {
			final var stopped = new S3AwsRequests.RequestStoppedException(
							"the request was cancelled because the load step stopped");
			stopped.initCause(failure);
			return stopped;
		}
		default:
			if (failure instanceof RuntimeException unchecked) {
				return unchecked;
			}
			return new RuntimeException(message, failure);
		}
	}

	@Override
	public void close() {
		if (registry != null) {
			registry.remove(this);
		}
	}
}
