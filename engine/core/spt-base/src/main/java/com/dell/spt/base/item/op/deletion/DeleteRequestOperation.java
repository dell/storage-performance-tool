package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.Operation;

/** One first-class standalone DELETE request operation. */
public interface DeleteRequestOperation extends Operation<IntegrityManifestDataItem> {
	/**
	 * Starts one provider transport attempt inside this logical invocation.
	 *
	 * <p>Transparent provider retries retain the first request marker but replace response timing.
	 */
	default void beginTransportAttempt() {}

	/** Clears incomplete or retryable response timing for the current transport attempt. */
	default void failTransportAttempt() {}

	/**
	 * Records the first request-byte transport submission marker.
	 *
	 * <p>The compatibility default leaves extension-supplied implementations without a sample.
	 */
	default void markRequestFirstByteSent() {}

	/**
	 * Records an exact transport-clock interval from the first request byte sent through the first
	 * response byte received.
	 *
	 * <p>Both values must be positive nanosecond timestamps from the same monotonic transport clock.
	 * Implementations retain the first request timestamp across transparent provider retries. The
	 * compatibility default leaves extension-supplied implementations without this direct sample.
	 */
	default void recordTransportRequestTiming(
					final long requestFirstByteNanos, final long responseFirstByteNanos) {}

	/**
	 * Returns the first request-byte transport submission time in monotonic epoch-relative
	 * microseconds, or {@code 0} when the driver did not expose that marker.
	 */
	default long requestFirstByteTime() {
		return 0;
	}

	/**
	 * Returns request latency in microseconds, or {@code 0} when a complete transport sample is
	 * unavailable.
	 *
	 * <p>The compatibility default derives the established marker interval. A transport with its own
	 * monotonic clock may override this method with a direct interval.
	 */
	default long transportRequestLatency() {
		final long requestStart = requestFirstByteTime();
		final long responseStart = responseFirstByteTime();
		return requestStart > 0 && responseStart > requestStart
						? responseStart - requestStart
						: 0;
	}

	/**
	 * Records the first response-byte transport callback and starts response timing.
	 *
	 * <p>The compatibility default preserves extension implementations while leaving the dedicated
	 * transport sample unavailable.
	 */
	default void markResponseFirstByteReceived() {
		startResponse();
	}

	/** Returns the first response-byte transport callback time, or {@code 0} when unavailable. */
	default long responseFirstByteTime() {
		return 0;
	}

	/**
	 * Records completion of the response transport stream.
	 *
	 * <p>The compatibility default only finishes an already-started response, so clients that do not
	 * expose a response stream cannot manufacture a last-byte sample.
	 */
	default void markResponseLastByteReceived() {
		if (respTimeStart() != 0) {
			finishResponse();
		}
	}

	/** Returns the complete immutable logical request; {@link #item()} is only a projection. */
	DeleteRequest deleteRequest();

	/** Reconciles and completes this logical request exactly once. */
	void completeDelete(DeleteTransportResult transportResult);

	/** Returns the ordered per-target result, or {@code null} before completion. */
	DeleteRequestResult deleteResult();

	/** Returns a completed-operation snapshot retaining the full request and result. */
	@Override
	DeleteRequestOperation result();
}
