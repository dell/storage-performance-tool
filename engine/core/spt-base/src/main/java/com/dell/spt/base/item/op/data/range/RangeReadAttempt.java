package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Bounded, serialized response state for one transport attempt. The instance itself is its token.
 * Callbacks retain this instance, never look up a mutable operation's current attempt. This class
 * owns no buffers, channels or output callbacks; adapters release their own attempt resources.
 */
public final class RangeReadAttempt {
	public enum Category {
		SUCCESS, HTTP, VALIDATION, TRANSPORT, UNRESOLVED
	}

	public record Timing(long dispatch, long requestComplete, long responseHeaders, long firstBody,
					long responseComplete) {
		public static final Timing EMPTY = new Timing(0, 0, 0, 0, 0);
	}

	public record Outcome(Category category, Operation.Status status, Integer httpStatus,
					RangeResponseValidator.Failure validationFailure, long receivedBytes,
					boolean receivedBytesOverflow, boolean requestHandedOff, Timing timing) {}

	private final OperationLifecycle lifecycle;
	private final ByteRange range;
	private final LongSupplier clock;
	private long dispatchTime, requestCompleteTime, responseHeadersTime, firstBodyTime, responseCompleteTime;
	private final RangeResponseValidator validator;
	private boolean handedOff;
	private boolean cancelledBeforeHandoff;
	private Integer httpStatus;
	private Outcome outcome;

	public RangeReadAttempt(final OperationLifecycle lifecycle, final ByteRange range) {
		this(lifecycle, range, RangeReadAttempt::clockMicros);
	}

	RangeReadAttempt(final OperationLifecycle lifecycle, final ByteRange range, final LongSupplier clock) {
		this.clock = Objects.requireNonNull(clock);
		this.lifecycle = Objects.requireNonNull(lifecycle);
		this.range = Objects.requireNonNull(range);
		validator = new RangeResponseValidator(range);
	}

	public OperationLifecycle lifecycle() {
		return lifecycle;
	}

	public ByteRange range() {
		return range;
	}

	/** True exactly once at the transport execute/write boundary, not at request construction. */
	public synchronized boolean requestHandoff() {
		if (outcome != null || cancelledBeforeHandoff || handedOff) {
			return false;
		}
		dispatchTime = clock.getAsLong();
		handedOff = true;
		return true;
	}

	static long clockMicros() {
		return Operation.START_OFFSET_MICROS + System.nanoTime() / 1000;
	}

	/** Optional actual request-write completion; absent observations do not produce latency samples. */
	public synchronized boolean requestComplete() {
		if (!handedOff || cancelledBeforeHandoff || outcome != null || requestCompleteTime != 0
						|| responseHeadersTime != 0) {
			return false;
		}
		requestCompleteTime = clock.getAsLong();
		return true;
	}

	/** The adapter supplies its existing HTTP status mapping, preserving driver compatibility. */
	public synchronized boolean headers(final int status, final Operation.Status mappedStatus,
					final List<String> ranges, final List<String> lengths, final List<String> types,
					final boolean transferEncoding) {
		if (outcome != null || cancelledBeforeHandoff) {
			return false;
		}
		requireHandoff();
		if ((status < 200 || status >= 300)
						&& (mappedStatus == null || mappedStatus == Operation.Status.SUCC
										|| mappedStatus == Operation.Status.PENDING)) {
			throw new IllegalArgumentException("HTTP failure requires a failure status mapping");
		}
		if (responseHeadersTime == 0) {
			responseHeadersTime = clock.getAsLong();
		}
		httpStatus = status;
		if (validator.headers(status, ranges, lengths, types, transferEncoding)) {
			return true;
		}
		if (validator.failure() == RangeResponseValidator.Failure.HTTP_STATUS) {
			retain(Category.HTTP, mappedStatus);
		} else {
			retain(Category.VALIDATION, Operation.Status.RESP_FAIL_CLIENT);
		}
		return false;
	}

	public synchronized boolean bodyBytes(final int count) {
		if (outcome != null || cancelledBeforeHandoff) {
			return false;
		}
		requireHandoff();
		if (count > 0 && firstBodyTime == 0) {
			firstBodyTime = clock.getAsLong();
		}
		if (validator.bodyBytes(count)) {
			return true;
		}
		retain(Category.VALIDATION, Operation.Status.RESP_FAIL_CLIENT);
		return false;
	}

	/** Exact bytes alone are insufficient: successful framing completion is required. */
	public synchronized boolean finish(final boolean framingValid) {
		if (outcome != null || cancelledBeforeHandoff) {
			return false;
		}
		requireHandoff();
		responseCompleteTime = clock.getAsLong();
		if (validator.finish(framingValid)) {
			retain(Category.SUCCESS, Operation.Status.SUCC);
		} else {
			retain(Category.VALIDATION, Operation.Status.RESP_FAIL_CLIENT);
		}
		return true;
	}

	/** Also covers a rejected submission before transport handoff, with zero requests. */
	public synchronized boolean transportFailure(final Operation.Status status) {
		if (outcome != null || cancelledBeforeHandoff) {
			return false;
		}
		if (status != Operation.Status.FAIL_IO && status != Operation.Status.FAIL_TIMEOUT
						&& status != Operation.Status.FAIL_UNKNOWN) {
			throw new IllegalArgumentException("Expected a transport failure status");
		}
		retain(Category.TRANSPORT, status);
		return true;
	}

	/** Only an indeterminate dispatched attempt is unresolved; a known outcome always wins. */
	public synchronized boolean unresolved() {
		if (outcome != null || cancelledBeforeHandoff || !handedOff) {
			return false;
		}
		retain(Category.UNRESOLVED, null);
		return true;
	}

	/** Fences a queued attempt without inventing a failed request or response outcome. */
	public synchronized boolean cancelBeforeHandoff() {
		if (handedOff || outcome != null || cancelledBeforeHandoff) {
			return false;
		}
		cancelledBeforeHandoff = true;
		return true;
	}

	public synchronized boolean wasHandedOff() {
		return handedOff;
	}

	public synchronized Outcome outcome() {
		return outcome;
	}

	private void requireHandoff() {
		if (!handedOff) {
			throw new IllegalStateException("Response arrived before transport handoff");
		}
	}

	private void retain(final Category category, final Operation.Status status) {
		outcome = new Outcome(category, status, httpStatus, validator.failure(),
						validator.receivedBytes(), validator.receivedBytesOverflow(), handedOff,
						new Timing(dispatchTime, requestCompleteTime, responseHeadersTime, firstBodyTime, responseCompleteTime));
	}
}
