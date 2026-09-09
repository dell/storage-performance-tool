package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import java.util.List;
import java.util.Objects;

/**
 * Bounded, serialized response state for one transport attempt. The instance itself is its token.
 * Callbacks retain this instance, never look up a mutable operation's current attempt. This class
 * owns no buffers, channels or output callbacks; adapters release their own attempt resources.
 */
public final class RangeReadAttempt {
	public enum Category {
		SUCCESS, HTTP, VALIDATION, TRANSPORT, UNRESOLVED
	}

	public record Outcome(Category category, Operation.Status status, Integer httpStatus,
					RangeResponseValidator.Failure validationFailure, long receivedBytes,
					boolean receivedBytesOverflow, boolean requestHandedOff) {}

	private final OperationLifecycle lifecycle;
	private final ByteRange range;
	private final RangeResponseValidator validator;
	private boolean handedOff;
	private Integer httpStatus;
	private Outcome outcome;

	public RangeReadAttempt(final OperationLifecycle lifecycle, final ByteRange range) {
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
		if (outcome != null || handedOff) {
			return false;
		}
		handedOff = true;
		return true;
	}

	/** The adapter supplies its existing HTTP status mapping, preserving driver compatibility. */
	public synchronized boolean headers(final int status, final Operation.Status mappedStatus,
					final List<String> ranges, final List<String> lengths, final List<String> types,
					final boolean transferEncoding) {
		if (outcome != null) {
			return false;
		}
		requireHandoff();
		if ((status < 200 || status >= 300)
						&& (mappedStatus == null || mappedStatus == Operation.Status.SUCC
										|| mappedStatus == Operation.Status.PENDING)) {
			throw new IllegalArgumentException("HTTP failure requires a failure status mapping");
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
		if (outcome != null) {
			return false;
		}
		requireHandoff();
		if (validator.bodyBytes(count)) {
			return true;
		}
		retain(Category.VALIDATION, Operation.Status.RESP_FAIL_CLIENT);
		return false;
	}

	/** Exact bytes alone are insufficient: successful framing completion is required. */
	public synchronized boolean finish(final boolean framingValid) {
		if (outcome != null) {
			return false;
		}
		requireHandoff();
		if (validator.finish(framingValid)) {
			retain(Category.SUCCESS, Operation.Status.SUCC);
		} else {
			retain(Category.VALIDATION, Operation.Status.RESP_FAIL_CLIENT);
		}
		return true;
	}

	/** Also covers a rejected submission before transport handoff, with zero requests. */
	public synchronized boolean transportFailure(final Operation.Status status) {
		if (outcome != null) {
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
		if (outcome != null || !handedOff) {
			return false;
		}
		retain(Category.UNRESOLVED, null);
		return true;
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
						validator.receivedBytes(), validator.receivedBytesOverflow(), handedOff);
	}
}
