package com.dell.spt.base.item.op;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;

/** Shared existing SPT transient-status policy and bounded full-jitter delay. */
public final class OperationRetryPolicy {
	private static final long BASE_MILLIS = 200;
	private static final long CAP_MILLIS = 1000;

	private OperationRetryPolicy() {}

	public static boolean isRetryableStatus(final Operation.Status status) {
		if (status == null) {
			return false;
		}
		return switch (status) {
		case FAIL_IO, FAIL_TIMEOUT, FAIL_UNKNOWN, RESP_FAIL_UNKNOWN, RESP_FAIL_SVC -> true;
		default -> false;
		};
	}

	/** One-based retry number; the first retry waits uniformly from zero through 200 ms. */
	public static long backoffMillis(final int attempt) {
		return backoffMillis(attempt, bound -> ThreadLocalRandom.current().nextLong(bound));
	}

	static long backoffMillis(final int attempt, final LongUnaryOperator boundedDraw) {
		final int shift = attempt <= 1 ? 0 : Math.min(attempt - 1, 16);
		final long capped = Math.min(BASE_MILLIS << shift, CAP_MILLIS);
		final long delay = boundedDraw.applyAsLong(capped + 1);
		if (delay < 0 || delay > capped) {
			throw new IllegalArgumentException("Retry draw outside its bound");
		}
		return delay;
	}
}
