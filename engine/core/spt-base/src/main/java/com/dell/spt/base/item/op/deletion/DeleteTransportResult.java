package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.op.Operation.Status;
import java.util.List;

/** Neutral storage-adapter result, either target responses or one request-level failure. */
public record DeleteTransportResult(
			List<DeleteTransportTargetResult> targetResults,
			Status failureStatus,
			String failureMessage) {

	public DeleteTransportResult {
		targetResults = targetResults == null ? null : List.copyOf(targetResults);
		if (failureStatus != null && !isFailure(failureStatus)) {
			throw new IllegalArgumentException("DELETE transport failure requires a failure status");
		}
		if (failureStatus != null && targetResults != null && !targetResults.isEmpty()) {
			throw new IllegalArgumentException(
						"A DELETE transport result cannot contain target responses and a request failure");
		}
	}

	/** Creates a complete successful transport result for the supplied targets. */
	public static DeleteTransportResult success(final List<DeleteTarget> targets) {
		return new DeleteTransportResult(
					targets.stream().map(DeleteTransportTargetResult::succeeded).toList(), null, null);
	}

	/** Creates one request-level operational transport failure. */
	public static DeleteTransportResult failure(final Status status, final String message) {
		return new DeleteTransportResult(List.of(), status, message);
	}

	private static boolean isFailure(final Status status) {
		return status != null
					&& switch (status) {
					case FAIL_UNKNOWN,
							FAIL_IO,
							FAIL_TIMEOUT,
							RESP_FAIL_UNKNOWN,
							RESP_FAIL_CLIENT,
							RESP_FAIL_SVC,
							RESP_FAIL_NOT_FOUND,
							RESP_FAIL_AUTH,
							RESP_FAIL_CORRUPT,
							RESP_FAIL_SPACE -> true;
					default -> false;
					};
	}
}
