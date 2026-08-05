package com.dell.spt.base.integrity;

/** Protocol-complete result of observing one metadata-mode GET response. */
public record IntegrityVerificationResult(
			IntegrityMetadata expected,
			String actualDigest,
			long actualSize,
			long workerNanos,
			IntegrityFailureReason failureReason,
			String detail) {

	public boolean verified() {
		return failureReason == null;
	}
}
