package com.dell.spt.base.integrity;

/** Indicates that SPT integrity metadata is absent or violates the versioned contract. */
public final class IntegrityMetadataException extends Exception {

	private final IntegrityFailureReason reason;

	public IntegrityMetadataException(final IntegrityFailureReason reason, final String message) {
		super(message);
		this.reason = reason;
	}

	public IntegrityFailureReason reason() {
		return reason;
	}
}
