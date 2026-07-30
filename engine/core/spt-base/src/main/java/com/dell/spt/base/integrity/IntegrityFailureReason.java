package com.dell.spt.base.integrity;

/** Stable machine-readable integrity failure reasons used by public result artifacts. */
public enum IntegrityFailureReason {
	METADATA_MISSING("metadata_missing"), METADATA_INVALID("metadata_invalid"), ALGORITHM_UNSUPPORTED("algorithm_unsupported"), SIZE_MISMATCH("size_mismatch"), DIGEST_MISMATCH("digest_mismatch");

	private final String value;

	IntegrityFailureReason(final String value) {
		this.value = value;
	}

	public String value() {
		return value;
	}

	@Override
	public String toString() {
		return value;
	}
}
