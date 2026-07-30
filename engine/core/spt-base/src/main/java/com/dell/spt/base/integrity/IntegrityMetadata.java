package com.dell.spt.base.integrity;

/** Canonical version 1 whole-object integrity metadata. */
public record IntegrityMetadata(String version, String algorithm, String digest, long size) {

	public IntegrityMetadata {
		if (version == null || algorithm == null || digest == null) {
			throw new IllegalArgumentException("integrity metadata values must not be null");
		}
		if (size < 0) {
			throw new IllegalArgumentException("integrity metadata size must be nonnegative");
		}
	}
}
