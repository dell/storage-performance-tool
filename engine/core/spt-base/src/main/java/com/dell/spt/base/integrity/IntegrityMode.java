package com.dell.spt.base.integrity;

/** Supported integrity workload modes. */
public enum IntegrityMode {
	NONE("none"), METADATA("metadata");

	private final String value;

	IntegrityMode(final String value) {
		this.value = value;
	}

	public String value() {
		return value;
	}
}
