package com.dell.spt.base.metrics.bucket;

/** How much of a row's expected data it holds, by the precedence incomplete, unmeasured, complete. */
public enum BucketData {
	COMPLETE("complete"), INCOMPLETE("incomplete"), UNMEASURED("unmeasured");

	private final String label;

	BucketData(final String label) {
		this.label = label;
	}

	public String label() {
		return label;
	}
}
