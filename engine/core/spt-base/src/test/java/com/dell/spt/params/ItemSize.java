package com.dell.spt.params;

import com.github.akurilov.commons.system.SizeInBytes;

/** Created by andrey on 11.08.17. */
public enum ItemSize {
	EMPTY(0L), SMALL("10KB"), MEDIUM("1MB"), LARGE("100MB"), HUGE("10GB");

	public static final String KEY_ENV = "ITEM_SIZE";

	private final String sizeSpec;
	private final Long sizeBytes;

	ItemSize(final String sizeSpec) {
		this.sizeSpec = sizeSpec;
		this.sizeBytes = null;
	}

	ItemSize(final long sizeBytes) {
		this.sizeSpec = null;
		this.sizeBytes = sizeBytes;
	}

	public final SizeInBytes getValue() {
		return sizeSpec != null ? new SizeInBytes(sizeSpec) : new SizeInBytes(sizeBytes);
	}
}
