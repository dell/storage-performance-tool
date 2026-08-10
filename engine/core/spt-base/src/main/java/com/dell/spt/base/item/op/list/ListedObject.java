package com.dell.spt.base.item.op.list;

/** One exact object identity returned by a LIST page for integrity discovery. */
public record ListedObject(String key, long size, String versionId) {

	public ListedObject {
		if (key == null || key.isEmpty()) {
			throw new IllegalArgumentException("listed object key must not be empty");
		}
		if (size < 0) {
			throw new IllegalArgumentException("listed object size must be nonnegative");
		}
		versionId = versionId == null || versionId.isEmpty() ? null : versionId;
	}

	public ListedObject(final String key, final long size) {
		this(key, size, null);
	}
}
