package com.dell.spt.base.item.op.list;

/** One current object returned by a LIST page for integrity discovery. */
public record ListedObject(String key, long size) {

	public ListedObject {
		if (key == null || key.isEmpty()) {
			throw new IllegalArgumentException("listed object key must not be empty");
		}
		if (size < 0) {
			throw new IllegalArgumentException("listed object size must be nonnegative");
		}
	}
}
