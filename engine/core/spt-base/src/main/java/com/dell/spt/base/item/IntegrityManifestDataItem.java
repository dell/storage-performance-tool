package com.dell.spt.base.item;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/** Data item reconstructed from the canonical integrity manifest without numeric key parsing. */
public final class IntegrityManifestDataItem extends DataItemImpl implements VersionedItem {

	private String bucket;
	private String versionId;

	public IntegrityManifestDataItem() {
		super();
	}

	public IntegrityManifestDataItem(
					final String bucket, final String key, final long size, final String versionId) {
		super(requireKey(key), 0, requireSize(size));
		this.bucket = requireBucket(bucket);
		this.versionId = emptyToNull(versionId);
	}

	public String bucket() {
		return bucket;
	}

	public void bucket(final String bucket) {
		this.bucket = requireBucket(bucket);
	}

	@Override
	public String versionId() {
		return versionId;
	}

	@Override
	public void versionId(final String versionId) {
		this.versionId = emptyToNull(versionId);
	}

	@Override
	public void writeExternal(final ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeUTF(bucket);
		out.writeBoolean(versionId != null);
		if (versionId != null) {
			out.writeUTF(versionId);
		}
	}

	@Override
	public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		bucket = requireBucket(in.readUTF());
		versionId = in.readBoolean() ? in.readUTF() : null;
	}

	private static String requireBucket(final String bucket) {
		if (bucket == null || bucket.isEmpty()) {
			throw new IllegalArgumentException("integrity manifest bucket must not be empty");
		}
		return bucket;
	}

	private static String requireKey(final String key) {
		if (key == null || key.isEmpty()) {
			throw new IllegalArgumentException("integrity manifest key must not be empty");
		}
		return key;
	}

	private static long requireSize(final long size) {
		if (size < 0) {
			throw new IllegalArgumentException("integrity manifest size must be nonnegative");
		}
		return size;
	}

	private static String emptyToNull(final String value) {
		return value == null || value.isEmpty() ? null : value;
	}
}
