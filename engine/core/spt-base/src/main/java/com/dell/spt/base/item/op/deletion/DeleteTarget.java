package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import java.util.Objects;

/** Immutable canonical identity retained by one standalone DELETE request. */
public final class DeleteTarget {

	private final IntegrityManifestDataItem item;
	private final String bucket;
	private final String key;
	private final long size;
	private final String versionId;
	private final DeleteTargetIdentity identity;
	private final long selectionIndex;

	/** Snapshots the canonical identity fields from one manifest item. */
	public DeleteTarget(final IntegrityManifestDataItem item) {
		this(item, -1);
	}

	/** Snapshots one canonical identity and its zero-based position in the frozen selection. */
	public DeleteTarget(final IntegrityManifestDataItem item, final long selectionIndex) {
		this.item = Objects.requireNonNull(item, "DELETE target item");
		this.bucket = requireText(item.bucket(), "DELETE target bucket");
		this.key = requireText(item.name(), "DELETE target key");
		this.size = item.size();
		if (size < 0) {
			throw new IllegalArgumentException("DELETE target size must not be negative");
		}
		this.versionId = emptyToNull(item.versionId());
		this.identity = new DeleteTargetIdentity(key, versionId);
		if (selectionIndex < -1) {
			throw new IllegalArgumentException("DELETE target selection index must be nonnegative or unavailable");
		}
		this.selectionIndex = selectionIndex;
	}

	/** Returns the manifest item retained only for the inherited compatibility projection. */
	public IntegrityManifestDataItem item() {
		return item;
	}

	/** Returns the canonical bucket snapshot. */
	public String bucket() {
		return bucket;
	}

	/** Returns the canonical object-key snapshot. */
	public String key() {
		return key;
	}

	/** Returns the canonical object-size snapshot. */
	public long size() {
		return size;
	}

	/** Returns the exact requested version, or {@code null} for current-key deletion. */
	public String versionId() {
		return versionId;
	}

	/** Returns the zero-based frozen-selection position, or {@code -1} for compatibility callers. */
	public long selectionIndex() {
		return selectionIndex;
	}

	DeleteTargetIdentity identity() {
		return identity;
	}

	static String emptyToNull(final String value) {
		return value == null || value.isEmpty() ? null : value;
	}

	private static String requireText(final String value, final String label) {
		if (value == null || value.isEmpty()) {
			throw new IllegalArgumentException(label + " must not be empty");
		}
		return value;
	}

}

record DeleteTargetIdentity(String key, String versionId) {}
