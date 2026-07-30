package com.dell.spt.base.item;

/**
 * Item carrying an optional exact storage version without encoding it into the item name.
 *
 * <p>A {@code null} or empty requested version means current-version semantics.
 */
public interface VersionedItem extends Item {

	String versionId();

	void versionId(String versionId);
}
