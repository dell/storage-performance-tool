package com.dell.spt.base.item.op.list;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.path.PathOperation;
import com.dell.spt.base.storage.driver.ListOptions;

/**
 * Specialized path operation representing an S3 LIST page.
 */
public interface ListOperation<I extends PathItem> extends PathOperation<I> {

	/** @return number of concrete objects returned in this page */
	int objectsListed();

	void objectsListed(int n);

	/** @return aggregated bytes across listed objects (0 when metadata absent) */
	long bytesListed();

	void bytesListed(long bytes);

	/** @return continuation token to resume listing, or null if listing is complete */
	String continuationToken();

	void continuationToken(String token);

	/**
	 * Last fully processed object key within the shard. Used to seed the next request's `start-after`
	 * when dynamic sharding needs to skip already processed keys.
	 */
	String startAfter();

	void startAfter(String startAfter);

	/**
	 * Flag indicating the underlying API reported truncation. Allows the driver to request the next
	 * page while preserving metrics for the current one.
	 */
	boolean truncated();

	void truncated(boolean truncated);

	ListOptions options();

	void options(ListOptions options);

	ListShard shard();

	void shard(ListShard shard);

	/** First object key on this page (used for split LCP). */
	String pageFirstKey();

	void pageFirstKey(String key);
}
