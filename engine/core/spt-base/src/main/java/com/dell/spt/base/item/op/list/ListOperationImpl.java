package com.dell.spt.base.item.op.list;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.path.PathOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;

/** Default implementation for {@link ListOperation}. */
public class ListOperationImpl<I extends PathItem> extends PathOperationImpl<I>
				implements ListOperation<I> {

	private int objectsListed;
	private long bytesListed;
	private String continuationToken;
	private boolean truncated;
	private String startAfter;
	private ListOptions options = ListOptions.DEFAULT;
	private ListShard shard;
	private String pageFirstKey;

	public ListOperationImpl() {
		super();
	}

	public ListOperationImpl(
					final int originIndex, final OpType opType, final I item, final Credential credential) {
		super(originIndex, opType, item, credential);
	}

	protected ListOperationImpl(final ListOperationImpl<I> other) {
		super(other);
		this.objectsListed = other.objectsListed;
		this.bytesListed = other.bytesListed;
		this.continuationToken = other.continuationToken;
		this.truncated = other.truncated;
		this.startAfter = other.startAfter;
		this.options = other.options;
		this.shard = other.shard;
	}

	@Override
	public ListOperationImpl<I> result() {
		return new ListOperationImpl<>(this);
	}

	@Override
	public int objectsListed() {
		return objectsListed;
	}

	@Override
	public void objectsListed(final int n) {
		this.objectsListed = n;
	}

	@Override
	public long bytesListed() {
		return bytesListed;
	}

	@Override
	public void bytesListed(final long bytes) {
		this.bytesListed = bytes;
	}

	@Override
	public String continuationToken() {
		return continuationToken;
	}

	@Override
	public void continuationToken(final String token) {
		this.continuationToken = token;
		if (shard != null) {
			shard = shard.withContinuationToken(token);
		}
	}

	@Override
	public String startAfter() {
		return startAfter;
	}

	@Override
	public void startAfter(final String startAfter) {
		this.startAfter = startAfter;
		if (shard != null) {
			shard = shard.withStartAfter(startAfter).withLastKey(startAfter);
		}
	}

	@Override
	public boolean truncated() {
		return truncated;
	}

	@Override
	public void truncated(final boolean truncated) {
		this.truncated = truncated;
	}

	@Override
	public ListOptions options() {
		return options;
	}

	@Override
	public void options(final ListOptions options) {
		this.options = options == null ? ListOptions.DEFAULT : options;
	}

	@Override
	public void reset() {
		super.reset();
		objectsListed = 0;
		bytesListed = 0;
		truncated = false;
		countBytesDone(0);
		pageFirstKey = null;
	}

	public ListShard shard() {
		return shard;
	}

	public void shard(final ListShard shard) {
		if (shard == null) {
			this.shard = null;
			return;
		}
		ListShard updated = shard;
		final String shardToken = updated.continuationToken();
		if (shardToken == null) {
			if (continuationToken != null) {
				updated = updated.withContinuationToken(continuationToken);
			}
		} else {
			this.continuationToken = shardToken;
		}
		final String shardStartAfter = updated.startAfter();
		if (shardStartAfter == null) {
			if (startAfter != null) {
				updated = updated.withStartAfter(startAfter).withLastKey(startAfter);
			}
		} else {
			this.startAfter = shardStartAfter;
			updated = updated.withLastKey(shardStartAfter);
		}
		this.shard = updated;
	}

	@Override
	public String pageFirstKey() {
		return pageFirstKey;
	}

	@Override
	public void pageFirstKey(final String key) {
		this.pageFirstKey = key;
	}
}
