package com.dell.spt.base.item.io;

import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import java.util.List;

/** Emits one {@link PathItem} per shard so each worker can drive an independent LIST prefix. */
public final class ShardedListPathItemInput<I extends PathItem> implements Input<I> {

	private final ItemFactory<I> itemFactory;
	private final List<ListShard> shards;
	private int emitted;

	public ShardedListPathItemInput(final ItemFactory<I> itemFactory, final List<ListShard> shards) {
		this.itemFactory = itemFactory;
		this.shards = List.copyOf(shards);
	}

	@Override
	public I get() {
		if (emitted >= shards.size()) {
			return null;
		}
		final var shard = shards.get(emitted++);
		if (Loggers.MSG.isDebugEnabled()) {
			Loggers.MSG.debug(
							"LIST shard seed emitted prefix={} kind={} delimiter={}",
							shard.prefix(), shard.kind(), shard.delimiter());
		}
		return itemFactory.getItem(shard.prefix(), 0, 0);
	}

	@Override
	public int get(final List<I> buffer, final int limit) {
		if (limit <= 0 || emitted >= shards.size()) {
			return 0;
		}
		int count = Math.min(limit, shards.size() - emitted);
		for (var i = 0; i < count; i++) {
			final var shard = shards.get(emitted++);
			if (Loggers.MSG.isDebugEnabled()) {
				Loggers.MSG.debug(
								"LIST shard seed emitted prefix={} (batch) kind={} delimiter={}",
								shard.prefix(), shard.kind(), shard.delimiter());
			}
			buffer.add(itemFactory.getItem(shard.prefix(), 0, 0));
		}
		return count;
	}

	@Override
	public long skip(final long count) {
		if (count <= 0 || emitted >= shards.size()) {
			return 0;
		}
		final long skipped = Math.min(count, shards.size() - emitted);
		emitted += skipped;
		return skipped;
	}

	@Override
	public void reset() {
		emitted = 0;
	}

	@Override
	public void close() {}

	public List<ListShard> shards() {
		return shards;
	}
}
