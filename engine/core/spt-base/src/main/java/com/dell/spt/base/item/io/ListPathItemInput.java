package com.dell.spt.base.item.io;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.ItemFactory;
import com.github.akurilov.commons.io.Input;
import java.util.List;

/**
 * Minimal item input for LIST workloads that emits a bounded set of prefix seeds without touching the
 * storage backend.
 */
public final class ListPathItemInput<I extends PathItem> implements Input<I> {

	private final ItemFactory<I> itemFactory;
	private final String seedPrefix;
	private final int seedCount;
	private int emitted;

	public ListPathItemInput(final ItemFactory<I> itemFactory, final String seedPrefix, final int seedCount) {
		this.itemFactory = itemFactory;
		this.seedPrefix = normalizeSeedPrefix(seedPrefix);
		this.seedCount = Math.max(seedCount, 1);
	}

	@Override
	public I get() {
		if (emitted >= seedCount) {
			return null;
		}
		emitted++;
		return itemFactory.getItem(seedPrefix, 0, 0);
	}

	@Override
	public int get(final List<I> buffer, final int limit) {
		if (limit <= 0 || emitted >= seedCount) {
			return 0;
		}
		final int available = Math.min(limit, seedCount - emitted);
		for (var i = 0; i < available; i++) {
			buffer.add(itemFactory.getItem(seedPrefix, 0, 0));
		}
		emitted += available;
		return available;
	}

	@Override
	public long skip(final long count) {
		if (count <= 0 || emitted >= seedCount) {
			return 0;
		}
		final long skipped = Math.min(count, seedCount - emitted);
		emitted += skipped;
		return skipped;
	}

	@Override
	public void reset() {
		emitted = 0;
	}

	@Override
	public void close() {}

	private static String normalizeSeedPrefix(final String value) {
		if (value == null || value.isEmpty()) {
			return "/";
		}
		return value;
	}
}
