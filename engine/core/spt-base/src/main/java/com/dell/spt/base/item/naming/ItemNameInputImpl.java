package com.dell.spt.base.item.naming;

import com.github.akurilov.commons.io.Input;
import it.unimi.dsi.fastutil.longs.Long2LongFunction;

import java.util.List;

public final class ItemNameInputImpl
				implements ItemNameInput {
	private static final int SHARD_LABEL_WIDTH = 8;
	private static final char SHARD_LABEL_PREFIX = 's';
	private static final int SHARD_LABEL_RADIX = Character.MAX_RADIX;
	private static final char PATH_SEPARATOR = '/';

	private final long initialId;
	private final Long2LongFunction idFunction;
	private volatile long lastId;
	private final Input<String> prefixInput;
	private final int radix;
	private final int shardCount;

	public ItemNameInputImpl(
					final Long2LongFunction idFunction, final long offset, final Input<String> prefixInput, final int radix) {
		this(idFunction, offset, prefixInput, radix, 0);
	}

	public ItemNameInputImpl(
					final Long2LongFunction idFunction, final long offset, final Input<String> prefixInput,
					final int radix, final int shardCount) {
		if (shardCount < 0) {
			throw new IllegalArgumentException("Item naming shard count must be non-negative");
		}
		this.initialId = offset;
		this.lastId = initialId;
		this.idFunction = idFunction;
		this.prefixInput = prefixInput;
		this.radix = radix;
		this.shardCount = shardCount;
	}

	@Override
	public final long lastId() {
		return lastId;
	}

	private void eval() {
		lastId = idFunction.applyAsLong(lastId);
	}

	private String convert() {
		final var prefix = prefixInput.get();
		if (shardCount == 0) {
			return prefix + Long.toString(lastId, radix);
		}
		final var shardId = Integer.toString(Math.floorMod(lastId, shardCount), SHARD_LABEL_RADIX);
		final var shardLabel = new StringBuilder(SHARD_LABEL_WIDTH + 1);
		shardLabel.append(SHARD_LABEL_PREFIX);
		shardLabel.append("0".repeat(SHARD_LABEL_WIDTH - 1 - shardId.length()));
		shardLabel.append(shardId);
		shardLabel.append(PATH_SEPARATOR);
		return prefix + shardLabel + Long.toString(lastId, radix);
	}

	@Override
	public final String get() {
		eval();
		return convert();
	}

	@Override
	public final int get(final List<String> buffer, final int limit) {
		for (var i = 0; i < limit; i++) {
			eval();
			buffer.add(convert());
		}
		return limit;
	}

	@Override
	public final long skip(final long count) {
		for (var i = 0L; i < count; i++) {
			idFunction.applyAsLong(lastId);
		}
		return count;
	}

	@Override
	public final void reset() {
		lastId = initialId;
	}

	@Override
	public final void close()
					throws Exception {
		prefixInput.close();
	}
}
