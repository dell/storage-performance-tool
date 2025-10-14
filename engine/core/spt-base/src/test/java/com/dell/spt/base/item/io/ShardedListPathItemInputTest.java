package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.PathItemFactoryImpl;
import com.dell.spt.base.item.op.list.shard.ListShard;
import java.util.List;
import org.junit.jupiter.api.Test;

class ShardedListPathItemInputTest {

	@Test
	void emitsEachShardOnce() {
		final var shards = List.of(
						new ListShard("a", null, null, null),
						new ListShard("b", null, null, null));
		final var input = new ShardedListPathItemInput<PathItem>(new PathItemFactoryImpl<>(), shards);
		final var first = input.get();
		final var second = input.get();
		assertEquals("a", first.name());
		assertEquals("b", second.name());
		assertNull(input.get());
	}

	@Test
	void resetAllowsReiteration() {
		final var shards = List.of(new ListShard("x", null, null, null));
		final var input = new ShardedListPathItemInput<PathItem>(new PathItemFactoryImpl<>(), shards);
		assertNotNull(input.get());
		assertNull(input.get());
		input.reset();
		assertNotNull(input.get());
	}
}
