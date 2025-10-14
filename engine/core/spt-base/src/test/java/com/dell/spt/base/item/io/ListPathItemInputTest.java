package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.PathItemFactoryImpl;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

final class ListPathItemInputTest {

	@Test
	void emitsConfiguredSeedCountOnce() {
		final var factory = new PathItemFactoryImpl<PathItem>();
		final var input = new ListPathItemInput<>(factory, "prefix", 2);
		final var buff = new ArrayList<PathItem>();

		assertEquals(2, input.get(buff, 10));
		assertEquals("prefix", buff.get(0).name());
		assertEquals("prefix", buff.get(1).name());
		assertEquals(0, input.get(new ArrayList<>(), 5));

		input.reset();
		assertEquals(1, input.get(new ArrayList<>(), 1));
	}

	@Test
	void defaultsToRootWhenPrefixMissing() {
		final var factory = new PathItemFactoryImpl<PathItem>();
		final var input = new ListPathItemInput<>(factory, null, 1);

		final var item = input.get();
		assertEquals("/", item.name());
	}

	@Test
	void treatsEmptyPrefixAsRoot() {
		final var factory = new PathItemFactoryImpl<PathItem>();
		final var input = new ListPathItemInput<>(factory, "", 1);

		final var buff = new ArrayList<PathItem>();
		input.get(buff, 1);
		assertEquals("/", buff.get(0).name());
	}
}
