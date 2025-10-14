package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.ItemImpl;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

final class SingleItemOutputTest {

	@Test
	void putStoresItemAndGetInputYieldsIt() {
		final var item = new ItemImpl("xx");
		final var out = new SingleItemOutput<ItemImpl>();
		assertTrue(out.put(item));
		assertSame(item, out.getInput().get());
	}

	@Test
	void putListStoresLastOrNullOnEmpty() {
		final var out = new SingleItemOutput<ItemImpl>();

		// Non-empty buffer -> stores last element
		final var b1 = new ArrayList<ItemImpl>();
		b1.add(new ItemImpl("a"));
		b1.add(new ItemImpl("b"));
		out.put(b1);
		final var last = out.getInput().get();
		assertNotNull(last);
		assertEquals("b", last.name());

		// Empty buffer -> stores null
		final var b2 = new ArrayList<ItemImpl>();
		out.put(b2);
		assertNull(out.getInput().get());
	}

	@Test
	void closeNullsItem() {
		final var out = new SingleItemOutput<ItemImpl>();
		out.put(new ItemImpl("z"));
		out.close();
		assertNull(out.getInput().get());
	}
}
