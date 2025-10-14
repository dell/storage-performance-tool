package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.ItemImpl;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;

final class SingleItemInputTest {

	@Test
	void getReturnsSameInstance() {
		final var item = new ItemImpl("item-1");
		final var in = new SingleItemInput<>(item);
		assertSame(item, in.get());
	}

	@Test
	void getListFillsBufferWithSameReference() {
		final var item = new ItemImpl("x");
		final var in = new SingleItemInput<>(item);
		final var buff = new ArrayList<ItemImpl>();
		final int n = in.get(buff, 5);
		assertEquals(5, n);
		assertEquals(5, buff.size());
		buff.forEach(it -> assertSame(item, it));
	}

	@Test
	void skipResetCloseAreNoops() {
		final var item = new ItemImpl("a");
		final var in = new SingleItemInput<>(item);
		assertEquals(7, in.skip(7));
		// Should not throw
		in.reset();
		in.close();
		// Still returns the same item
		assertSame(item, in.get());
	}
}
