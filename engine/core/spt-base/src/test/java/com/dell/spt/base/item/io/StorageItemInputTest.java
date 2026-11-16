package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemFactoryImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriver;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

final class StorageItemInputTest {

	private static final String PATH = "/pp";

	/**
	 * A stub driver over a sorted key list that pages after the cursor's name, as every driver
	 * does, and records each cursor it was given.
	 */
	@SuppressWarnings("unchecked")
	private static StorageDriver<Item, Operation<Item>> pagingDriver(
					final List<String> names, final List<Item> cursors) throws Exception {
		final StorageDriver<Item, Operation<Item>> driver = mock(StorageDriver.class);
		when(driver.list(any(), eq(PATH), any(), anyInt(), any(), anyInt(), any(ListOptions.class)))
						.thenAnswer(invocation -> {
							final ItemFactory<Item> factory = invocation.getArgument(0);
							final Item cursor = invocation.getArgument(4);
							final int count = invocation.getArgument(5);
							cursors.add(cursor);
							final String after = cursor == null ? "" : cursor.name();
							final List<Item> page = new ArrayList<>();
							for (final String name : names) {
								if (name.compareTo(after) > 0 && page.size() < count) {
									page.add(factory.getItem(name, 0, 1));
								}
							}
							return page;
						});
		return driver;
	}

	/** Drains the input in batches of two, building a READ on each item as the generator does. */
	private static List<String> drainBuildingReads(final StorageItemInput<Item> input) {
		final List<String> listed = new ArrayList<>();
		try {
			while (true) {
				final List<Item> batch = new ArrayList<>();
				input.get(batch, 2);
				for (final Item item : batch) {
					listed.add(item.name());
					// the constructor moves the item's parent path into srcPath, renaming it in place
					new OperationImpl<>(0, OpType.READ, item, PATH, null, null);
				}
			}
		} catch (final Exception e) {
			// the input ends after the last page with an EOFException, thrown unchecked
			if (!(e instanceof EOFException)) {
				throw e;
			}
		}
		return listed;
	}

	@Test
	void nextPageStartsAfterTheNameAsListedNotAsRenamedByAnOperation() throws Exception {
		final List<String> names = List.of("/pp/dir/k0", "/pp/dir/k1", "/pp/dir/k2", "/pp/dir/k3");
		final List<Item> cursors = new ArrayList<>();
		final var driver = pagingDriver(names, cursors);
		try (var input = new StorageItemInput<>(driver, 2, new ItemFactoryImpl<>(), PATH, null, 36)) {
			assertEquals(names, drainBuildingReads(input));
		}
		assertEquals(3, cursors.size(), "two full pages, then an empty one");
		assertNull(cursors.get(0));
		assertEquals("/pp/dir/k1", cursors.get(1).name());
		assertEquals("/pp/dir/k3", cursors.get(2).name());
	}

	@Test
	void cursorIsNotTheItemHandedOut() throws Exception {
		final List<Item> cursors = new ArrayList<>();
		final var driver = pagingDriver(List.of("/pp/a", "/pp/b", "/pp/c"), cursors);
		try (var input = new StorageItemInput<>(driver, 2, new ItemFactoryImpl<>(), PATH, null, 36)) {
			final List<Item> page1 = new ArrayList<>();
			assertEquals(2, input.get(page1, 2));
			final List<Item> page2 = new ArrayList<>();
			assertEquals(1, input.get(page2, 2));
			assertEquals("/pp/c", page2.get(0).name());
			assertNotSame(page1.get(1), cursors.get(1));
			assertEquals("/pp/b", cursors.get(1).name());
		}
	}

	@Test
	void resetRewindsThePageAndTheNextPageStillFollowsIt() throws Exception {
		final List<Item> cursors = new ArrayList<>();
		final var driver = pagingDriver(List.of("/pp/a", "/pp/b", "/pp/c"), cursors);
		try (var input = new StorageItemInput<>(driver, 2, new ItemFactoryImpl<>(), PATH, null, 36)) {
			final List<Item> page1 = new ArrayList<>();
			assertEquals(2, input.get(page1, 2));
			page1.forEach(item -> new OperationImpl<>(0, OpType.READ, item, PATH, null, null));
			input.reset();
			assertEquals(2, input.get(new ArrayList<>(), 2), "reset rewinds the buffered page");
			final List<Item> page2 = new ArrayList<>();
			assertEquals(1, input.get(page2, 2));
			assertEquals("/pp/c", page2.get(0).name());
			assertEquals("/pp/b", cursors.get(1).name());
		}
	}
}
