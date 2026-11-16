package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class ListingHelperTest {

	@Test
	public void prefixFilterAcceptsMatchingNames() throws Exception {
		final ListingHelper.PrefixDirectoryStreamFilter filter = new ListingHelper.PrefixDirectoryStreamFilter("foo");
		assertTrue(filter.accept(Paths.get("/x/foo123")));
		assertTrue(filter.accept(Paths.get("foo")));
		assertFalse(filter.accept(Paths.get("/x/bar")));
		assertFalse(filter.accept(Paths.get("barfoo"))); // name doesn't start with prefix
	}

	@Test
	public void listWithPrefixHonorsCountAndPrefix() throws Exception {
		final Path tmpDir = Files.createTempDirectory("fs-listing-");
		final String prefix = "pre";
		final Set<String> expected = new HashSet<>();

		// create 5 prefixed and 3 non-prefixed files
		for (int i = 0; i < 5; i++) {
			final File f = tmpDir.resolve(prefix + i).toFile();
			assertTrue(f.createNewFile());
			expected.add(f.getAbsolutePath());
		}
		for (int i = 0; i < 3; i++) {
			assertTrue(tmpDir.resolve("x" + i).toFile().createNewFile());
		}

		final int count = 3;
		final List<Item> result = ListingHelper.list(new ItemFactoryImpl<>(), tmpDir.toString(), prefix, 10, null, count);
		assertNotNull(result);
		assertEquals(count, result.size());
		for (Item it : result) {
			assertTrue(expected.contains(it.name()), "Returned item must have expected prefix");
		}
	}

	@Test
	public void listWithoutPrefixReturnsUpToCount() throws Exception {
		final Path tmpDir = Files.createTempDirectory("fs-listing-noprefix-");
		final int total = 7;
		for (int i = 0; i < total; i++) {
			assertTrue(tmpDir.resolve("f" + i).toFile().createNewFile());
		}
		final int count = 4;
		final List<Item> result = ListingHelper.list(new ItemFactoryImpl<>(), tmpDir.toString(), null, 10, null, count);
		assertNotNull(result);
		assertEquals(count, result.size());
	}

	private static final int RADIX = 36;

	// offsets in the range SPT generates; names are Long.toString(offset, 36), as SPT writes them
	private static final long[] OFFSETS = {
			0x15424380decad4c0L, 0x0123456789abcdefL, 0x2fedcba987654321L, 0x1000000000000000L, 0x3ffffffffffffffL
	};

	/** Creates one file per offset named prefix + the offset in radix 36; returns absolute path to offset. */
	private static Map<String, Long> createGeneratedFiles(final Path dir, final String prefix) throws Exception {
		final Map<String, Long> expected = new HashMap<>();
		for (final long offset : OFFSETS) {
			final File f = dir.resolve(prefix + Long.toString(offset, RADIX)).toFile();
			assertTrue(f.createNewFile());
			expected.put(f.getAbsolutePath(), offset);
		}
		return expected;
	}

	private static List<DataItem> listAll(final Path dir, final String prefix, final DataItem lastPrevItem)
					throws Exception {
		return ListingHelper.list(new DataItemFactoryImpl<>(), dir.toString(), prefix, RADIX, lastPrevItem, 100);
	}

	private static void assertOffsets(final Map<String, Long> expected, final List<DataItem> listed) {
		assertEquals(expected.size(), listed.size());
		for (final DataItem item : listed) {
			assertTrue(expected.containsKey(item.name()), "unexpected item " + item.name());
			assertEquals(expected.get(item.name()).longValue(), item.offset(), "offset of " + item.name());
		}
	}

	@Test
	public void listParsesOffsetFromFileName(@TempDir final Path dir) throws Exception {
		final Map<String, Long> expected = createGeneratedFiles(dir, "");
		assertOffsets(expected, listAll(dir, null, null));
	}

	@Test
	public void listParsesOffsetFromFileNameAfterPrefix(@TempDir final Path dir) throws Exception {
		final Map<String, Long> expected = createGeneratedFiles(dir, "pre");
		// a file without the prefix is filtered out
		assertTrue(dir.resolve(Long.toString(OFFSETS[0], RADIX)).toFile().createNewFile());
		assertOffsets(expected, listAll(dir, "pre", null));
	}

	@Test
	public void listGivesForeignNameOffsetZero(@TempDir final Path dir) throws Exception {
		final File f = dir.resolve("photo.jpg").toFile();
		assertTrue(f.createNewFile());
		final List<DataItem> listed = listAll(dir, null, null);
		assertEquals(1, listed.size());
		assertEquals(f.getAbsolutePath(), listed.get(0).name());
		assertEquals(0, listed.get(0).offset());
	}

	@Test
	public void listGivesNameShorterThanGlobPrefixOffsetZero(@TempDir final Path dir) throws Exception {
		// the prefix is a glob, so "[ab]c" matches "ac", which is shorter than the prefix string
		final File f = dir.resolve("ac").toFile();
		assertTrue(f.createNewFile());
		final List<DataItem> listed = listAll(dir, "[ab]c", null);
		assertEquals(1, listed.size());
		assertEquals(f.getAbsolutePath(), listed.get(0).name());
		assertEquals(0, listed.get(0).offset());
	}

	@Test
	public void listGivesNameThatParsesNegativeOffsetZero(@TempDir final Path dir) throws Exception {
		assertTrue(dir.resolve("-abc").toFile().createNewFile());
		final List<DataItem> listed = listAll(dir, null, null);
		assertEquals(1, listed.size());
		assertEquals(0, listed.get(0).offset());
	}

	@Test
	public void listParsesForeignNameThatIsValidRadix36(@TempDir final Path dir) throws Exception {
		// known limit: a foreign name that parses in the id radix gets its parsed value as the offset
		assertTrue(dir.resolve("readme").toFile().createNewFile());
		final List<DataItem> listed = listAll(dir, null, null);
		assertEquals(1, listed.size());
		assertEquals(Long.parseLong("readme", RADIX), listed.get(0).offset());
	}

	@Test
	public void listResumesAfterLastPrevItem(@TempDir final Path dir) throws Exception {
		final Map<String, Long> expected = createGeneratedFiles(dir, "");
		final List<DataItem> all = listAll(dir, null, null);
		assertOffsets(expected, all);

		final int pageSize = 2;
		final List<DataItem> firstPage = ListingHelper.list(new DataItemFactoryImpl<>(), dir.toString(), null, RADIX, null, pageSize);
		assertEquals(all.subList(0, pageSize).stream().map(Item::name).toList(),
						firstPage.stream().map(Item::name).toList());

		final List<DataItem> rest = listAll(dir, null, firstPage.get(pageSize - 1));
		assertEquals(all.subList(pageSize, all.size()).stream().map(Item::name).toList(),
						rest.stream().map(Item::name).toList());
		for (final DataItem item : rest) {
			assertEquals(expected.get(item.name()).longValue(), item.offset(), "offset of " + item.name());
		}
	}
}
