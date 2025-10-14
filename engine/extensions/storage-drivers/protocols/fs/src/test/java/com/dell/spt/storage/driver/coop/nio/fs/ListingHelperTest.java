package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
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
}
