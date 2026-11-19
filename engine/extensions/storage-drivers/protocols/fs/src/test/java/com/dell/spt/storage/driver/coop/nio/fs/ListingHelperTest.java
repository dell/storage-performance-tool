package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import com.dell.spt.base.logging.Loggers;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

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

	private static final String SKIP_LOG_START = "Listing \"";

	private static Path writeFile(final Path dir, final String name, final int size) throws Exception {
		return Files.write(dir.resolve(name), new byte[size]);
	}

	private static Map<String, Long> sizesByName(final List<DataItem> listed) throws Exception {
		final Map<String, Long> sizes = new HashMap<>();
		for (final DataItem item : listed) {
			assertNull(sizes.put(item.name(), item.size()), "listed twice: " + item.name());
		}
		return sizes;
	}

	/** Makes a FIFO with mkfifo, or returns null when the host cannot. */
	private static Path makeFifo(final Path dir, final String name) {
		final Path fifo = dir.resolve(name);
		try {
			final Process p = new ProcessBuilder("mkfifo", fifo.toString()).redirectErrorStream(true).start();
			if (p.waitFor(10, TimeUnit.SECONDS) && p.exitValue() == 0 && Files.exists(fifo)) {
				return fifo;
			}
			p.destroyForcibly();
		} catch (final Exception e) {
			// no mkfifo on this host
		}
		return null;
	}

	@Test
	public void listYieldsRegularFilesOnlyFollowingLinks(@TempDir final Path dir) throws Exception {
		final Path f0 = writeFile(dir, "f0", 5);
		final Path f1 = writeFile(dir, "f1", 0);
		final Path f2 = writeFile(dir, "f2", 3);
		final Path sub = Files.createDirectory(dir.resolve("sub"));
		writeFile(sub, "inner0", 7);
		Files.createDirectory(dir.resolve("emp"));
		final Path lnf = Files.createSymbolicLink(dir.resolve("lnf"), f2);
		Files.createSymbolicLink(dir.resolve("lnd"), sub);
		Files.createSymbolicLink(dir.resolve("dang"), dir.resolve("missing"));
		Files.createSymbolicLink(dir.resolve("loop"), dir.resolve("loop"));

		final Map<String, Long> expected = new HashMap<>();
		expected.put(f0.toFile().getAbsolutePath(), 5L);
		expected.put(f1.toFile().getAbsolutePath(), 0L);
		expected.put(f2.toFile().getAbsolutePath(), 3L);
		// a symbolic link to a regular file is kept under its own name, with the target's size
		expected.put(lnf.toFile().getAbsolutePath(), 3L);

		assertEquals(expected, sizesByName(listAll(dir, null, null)));
	}

	@Test
	public void listSkipsFifo(@TempDir final Path dir) throws Exception {
		final Path fifo = makeFifo(dir, "fifo");
		Assumptions.assumeTrue(fifo != null, "mkfifo is not available");
		final Path f0 = writeFile(dir, "f0", 4);
		assertEquals(Map.of(f0.toFile().getAbsolutePath(), 4L), sizesByName(listAll(dir, null, null)));
	}

	@Test
	public void listKeepsFileSize(@TempDir final Path dir) throws Exception {
		final Path f = writeFile(dir, "f", 12345);
		final List<DataItem> listed = listAll(dir, null, null);
		assertEquals(1, listed.size());
		assertEquals(Files.size(f), listed.get(0).size());
	}

	@Test
	public void listSkipsEntryRemovedBeforeItsAttributesAreRead(@TempDir final Path dir) throws Exception {
		final Path f0 = writeFile(dir, "f0", 1);
		final Path f1 = writeFile(dir, "f1", 2);
		// "gone" was in the directory when it was read, and is removed before its attributes are read
		final List<Path> entries = List.of(f0, dir.resolve("gone"), f1);
		final List<DataItem> listed = ListingHelper.listEntries(
						new DataItemFactoryImpl<>(), entries, dir.toString(), 0, RADIX, null, 2);
		assertEquals(List.of(f0.toFile().getAbsolutePath(), f1.toFile().getAbsolutePath()),
						listed.stream().map(Item::name).toList());
	}

	@Test
	public void listPagesOverSkippedEntries(@TempDir final Path dir) throws Exception {
		final Path f0 = writeFile(dir, "f0", 1);
		final Path f1 = writeFile(dir, "f1", 2);
		final Path f2 = writeFile(dir, "f2", 3);
		final Path sub = Files.createDirectory(dir.resolve("sub"));
		final Path emp = Files.createDirectory(dir.resolve("emp"));
		final List<Path> entries = List.of(f0, sub, f1, emp, f2);
		final DataItemFactoryImpl<DataItem> factory = new DataItemFactoryImpl<>();

		// skipped entries do not count toward the page size
		final List<DataItem> first = ListingHelper.listEntries(factory, entries, dir.toString(), 0, RADIX, null, 2);
		assertEquals(List.of(f0.toFile().getAbsolutePath(), f1.toFile().getAbsolutePath()),
						first.stream().map(Item::name).toList());
		final List<DataItem> second = ListingHelper.listEntries(factory, entries, dir.toString(), 0, RADIX, first.get(1), 2);
		assertEquals(List.of(f2.toFile().getAbsolutePath()), second.stream().map(Item::name).toList());
		final List<DataItem> third = ListingHelper.listEntries(factory, entries, dir.toString(), 0, RADIX, second.get(0), 2);
		assertTrue(third.isEmpty());

		// the same through a real directory, whatever order it lists in
		final Set<String> paged = new HashSet<>();
		DataItem last = null;
		for (int page = 0; page < 10; page++) {
			final List<DataItem> items = ListingHelper.list(factory, dir.toString(), null, RADIX, last, 1);
			if (items.isEmpty()) {
				break;
			}
			assertEquals(1, items.size());
			assertTrue(paged.add(items.get(0).name()), "listed twice: " + items.get(0).name());
			last = items.get(0);
		}
		assertEquals(Set.of(f0.toFile().getAbsolutePath(), f1.toFile().getAbsolutePath(), f2.toFile().getAbsolutePath()),
						paged);
	}

	@Test
	public void listLogsOneLinePerListingThatSkips(@TempDir final Path dir, @TempDir final Path cleanDir) throws Exception {
		writeFile(dir, "f0", 1);
		writeFile(dir, "f1", 1);
		Files.createDirectory(dir.resolve("sub"));
		Files.createDirectory(dir.resolve("emp"));
		writeFile(cleanDir, "g0", 1);

		final List<String> lines = captureSkipLines(dir, () -> {
			try {
				assertEquals(2, listAll(dir, null, null).size());
				assertEquals(1, listAll(cleanDir, null, null).size());
				assertEquals(2, listAll(dir, null, null).size());
			} catch (final Exception e) {
				throw new AssertionError(e);
			}
			return null;
		});
		assertEquals(2, lines.size(), lines::toString);
		final String expectedLine = "Listing \"" + dir + "\" skipped entries that are not regular files, such as "
						+ "subdirectories: 2. A listing covers the regular files of one directory level";
		assertEquals(List.of(expectedLine, expectedLine), lines);
	}

	@Test
	public void listCountsSkippedEntriesAfterTheCursorAndNotOnThePageAfterTheLastItem(@TempDir final Path dir)
					throws Exception {
		final Path d0 = Files.createDirectory(dir.resolve("d0"));
		final Path f0 = writeFile(dir, "f0", 1);
		final Path d1 = Files.createDirectory(dir.resolve("d1"));
		final Path f1 = writeFile(dir, "f1", 1);
		final Path d2 = Files.createDirectory(dir.resolve("d2"));
		final List<Path> entries = List.of(d0, f0, d1, f1, d2);
		final DataItemFactoryImpl<DataItem> factory = new DataItemFactoryImpl<>();

		final List<String> lines = captureSkipLines(dir, () -> {
			final List<DataItem> first = ListingHelper.listEntries(factory, entries, dir.toString(), 0, RADIX, null, 1);
			final List<DataItem> second = ListingHelper.listEntries(
							factory, entries, dir.toString(), 0, RADIX, first.get(0), 1);
			// the page after the last item finds only d2, which a listing ending at the directory's end has counted
			assertTrue(ListingHelper.listEntries(factory, entries, dir.toString(), 0, RADIX, second.get(0), 1).isEmpty());
			return null;
		});
		final String expectedLine = "Listing \"" + dir + "\" skipped entries that are not regular files, such as "
						+ "subdirectories: 1. A listing covers the regular files of one directory level";
		assertEquals(List.of(expectedLine, expectedLine), lines);
	}

	@Test
	public void listWarnsOfEntriesWhoseAttributesItCannotRead(@TempDir final Path dir) throws Exception {
		final Path locked = Files.createDirectory(dir.resolve("locked"));
		final Path hidden = writeFile(locked, "hidden", 1);
		final Path f0 = writeFile(dir, "f0", 1);
		final Path linkToHidden = Files.createSymbolicLink(dir.resolve("lnh"), hidden);
		Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rw-------"));
		try {
			Assumptions.assumeTrue(cannotReadAttributes(hidden), "this user can read any file's attributes");
			final List<DataItem> listed = new ArrayList<>();
			final List<String> warnings = captureLines(Loggers.ERR.getName(), "Listing \"" + dir + "\"", () -> {
				try {
					final List<String> skipLines = captureSkipLines(dir, () -> {
						listed.addAll(ListingHelper.listEntries(new DataItemFactoryImpl<>(), List.of(hidden, linkToHidden, f0),
										dir.toString(), 0, RADIX, null, 3));
						return null;
					});
					assertEquals(List.of(), skipLines, "an unreadable entry is not reported as a non-regular one");
				} catch (final Exception e) {
					throw new AssertionError(e);
				}
				return null;
			});
			assertEquals(List.of(f0.toFile().getAbsolutePath()), listed.stream().map(Item::name).toList());
			assertEquals(1, warnings.size(), warnings::toString);
			assertTrue(warnings.get(0).startsWith("Listing \"" + dir + "\" skipped entries whose attributes could not be "
							+ "read: 2. The first: java.nio.file.AccessDeniedException"), warnings.get(0));
		} finally {
			Files.setPosixFilePermissions(locked, PosixFilePermissions.fromString("rwx------"));
		}
	}

	@Test
	public void listCountsBrokenLinksAsNotRegularAndLogsAPageWithNoItem(@TempDir final Path dir) throws Exception {
		final Path loop = Files.createSymbolicLink(dir.resolve("loop"), dir.resolve("loop"));
		final Path dangling = Files.createSymbolicLink(dir.resolve("dang"), dir.resolve("missing"));
		final Path sub = Files.createDirectory(dir.resolve("sub"));
		final List<DataItem> listed = new ArrayList<>();
		final List<String> warnings = captureLines(Loggers.ERR.getName(), "Listing \"" + dir + "\"", () -> {
			try {
				final List<String> skipLines = captureSkipLines(dir, () -> {
					listed.addAll(ListingHelper.listEntries(
									new DataItemFactoryImpl<>(), List.of(loop, dangling, sub), dir.toString(), 0, RADIX, null, 3));
					return null;
				});
				assertEquals(List.of("Listing \"" + dir + "\" skipped entries that are not regular files, such as "
								+ "subdirectories: 3. A listing covers the regular files of one directory level"), skipLines);
			} catch (final Exception e) {
				throw new AssertionError(e);
			}
			return null;
		});
		assertTrue(listed.isEmpty());
		assertEquals(List.of(), warnings, "a broken link is not an unreadable entry");
	}

	private static boolean cannotReadAttributes(final Path path) {
		try {
			Files.readAttributes(path, BasicFileAttributes.class);
			return false;
		} catch (final IOException e) {
			return true;
		}
	}

	/** The skip lines logged for this directory only, so that a line another test's listing logs late is not kept. */
	private static List<String> captureSkipLines(final Path dir, final Supplier<Void> action) throws Exception {
		return captureLines(Loggers.MSG.getName(), SKIP_LOG_START + dir + "\"", action).stream()
						.filter(line -> line.contains("not regular files"))
						.toList();
	}

	private static List<String> captureLines(final String loggerName, final String start, final Supplier<Void> action)
					throws Exception {
		final List<String> lines = Collections.synchronizedList(new ArrayList<>());
		final String endMarker = "ListingHelperTest end " + System.nanoTime();
		final AtomicBoolean drained = new AtomicBoolean();
		final AbstractAppender appender = new AbstractAppender(
						"listingHelperTestSkipLines", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final String message = event.getMessage().getFormattedMessage();
				if (message.equals(endMarker)) {
					drained.set(true);
				} else if (message.startsWith(start)) {
					lines.add(message);
				}
			}
		};
		appender.start();
		final org.apache.logging.log4j.core.Logger logger = LoggerContext.getContext(false).getLogger(loggerName);
		logger.addAppender(appender);
		try {
			action.get();
			logger.info(endMarker);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (!drained.get() && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertTrue(drained.get(), "the end marker was not logged");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		return new ArrayList<>(lines);
	}
}
