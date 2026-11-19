package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.logging.Loggers;

import java.io.File;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

public interface ListingHelper {

	DirectoryStream.Filter<Path> ACCEPT_ALL_PATHS_FILTER = entry -> true;

	final class PrefixDirectoryStreamFilter
					implements DirectoryStream.Filter<Path> {

		private final PathMatcher pathPrefixMatcher;

		public PrefixDirectoryStreamFilter(final String prefix) {
			pathPrefixMatcher = FsConstants.FS.getPathMatcher("glob:" + prefix + "*");
		}

		@Override
		public final boolean accept(final Path entry)
						throws IOException {
			return pathPrefixMatcher.matches(entry.getFileName());
		}
	}

	static <I extends Item> List<I> list(
					final ItemFactory<I> itemFactory, final String path, final String prefix, final int idRadix,
					final I lastPrevItem, final int count) throws IOException {

		final DirectoryStream.Filter<Path> filter = (prefix == null || prefix.isEmpty()) ? ACCEPT_ALL_PATHS_FILTER : new PrefixDirectoryStreamFilter(prefix);
		final int prefixLength = (prefix == null || prefix.isEmpty()) ? 0 : prefix.length();
		try (
						final DirectoryStream<Path> dirStream = FsConstants.FS_PROVIDER.newDirectoryStream(Paths.get(path), filter)) {
			return listEntries(itemFactory, dirStream, path, prefixLength, idRadix, lastPrevItem, count);
		} catch (final DirectoryIteratorException e) {
			throw e.getCause(); // according the JDK documentation
		}
	}

	/**
	 * Lists the regular files among the directory entries, following symbolic links: directories, other
	 * non-regular entries, broken links, entries removed since the directory was read and entries whose
	 * attributes cannot be read, also through a link, are skipped and do not count toward
	 * {@code count}. One attribute read per examined entry gives both the type and the size. Entries
	 * that could not be read are logged as a warning. The skipped non-regular entries are logged once per page, except on a page after the
	 * last item: the page before it may already have counted them.
	 */
	static <I extends Item> List<I> listEntries(
					final ItemFactory<I> itemFactory, final Iterable<Path> entries, final String path,
					final int prefixLength, final int idRadix, final I lastPrevItem, final int count) {

		final List<I> buff = new ArrayList<>(count);
		final String lastPrevItemName;
		boolean lastPrevItemNameFound;
		if (lastPrevItem == null) {
			lastPrevItemName = null;
			lastPrevItemNameFound = true;
		} else {
			lastPrevItemName = lastPrevItem.name();
			lastPrevItemNameFound = false;
		}
		int skipped = 0;
		int unreadable = 0;
		IOException firstUnreadable = null;

		for (final Path nextPath : entries) {
			final File nextFile = new File(nextPath.toString());
			final String nextFileName = nextFile.getAbsolutePath();
			if (lastPrevItemNameFound) {
				final BasicFileAttributes attrs;
				try {
					attrs = Files.readAttributes(nextPath, BasicFileAttributes.class);
				} catch (final NoSuchFileException e) {
					// removed since the directory was read, or a dangling symbolic link
					skipped++;
					continue;
				} catch (final IOException e) {
					if (!(e instanceof AccessDeniedException) && Files.isSymbolicLink(nextPath)) {
						skipped++; // a broken link, such as a loop
					} else {
						// skipped, not thrown: File.length() let the listing go on, so one entry must not end it
						unreadable++;
						if (firstUnreadable == null) {
							firstUnreadable = e;
						}
					}
					continue;
				}
				if (!attrs.isRegularFile()) {
					skipped++;
					continue;
				}
				// the offset is encoded in the file name, not in the full path
				buff.add(itemFactory.getItem(
								nextFileName, offsetFromFileName(nextFile.getName(), prefixLength, idRadix),
								attrs.size()));
				if (count == buff.size()) {
					break;
				}
			} else {
				lastPrevItemNameFound = nextFileName.equals(lastPrevItemName);
			}
		}

		if (unreadable > 0) {
			Loggers.ERR.warn(
							"Listing \"{}\" skipped entries whose attributes could not be read: {}. The first: {}",
							path, unreadable, firstUnreadable.toString());
		}
		if (skipped > 0 && (lastPrevItem == null || !buff.isEmpty())) {
			Loggers.MSG.info(
							"Listing \"{}\" skipped entries that are not regular files, such as subdirectories: {}. "
											+ "A listing covers the regular files of one directory level",
							path, skipped);
		}
		return buff;
	}

	/**
	 * The offset an SPT-written file name encodes after the naming prefix, or 0 when the name encodes
	 * none (read verification should then be disabled). The listing filter is a glob, so a matched
	 * name can be shorter than the prefix string; SPT never writes a negative offset.
	 */
	static long offsetFromFileName(final String leafName, final int prefixLength, final int idRadix) {
		if (leafName.length() <= prefixLength) {
			return 0;
		}
		try {
			final long offset = Long.parseLong(leafName.substring(prefixLength), idRadix);
			return offset < 0 ? 0 : offset;
		} catch (final NumberFormatException e) {
			return 0;
		}
	}
}
