package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
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
		final List<I> buff = new ArrayList<>(count);

		try (
						final DirectoryStream<Path> dirStream = FsConstants.FS_PROVIDER.newDirectoryStream(Paths.get(path), filter)) {
			final int prefixLength = (prefix == null || prefix.isEmpty()) ? 0 : prefix.length();

			File nextFile;
			String nextFileName;
			I nextItem;

			final String lastPrevItemName;
			boolean lastPrevItemNameFound;
			if (lastPrevItem == null) {
				lastPrevItemName = null;
				lastPrevItemNameFound = true;
			} else {
				lastPrevItemName = lastPrevItem.name();
				lastPrevItemNameFound = false;
			}

			for (final Path nextPath : dirStream) {
				nextFile = new File(nextPath.toString());
				nextFileName = nextFile.getAbsolutePath();
				if (lastPrevItemNameFound) {
					// the offset is encoded in the file name, not in the full path
					nextItem = itemFactory.getItem(
									nextFileName, offsetFromFileName(nextFile.getName(), prefixLength, idRadix),
									nextFile.length());
					buff.add(nextItem);
					if (count == buff.size()) {
						break;
					}
				} else {
					lastPrevItemNameFound = nextFileName.equals(lastPrevItemName);
				}
			}
		} catch (final DirectoryIteratorException e) {
			throw e.getCause(); // according the JDK documentation
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
