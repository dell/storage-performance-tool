package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.env.FsUtil;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.io.file.TextFileOutput;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Created by kurila on 09.01.17.
 * Item output to file with support for the single object and batch calls
 */
public final class ItemInfoFileOutput<O extends Operation> implements Output<O> {

	private final Output<String> itemInfoOutput;

	public ItemInfoFileOutput(final Path filePath) throws IOException {
		FsUtil.createParentDirsIfNotExist(filePath);
		itemInfoOutput = new TextFileOutput(filePath);
	}

	@Override
	public final boolean put(final O ioResult) {
		if (ioResult == null) { // poison
			try {
				close();
			} catch (final Exception e) {
				throwUnchecked(e);
			}
			return true;
		}
		return itemInfoOutput.put(itemInfo(ioResult));
	}

	@Override
	public final int put(final List<O> ioResults, final int from, final int to) {
		final int n = to - from;
		final List<String> itemsInfo = new ArrayList<>(n);
		O ioResult;
		for (int i = from; i < to; i++) {
			ioResult = ioResults.get(i);
			if (ioResult == null) { // poison
				try {
					return itemInfoOutput.put(itemsInfo, 0, i);
				} finally {
					try {
						close();
					} catch (final Exception e) {
						throwUnchecked(e);
					}
				}
			}
			itemsInfo.add(itemInfo(ioResult));
		}
		return itemInfoOutput.put(itemsInfo, 0, n);
	}

	@Override
	public final int put(final List<O> ioResults) {
		final List<String> itemsInfo = new ArrayList<>(ioResults.size());
		for (final O nextIoResult : ioResults) {
			if (nextIoResult == null) { // poison
				try {
					return itemInfoOutput.put(itemsInfo);
				} finally {
					try {
						close();
					} catch (final Exception e) {
						throwUnchecked(e);
					}
				}
			}
			itemsInfo.add(itemInfo(nextIoResult));
		}
		return itemInfoOutput.put(itemsInfo);
	}

	/**
	 * Returns the row for one result. A plain item's name already holds its whole path, because the
	 * operation built it before the request. An item read from a canonical manifest keeps its bucket
	 * apart and its name is the bare key, so its row is built here from the path the request used.
	 */
	private static String itemInfo(final Operation<?> ioResult) {
		final Item item = ioResult.item();
		if (item instanceof IntegrityManifestDataItem manifestItem) {
			return manifestItem.toString(manifestItemPath(ioResult, manifestItem));
		}
		return item.toString();
	}

	/**
	 * Returns the row's name: the path the request addressed, in the form a plain item's row keeps it.
	 * The path is the operation's output path as written for a CREATE, and the manifest's bucket,
	 * rooted as the operation's source path is, for any other operation, whatever output path the step
	 * sets. A key that already holds that path ({@link ItemNamePaths#holdsPath}, the test of
	 * {@code HttpStorageDriverBase.dataUriPath}) is the name as it is; otherwise the key goes under the
	 * path with one slash between them. A relative CREATE path stays relative, as the filesystem driver
	 * creates the file under it and as {@code Operation.buildItemPath} keeps a plain item's; an HTTP
	 * driver roots it, and so roots the row when it reads it back. A CREATE with no output path keeps
	 * the bare key.
	 *
	 * <p>Known limits of this legacy format: it has no version ID column, so whether a row keeps its
	 * version depends on the driver and its versioning mode (Netty {@code s3} with versioning on
	 * renames the item {@code key~versionId}, so the row carries it; {@code s3-aws} never renames). It
	 * cannot hold a key with a comma or a line break, and a key with an empty segment ({@code a//b})
	 * reads back as another path, as a plain item's does. A rooted key under a relative CREATE path
	 * that it holds, such as {@code /dst/k1} under {@code dst}, is written as the key: right for an HTTP
	 * driver, which requests {@code /dst/k1}, while the filesystem driver creates {@code dst/dst/k1}.
	 * The row is the one written before manifest rows held a path.
	 */
	private static String manifestItemPath(
					final Operation<?> ioResult, final IntegrityManifestDataItem manifestItem) {
		final String key = manifestItem.name();
		final String path;
		if (OpType.CREATE.equals(ioResult.type())) {
			path = ioResult.dstPath();
			if (path == null || path.isEmpty()) {
				return key;
			}
		} else {
			final String bucket = manifestItem.bucket();
			path = bucket.startsWith(Operation.SLASH) ? bucket : Operation.SLASH + bucket;
		}
		// A relative key holds only a relative path; a rooted key can hold a path in either form.
		return ItemNamePaths.holdsPath(key, path) ? key : ItemNamePaths.join(path, key);
	}

	@Override
	public final Input<O> getInput() {
		throw new AssertionError();
	}

	@Override
	public final void close() throws Exception {
		itemInfoOutput.close();
	}
}
