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
	 * A CREATE writes the key under the operation's output path; any other operation addresses the
	 * key in the manifest's own bucket, whatever output path the step sets. A CREATE with no output
	 * path keeps the bare key. Known limits of this legacy format: it has no version ID column, so a
	 * versioned row reads back as the latest version, and it cannot escape a comma in a key.
	 */
	private static String manifestItemPath(
					final Operation<?> ioResult, final IntegrityManifestDataItem manifestItem) {
		final String key = manifestItem.name();
		if (OpType.CREATE.equals(ioResult.type())) {
			final String dstPath = ioResult.dstPath();
			return dstPath == null || dstPath.isEmpty() ? key : ItemNamePaths.join(dstPath, key);
		}
		final String bucket = manifestItem.bucket();
		return (bucket.startsWith("/") ? bucket : "/" + bucket) + "/" + key;
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
