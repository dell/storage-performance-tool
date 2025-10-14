package com.dell.spt.base.item.io;

import com.dell.spt.base.env.FsUtil;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.io.file.TextFileOutput;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

/**
 * Item latency and duration output to file with support for the single object and batch calls.
 *
 * The format: "latency duration", e.g.
 * "100 200
 *  300 400
 *  500 700"
 */

public class ItemTimingMetricsFileOutput<I extends Item, O extends Operation> implements Output<O> {

	private final Output<String> itemInfoOutput;

	public ItemTimingMetricsFileOutput(final Path filePath) throws IOException {
		FsUtil.createParentDirsIfNotExist(filePath);
		itemInfoOutput = new TextFileOutput(filePath);
	}

	@Override
	public final boolean put(final O ioResult) {
		if (ioResult == null) { // poison. Basically a flag that indicates finish of ingest.
			try {
				close();
			} catch (final Exception e) {
				throwUnchecked(e);
			}
			return true;
		}
		return itemInfoOutput.put(ioResult.latency() + " " + ioResult.duration());
	}

	@Override
	public final int put(final List<O> ioResults, final int from, final int to) {
		final int n = to - from;
		final List<String> itemsInfo = new ArrayList<>(n);
		O ioResult;
		for (int i = from; i < to; i++) {
			ioResult = ioResults.get(i);
			if (ioResult == null) { // poison. Basically a flag that indicates finish of ingest.
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
			itemsInfo.add(ioResult.latency() + " " + ioResult.duration());
		}
		return itemInfoOutput.put(itemsInfo, 0, n);
	}

	@Override
	public final int put(final List<O> ioResults) {
		final List<String> itemsInfo = new ArrayList<>(ioResults.size());
		for (final O nextIoResult : ioResults) {
			if (nextIoResult == null) { // poison. Basically a flag that indicates finish of ingest.
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
			itemsInfo.add(nextIoResult.latency() + " " + nextIoResult.duration());
		}
		return itemInfoOutput.put(itemsInfo);
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
