package com.dell.spt.base.nic.pool;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.item.io.ItemInputStatus;
import com.github.akurilov.commons.io.Input;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Reads one engine's own item file once before the load, keeping only its sizes (D54). It goes
 * through the parser {@link ItemInputFactory} chooses for the file, so an item-list CSV, a canonical
 * integrity manifest and the binary item format are read as the workload reads them, and asks
 * {@link ItemInputStatus} whether the end it saw was the real one (D54-R1).
 */
public final class ItemSizeScan {

	private static final int BATCH = 4096;

	private ItemSizeScan() {}

	/**
	 * Reads the file until its end, a failure or {@code deadlineNanos} of {@code nanoTime}. Nothing it
	 * meets fails the step: the summary says what happened.
	 */
	public static <I extends Item> ItemSizeSummary scan(
					final String file, final ItemFactory<I> itemFactory, final long deadlineNanos, final LongSupplier nanoTime) {
		final var recorder = new ItemSizeSummary.Recorder(file);
		final Input<I> input;
		try {
			input = ItemInputFactory.createFileItemInput(itemFactory, file);
		} catch (final RuntimeException e) {
			recorder.fail(ItemSizeSummary.Reason.READ_FAILED, e.toString());
			return recorder.summary();
		}
		if (input == null) {
			recorder.fail(ItemSizeSummary.Reason.READ_FAILED, "it cannot be opened");
			return recorder.summary();
		}
		try (input) {
			read(input, recorder, deadlineNanos, nanoTime);
		} catch (final IllegalArgumentException | IndexOutOfBoundsException e) {
			// A manifest throws on a bad record; a CSV line with no comma reaches here too.
			recorder.fail(ItemSizeSummary.Reason.MALFORMED_ITEM, e.getMessage());
		} catch (final Exception e) {
			recorder.fail(ItemSizeSummary.Reason.READ_FAILED, e.toString());
		}
		return recorder.summary();
	}

	private static <I extends Item> void read(
					final Input<I> input, final ItemSizeSummary.Recorder recorder, final long deadlineNanos, final LongSupplier nanoTime)
					throws Exception {
		final List<I> batch = new ArrayList<>(BATCH);
		while (true) {
			if (nanoTime.getAsLong() - deadlineNanos >= 0 || Thread.currentThread().isInterrupted()) {
				recorder.fail(ItemSizeSummary.Reason.BUDGET_EXPIRED, null);
				return;
			}
			batch.clear();
			final int count;
			try {
				count = input.get(batch, BATCH);
			} catch (final Exception e) {
				if (!(e instanceof EOFException)) {
					throw e;
				}
				recordEach(batch, recorder);
				finish(input, recorder);
				return;
			}
			recordEach(batch, recorder);
			if (count <= 0) {
				finish(input, recorder);
				return;
			}
		}
	}

	private static <I extends Item> void recordEach(final List<I> items, final ItemSizeSummary.Recorder recorder) throws IOException {
		for (final I item : items) {
			recordItem(item, recorder);
		}
	}

	/** Records {@code item}'s size; an item with no size, such as a path, has none to record. */
	public static void recordItem(final Item item, final ItemSizeSummary.Recorder recorder) throws IOException {
		if (item instanceof DataItem data) {
			recorder.record(data.size());
		}
	}

	/**
	 * Closes the record of an input that reported no more items: its end if nothing was malformed. A
	 * CSV input returns no items at a malformed line as it does at its end, and the status tells them
	 * apart.
	 */
	public static void finish(final Object input, final ItemSizeSummary.Recorder recorder) {
		if (input instanceof ItemInputStatus status && status.malformedItems() > 0) {
			recorder.fail(ItemSizeSummary.Reason.MALFORMED_ITEM, firstLine(status.firstMalformedItem()));
			return;
		}
		recorder.finish();
	}

	private static String firstLine(final String line) {
		if (line == null) {
			return "";
		}
		return line.length() > 120 ? line.substring(0, 120) + "..." : line;
	}
}
