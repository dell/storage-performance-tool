package com.dell.spt.base.item.io;

import com.dell.spt.base.item.DataItemFactory;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.collection.BufferingInputBase;
import java.io.IOException;

/** Created by andrey on 02.12.16. */
public final class StorageItemInput<I extends Item> extends BufferingInputBase<I> {

	private final StorageDriver<I, ? extends Operation<I>> storageDriver;
	private final ItemFactory<I> itemFactory;
	private final String path;
	private final String prefix;
	private final int idRadix;
	private final ListOptions listOptions;

	private boolean poisonedFlag = false;
	/**
	 * Name of the last item buffered, as the driver listed it. An operation built on a handed-out
	 * item renames that item in place, so the next page's cursor must not be read from it.
	 */
	private String lastListedName = null;

	public StorageItemInput(
					final StorageDriver<I, ? extends Operation<I>> storageDriver,
					final int batchSize,
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix) {
		super(batchSize);
		this.storageDriver = storageDriver;
		this.itemFactory = itemFactory;
		this.path = path;
		this.prefix = prefix;
		this.idRadix = idRadix;
		this.listOptions = ListOptions.DEFAULT;
	}

	@Override
	protected final int loadMoreItems(final I lastItem) throws IOException {
		if (poisonedFlag) {
			return 0;
		}
		// The base passes the last buffered item, or null when nothing is buffered; page after that
		// item's name as listed, whatever consumers have done to the item since.
		final I cursor = null == lastItem ? null : itemFactory.getItem(lastListedName, 0, 0);
		final var newItems = storageDriver.list(
						itemFactory, path, prefix, idRadix, cursor, capacity, listOptions);
		final var n = newItems.size();
		I nextItem;
		for (var i = 0; i < n; i++) {
			nextItem = newItems.get(i);
			if (null == nextItem) {
				poisonedFlag = true;
				return i;
			} else {
				items.add(nextItem);
				lastListedName = nextItem.name();
			}
		}
		return n;
	}

	@Override
	public final String toString() {
		return (itemFactory instanceof DataItemFactory ? "Data" : "") + "ItemsFromPath(" + path + ")";
	}

	@Override
	public final void reset() {
		super.reset();
		poisonedFlag = false;
		// lastListedName stays: the base only rewinds its buffer, and the next page still follows
		// the last buffered item.
	}
}
