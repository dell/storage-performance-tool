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
		final var newItems = storageDriver.list(
						itemFactory, path, prefix, idRadix, lastItem, capacity, listOptions);
		final var n = newItems.size();
		I nextItem;
		for (var i = 0; i < n; i++) {
			nextItem = newItems.get(i);
			if (null == nextItem) {
				poisonedFlag = true;
				return i;
			} else {
				items.add(nextItem);
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
	}
}
