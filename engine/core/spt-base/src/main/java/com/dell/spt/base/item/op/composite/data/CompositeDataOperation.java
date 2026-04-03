package com.dell.spt.base.item.op.composite.data;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import java.util.List;

/** Created by andrey on 25.11.16. */
public interface CompositeDataOperation<I extends DataItem> extends CompositeOperation<I> {

	@Override
	List<? extends PartialDataOperation<I>> subOperations();

	/** Returns the size threshold used to partition the item into sub-operations. */
	long sizeThreshold();
}
