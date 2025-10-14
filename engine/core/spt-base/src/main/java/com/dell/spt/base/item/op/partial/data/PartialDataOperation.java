package com.dell.spt.base.item.op.partial.data;

import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;

/** Created by andrey on 25.11.16. */
public interface PartialDataOperation<I extends DataItem>
				extends DataOperation<I>, PartialOperation<I> {

	@Override
	I item();

	@Override
	CompositeDataOperation<I> parent();
}
