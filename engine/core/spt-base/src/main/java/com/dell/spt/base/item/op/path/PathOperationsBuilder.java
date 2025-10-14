package com.dell.spt.base.item.op.path;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OperationsBuilder;

/** Created by andrey on 31.01.17. */
public interface PathOperationsBuilder<I extends PathItem, O extends PathOperation<I>>
				extends OperationsBuilder<I, O> {}
