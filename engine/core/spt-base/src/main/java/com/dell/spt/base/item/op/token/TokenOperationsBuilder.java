package com.dell.spt.base.item.op.token;

import com.dell.spt.base.item.TokenItem;
import com.dell.spt.base.item.op.OperationsBuilder;

/** Created by kurila on 14.07.16. */
public interface TokenOperationsBuilder<I extends TokenItem, O extends TokenOperation<I>>
				extends OperationsBuilder<I, O> {}
