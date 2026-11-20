package com.dell.spt.base.item.op.partial;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;

/** Created by andrey on 23.11.16. */
public interface PartialOperation<I extends Item> extends Operation<I> {

	int partNumber();

	CompositeOperation<I> parent();

	/** Number of times this part has been retried after failure. */
	default int retryCount() {
		return 0;
	}

	/** Increment the retry counter for this part. */
	default void incrementRetryCount() {}
}
