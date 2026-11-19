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

	/**
	 * Marks this part as out of retries with its failure recorded on its parent, which may then end.
	 * The driver calls it, and reads {@link #failureRecorded()}, holding the parent's monitor. An
	 * implementation that does not count retries never runs out of them, so it need not keep the mark.
	 */
	default void markFailureRecorded() {}

	/** Whether {@link #markFailureRecorded()} was called for this part's current attempts. */
	default boolean failureRecorded() {
		return false;
	}
}
