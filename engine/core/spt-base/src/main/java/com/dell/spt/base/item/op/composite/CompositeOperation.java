package com.dell.spt.base.item.op.composite;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import java.util.List;

/** Created by andrey on 25.11.16. Marker interface */
public interface CompositeOperation<I extends Item> extends Operation<I> {

	@Override
	I item();

	String get(final String key);

	void put(final String key, final String value);

	List<? extends PartialOperation> subOperations();

	/** 
	 * Returns up to `limit` un-yielded sub-operations.
	 * This method allows a driver to request sub-operations in batches. 
	 * Calls to this method are stateful and drain the un-yielded sub-operations.
	 */
	default List<? extends PartialOperation> nextSubOperations(final int limit) {
		return subOperations();
	}

	/** Should be invoked only after subOperations() * */
	void markSubTaskCompleted();

	/** Re-increment the pending count to undo a markSubTaskCompleted() call (used for part retry). */
	default void undoMarkSubTaskCompleted() {}

	boolean allSubOperationsDone();

	/**
	 * Forgets the sub-operations and context data of an earlier attempt, so that a retried composite
	 * starts again from its initiation instead of resuming or finalizing the failed attempt.
	 */
	default void resetSubOperations() {}

	/**
	 * Whether the driver ran this attempt as its sub-operations alone: it sent no request and did no I/O
	 * for the operation's own passes, so its sub-operations' timings are the only request timings it has.
	 * The driver says so with {@link #markRequestless()}; a reset of the operation, or of its
	 * sub-operations, clears it.
	 */
	default boolean requestless() {
		return false;
	}

	/**
	 * Called by a driver that issues this attempt's sub-operations as its only requests, as the {@code
	 * s3}, {@code s3-rdma} and {@code s3-aws} drivers do for a read split by the size threshold.
	 */
	default void markRequestless() {}
}
