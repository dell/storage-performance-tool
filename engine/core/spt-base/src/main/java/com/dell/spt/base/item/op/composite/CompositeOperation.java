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
}
