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

	/** Should be invoked only after subOperations() * */
	void markSubTaskCompleted();

	boolean allSubOperationsDone();
}
