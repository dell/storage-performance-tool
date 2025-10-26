package com.dell.spt.base.load.generator;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.fiber4j.Fiber;
import java.io.IOException;

/** Created on 11.07.16. */
public interface LoadGenerator<I extends Item, O extends Operation<I>> extends Fiber, AutoCloseable {

	/** Returns true when the item input has been fully consumed. */
	boolean isItemInputFinished();

	/** Returns the number of operations generated including recycled ones. */
	long generatedOpCount();

	/**
	 * Enqueues the task for further recycling.
	 *
	 * @param op the task to recycle
	 */
	void recycle(final O op);

	/** Returns true when the recycle queue is currently empty. */
	boolean isNothingToRecycle();

	@Override
	void close() throws IOException;
}
