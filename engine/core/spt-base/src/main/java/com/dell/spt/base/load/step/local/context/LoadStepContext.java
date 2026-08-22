package com.dell.spt.base.load.step.local.context;

import com.dell.spt.base.concurrent.Daemon;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.concurrent.AsyncRunnable;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.IOException;

/** Created on 11.07.16. */
public interface LoadStepContext<I extends Item, O extends Operation<I>> extends Daemon, Output<O> {

	void operationsResultsOutput(final Output<O> opsResultsOutput);

	void operationsMetricsOutput(final Output<O> opsMetricsOutput);

	int activeOpCount();

	/** Returns lifecycle counts and recoverable terminal identities for this step. */
	default OperationLifecycleSnapshot<O> operationLifecycle() {
		return new OperationLifecycleSnapshot<>(0, 0, 0, 0, 0, 0, 0, java.util.List.of(), java.util.List.of());
	}

	/** Returns object-level identity accounting for a standalone DELETE step. */
	default DeleteObjectLifecycleSnapshot deleteObjectLifecycle() {
		return DeleteObjectLifecycleSnapshot.empty();
	}

	boolean isDone();

	@Override
	default Input<O> getInput() {
		throw new AssertionError("Shouldn't be invoked");
	}

	@Override
	AsyncRunnable stop();

	@Override
	void close() throws IOException;
}
