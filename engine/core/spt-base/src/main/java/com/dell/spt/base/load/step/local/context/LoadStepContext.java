package com.dell.spt.base.load.step.local.context;

import com.dell.spt.base.concurrent.Daemon;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.concurrent.AsyncRunnable;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.IOException;

/** Created on 11.07.16. */
public interface LoadStepContext<I extends Item, O extends Operation<I>> extends Daemon, Output<O> {

	void operationsResultsOutput(final Output<O> opsResultsOutput);

	void operationsMetricsOutput(final Output<O> opsMetricsOutput);

	int activeOpCount();

	boolean isDone();

	default Input<O> getInput() {
		throw new AssertionError("Shouldn't be invoked");
	}

	@Override
	AsyncRunnable stop();

	@Override
	void close() throws IOException;
}
