package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.op.Operation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.List;

/**
 * The driver's result output of a count-limited mixed step: passes each result on, then counts it
 * on the generator if it is its operation's final one, so the generator knows when every operation
 * it dispatched has ended. A step with no count limit does not use it.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
final class FinalResultCountingOutput implements Output {

	private final Output output;
	private final MixedLoadGenerator generator;

	FinalResultCountingOutput(final Output output, final MixedLoadGenerator generator) {
		this.output = output;
		this.generator = generator;
	}

	@Override
	public boolean put(final Object op) {
		final boolean accepted = output.put(op);
		if (MixedLoadGenerator.isFinalResult((Operation) op)) {
			generator.onFinalResult();
		}
		return accepted;
	}

	@Override
	public int put(final List ops, final int from, final int to) {
		final int n = output.put(ops, from, to);
		for (int i = from; i < from + n; i++) {
			if (MixedLoadGenerator.isFinalResult((Operation) ops.get(i))) {
				generator.onFinalResult();
			}
		}
		return n;
	}

	@Override
	public int put(final List ops) {
		return put(ops, 0, ops.size());
	}

	@Override
	public Input getInput() {
		return output.getInput();
	}

	@Override
	public void close() throws Exception {
		output.close();
	}
}
