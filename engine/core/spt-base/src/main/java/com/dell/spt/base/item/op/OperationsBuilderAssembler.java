package com.dell.spt.base.item.op;

import com.dell.spt.base.item.Item;
import java.io.IOException;
import java.util.List;

/** Preserves the existing one-input-item/one-operation builder contract behind an assembler. */
public final class OperationsBuilderAssembler<I extends Item, O extends Operation<I>>
				implements OperationAssembler<I, O> {

	private final OperationsBuilder<I, O> operationsBuilder;
	private OperationAssemblyResult lastResult = new OperationAssemblyResult(0, 0);

	/**
	 * Creates a cardinality-neutral adapter for an existing one-item/one-operation builder.
	 *
	 * @param operationsBuilder builder whose behavior and resource ownership are preserved
	 */
	public OperationsBuilderAssembler(final OperationsBuilder<I, O> operationsBuilder) {
		this.operationsBuilder = operationsBuilder;
	}

	@Override
	public int originIndex() {
		return operationsBuilder.originIndex();
	}

	@Override
	public OpType opType() {
		return operationsBuilder.opType();
	}

	@Override
	public OperationAssemblyResult assemble(final List<I> items, final List<O> operations) throws IOException {
		operationsBuilder.buildOps(items, operations);
		final var count = items.size();
		if (lastResult.consumedIdentityCount() != count || lastResult.emittedOperationCount() != count) {
			lastResult = new OperationAssemblyResult(count, count);
		}
		return lastResult;
	}

	@Override
	public void close() {
		operationsBuilder.close();
	}
}
