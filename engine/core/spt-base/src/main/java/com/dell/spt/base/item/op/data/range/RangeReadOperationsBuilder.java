package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationsBuilderImpl;
import java.util.List;
import java.util.Objects;

/** Construction-selected builder; unlike the legacy builder it never requires size before identity. */
public final class RangeReadOperationsBuilder<I extends DataItem>
				extends OperationsBuilderImpl<I, RangeReadOperation<I>> {
	private final RangeReadPolicy policy;

	public RangeReadOperationsBuilder(final int originIndex, final RangeReadPolicy policy) {
		super(originIndex);
		this.policy = Objects.requireNonNull(policy);
		super.opType(OpType.READ);
	}

	@Override
	public RangeReadOperationsBuilder<I> opType(final OpType type) {
		if (type != OpType.READ) {
			throw new IllegalArgumentException("Single-range policy supports READ only");
		}
		return this;
	}

	@Override
	public RangeReadOperation<I> buildOp(final I item) {
		final var outputPath = getNextOutputPath();
		return new RangeReadOperation<>(originIndex, item, inputPath, outputPath,
						getNextCredential(outputPath), policy);
	}

	@Override
	public void buildOps(final List<I> items, final List<RangeReadOperation<I>> output) {
		for (final var item : items) {
			output.add(buildOp(item));
		}
	}
}
