package com.dell.spt.base.item.op.data;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataOperationsBuilderImplTest {

	private static DataItemImpl newItem(final String name, final long size) {
		return new DataItemImpl(name, 0, size);
	}

	@Test
	void buildOpReturnsCompositeWhenItemSizeAboveThreshold() throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src")
						.sizeThreshold(100);
		final var item = newItem("obj", 1000);
		final var op = builder.buildOp(item);
		assertTrue(op instanceof com.dell.spt.base.item.op.composite.data.CompositeDataOperation);
	}

	@Test
	void buildOpRejectsRangesWhenSizeThresholdConfiguredForLargeItems() {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src")
						.sizeThreshold(100);
		// Configure random ranges
		builder.randomRangesCount(1);
		final var item = newItem("obj", 1000);
		assertThrows(IllegalArgumentException.class, () -> builder.buildOp(item));
	}

	@Test
	void buildOpRejectsIfRandomRangesExceedAllowedRangeCount() {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src");
		final var small = newItem("s", 7); // very small item
		// randomRangesCount > rangeCount(small.size()) should fail
		final int tooMany = DataItem.rangeCount(small.size()) + 1;
		builder.randomRangesCount(tooMany);
		assertThrows(IllegalArgumentException.class, () -> builder.buildOp(small));
	}

	@Test
	void buildOpsProducesListAndHonorsThresholdOrRanges() throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src");
		final var small = newItem("s", 64);
		final var large = newItem("l", 10_000);
		builder.sizeThreshold(1024);

		final List<DataOperation<DataItemImpl>> out = new ArrayList<>();
		builder.buildOps(List.of(small, large), out);
		assertEquals(2, out.size());
		assertTrue(out.get(0) instanceof DataOperationImpl);
		assertTrue(out.get(1) instanceof com.dell.spt.base.item.op.composite.data.CompositeDataOperation);
	}
}
