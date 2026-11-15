package com.dell.spt.base.item.op.data;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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

	@Test
	void compositeReadSizeThresholdReturnsConfiguredValue() throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src")
						.sizeThreshold(300);
		final var item = newItem("obj", 1000);
		final var op = (CompositeDataOperation<DataItemImpl>) builder.buildOp(item);
		assertEquals(300, op.sizeThreshold());
	}

	@Test
	void compositeReadSubOperationsHaveCorrectSizes() throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src")
						.sizeThreshold(30);
		// 100 / 30 = 3 equal parts (30 each) + 1 tail (10)
		final var item = newItem("obj", 100);
		final var op = (CompositeDataOperation<DataItemImpl>) builder.buildOp(item);
		final var parts = op.subOperations();
		assertEquals(4, parts.size());
		assertEquals(30, parts.get(0).item().size());
		assertEquals(30, parts.get(1).item().size());
		assertEquals(30, parts.get(2).item().size());
		assertEquals(10, parts.get(3).item().size());
		// Verify part numbers are sequential
		for (int i = 0; i < parts.size(); i++) {
			assertEquals(i, parts.get(i).partNumber());
		}
		// Verify op type is propagated
		assertEquals(OpType.READ, parts.get(0).type());
	}

	@Test
	void compositeReadSubOperationsShareParentReference() throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(OpType.READ)
						.inputPath("/src")
						.sizeThreshold(50);
		final var item = newItem("obj", 100);
		final var op = (CompositeDataOperation<DataItemImpl>) builder.buildOp(item);
		final var parts = op.subOperations();
		assertEquals(2, parts.size());
		assertSame(op, parts.get(0).parent());
		assertSame(op, parts.get(1).parent());
	}

	private static DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>> builder(
					final OpType opType, final boolean thresholdFirst) {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0);
		if (thresholdFirst) {
			builder.sizeThreshold(100).opType(opType);
		} else {
			builder.opType(opType).sizeThreshold(100);
		}
		return builder.inputPath("/src");
	}

	/** Only CREATE and READ have a multipart implementation; see the part-size cleanup hang. */
	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"DELETE", "UPDATE", "NOOP", "STAT"
	})
	void typesWithoutMultipartGetPlainOperationsAboveThreshold(final OpType opType) throws Exception {
		for (final boolean thresholdFirst : List.of(true, false)) {
			final var builder = builder(opType, thresholdFirst);
			final var item = newItem("obj", 1000);
			final var op = builder.buildOp(item);
			assertFalse(op instanceof CompositeOperation, opType + " buildOp, thresholdFirst=" + thresholdFirst);
			assertEquals(opType, op.type());
			final List<DataOperation<DataItemImpl>> out = new ArrayList<>();
			builder.buildOps(List.of(newItem("a", 1000), newItem("b", 10)), out);
			assertEquals(2, out.size());
			for (final var built : out) {
				assertFalse(built instanceof CompositeOperation, opType + " buildOps, thresholdFirst=" + thresholdFirst);
				assertEquals(opType, built.type());
			}
			assertEquals(100, builder.sizeThreshold(), "the configured threshold is still reported");
		}
	}

	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"CREATE", "READ"
	})
	void multipartTypesGetCompositeOperationsAboveThreshold(final OpType opType) throws Exception {
		for (final boolean thresholdFirst : List.of(true, false)) {
			final var builder = builder(opType, thresholdFirst);
			final var op = builder.buildOp(newItem("obj", 1000));
			assertTrue(op instanceof CompositeDataOperation, opType + " buildOp, thresholdFirst=" + thresholdFirst);
			assertEquals(100, ((CompositeDataOperation<DataItemImpl>) op).sizeThreshold());
			final List<DataOperation<DataItemImpl>> out = new ArrayList<>();
			builder.buildOps(List.of(newItem("a", 1000), newItem("b", 10)), out);
			assertTrue(out.get(0) instanceof CompositeDataOperation, opType + " buildOps, thresholdFirst=" + thresholdFirst);
			assertFalse(out.get(1) instanceof CompositeOperation);
		}
	}

	@Test
	void switchingOpTypeAwayFromReadDropsTheCompositeAndBack() throws Exception {
		final var builder = builder(OpType.READ, true);
		assertTrue(builder.buildOp(newItem("obj", 1000)) instanceof CompositeDataOperation);
		builder.opType(OpType.DELETE);
		assertFalse(builder.buildOp(newItem("obj", 1000)) instanceof CompositeOperation);
		builder.opType(OpType.CREATE);
		assertTrue(builder.buildOp(newItem("obj", 1000)) instanceof CompositeDataOperation);
	}
}
