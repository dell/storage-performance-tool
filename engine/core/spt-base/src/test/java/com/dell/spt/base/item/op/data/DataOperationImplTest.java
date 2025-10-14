package com.dell.spt.base.item.op.data;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.commons.collection.Range;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DataOperationImplTest {

	private static DataItemImpl newItem(final String name, final long size) {
		return new DataItemImpl(name, 0, size);
	}

	@Test
	void startDataResponseThrowsIfRequestNotFinished() {
		final var item = newItem("obj", 1024);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		// Not calling finishRequest() should cause an error
		assertThrows(IllegalStateException.class, op::startDataResponse);
	}

	@Test
	void dataLatencyNonNegativeAfterFinishRequestAndStartDataResponse() {
		final var item = newItem("obj", 1024);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		op.startRequest();
		op.finishRequest();
		assertDoesNotThrow(op::startDataResponse);
		assertTrue(op.dataLatency() >= 0);
	}

	@Test
	void currRangeIsCachedAndHasExpectedLayer() {
		final var item = newItem("obj", 64);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		// Select a valid range index
		final int rangeIdx = Math.min(1, DataItem.rangeCount(item.size()) - 1);
		op.currRangeIdx(rangeIdx);
		final var r1 = op.currRange();
		final var r2 = op.currRange();
		assertSame(r1, r2, "currRange should be cached for the same index");
		// If the original item range isn't updated, the layer remains unchanged
		assertEquals(item.layer(), r1.layer());
	}

	@Test
	void currRangeUpdateRespectsMarkedLayers() {
		final var item = newItem("obj", 128);
		final var op = new DataOperationImpl<>(0, OpType.UPDATE, item, "/src", null, null, null, 0);
		final int rangeIdx = 0;
		op.currRangeIdx(rangeIdx);
		// Mark 1st-layer update
		final BitSet[] masks = op.markedRangesMaskPair();
		masks[0].set(rangeIdx);
		final var r1 = op.currRangeUpdate();
		assertNotNull(r1);
		assertEquals(item.layer() + 1, r1.layer());
		// Mark 2nd-layer update (clear first, set second)
		masks[0].clear(rangeIdx);
		masks[1].set(rangeIdx);
		final var r2 = op.currRangeUpdate();
		assertNotNull(r2);
		assertEquals(item.layer() + 2, r2.layer());
	}

	@Test
	void markedRangesSizeForFixedRangesVariousForms() {
		final var size = 1000L;
		final var item = newItem("obj", size);
		final List<Range> fixedRanges = new ArrayList<>();
		// explicit size form
		fixedRanges.add(new Range(100L, 200L, 50L));
		// beg..end inclusive
		fixedRanges.add(new Range(0L, 9L, -1)); // 10 bytes
		// from beg to end-of-item
		fixedRanges.add(new Range(900L, -1L, -1L)); // size - 900
		// from start until end value
		fixedRanges.add(new Range(-1L, 20L, -1L)); // 20 bytes

		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/in", null, null, fixedRanges, 0);
		long expected = 50L + 10L + (size - 900L) + 20L;
		assertEquals(expected, op.markedRangesSize());
	}

	@Test
	void resetReadWithFixedRangesExceedingItemSizeThrows() {
		final var item = newItem("obj", 100);
		final List<Range> fixedRanges = new ArrayList<>();
		// Sum of ranges intentionally larger than item size
		fixedRanges.add(new Range(0L, 200L, -1L));
		assertThrows(
						IllegalArgumentException.class,
						() -> new DataOperationImpl<>(0, OpType.READ, item, "/in", null, null, fixedRanges, 0));
	}

	@Test
	void markRandomRangesSetsSomeMaskBitsAndHasSizeWithinBounds() {
		final var item = newItem("obj", 1024);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/in", null, null, null, 0);
		final int count = Math.max(1, Math.min(3, DataItem.rangeCount(item.size())));
		op.markRandomRanges(count);
		assertTrue(op.hasMarkedRanges());
		// size should be between 1 and item.size
		final long size = op.markedRangesSize();
		assertTrue(size > 0 && size <= item.size());
		// verify consistent with mask bits
		long recomputed = 0L;
		final BitSet[] masks = op.markedRangesMaskPair();
		for (int i = 0; i < DataItem.rangeCount(item.size()); i++) {
			if (masks[0].get(i) || masks[1].get(i)) {
				recomputed += item.rangeSize(i);
			}
		}
		assertEquals(recomputed, size);
	}

	@Test
	void countBytesDoneGetterSetter() {
		final var item = newItem("obj", 10);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);
		assertEquals(0L, op.countBytesDone());
		op.countBytesDone(7L);
		assertEquals(7L, op.countBytesDone());
	}

	@Test
	void resultBuildsItemPath() {
		final var item = newItem("name", 10);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/bucket", null, null, null, 0);
		final var result = op.result();
		assertNotNull(result);
		assertTrue(result.item().name().startsWith("/bucket/"));
	}
}
