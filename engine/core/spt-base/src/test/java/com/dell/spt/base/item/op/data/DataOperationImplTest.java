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
	void resetAttemptProgressClearsTheAttemptsProgressAndKeepsItsPlan() throws Exception {
		// Layer 1, so the verifier walks the cursor's cached range slice, as an updated item's read does.
		final var item = new DataItemImpl("obj", 0, 4096, 1);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		assertTrue(DataItem.rangeCount(item.size()) > 2, "the item has ranges past the first");
		// The failed attempt's progress: bytes, a data-response start, and a cursor in its second range.
		op.startRequest();
		op.finishRequest();
		op.startDataResponse();
		op.countBytesDone(17L);
		op.currRangeIdx(1);
		final var staleRange = op.currRange();
		staleRange.position(5);
		// Its plan: a marked range and a content size that reset() would recompute.
		op.markedRangesMaskPair()[0].set(2);
		op.contentSize = 77L;
		final long reqTimeDone = op.reqTimeDone();

		op.resetAttemptProgress();

		assertEquals(0L, op.countBytesDone());
		assertEquals(0L, op.respDataTimeStart());
		assertEquals(0L, op.dataLatency());
		assertEquals(0, op.currRangeIdx(), "the cursor is back at the first range");
		final var range = op.currRange();
		assertNotSame(staleRange, range, "the failed attempt's cached slice is dropped");
		assertEquals(0L, range.position(), "the first range is verified from its start");
		assertEquals(item.rangeSize(0), range.size());
		assertTrue(op.markedRangesMaskPair()[0].get(2), "the range marks are kept");
		assertEquals(77L, op.contentSize, "the content size is kept");
		assertEquals(reqTimeDone, op.reqTimeDone(), "the timing is resetTiming()'s, not this method's");
	}

	@Test
	void resultBuildsItemPath() {
		final var item = newItem("name", 10);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/bucket", null, null, null, 0);
		final var result = op.result();
		assertNotNull(result);
		assertTrue(result.item().name().startsWith("/bucket/"));
	}

	@Test
	void dataLatencyReturnsZeroWhenReqTimeDoneNotSet() {
		final var item = newItem("obj", 1024);
		final var op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		op.startRequest();
		// Skip finishRequest — simulate the Netty race where the response arrives
		// before the channel write-complete listener fires
		assertEquals(0, op.dataLatency(), "dataLatency must be 0 when finishRequest was never called");
	}

	/**
	 * Invariant: dataLatency() must never return an epoch-scale timestamp.
	 * Before the fix, skipping finishRequest() caused dataLatency() to return
	 * {@code respDataTimeStart - 0}, an absolute wall-clock timestamp (~1.77e15 µs).
	 */
	@Test
	void dataLatencyNeverReturnsEpochTimestamp() {
		final long MAX_SANE_LATENCY_MICROS = 1_000_000_000L; // 1000 seconds

		// Case 1: no lifecycle methods — both timestamps zero
		var item = newItem("a", 1024);
		var op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		assertTrue(op.dataLatency() < MAX_SANE_LATENCY_MICROS,
						"dataLatency must not be epoch-scale when no lifecycle methods called");

		// Case 2: full lifecycle including startDataResponse
		item = newItem("b", 1024);
		op = new DataOperationImpl<>(0, OpType.READ, item, "/src", null, null, null, 0);
		op.startRequest();
		op.finishRequest();
		op.startDataResponse();
		assertTrue(op.dataLatency() < MAX_SANE_LATENCY_MICROS,
						"dataLatency must not be epoch-scale in normal lifecycle");
	}

	@Test
	void resultCopyPreservesOpRetryCount() {
		// load-op-retry's bounded check reads whatever concrete Operation subclass a
		// driver actually produces, not just base OperationImpl - DataOperationImpl's own
		// copy constructor/result() must still correctly delegate to the base class's
		// opRetryCount handling rather than, say, forgetting to call super(other).
		final var item = newItem("retry-copy-obj", 1024);
		final var op = new DataOperationImpl<>(0, OpType.CREATE, item, null, "/dst", null, null, 0);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		op.finishResponse();

		final var copy = op.result();

		assertEquals(2, copy.opRetryCount(), "DataOperationImpl's result() copy must preserve opRetryCount");
		op.incrementOpRetryCount();
		assertEquals(2, copy.opRetryCount(), "copy opRetryCount must not change when original is mutated afterward");

		op.reset();
		assertEquals(3, op.opRetryCount(), "opRetryCount must survive reset() on the original");
	}

	// --- a retried random-range UPDATE keeps the failed attempt's ranges ---

	/** A 1 KiB item: 11 ranges, the first {@code updated} already recorded as updated. */
	private static DataItemImpl rangedItem(final int updated) {
		final var item = newItem("obj", 1024);
		if (updated > 0) {
			final var preset = new BitSet[]{new BitSet(), new BitSet()
			};
			preset[0].set(0, updated);
			item.commitUpdatedRanges(preset);
		}
		return item;
	}

	private static BitSet[] copyOf(final BitSet[] pair) {
		return new BitSet[]{(BitSet) pair[0].clone(), (BitSet) pair[1].clone()
		};
	}

	/**
	 * A failed random-range UPDATE keeps its ranges through result() and the retry's reset(),
	 * however often it is retried. Rows: 3 ranges of 11 (a redraw matches by chance once in 165),
	 * and a partial layer switch (10 of 11 recorded, 2 ranges, one in each layer).
	 */
	@org.junit.jupiter.params.ParameterizedTest
	@org.junit.jupiter.params.provider.CsvSource({"0, 3, RESP_FAIL_SVC", "0, 3, FAIL_IO", "10, 2, FAIL_TIMEOUT"
	})
	void aFailedRandomRangeUpdateKeepsItsRangesThroughResultAndRetryReset(
					final int preset, final int count, final com.dell.spt.base.item.op.Operation.Status status) {
		final var item = rangedItem(preset);
		DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, "/b", null, null, count);
		final var marked = copyOf(op.markedRangesMaskPair());
		assertTrue(op.hasMarkedRanges());
		if (preset == 10) {
			assertFalse(marked[0].isEmpty(), "one range in the current layer");
			assertFalse(marked[1].isEmpty(), "one range in the next layer");
		}
		for (int attempt = 1; attempt <= 3; attempt++) {
			op.status(status);
			final var copy = op.result();
			assertArrayEquals(marked, copy.markedRangesMaskPair(), "the result copy of attempt " + attempt);
			copy.incrementOpRetryCount();
			copy.reset();
			assertArrayEquals(marked, copy.markedRangesMaskPair(), "the retry of attempt " + attempt);
			assertEquals(copy.markedRangesSize(), copy.contentSize, "the retry sends the same bytes");
			op = copy;
		}
	}

	/** A successful random-range UPDATE is copied without its ranges. */
	@Test
	void aSuccessfulRandomRangeUpdateIsCopiedWithoutItsRanges() {
		final var op = new DataOperationImpl<>(0, OpType.UPDATE, rangedItem(0), null, "/b", null, null, 3);
		assertTrue(op.hasMarkedRanges());
		op.status(com.dell.spt.base.item.op.Operation.Status.SUCC);
		assertFalse(op.result().hasMarkedRanges());
	}

	/**
	 * A recycled UPDATE after a success (the step context resets the retry count) draws new ranges:
	 * none of the ones the item now records as updated.
	 */
	@Test
	void aRecycledRandomRangeUpdateAfterSuccessDrawsNewRanges() {
		final var item = rangedItem(0);
		final var op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, "/b", null, null, 3);
		op.status(com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC);
		final var retry = op.result();
		retry.incrementOpRetryCount();
		retry.reset();
		retry.status(com.dell.spt.base.item.op.Operation.Status.SUCC);
		item.commitUpdatedRanges(retry.markedRangesMaskPair());
		retry.resetOpRetryCount();
		final var recycled = retry.result();
		recycled.reset();
		assertEquals(3, recycled.markedRangesMaskPair()[0].cardinality());
		assertTrue(recycled.markedRangesMaskPair()[1].isEmpty());
		for (int i = 0; i < DataItem.rangeCount(item.size()); i++) {
			if (item.isRangeUpdated(i)) {
				assertFalse(recycled.markedRangesMaskPair()[0].get(i), "range " + i + " is already updated");
			}
		}
	}

	/**
	 * Without a retry, reset() draws new ranges even when the operation still holds marks: here the
	 * constructor's, which the item has since recorded (as a dispatch after a success would see).
	 */
	@Test
	void aResetWithoutARetryDrawsNewRangesOverHeldMarks() {
		final var item = rangedItem(0);
		final var op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, "/b", null, null, 3);
		final var held = copyOf(op.markedRangesMaskPair());
		item.commitUpdatedRanges(held);
		assertEquals(0, op.opRetryCount());
		op.reset();
		assertEquals(3, op.markedRangesMaskPair()[0].cardinality());
		assertFalse(op.markedRangesMaskPair()[0].intersects(held[0]), "the recorded ranges are not drawn again");
	}

	/** Only an UPDATE keeps its ranges: a failed random-range READ is copied without them. */
	@Test
	void aFailedRandomRangeReadIsCopiedWithoutItsRanges() {
		final var op = new DataOperationImpl<>(0, OpType.READ, rangedItem(0), "/b", null, null, null, 3);
		assertTrue(op.hasMarkedRanges());
		op.status(com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC);
		assertFalse(op.result().hasMarkedRanges());
	}
}
