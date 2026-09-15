package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RangeReadOperationTest {
	@Test
	void retryResetPreservesSelectionAndSourceMetadata() throws Exception {
		final var item = new DataItemImpl("item", 7, 1024);
		final AtomicInteger draws = new AtomicInteger();
		final var op = new RangeReadOperation<>(0, item, "/bucket", null, null,
						new RangeReadPolicy(16, null, 3), bound -> draws.getAndIncrement());
		final var range = op.selection();
		op.incrementOpRetryCount();
		op.countBytesDone(8);
		op.reset();
		assertSame(range, op.selection());
		assertEquals(1, draws.get());
		assertEquals(1, op.opRetryCount());
		assertEquals(0, op.countBytesDone());
		assertEquals(1024, item.size());
		assertEquals(7, item.offset());
		assertFalse(((Object) op) instanceof CompositeOperation);
		assertFalse(((Object) op) instanceof PartialOperation);
		assertFalse(op.hasMarkedRanges());
	}

	@Test
	void successfulRecycleResamplesButOldResultKeepsItsRangeAndLifecycle() {
		final var item = new DataItemImpl("item", 0, 1024);
		final AtomicInteger draws = new AtomicInteger();
		final var op = new RangeReadOperation<>(0, item, "/bucket", null, null,
						new RangeReadPolicy(16, null, 3), bound -> draws.getAndIncrement());
		final var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		assertTrue(tracker.driverQueued(op));
		assertTrue(tracker.dispatched(op));
		assertTrue(tracker.completionStarted(op));
		op.status(Operation.Status.SUCC);
		op.countBytesDone(16);
		final var result = op.result();
		assertTrue(tracker.terminal(op));
		assertTrue(tracker.generatorBuffered(result));
		assertNotSame(op.lifecycle(), result.lifecycle());
		assertEquals(0, op.selection().range().offset());
		assertEquals(3, result.selection().range().offset());
		assertEquals(16, op.countBytesDone());
		result.reset();
		assertEquals(0, result.countBytesDone());
		assertEquals(16, op.countBytesDone());
		assertFalse(tracker.localFailure(result, op.lifecycle()));
		assertTrue(tracker.unattempted(result));
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void builderRetainsUnknownAndUndersizedSelectionsInSingleAndBatchPaths() throws Exception {
		final var unknown = mock(DataItem.class);
		when(unknown.name()).thenReturn("unknown");
		when(unknown.size()).thenThrow(new IOException("size unavailable"));
		final var builder = new RangeReadOperationsBuilder<DataItem>(0, new RangeReadPolicy(16, null, 1));
		assertEquals(OpType.READ, builder.opType());
		assertEquals(RangeReadPolicy.SelectionError.SIZE_UNAVAILABLE, builder.buildOp(unknown).selection().error());
		final List<RangeReadOperation<DataItem>> output = new ArrayList<>();
		builder.buildOps(List.of(unknown, new DataItemImpl("small", 0, 1)), output);
		assertEquals(2, output.size());
		assertEquals(RangeReadPolicy.SelectionError.UNDERSIZED_OBJECT, output.get(1).selection().error());
		verify(unknown, never()).reset();
		verify(unknown, never()).offset(anyLong());
		assertThrows(IllegalArgumentException.class, () -> builder.opType(OpType.CREATE));
	}

	@Test
	void fixedModeNeverReadsSizeAndLocalFailureCannotRecycle() throws Exception {
		final var item = mock(DataItem.class);
		when(item.name()).thenReturn("unknown");
		final var fixed = new RangeReadOperationsBuilder<DataItem>(0, new RangeReadPolicy(16, 100L, 1)).buildOp(item);
		assertEquals(new ByteRange(100, 16), fixed.selection().range());
		verify(item, never()).size();
		final var invalid = new RangeReadOperationsBuilder<DataItemImpl>(0, new RangeReadPolicy(16, null, 1))
						.buildOp(new DataItemImpl("empty", 0, 0));
		final var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItemImpl>>();
		assertTrue(tracker.driverQueued(invalid));
		assertTrue(tracker.localFailure(invalid, invalid.lifecycle()));
		assertFalse(tracker.generatorBuffered(invalid));
		assertTrue(tracker.counters().reconciled());
	}

	@Test
	void prohibitsLegacyMutationAndOutOfRangeSuccessfulBytes() {
		final var op = new RangeReadOperationsBuilder<DataItemImpl>(0, new RangeReadPolicy(16, 0L, 1))
						.buildOp(new DataItemImpl("item", 0, 100));
		assertThrows(UnsupportedOperationException.class, () -> op.markRandomRanges(1));
		assertThrows(UnsupportedOperationException.class, op::currRange);
		assertThrows(IllegalArgumentException.class, () -> op.countBytesDone(17));
		assertThrows(IllegalArgumentException.class, () -> op.countBytesDone(-1));
		assertThrows(IllegalStateException.class, op::startDataResponse);
	}
}
