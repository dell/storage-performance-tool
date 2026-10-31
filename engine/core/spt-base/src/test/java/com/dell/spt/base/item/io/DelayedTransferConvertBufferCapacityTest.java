package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import java.io.EOFException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** The handoff between pipeline contexts holds at most its limit and ends once poisoned and drained. */
class DelayedTransferConvertBufferCapacityTest {

	private static final Duration BOUND = Duration.ofSeconds(5);

	@Test
	void putsAndGetsAtTheLimitNeverBlockAPutWhileThereIsRoom() {
		final var buffer = buffer(2);
		final List<DataItem> taken = new ArrayList<>();

		assertTimeoutPreemptively(BOUND, () -> {
			for (int i = 0; i < 100; i++) {
				buffer.put(op("a" + i));
				buffer.put(op("b" + i));
				assertEquals(2, buffer.get(taken, 10));
			}
		});
		assertEquals(200, taken.size());
	}

	@Test
	void singleGetsAndSkipsFreeTheirRoom() {
		final var buffer = buffer(2);

		assertTimeoutPreemptively(BOUND, () -> {
			for (int i = 0; i < 100; i++) {
				buffer.put(op("a" + i));
				buffer.put(op("b" + i));
				assertEquals("a" + i, buffer.get().name());
				assertEquals(1, buffer.skip(5));
			}
		});
	}

	@Test
	void aPutWaitsWhileTheBufferIsFullAndGoesInOnceAnItemIsTaken() throws Exception {
		final var buffer = buffer(1);
		buffer.put(op("first"));

		final var second = CompletableFuture.runAsync(() -> buffer.put(op("second")));
		TimeUnit.MILLISECONDS.sleep(100);
		assertTrue(!second.isDone(), "a full buffer holds the put back");

		assertEquals("first", buffer.get().name());
		second.get(BOUND.toMillis(), TimeUnit.MILLISECONDS);
		assertEquals("second", buffer.get().name());
	}

	@Test
	void aPoisonedBufferEndsOnceDrained() {
		final var buffer = buffer(10);
		buffer.put(op("last"));
		buffer.put((Operation<DataItem>) null);
		final List<DataItem> taken = new ArrayList<>();

		assertEquals(1, buffer.get(taken, 10));
		assertThrows(EOFException.class, () -> buffer.get(taken, 10));
		assertThrows(EOFException.class, buffer::get);
	}

	@Test
	void anEmptyBufferThatIsNotPoisonedIsOnlyEmpty() {
		final var buffer = buffer(10);
		buffer.put(op("taken"));
		final List<DataItem> taken = new ArrayList<>();
		assertEquals(1, buffer.get(taken, 10));

		assertEquals(0, buffer.get(taken, 10));
		assertNull(buffer.get());
	}

	@Test
	void aRangeLargerThanTheRoomGoesInWholeAndInOrderAsTheReaderTakesIt() throws Exception {
		final var buffer = buffer(2);
		final List<Operation<DataItem>> range = new ArrayList<>();
		for (int i = 0; i < 50; i++) {
			range.add(op("item-" + i));
		}

		final var put = CompletableFuture.supplyAsync(() -> buffer.put(range, 0, range.size()));
		final List<DataItem> taken = new ArrayList<>();
		assertTimeoutPreemptively(BOUND, () -> {
			while (taken.size() < range.size()) {
				buffer.get(taken, 10);
			}
		});

		assertEquals(range.size(), put.get(BOUND.toMillis(), TimeUnit.MILLISECONDS));
		assertEquals(range.stream().map(op -> op.item().name()).toList(), taken.stream().map(DataItem::name).toList());
		assertEquals(0, buffer.get(taken, 10));
	}

	@Test
	void aRangeEndingInPoisonEndsTheBufferAfterItsItems() {
		final var buffer = buffer(10);
		final List<Operation<DataItem>> range = Arrays.asList(op("x"), op("y"), null);

		assertEquals(3, buffer.put(range));

		final List<DataItem> taken = new ArrayList<>();
		assertEquals(2, buffer.get(taken, 10));
		assertThrows(EOFException.class, () -> buffer.get(taken, 10));
	}

	private static DelayedTransferConvertBuffer<DataItem, Operation<DataItem>> buffer(final int limit) {
		return new DelayedTransferConvertBuffer<>(limit, 0, TimeUnit.SECONDS);
	}

	private static Operation<DataItem> op(final String name) {
		return new OperationImpl<>(0, OpType.CREATE, new DataItemImpl(name, 0, 1), null, null, null);
	}
}
