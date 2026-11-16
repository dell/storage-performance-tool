package com.dell.spt.storage.driver.coop.netty.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A part of a split read of an item whose ranges were updated is verified against the whole item's
 * cells, which are fixed by offset in the whole item, not by offset in the part.
 */
final class ResponseContentUtilUpdatedPartTest {

	private static final int PART_SIZE = 1024;
	/** Not a divisor of any cell boundary, so chunks end mid-cell. */
	private static final int CHUNK = 100;

	@Test
	void aPartOfAnItemWithNoUpdatedRangesVerifies() throws Exception {
		assertPartsVerify(3 * PART_SIZE, 0, ring());
	}

	@Test
	void aPartOfAnItemWithAnUpdatedCellInsideTheFirstPartVerifies() throws Exception {
		assertPartsVerify(3 * PART_SIZE, 0, ring(), 5);
	}

	@Test
	void aPartOfAnItemWithAnUpdatedCellAcrossAPartBoundaryVerifies() throws Exception {
		assertPartsVerify(3 * PART_SIZE, 0, ring(), 10);
	}

	@Test
	void aPartOfAnItemWithAnUpdatedCellInTheLastPartVerifies() throws Exception {
		assertPartsVerify(3 * PART_SIZE, 0, ring(), 11);
	}

	@Test
	void aPartOfAnItemWithTwoUpdatedCellsVerifies() throws Exception {
		assertPartsVerify(3 * PART_SIZE, 0, ring(), 3, 10);
	}

	@Test
	void aPartOfALayer1ItemWithAnUpdatedCellVerifies() throws Exception {
		assertPartsVerify(3 * PART_SIZE, 1, ring(), 5);
	}

	@Test
	void aTailPartShorterThanThePartSizeVerifies() throws Exception {
		assertPartsVerify(3000, 0, ring(), 5, 11);
	}

	@Test
	void aPartOfAnItemLargerThanItsDataRingVerifies() throws Exception {
		assertPartsVerify(3000, 0, new SeedDataInput(1, 1000, 3, true), 3, 10, 11);
	}

	/** The mask says the cell is updated, but the target returned the base layer's bytes. */
	@Test
	void aPartWithStaleContentForAnUpdatedCellIsCorruptAtTheCellsOffsetInThePart() throws Exception {
		final var input = ring();
		final var item = item(3 * PART_SIZE, 0, input, 10);
		// Cell 10 is bytes 1023..2046: the last byte of part 0 and most of part 1.
		final byte[] stale = content(item(3 * PART_SIZE, 0, input));
		final var parts = parts(item);

		final var part0 = parts.get(0);
		feed(part0, stale, 0);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, part0.status());
		assertEquals(1023, part0.countBytesDone());

		final var part1 = parts.get(1);
		feed(part1, stale, PART_SIZE);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, part1.status());
		assertEquals(0, part1.countBytesDone());

		final var part2 = parts.get(2);
		feed(part2, stale, 2 * PART_SIZE);
		assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, part2.status());
	}

	/** A retried part starts again from no bytes and verifies from its start. */
	@Test
	void aRetriedPartOfAnUpdatedItemVerifiesFromItsStart() throws Exception {
		final var input = ring();
		final var item = item(3 * PART_SIZE, 0, input, 10);
		final byte[] bytes = content(item);
		final var part1 = parts(item).get(1);
		// The first attempt verifies half the part, then the connection is cut.
		final ByteBuf half = Unpooled.wrappedBuffer(bytes, PART_SIZE, PART_SIZE / 2);
		ResponseContentUtil.verifyChunk(part1, 0, half, PART_SIZE / 2);
		assertEquals(PART_SIZE / 2, part1.countBytesDone());
		part1.countBytesDone(0);
		feed(part1, bytes, PART_SIZE);
		assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, part1.status());
		assertEquals(PART_SIZE, part1.countBytesDone());
	}

	private void assertPartsVerify(final long size, final int layer, final DataInput input, final int... cells)
					throws Exception {
		final var item = item(size, layer, input, cells);
		final byte[] bytes = content(item);
		final var parts = parts(item);
		for (final var part : parts) {
			final long partSize = part.item().size();
			feed(part, bytes, (long) part.partNumber() * PART_SIZE);
			assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, part.status(),
							() -> "part " + part.partNumber() + " corrupt at " + part.countBytesDone());
			assertEquals(partSize, part.countBytesDone());
		}
	}

	/** Verifies a part's bytes from {@code bytes} at {@code start}, chunk by chunk. */
	private static void feed(final PartialDataOperation<?> part, final byte[] bytes, final long start) throws Exception {
		final long partSize = part.item().size();
		long done = 0;
		while (done < partSize && part.status() != Operation.Status.RESP_FAIL_CORRUPT) {
			final int n = (int) Math.min(CHUNK, partSize - done);
			final ByteBuf chunk = Unpooled.wrappedBuffer(bytes, (int) (start + done), n);
			ResponseContentUtil.verifyChunk(part, part.countBytesDone(), chunk, n);
			done += n;
		}
	}

	private static List<? extends PartialDataOperation<DataItemImpl>> parts(final DataItemImpl item) {
		return new CompositeDataOperationImpl<>(0, OpType.READ, item, null, "/bucket", null, null, 0, PART_SIZE)
						.subOperations();
	}

	private static DataInput ring() {
		return new SeedDataInput(0x7a42d9c483244167L, 64 * 1024, 16, true);
	}

	private static DataItemImpl item(final long size, final int layer, final DataInput input, final int... cells) {
		final var item = new DataItemImpl("updated", 9, size, layer);
		item.dataInput(input);
		if (cells.length > 0) {
			final var mask = new BitSet();
			for (final int cell : cells) {
				mask.set(cell);
			}
			item.commitUpdatedRanges(new BitSet[]{mask, new BitSet()
			});
		}
		return item;
	}

	/** What a target holds for the item: its layer, with each updated cell at the next layer. */
	private static byte[] content(final DataItemImpl item) throws Exception {
		final int size = (int) item.size();
		final byte[] all = read(item.slice(0, size), size);
		for (int i = 0; DataItem.rangeOffset(i) < size; i++) {
			if (item.isRangeUpdated(i)) {
				final int cellOffset = (int) DataItem.rangeOffset(i);
				final int cellSize = (int) item.rangeSize(i);
				final var cell = item.slice(cellOffset, cellSize);
				cell.layer(item.layer() + 1);
				System.arraycopy(read(cell, cellSize), 0, all, cellOffset, cellSize);
			}
		}
		return all;
	}

	private static byte[] read(final DataItemImpl item, final int size) {
		final ByteBuffer bb = ByteBuffer.allocate(size);
		while (bb.hasRemaining()) {
			item.read(bb);
		}
		return bb.array();
	}
}
