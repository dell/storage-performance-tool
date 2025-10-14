package com.dell.spt.storage.driver.coop.netty.data;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.commons.collection.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class ResponseContentUtilTest {

	private DataInput dataInput;

	@BeforeEach
	public void setUp() throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
	}

	@AfterEach
	public void tearDown() throws Exception {
		if (dataInput != null)
			dataInput.close();
	}

	@Test
	public void verifiesFixedRangesViaList() throws Exception {
		final int itemSize = 4096;
		final int chunk = 300;

		final var item = new DataItemImpl("itemFixed", 0, itemSize);
		item.dataInput(dataInput);

		final int beg = 123;
		final int end = beg + 600; // make the fixed range sufficiently large
		final java.util.List<Range> ranges = java.util.List.of(new Range((long) beg, (long) end, -1L));

		final var op = new DataOperationImpl<DataItemImpl>(
						0, OpType.READ, item, null, null, null, ranges, 0);

		// Content chunk corresponds to the first part of the fixed range
		final ByteBuf content = generateChunk(item.slice(beg, chunk), chunk);

		ResponseContentUtil.verifyChunk(op, 0, content, chunk);

		assertEquals(chunk, op.countBytesDone());
		assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
	}

	@Test
	public void verifiesMarkedRangesViaBitSet() throws Exception {
		final int itemSize = 8192;
		final var item = new DataItemImpl("itemMarked", 0, itemSize);
		item.dataInput(dataInput);

		final var op = new DataOperationImpl<DataItemImpl>(
						0, OpType.READ, item, null, null, null, null, 0);

		// Mark a non-trivial range (index 10 ~ 1024 bytes)
		final int rangeIdx = 10;
		op.markedRangesMaskPair()[0].set(rangeIdx);

		final int chunk = 400; // smaller than the range size
		final long rangeBeg = DataItem.rangeOffset(rangeIdx);

		// Build content chunk matching the selected range
		final ByteBuf content = generateChunk(item.slice(rangeBeg, chunk), chunk);

		ResponseContentUtil.verifyChunk(op, 0, content, chunk);

		assertEquals(chunk, op.countBytesDone());
		assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
	}

	@Test
	public void verifiesUpdatedItemRanges() throws Exception {
		final int itemSize = 8192;
		final var item = new DataItemImpl("itemUpdated", 0, itemSize);
		item.dataInput(dataInput);

		// Mark one range as updated on the item
		final int updatedIdx = 8; // size ~ 256 bytes
		final java.util.BitSet[] mask = new java.util.BitSet[]{new java.util.BitSet(), new java.util.BitSet()
		};
		mask[0].set(updatedIdx);
		item.commitUpdatedRanges(mask);

		final var op = new DataOperationImpl<DataItemImpl>(
						0, OpType.READ, item, null, null, null, null, 0);

		// Start verification at the beginning of the updated range
		op.currRangeIdx(updatedIdx);
		final long startOffset = DataItem.rangeOffset(updatedIdx);
		final int chunk = 128; // within the updated range

		// Generate content from the next layer to reflect the update
		final var updatedSlice = item.slice(startOffset, chunk);
		updatedSlice.layer(item.layer() + 1);
		final ByteBuf content = generateChunk(updatedSlice, chunk);

		ResponseContentUtil.verifyChunk(op, startOffset, content, chunk);

		assertEquals(startOffset + chunk, op.countBytesDone());
		assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
	}

	private static ByteBuf generateChunk(final DataItemImpl item, final int size) {
		final ByteBuffer bb = ByteBuffer.allocate(size);
		int n = 0;
		while (n < size) {
			n += item.read(bb);
		}
		item.reset();
		bb.flip();
		return Unpooled.copiedBuffer(bb);
	}

	private static DataOperationImpl<DataItemImpl> newReadOp(final DataItemImpl item) {
		try {
			final var op = new DataOperationImpl<DataItemImpl>(
							0, OpType.READ, item, null, null, null, null, 0);
			return op;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	public void verifyWholeItemChunkSucceeds() throws Exception {
		final int size = 1024;
		final int chunk = 256;

		final var item = new DataItemImpl("itemA", 0, size);
		item.dataInput(dataInput);

		final var op = newReadOp(item);
		final ByteBuf content = generateChunk(item, chunk);

		ResponseContentUtil.verifyChunk(op, 0, content, chunk);

		assertEquals(chunk, op.countBytesDone());
		assertNotEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
	}

	@Test
	public void detectsCorruptionAndMarksOp() throws Exception {
		final int size = 512;
		final int chunk = 128;

		final var item = new DataItemImpl("itemB", 0, size);
		item.dataInput(dataInput);

		final var op = newReadOp(item);
		final ByteBuf content = generateChunk(item, chunk).copy();

		// Flip one byte in the middle
		final int badAt = chunk / 2;
		final byte orig = content.getByte(badAt);
		content.setByte(badAt, orig ^ 0xFF);

		ResponseContentUtil.verifyChunk(op, 0, content, chunk);

		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
		assertEquals(badAt, op.countBytesDone(), "countBytesDone should equal mismatch offset");
	}

	@Test
	public void sizeTooBigMarksCorruptAndSetsActualCount() throws Exception {
		final int size = 256;

		final var item = new DataItemImpl("itemC", 0, size);
		item.dataInput(dataInput);

		final var op = newReadOp(item);
		final int tooBig = size + 10;

		// Content is irrelevant here; verification fails on size check first
		final ByteBuf content = Unpooled.buffer(0, 0);

		ResponseContentUtil.verifyChunk(op, 0, content, tooBig);

		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
		assertEquals(tooBig, op.countBytesDone(), "countBytesDone should equal actual size passed");
	}
}
