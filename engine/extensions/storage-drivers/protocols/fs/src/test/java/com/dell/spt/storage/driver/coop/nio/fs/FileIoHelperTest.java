package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.collection.Range;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class FileIoHelperTest {

	@Test
	public void openSrcFileReturnsChannelWhenPresent() throws Exception {
		final Path dir = Files.createTempDirectory("fs-open-src-");
		final Path src = dir.resolve("file.bin");
		Files.write(src, new byte[]{1, 2, 3
		});

		final DataItemImpl item = new DataItemImpl("file.bin", 0, 3);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, dir.toString(), null, null, null, 0);

		final FileChannel ch = FileIoHelper.openSrcFile(op);
		assertNotNull(ch);
		ch.close();
	}

	@Test
	public void openSrcFileDoesNotTakeASiblingDirectoryForTheSourcePath() throws Exception {
		final Path dir = Files.createTempDirectory("fs-open-src-boundary-");
		final Path source = Files.createDirectories(dir.resolve("abc"));
		final Path sibling = Files.createDirectories(dir.resolve("abcdef"));
		Files.write(sibling.resolve("file.bin"), new byte[]{1
		});

		// The name starts with the source path's letters, not with the source path.
		final DataItemImpl item = new DataItemImpl(sibling.resolve("file.bin").toString(), 0, 1);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.CREATE, item, source.toString(), null, null, null, 0);

		assertNull(FileIoHelper.openSrcFile(op));
		assertEquals(Operation.Status.FAIL_IO, op.status());
	}

	@Test
	public void openSrcFileReturnsNullWhenSrcPathEmpty() {
		final DataItemImpl item = new DataItemImpl("file.bin", 0, 0);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);
		assertNull(FileIoHelper.openSrcFile(op));
	}

	@Test
	public void openSrcFileSetsFailStatusOnError() {
		final DataItemImpl item = new DataItemImpl("missing.bin", 0, 0);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, "/unlikely/path", null, null, null, 0);
		final FileChannel ch = FileIoHelper.openSrcFile(op);
		assertNull(ch);
		assertEquals(Operation.Status.FAIL_IO, op.status());
	}

	@Test
	public void invokeCopyTransfersBytes() throws Exception {
		final Path src = Files.createTempFile("fs-copy-src-", ".bin");
		final Path dst = Files.createTempFile("fs-copy-dst-", ".bin");
		Files.write(src, new byte[]{10, 20, 30, 40, 50, 60, 70, 80
		});

		try (final FileChannel srcCh = FileChannel.open(src, StandardOpenOption.READ);
						final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {

			final DataItemImpl item = new DataItemImpl("x", 0, Files.size(src));
			final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);
			op.startRequest(); // set ACTIVE

			boolean done = FileIoHelper.invokeCopy(item, op, srcCh, dstCh);
			if (!done) {
				// complete in a second pass if needed
				done = FileIoHelper.invokeCopy(item, op, srcCh, dstCh);
			}
			assertTrue(done);
			assertEquals(Files.size(src), op.countBytesDone());
		}

		assertArrayEquals(Files.readAllBytes(src), Files.readAllBytes(dst));
	}

	@Test
	public void invokeCreateWritesFromDataItem() throws Exception {
		final Path dst = Files.createTempFile("fs-create-", ".bin");
		final long size = 1024;

		final DataItemImpl item = new DataItemImpl("x", 0, size);
		item.dataInput(new SeedDataInput(0xABCDEF, 4096, 1, true));
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.CREATE, item, null, null, null, null, 0);
		op.startRequest();

		try (final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
			boolean done = FileIoHelper.invokeCreate(item, op, dstCh);
			// Might need multiple iterations depending on ring buffer boundaries
			int guard = 0;
			while (!done && guard++ < 8) {
				done = FileIoHelper.invokeCreate(item, op, dstCh);
			}
			assertTrue(done);
			assertEquals(size, op.countBytesDone());
		}
	}

	/**
	 * A part of a split write retried after its attempt wrote some bytes: the cooperative driver
	 * base's part retry rewinds the part item and resets the attempt's progress, and the retry, into
	 * the file reopened with truncation, writes the whole part, not only what the failed attempt left.
	 */
	@Test
	public void aRetriedPartWritesTheWholePartAfterItsProgressIsReset() throws Exception {
		final Path dst = Files.createTempFile("fs-part-retry-", ".bin");
		final long partSize = 1024;
		final long failedAttemptBytes = 512;
		final DataItemImpl part = new DataItemImpl("part", 0, partSize);
		part.dataInput(new SeedDataInput(0xABCDEF, 4096, 1, true));
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.CREATE, part, null, null, null, null, 0);
		// What a failed attempt leaves on the operation.
		op.startRequest();
		op.countBytesDone(failedAttemptBytes);
		part.position(failedAttemptBytes);

		// CoopStorageDriverBase's part retry: the item is rewound and the attempt's progress reset.
		part.reset();
		op.resetAttemptProgress();
		op.startRequest();

		try (final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
			boolean done = false;
			int guard = 0;
			while (!done && guard++ < 8) {
				done = FileIoHelper.invokeCreate(part, op, dstCh);
			}
			assertTrue(done);
		}
		assertEquals(partSize, op.countBytesDone());
		assertEquals(partSize, Files.size(dst), "the retry writes the whole part");
	}

	@Test
	public void invokeReadConsumesChannelUntilSizeOrEof() throws IOException {
		final byte[] data = new byte[]{1, 2, 3, 4, 5
		};
		final ReadableByteChannel ch = Channels.newChannel(new java.io.ByteArrayInputStream(data));

		final DataItemImpl item = new DataItemImpl("x", 0, data.length);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);

		boolean done = FileIoHelper.invokeRead(item, op, ch);
		if (!done) {
			done = FileIoHelper.invokeRead(item, op, ch);
		}
		assertTrue(done);
		assertEquals(data.length, op.countBytesDone());
	}

	@Test
	public void invokeReadStartsDataResponseOnFirstBytes() throws IOException {
		final byte[] data = new byte[]{1, 2, 3, 4, 5
		};
		final ReadableByteChannel ch = Channels.newChannel(new java.io.ByteArrayInputStream(data));

		final DataItemImpl item = new DataItemImpl("x", 0, data.length);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);
		op.startRequest();
		op.finishRequest();

		assertTrue(FileIoHelper.invokeRead(item, op, ch));
		assertEquals(data.length, op.countBytesDone());
		assertTrue(op.respDataTimeStart() > 0);
	}

	@Test
	public void invokeReadFixedRangesStartsDataResponseOnFirstBytes() throws IOException {
		final Path src = Files.createTempFile("fs-fixed-read-", ".bin");
		Files.write(src, new byte[]{1, 2, 3, 4, 5
		});
		final List<Range> ranges = List.of(new Range(1L, 3L, -1L));
		final DataItemImpl item = new DataItemImpl("x", 0, 5);
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, ranges, 0);
		op.startRequest();
		op.finishRequest();

		try (final FileChannel ch = FileChannel.open(src, StandardOpenOption.READ)) {
			assertTrue(FileIoHelper.invokeReadFixedRanges(item, op, ch, ranges));
		}
		assertTrue(op.respDataTimeStart() > 0);
	}

	@Test
	public void invokeReadAndVerifyWholeItem() throws Exception {
		final long size = 2048;
		final SeedDataInput dataInput = new SeedDataInput(0x123456, 4096, 1, true);

		final DataItemImpl item = new DataItemImpl("item", 0, size);
		item.dataInput(dataInput);

		// Separate source with the same data to read from
		final DataItemImpl src = new DataItemImpl("src", 0, size);
		src.dataInput(dataInput);

		final ReadableByteChannel ch = src; // DataItemImpl implements ReadableByteChannel
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);

		boolean done = false;
		int guard = 0;
		while (!done && guard++ < 16) {
			done = FileIoHelper.invokeReadAndVerify(item, op, ch);
		}
		assertTrue(done, "invokeReadAndVerify should complete within iterations");
		assertEquals(size, op.countBytesDone());
		assertEquals(size, item.position());
	}

	// Random-range UPDATE into a range whose data crosses the end of the data ring. The dedupable
	// writeToFileChannel stops at the ring end, so the range is finished over several calls; each call
	// must go on from the range's own position, not from its first byte.

	private static final int RING_SIZE = 4096;
	private static final long RING_SEED = 0x7a42d9c483244167L;

	/** A 1 KiB item whose range 9 (item bytes 511-1022) starts at ring position 3800: the ring ends 296 bytes in. */
	@Test
	public void aCurrentLayerRangeCrossingTheRingEndGetsItsOwnBytes() throws Exception {
		assertRandomRangesUpdateMatchesTheLayers(1024, 3289, false, 9);
	}

	/** Range 9 starts at ring position 4091: the ring ends 5 bytes into the range. */
	@Test
	public void aCurrentLayerRangeCrossingTheRingEndFiveBytesInGetsItsOwnBytes() throws Exception {
		assertRandomRangesUpdateMatchesTheLayers(1024, 3580, false, 9);
	}

	/** The next-layer slice is cached by the operation, so it kept its position before the fix too. */
	@Test
	public void aNextLayerRangeCrossingTheRingEndGetsItsOwnBytes() throws Exception {
		assertRandomRangesUpdateMatchesTheLayers(1024, 3289, true, 9);
	}

	/** A 16 KiB item on a 4 KiB ring, every range updated: the larger ranges wrap the ring more than once. */
	@Test
	public void anItemLargerThanTheRingGetsEveryUpdatedRangeRight() throws Exception {
		final long size = 16 * 1024;
		final int[] all = new int[DataItem.rangeCount(size)];
		for (int i = 0; i < all.length; i++) {
			all[i] = i;
		}
		assertRandomRangesUpdateMatchesTheLayers(size, 1234, false, all);
	}

	private static void assertRandomRangesUpdateMatchesTheLayers(
					final long size, final long itemOffset, final boolean nextLayer, final int... rangeIdxs)
					throws Exception {
		final SeedDataInput dataInput = new SeedDataInput(RING_SEED, RING_SIZE, 16, true);
		final DataItemImpl item = new DataItemImpl("item", itemOffset, size);
		item.dataInput(dataInput);
		final BitSet marked = new BitSet();
		for (final int i : rangeIdxs) {
			marked.set(i);
		}
		if (nextLayer) {
			// Every range was updated once already, so this update writes the marked ones at the next layer.
			final BitSet all = new BitSet();
			all.set(0, DataItem.rangeCount(size));
			item.commitUpdatedRanges(new BitSet[]{all, new BitSet()
			});
		}
		final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, null, null, null, 0);
		op.markedRangesMaskPair()[nextLayer ? 1 : 0].or(marked);
		final int writeLayer = nextLayer ? 2 : 1;

		// The file starts as the item's recorded state; the update replaces the marked ranges.
		final byte[] before = new byte[(int) size];
		final byte[] expected = new byte[(int) size];
		final int rangeCount = DataItem.rangeCount(size);
		for (int i = 0; i < rangeCount; i++) {
			final int beforeLayer = item.isRangeUpdated(i) ? 1 : 0;
			fillFromLayer(before, dataInput, beforeLayer, itemOffset, i, item.rangeSize(i));
			fillFromLayer(expected, dataInput, marked.get(i) ? writeLayer : beforeLayer, itemOffset, i, item.rangeSize(i));
		}

		final Path dst = Files.createTempFile("fs-random-ranges-update-", ".bin");
		try {
			Files.write(dst, before);
			try (final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE)) {
				boolean done = false;
				int guard = 0;
				while (!done) {
					assertTrue(guard++ < 10_000, "the update makes progress");
					done = FileIoHelper.invokeRandomRangesUpdate(item, op, dstCh);
				}
			}
			assertArrayEquals(expected, Files.readAllBytes(dst), "the file holds each updated range's bytes of layer " + writeLayer);

			// A verified read of the item in its new recorded state agrees with the file.
			item.commitUpdatedRanges(op.markedRangesMaskPair());
			item.reset();
			final DataOperationImpl<DataItemImpl> read = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);
			try (final FileChannel srcCh = FileChannel.open(dst, StandardOpenOption.READ)) {
				boolean done = false;
				int guard = 0;
				while (!done) {
					assertTrue(guard++ < 10_000, "the verified read makes progress");
					done = FileIoHelper.invokeReadAndVerify(item, read, srcCh);
				}
			}
			assertEquals(size, read.countBytesDone());
		} finally {
			Files.deleteIfExists(dst);
		}
	}

	private static void fillFromLayer(
					final byte[] dst, final SeedDataInput dataInput, final int layerIdx, final long itemOffset,
					final int rangeIdx, final long rangeSize) {
		final ByteBuffer layer = dataInput.getLayer(layerIdx).asReadOnlyBuffer();
		final long rangeOffset = DataItem.rangeOffset(rangeIdx);
		for (long k = rangeOffset; k < rangeOffset + rangeSize; k++) {
			dst[(int) k] = layer.get((int) ((itemOffset + k) % RING_SIZE));
		}
	}

	// @Test
	// public void invokeReadAndVerifyUpdatedRanges() throws Exception {
	//     final long size = 4096; // spans multiple ranges
	//     final SeedDataInput dataInput = new SeedDataInput(0xABCDEF, 4096, 1, true);

	//     final DataItemImpl item = new DataItemImpl("itemUpd", 0, size);
	//     item.dataInput(dataInput);
	//     // Mark as updated via layer to exercise the range path
	//     item.layer(1);

	//     final DataItemImpl src = new DataItemImpl("srcUpd", 0, size);
	//     src.dataInput(dataInput);
	//     src.layer(1);

	//     final ReadableByteChannel ch = src;
	//     final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.READ, item, null, null, null, null, 0);

	//     boolean done = false;
	//     int guard = 0;
	//     while (!done && guard++ < 32) {
	//         done = FileIoHelper.invokeReadAndVerify(item, op, ch);
	//     }
	//     assertTrue(done, "invokeReadAndVerify (updated) should complete");
	//     assertEquals(size, op.countBytesDone());
	//     assertEquals(size, item.position());
	// }
}
