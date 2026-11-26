package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.collection.Range;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

	// Fixed-range UPDATE (item.data.ranges.fixed). A dedupable write stops at the data ring end, so a range whose bytes
	// cross it is finished over several calls. An append starts at the size from before it and grows the size by its
	// own length, once; an overwrite never changes the recorded size, even when it reaches past the item's end.

	/** The append covers ring positions 3584-4607: the ring ends 512 bytes in. */
	@Test
	public void anAppendCrossingTheRingEndLeavesNoHole() throws Exception {
		assertFixedRangesUpdate(2560, 1024, 2048, 1024, "-1024-");
	}

	/** An append of the ring size always crosses its end: it resumes once. */
	@Test
	public void anAppendOfTheRingSizeLeavesNoHole() throws Exception {
		assertFixedRangesUpdate(2560, 1024, 1024 + 4096, 4096, "-4096-");
	}

	/** An append of more than twice the ring size resumes several times. */
	@Test
	public void anAppendOfSeveralRingsLeavesNoHole() throws Exception {
		assertFixedRangesUpdate(2560, 1024, 1024 + 10_000, 10_000, "-10000-");
	}

	@Test
	public void twoAppendsAreContiguous() throws Exception {
		assertFixedRangesUpdate(0, 1024, 2048, 1024, "-512-", "-512-");
	}

	@Test
	public void twoAppendsCrossingTheRingEndAreContiguous() throws Exception {
		assertFixedRangesUpdate(2560, 1024, 3072, 2048, "-1024-", "-1024-");
	}

	@Test
	public void anOverwriteThenAnAppendGrowTheSizeByTheAppendOnly() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 612, "0-99", "-512-");
	}

	@Test
	public void anAppendThenAnOverwriteGrowTheSizeByTheAppendOnly() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 612, "-512-", "0-99");
	}

	@Test
	public void anAppendNotCrossingTheRingEndIsUnchanged() throws Exception {
		assertFixedRangesUpdate(0, 1024, 2048, 1024, "-1024-");
	}

	// Overwrites of a 1 KiB item at ring position 3500: the ring ends at item byte 596, inside each range.

	@Test
	public void anOverwriteCrossingTheRingEndIsUnchanged() throws Exception {
		assertFixedRangesUpdate(3500, 1024, 1024, 801, "100-900");
	}

	@Test
	public void aSuffixOverwriteCrossingTheRingEndIsUnchanged() throws Exception {
		assertFixedRangesUpdate(3500, 1024, 1024, 500, "-500");
	}

	@Test
	public void aFromOffsetOverwriteCrossingTheRingEndIsUnchanged() throws Exception {
		assertFixedRangesUpdate(3500, 1024, 1024, 724, "300-");
	}

	@Test
	public void twoOverwritesCrossingTheRingEndAreUnchanged() throws Exception {
		assertFixedRangesUpdate(3500, 1024, 1024, 500, "0-99", "500-899");
	}

	// An overwrite may reach past the item's end (the byte-ranges README: "N2 may be more than data item size"). It
	// writes those bytes but keeps the recorded size.

	@Test
	public void anOverwritePastTheItemEndKeepsTheRecordedSize() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1024, 4096, "4096-8191");
	}

	@Test
	public void anOverwriteReachingPastTheItemEndKeepsTheRecordedSize() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1024, 2048, "0-2047");
	}

	/** A file channel that writes at most 7 bytes a call: OS short writes away from the ring end. */
	@Test
	public void appendsSplitByShortWritesLeaveNoHole() throws Exception {
		assertFixedRangesUpdate(2560, 1024, 3072, 2148, 7, "-512-", "-1024-", "0-99", "-512-");
	}

	// A from-offset range (N-) evaluates against the size grown by the earlier appends of the same list, and counts the
	// bytes it writes. One that starts past the end writes nothing and does not stop a later append.

	@Test
	public void aFromOffsetOverwriteThenAnAppendCountTheBytesWritten() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 1236, "300-", "-512-");
	}

	@Test
	public void anAppendThenAFromOffsetOverwriteCountTheBytesWritten() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 1748, "-512-", "300-");
	}

	@Test
	public void twoAppendsThenAnOverwriteCountTheBytesWritten() throws Exception {
		assertFixedRangesUpdate(0, 1024, 2048, 1124, "-512-", "-512-", "0-99");
	}

	@Test
	public void aFromOffsetOverwritePastTheEndDoesNotStopALaterAppend() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 512, "2000-", "-512-");
	}

	@Test
	public void aFromOffsetOverwritePastTheGrownEndWritesNothing() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 512, "-512-", "2000-");
	}

	@Test
	public void aFromOffsetOverwriteThenAnAppendSplitByShortWritesCountTheBytesWritten() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 724 + 512, 7, "300-", "-512-");
	}

	/** A suffix range (-N) after an append also addresses the grown size: it writes bytes 336-1535. */
	@Test
	public void anAppendThenASuffixOverwriteLongerThanTheItemCountTheBytesWritten() throws Exception {
		assertFixedRangesUpdate(0, 1024, 1536, 512 + 1200, "-512-", "-1200");
	}

	// A write that fails after a range of the list was written leaves the recorded size as it was; the retry (the driver
	// resets the operation) writes the same ranges at the same offsets.

	@Test
	public void aRetryAfterTheSecondOfTwoAppendsFailedAppendsOnce() throws Exception {
		assertFixedRangesUpdateFailsThenRetries(512 + 100, 2048, 1024, "-512-", "-512-");
	}

	@Test
	public void aRetryAfterTheSecondOfTwoAppendsFailedAtItsStartAppendsOnce() throws Exception {
		assertFixedRangesUpdateFailsThenRetries(512, 2048, 1024, "-512-", "-512-");
	}

	@Test
	public void aRetryAfterAnOverwriteFollowingAnAppendFailedAppendsOnce() throws Exception {
		assertFixedRangesUpdateFailsThenRetries(512 + 50, 1536, 612, "-512-", "0-99");
	}

	@Test
	public void aRetryAfterAFromOffsetOverwriteFollowingAnAppendFailedAppendsOnce() throws Exception {
		assertFixedRangesUpdateFailsThenRetries(512 + 600, 1536, 1748, "-512-", "300-");
	}

	@Test
	public void aRetryAfterTheSecondAppendOfThreeRangesFailedAppendsOnce() throws Exception {
		assertFixedRangesUpdateFailsThenRetries(512 + 100, 2048, 1124, "-512-", "-512-", "0-99");
	}

	@Test
	public void aRetryAfterTheLastOfThreeRangesFailedAppendsOnce() throws Exception {
		assertFixedRangesUpdateFailsThenRetries(1024 + 50, 2048, 1124, "-512-", "-512-", "0-99");
	}

	/** Calling the helper again after it returned {@code true} changes nothing. */
	@Test
	public void aCallAfterTheUpdateIsDoneChangesNothing() throws Exception {
		assertACallAfterTheUpdateIsDoneChangesNothing(1536, 512, "-512-");
	}

	/** The same after a last range that writes nothing (from offset 2000 of the grown 1,536 bytes). */
	@Test
	public void aCallAfterAnUpdateEndingInAnEmptyRangeIsDoneChangesNothing() throws Exception {
		assertACallAfterTheUpdateIsDoneChangesNothing(1536, 512, "-512-", "2000-");
	}

	@Test
	public void aCallAfterAnOverwriteIsDoneChangesNothing() throws Exception {
		assertACallAfterTheUpdateIsDoneChangesNothing(1024, 400, "0-99", "-300");
	}

	private static void assertACallAfterTheUpdateIsDoneChangesNothing(
					final long expectedSize, final long expectedOpBytes, final String... ranges) throws Exception {
		final DataItemImpl item = new DataItemImpl("item", 0, 1024);
		item.dataInput(new SeedDataInput(RING_SEED, RING_SIZE, 1, true));
		final List<Range> fixed = rangesOf(ranges);
		final Path dst = Files.createTempFile("fs-fixed-ranges-update-", ".bin");
		try {
			Files.write(dst, new byte[1024]);
			final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, null, null, fixed, 0);
			op.startRequest();
			try (final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE)) {
				int guard = 0;
				while (!FileIoHelper.invokeFixedRangesUpdate(item, op, dstCh, fixed)) {
					assertTrue(guard++ < 10_000, "the update makes progress");
				}
				assertTrue(FileIoHelper.invokeFixedRangesUpdate(item, op, dstCh, fixed), "still done");
				assertTrue(FileIoHelper.invokeFixedRangesUpdate(item, op, dstCh, fixed), "still done");
			}
			assertEquals(expectedSize, item.size(), "recorded size");
			assertEquals(expectedOpBytes, op.countBytesDone(), "the operation's byte count");
			assertEquals(expectedSize, Files.size(dst), "file length");
		} finally {
			Files.deleteIfExists(dst);
		}
	}

	// A reversed closed range (N1 > N2, N1 - 1 included) fails the update once the walk reaches it, as the slice of
	// its negative or zero size did before; the ranges before it stay written and counted and the recorded size is
	// kept. A list whose range sizes add up to zero or less, a reversed range's size counted as N2 - N1 + 1 and a
	// from-offset range's as the item size less N, is done at once and writes nothing, as before. Neither may loop.

	@Test
	@Timeout(10)
	public void aReversedFirstRangeFailsTheUpdate() throws Exception {
		assertFixedRangesUpdateFailsOnAReversedRange(1024, 0, "500-400", "0-199");
	}

	@Test
	@Timeout(10)
	public void aReversedRangeBeforeAnAppendFailsTheUpdate() throws Exception {
		assertFixedRangesUpdateFailsOnAReversedRange(1024, 0, "500-400", "-512-");
	}

	/** On the code before the fix this one never finished: the earlier range's negative size put the offset past the next. */
	@Test
	@Timeout(10)
	public void aReversedRangeBeforeAShorterRangeFailsTheUpdate() throws Exception {
		assertFixedRangesUpdateFailsOnAReversedRange(1024, 0, "500-300", "0-99", "-512-");
	}

	@Test
	@Timeout(10)
	public void aReversedMiddleRangeFailsTheUpdateAfterTheEarlierRanges() throws Exception {
		assertFixedRangesUpdateFailsOnAReversedRange(1024, 100, "0-99", "500-400", "-512-");
	}

	@Test
	@Timeout(10)
	public void aReversedLastRangeFailsTheUpdateAfterAnAppend() throws Exception {
		assertFixedRangesUpdateFailsOnAReversedRange(1536, 612, "0-99", "-512-", "500-400");
	}

	@Test
	@Timeout(10)
	public void aClosedRangeEndingBeforeItsStartFailsTheUpdate() throws Exception {
		assertFixedRangesUpdateFailsOnAReversedRange(1024, 0, "500-499", "0-99");
	}

	@Test
	@Timeout(10)
	public void aListWithAReversedRangeSummingToNothingWritesNothing() throws Exception {
		assertDoneInOneCallWritingNothing(1024, "500-300", "0-99");
	}

	@Test
	@Timeout(10)
	public void aListEndingInAReversedRangeSummingToNothingWritesNothing() throws Exception {
		assertDoneInOneCallWritingNothing(1024, "0-99", "500-300");
	}

	@Test
	@Timeout(10)
	public void aListWithAReversedRangeAndAnAppendSummingToNothingWritesNothing() throws Exception {
		assertDoneInOneCallWritingNothing(1024, "2000-", "500-400", "-512-");
	}

	@Test
	@Timeout(10)
	public void aLoneReversedRangeWritesNothing() throws Exception {
		assertDoneInOneCallWritingNothing(1024, "500-300");
	}

	@Test
	@Timeout(10)
	public void aLoneClosedRangeEndingBeforeItsStartWritesNothing() throws Exception {
		assertDoneInOneCallWritingNothing(1024, "500-499");
	}

	/**
	 * Updates a 1 KiB item until the helper throws {@link IllegalArgumentException}, and checks the file length, the
	 * kept recorded size and the bytes counted before the reversed range.
	 */
	private static void assertFixedRangesUpdateFailsOnAReversedRange(
					final long expectedFileLength, final long expectedOpBytes, final String... ranges) throws Exception {
		final DataItemImpl item = new DataItemImpl("item", 0, 1024);
		item.dataInput(new SeedDataInput(RING_SEED, RING_SIZE, 1, true));
		final List<Range> fixed = rangesOf(ranges);
		final Path dst = Files.createTempFile("fs-fixed-ranges-update-", ".bin");
		try {
			Files.write(dst, new byte[1024]);
			final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, null, null, fixed, 0);
			op.startRequest();
			try (final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE)) {
				assertThrows(IllegalArgumentException.class, () -> {
					int guard = 0;
					while (!FileIoHelper.invokeFixedRangesUpdate(item, op, dstCh, fixed)) {
						assertTrue(guard++ < 10_000, "the update makes progress");
					}
				}, "the reversed range fails the update");
			}
			assertEquals(1024, item.size(), "a failed update leaves the recorded size");
			assertEquals(expectedOpBytes, op.countBytesDone(), "the bytes written before the reversed range");
			assertEquals(expectedFileLength, Files.size(dst), "file length");
		} finally {
			Files.deleteIfExists(dst);
		}
	}

	@Test
	public void aWholeItemOverwriteOfAnEmptyItemIsDoneInOneCall() throws Exception {
		assertDoneInOneCallWritingNothing(0, "0-");
	}

	@Test
	public void aFromOffsetOverwritePastTheEndIsDoneInOneCall() throws Exception {
		assertDoneInOneCallWritingNothing(1024, "2000-");
	}

	private static void assertDoneInOneCallWritingNothing(final long size, final String... ranges) throws Exception {
		final DataItemImpl item = new DataItemImpl("item", 0, size);
		item.dataInput(new SeedDataInput(RING_SEED, RING_SIZE, 1, true));
		final List<Range> fixed = rangesOf(ranges);
		final Path dst = Files.createTempFile("fs-fixed-ranges-update-", ".bin");
		try {
			Files.write(dst, new byte[(int) size]);
			final DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, null, null, fixed, 0);
			op.startRequest();
			try (final FileChannel dstCh = FileChannel.open(dst, StandardOpenOption.WRITE)) {
				assertTrue(FileIoHelper.invokeFixedRangesUpdate(item, op, dstCh, fixed), "done in one call");
			}
			assertEquals(size, item.size(), "recorded size");
			assertEquals(0, op.countBytesDone(), "the operation's byte count");
			assertEquals(size, Files.size(dst), "file length");
		} finally {
			Files.deleteIfExists(dst);
		}
	}

	private static List<Range> rangesOf(final String... ranges) {
		final List<Range> fixed = new java.util.ArrayList<>();
		for (final String r : ranges) {
			fixed.add(new Range(r));
		}
		return fixed;
	}

	private static void assertFixedRangesUpdate(
					final long itemOffset, final long size, final long expectedSize, final long expectedOpBytes,
					final String... ranges) throws Exception {
		assertFixedRangesUpdate(itemOffset, size, expectedSize, expectedOpBytes, 0, ranges);
	}

	private static void assertFixedRangesUpdate(
					final long itemOffset, final long size, final long expectedSize, final long expectedOpBytes,
					final int writeCap, final String... ranges) throws Exception {
		assertFixedRangesUpdate(itemOffset, size, expectedSize, expectedOpBytes, writeCap, -1, ranges);
	}

	/** On a 1 KiB item at ring position 2560, the first attempt fails after {@code failAfter} bytes; the retry finishes. */
	private static void assertFixedRangesUpdateFailsThenRetries(
					final long failAfter, final long expectedSize, final long expectedOpBytes, final String... ranges)
					throws Exception {
		assertFixedRangesUpdate(2560, 1024, expectedSize, expectedOpBytes, 0, failAfter, ranges);
	}

	/**
	 * Writes the item's bytes to a file, updates it with the fixed ranges, and checks the file, the recorded size, the
	 * operation's byte count and a verified read of the item as recorded. Every byte the item holds or a range writes
	 * is the layer 0 ring byte at the item offset plus its file offset; bytes nothing wrote are zero. With
	 * {@code failAfter} not negative, a first attempt fails once that many bytes were written; it must leave the recorded
	 * size and count those bytes, and the retry must end as an update with no failure would.
	 */
	private static void assertFixedRangesUpdate(
					final long itemOffset, final long size, final long expectedSize, final long expectedOpBytes,
					final int writeCap, final long failAfter, final String... ranges) throws Exception {
		final SeedDataInput dataInput = new SeedDataInput(RING_SEED, RING_SIZE, 1, true);
		final DataItemImpl item = new DataItemImpl("item", itemOffset, size);
		item.dataInput(dataInput);
		final List<Range> fixed = rangesOf(ranges);

		// Which file bytes the update writes, applying each range in order to the size grown by earlier appends.
		final BitSet written = new BitSet();
		written.set(0, (int) size);
		long grownSize = size;
		for (final Range range : fixed) {
			final long beg = range.getBeg();
			final long end = range.getEnd();
			if (range.getSize() != -1) {
				written.set((int) grownSize, (int) (grownSize + range.getSize()));
				grownSize += range.getSize();
			} else if (beg == -1) {
				written.set((int) (grownSize - end), (int) grownSize);
			} else if (end == -1) {
				if (beg < grownSize) {
					written.set((int) beg, (int) grownSize);
				}
			} else {
				written.set((int) beg, (int) end + 1);
			}
		}
		final ByteBuffer layer = dataInput.getLayer(0).asReadOnlyBuffer();
		final byte[] expected = new byte[written.length()];
		for (int k = written.nextSetBit(0); k >= 0; k = written.nextSetBit(k + 1)) {
			expected[k] = layer.get((int) ((itemOffset + k) % RING_SIZE));
		}

		final Path dst = Files.createTempFile("fs-fixed-ranges-update-", ".bin");
		try {
			Files.write(dst, java.util.Arrays.copyOf(expected, (int) size));
			DataOperationImpl<DataItemImpl> op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, null, null, fixed, 0);
			op.startRequest();
			if (failAfter >= 0) {
				try (final FileChannel fileCh = FileChannel.open(dst, StandardOpenOption.WRITE);
								final FileChannel dstCh = new WriteCappedFileChannel(fileCh, Integer.MAX_VALUE, failAfter)) {
					final DataOperationImpl<DataItemImpl> failing = op;
					assertThrows(IOException.class, () -> {
						int guard = 0;
						while (!FileIoHelper.invokeFixedRangesUpdate(item, failing, dstCh, fixed)) {
							assertTrue(guard++ < 10_000, "the failing update makes progress");
						}
					}, "the first attempt fails");
				}
				assertEquals(size, item.size(), "a failed update leaves the recorded size");
				assertEquals(failAfter, op.countBytesDone(), "a failed update counts the bytes it wrote");
				// the retry: StorageDriverBase.prepare() resets the operation
				op = op.result();
				op.reset();
				op.startRequest();
			}
			try (final FileChannel fileCh = FileChannel.open(dst, StandardOpenOption.WRITE);
							final FileChannel dstCh = writeCap > 0 ? new WriteCappedFileChannel(fileCh, writeCap) : fileCh) {
				boolean done = false;
				int guard = 0;
				while (!done) {
					assertTrue(guard++ < 10_000, "the update makes progress");
					done = FileIoHelper.invokeFixedRangesUpdate(item, op, dstCh, fixed);
				}
			}
			assertEquals(expected.length, Files.size(dst), "file length");
			assertArrayEquals(expected, Files.readAllBytes(dst), "the file holds the ring bytes with no hole");
			assertEquals(expectedSize, item.size(), "recorded size");
			assertEquals(expectedOpBytes, op.countBytesDone(), "the operation's byte count");

			// A verified read of the item as recorded.
			final DataItemImpl recorded = new DataItemImpl("item", itemOffset, item.size());
			recorded.dataInput(dataInput);
			final DataOperationImpl<DataItemImpl> read = new DataOperationImpl<>(0, OpType.READ, recorded, null, null, null, null, 0);
			read.startRequest();
			try (final FileChannel srcCh = FileChannel.open(dst, StandardOpenOption.READ)) {
				boolean done = false;
				int guard = 0;
				while (!done) {
					assertTrue(guard++ < 10_000, "the verified read makes progress");
					done = FileIoHelper.invokeReadAndVerify(recorded, read, srcCh);
				}
			}
			assertEquals(expectedSize, read.countBytesDone());
		} finally {
			Files.deleteIfExists(dst);
		}
	}

	/** Delegates to a file channel, writing at most {@code cap} bytes a call. */
	private static final class WriteCappedFileChannel extends FileChannel {

		private final FileChannel delegate;
		private final int cap;
		private final long failAfter;
		private long written = 0;

		WriteCappedFileChannel(final FileChannel delegate, final int cap) {
			this(delegate, cap, Long.MAX_VALUE);
		}

		/** Also throws an {@link IOException} on a write once {@code failAfter} bytes were written. */
		WriteCappedFileChannel(final FileChannel delegate, final int cap, final long failAfter) {
			this.delegate = delegate;
			this.cap = cap;
			this.failAfter = failAfter;
		}

		private int writeCap(final ByteBuffer src) throws IOException {
			if (written >= failAfter) {
				throw new IOException("injected write failure");
			}
			return (int) Math.min(src.position() + Math.min(cap, failAfter - written), src.limit());
		}

		@Override
		public int write(final ByteBuffer src) throws IOException {
			final int limit = src.limit();
			src.limit(writeCap(src));
			try {
				final int n = delegate.write(src);
				written += n;
				return n;
			} finally {
				src.limit(limit);
			}
		}

		@Override
		public int read(final ByteBuffer dst) throws IOException {
			return delegate.read(dst);
		}

		@Override
		public long read(final ByteBuffer[] dsts, final int offset, final int length) throws IOException {
			return delegate.read(dsts, offset, length);
		}

		@Override
		public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
			return write(srcs[offset]);
		}

		@Override
		public long position() throws IOException {
			return delegate.position();
		}

		@Override
		public FileChannel position(final long newPosition) throws IOException {
			delegate.position(newPosition);
			return this;
		}

		@Override
		public long size() throws IOException {
			return delegate.size();
		}

		@Override
		public FileChannel truncate(final long size) throws IOException {
			delegate.truncate(size);
			return this;
		}

		@Override
		public void force(final boolean metaData) throws IOException {
			delegate.force(metaData);
		}

		@Override
		public long transferTo(final long position, final long count, final java.nio.channels.WritableByteChannel target) throws IOException {
			return delegate.transferTo(position, Math.min(count, cap), target);
		}

		@Override
		public long transferFrom(final ReadableByteChannel src, final long position, final long count) throws IOException {
			return delegate.transferFrom(src, position, Math.min(count, cap));
		}

		@Override
		public int read(final ByteBuffer dst, final long position) throws IOException {
			return delegate.read(dst, position);
		}

		@Override
		public int write(final ByteBuffer src, final long position) throws IOException {
			final int limit = src.limit();
			src.limit(writeCap(src));
			try {
				final int n = delegate.write(src, position);
				written += n;
				return n;
			} finally {
				src.limit(limit);
			}
		}

		@Override
		public java.nio.MappedByteBuffer map(final MapMode mode, final long position, final long size) throws IOException {
			return delegate.map(mode, position, size);
		}

		@Override
		public java.nio.channels.FileLock lock(final long position, final long size, final boolean shared) throws IOException {
			return delegate.lock(position, size, shared);
		}

		@Override
		public java.nio.channels.FileLock tryLock(final long position, final long size, final boolean shared) throws IOException {
			return delegate.tryLock(position, size, shared);
		}

		@Override
		protected void implCloseChannel() throws IOException {
			delegate.close();
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
