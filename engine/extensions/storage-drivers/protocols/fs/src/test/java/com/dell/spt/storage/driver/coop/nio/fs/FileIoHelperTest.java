package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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
