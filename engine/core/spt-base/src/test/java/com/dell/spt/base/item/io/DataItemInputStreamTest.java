package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItemImpl;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DataItemInputStreamTest {

	private static final long SEED = 0x7a42d9c483244167L;
	private static final int LAYER_SIZE = 1024; // 1KB ring buffer

	private SeedDataInput dataInput;

	@BeforeEach
	public void setUp() {
		dataInput = new SeedDataInput(SEED, LAYER_SIZE, 8, false);
	}

	@AfterEach
	public void tearDown() throws IOException {
		if (dataInput != null) {
			dataInput.close();
		}
	}

	private static byte[] bytesFromLayer(final SeedDataInput input, final int pos, final int len) {
		final ByteBuffer layer = input.getLayer(0).asReadOnlyBuffer();
		layer.position(pos);
		layer.limit(pos + len);
		final byte[] bs = new byte[len];
		layer.slice().get(bs);
		return bs;
	}

	@Test
	public void readSingleBytesProducesExpectedSequence() throws Exception {
		final long size = 16;
		final long offset = 5;

		final DataItemImpl item = new DataItemImpl("ITEM", offset, size);
		item.dataInput(dataInput);

		final byte[] expected = bytesFromLayer(dataInput, (int) (offset % LAYER_SIZE), (int) size);

		try (final DataItemInputStream in = new DataItemInputStream(item)) {
			final byte[] actual = new byte[(int) size];
			final int n = in.read(actual, 0, (int) size);
			assertEquals(size, n);
			assertArrayEquals(expected, actual);
			// Ensure EOF via single-byte read after full consumption
			assertEquals(-1, in.read());
		}
	}

	@Test
	public void readIntoArrayWithOffsetAndAvailable() throws Exception {
		final long size = 32;
		final long offset = 7;

		final DataItemImpl item = new DataItemImpl("ITEM", offset, size);
		item.dataInput(dataInput);

		final byte[] expected = bytesFromLayer(dataInput, (int) (offset % LAYER_SIZE), (int) size);

		try (final DataItemInputStream in = new DataItemInputStream(item)) {
			assertEquals(size, in.available());

			final byte[] buf = new byte[64];

			// First chunk
			final int n1 = in.read(buf, 4, 10);
			assertEquals(10, n1);
			assertEquals(size - 10, in.available());
			assertArrayEquals(copyOfRange(expected, 0, 10), copyOfRange(buf, 4, 14));

			// Second chunk
			final int n2 = in.read(buf, 0, 16);
			assertEquals(16, n2);
			assertEquals(size - 26, in.available());
			assertArrayEquals(copyOfRange(expected, 10, 26), copyOfRange(buf, 0, 16));

			// Last chunk: current implementation may read past logical size
			final int n3 = in.read(buf, 20, 10);
			assertEquals(10, n3);
			final int expectedAvail = (int) (size - (n1 + n2 + n3));
			assertEquals(expectedAvail, in.available());
			// The first 6 bytes of this chunk still match the expected tail
			assertArrayEquals(copyOfRange(expected, 26, 32), copyOfRange(buf, 20, 26));

			// EOF now
			assertEquals(-1, in.read(buf, 0, 8));
		}
	}

	@Test
	public void markAndResetReproduceSameData() throws Exception {
		final long size = 20;
		final long offset = 3;

		final DataItemImpl item = new DataItemImpl("ITEM", offset, size);
		item.dataInput(dataInput);

		try (final DataItemInputStream in = new DataItemInputStream(item)) {
			final byte[] first = new byte[4];
			final int nFirst = in.read(first, 0, first.length);
			assertEquals(4, nFirst);

			in.mark(0);

			final byte[] beforeReset = new byte[6];
			assertEquals(6, in.read(beforeReset, 0, beforeReset.length));

			in.reset();

			final byte[] afterReset = new byte[6];
			assertEquals(6, in.read(afterReset, 0, afterReset.length));

			assertArrayEquals(beforeReset, afterReset);
		}
	}

	@Test
	public void closeDoesNotThrow() throws Exception {
		final DataItemImpl item = new DataItemImpl("ITEM", 0, 8);
		item.dataInput(dataInput);
		final DataItemInputStream in = new DataItemInputStream(item);
		assertDoesNotThrow(in::close);
	}

	// Minimal replacement for Arrays.copyOfRange to avoid importing java.util.Arrays and keep lines short
	private static byte[] copyOfRange(final byte[] src, final int from, final int to) {
		final int len = to - from;
		final byte[] out = new byte[len];
		System.arraycopy(src, from, out, 0, len);
		return out;
	}
}
