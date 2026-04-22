package com.dell.spt.base.item;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataCorruptionException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.io.AsyncChannel;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.google.common.base.Splitter;

public class DataItemImplTest {

	private static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
	private static final long FNV64_PRIME = 0x100000001b3L;

	private static long fnv1a64(final String value) {
		long hash = FNV64_OFFSET_BASIS;
		for (final byte b : value.getBytes(StandardCharsets.UTF_8)) {
			hash ^= b & 0xFFL;
			hash *= FNV64_PRIME;
		}
		return hash;
	}

	/*
		Description: Verify that a new data instance can be read in human format with toString() method.
	   Steps:
	       1. Instance an item with offset, size and name
	       2. Assert that the toString() output matches the instance values
	*/
	@Test
	public void dataVerifyReadTest() {
		String name = "ITEM_A";
		long offset = 100L; // Offset gets printed in base 16 hexadecimal (100 = 64 in hex)
		long size = 500L;

		DataItemImpl item = new DataItemImpl(name, offset, size);
		System.out.println(item.toString());

		String itemStr = item.toString();
		List<String> components = Splitter.on(',').splitToList(itemStr);

		// Name
		assertEquals(name, components.get(0));
		// Offset
		assertEquals("64", components.get(1));
		// Size
		assertEquals(String.valueOf(size), components.get(2));
	}

	// I will dig more into this - but I believe write() takes a ByteBuffer and writes it to a "RingBuff" from the dataInput
	// If the ByteBuffer allocation in ByteBuffer do not match in the write() function, it will fail.
	// TODO: TEST WITH DIFFERENT LAYER SIZES, SEE IF THAT HAS TO MATCH WITH ALLOCATE
	/*
	    Description: Verify that data can be written to the data item without corruption.
	    Steps:
	        1. Create a data item with offset, size and name
	        2. Create a ByteBuffer and allocate ONE new byte buffer in the java heap
	        3. Create a DataInput instance with proper config values (needed for writing to a data item)
	            * Note: These config values are default from https://github.com/dell-spt/spt-base/tree/master/doc/usage/input/configuration#11-reference-table
	        4. Ensure that the data item can be written to without corruption
	 */
	@Test
	public void dataWriteTest() throws IllegalStateException, IllegalArgumentException, IOException {
		String name = "ITEM_A";
		long offset = 100L; // Offset gets printed in base 16 hexadecimal (100 = 64 in hex)
		long size = 500L;

		ByteBuffer data = ByteBuffer.allocate(1);
		data.put((byte) 69); // This NEEDS to match bs in item.write() function..
		data.rewind();

		System.out.println("Data: " + data.get());
		DataItemImpl item = new DataItemImpl(name, offset, size);
		//DataItemImpl item = new DataItemImpl(offset, size, 1);
		Config testConfig = TestConfigBuilder.config();

		// Proper config values to create a SeedDataInput type
		testConfig.val("load-batch-size", 4096);
		testConfig.val("item-data-verify", false);
		testConfig.val("item-data-input-layer-size", "4MB");
		testConfig.val("item-data-input-file", null);
		testConfig.val("item-data-input-seed", "7a42d9c483244167");
		testConfig.val("item-data-input-layer-heap", false);
		testConfig.val("item-data-input-layer-cache", 25);

		DataInput dataInput = generate_data_input(testConfig);

		item.dataInput(dataInput);
		assertDoesNotThrow(() -> item.write(data));
	}

	/*
	    Description: Verify that mismatching the byte buffers causes a data corruption when writing to the data item.
	    Steps:
	        1. Create a data item with offset, size and name
	        2. Create a ByteBuffer and allocate FIVE new byte buffer in the java heap (mismatch, there is only one in the data item currently)
	        3. Create a DataInput instance with proper config values (needed for writing to a data item)
	            * Note: These config values are default from https://github.com/dell-spt/spt-base/tree/master/doc/usage/input/configuration#11-reference-table
	        4. Ensure that the data item throws a DataCorruptionException when trying to write
	 */
	@Test
	public void dataWriteCorruptTest() throws IllegalStateException, IllegalArgumentException, IOException {
		String name = "ITEM_A";
		long offset = 100L; // Offset gets printed in base 16 hexadecimal (100 = 64 in hex)
		long size = 500L;

		// byte[] bytes = new byte[10];
		// ByteBuffer data = ByteBuffer.wrap(bytes);

		ByteBuffer data = ByteBuffer.allocate(5); // Mismatch
		data.put((byte) 10);
		data.rewind();

		System.out.println("Data: " + data.get());
		DataItemImpl item = new DataItemImpl(name, offset, size);
		//DataItemImpl item = new DataItemImpl(offset, size, 1);
		Config testConfig = TestConfigBuilder.config();

		// Proper config values to create a SeedDataInput type
		testConfig.val("load-batch-size", 4096);
		testConfig.val("item-data-verify", false);
		testConfig.val("item-data-input-layer-size", "4MB");
		testConfig.val("item-data-input-file", null);
		testConfig.val("item-data-input-seed", "7a42d9c483244167");
		testConfig.val("item-data-input-layer-heap", false);
		testConfig.val("item-data-input-layer-cache", 25);

		DataInput dataInput = generate_data_input(testConfig);

		item.dataInput(dataInput);

		assertThrows(DataCorruptionException.class, () -> item.write(data));
	}

	/* Utility Methods */
	private DataInput generate_data_input(final Config config) throws IllegalStateException, IllegalArgumentException, IOException {
		DataInput dataInput = null;
		final var itemConfig = config.configVal("item");
		final var itemDataConfig = itemConfig.configVal("data");
		final var itemDataInputConfig = itemDataConfig.configVal("input");
		final var itemDataInputLayerConfig = itemDataInputConfig.configVal("layer");
		final var itemDataInputLayerSizeRaw = itemDataInputLayerConfig.val("size");
		final SizeInBytes itemDataLayerSize;
		if (itemDataInputLayerSizeRaw instanceof String) {
			itemDataLayerSize = new SizeInBytes((String) itemDataInputLayerSizeRaw);
		} else {
			itemDataLayerSize = new SizeInBytes(TypeUtil.typeConvert(itemDataInputLayerSizeRaw, int.class));
		}
		final var itemDataInputFile = itemDataInputConfig.stringVal("file");
		final var itemDataInputSeed = itemDataInputConfig.stringVal("seed");
		final var itemDataInputLayerCacheSize = itemDataInputLayerConfig.intVal("cache");
		final var isInHeapMem = itemDataInputLayerConfig.boolVal("heap");

		try {
			dataInput = DataInput.instance(
							itemDataInputFile, itemDataInputSeed, itemDataLayerSize, itemDataInputLayerCacheSize, isInHeapMem);
		} catch (Exception e) {
			fail("Failed to create DataInput instance", e);
		}
		return dataInput;
	}

	/*
	    Description: Verify constructor parsing using string value (invokes internal (String,int)).
	    Steps:
	        1. Build a value string "NAME,offsetHex,sizeDec,layerHex/maskHex"
	        2. Construct DataItemImpl and assert parsed fields
	 */
	@Test
	public void constructorStringParsingTest() {
		final String name = "Foo";
		final String offsetHex = "64"; // 0x64 = 100
		final long size = 500L;
		final String layerHex = "1";
		final String maskHex = "0"; // empty mask
		final String value = String.join(",", name, offsetHex, Long.toString(size), layerHex + "/" + maskHex);

		final DataItemImpl item = new DataItemImpl(value);
		assertEquals(name, item.name());
		assertEquals(100L, item.offset());
		assertEquals(size, item.size());
		assertEquals(1, item.layer());
	}

	/*
	    Description: Verify() succeeds when input matches the data layer content at current position.
	 */
	@Test
	public void verifySuccessTest() throws Exception {
		final Config testConfig = TestConfigBuilder.config();
		testConfig.val("item-data-input-layer-size", "1MB");
		testConfig.val("item-data-input-file", null);
		testConfig.val("item-data-input-seed", "7a42d9c483244167");
		testConfig.val("item-data-input-layer-heap", false);
		testConfig.val("item-data-input-layer-cache", 25);

		final DataInput dataInput = generate_data_input(testConfig);
		final DataItemImpl item = new DataItemImpl("X", 0L, 4096L);
		item.dataInput(dataInput);

		final ByteBuffer src = dataInput.getLayer(item.layer()).asReadOnlyBuffer();
		src.position(0);
		src.limit(1024);
		final ByteBuffer in = src.slice();

		assertDoesNotThrow(() -> item.verify(in));
	}

	/*
	    Description: Verify() throws DataCorruptionException when input differs.
	 */
	@Test
	public void verifyFailureTest() throws Exception {
		final Config testConfig = TestConfigBuilder.config();
		testConfig.val("item-data-input-layer-size", "1MB");
		testConfig.val("item-data-input-file", null);
		testConfig.val("item-data-input-seed", "7a42d9c483244167");
		testConfig.val("item-data-input-layer-heap", false);
		testConfig.val("item-data-input-layer-cache", 25);

		final DataInput dataInput = generate_data_input(testConfig);
		final DataItemImpl item = new DataItemImpl("Y", 0L, 4096L);
		item.dataInput(dataInput);

		final ByteBuffer layer = dataInput.getLayer(item.layer()).asReadOnlyBuffer();
		layer.position(0);
		layer.limit(64);
		final ByteBuffer in = ByteBuffer.allocate(64);
		in.put(layer.slice());
		in.flip();
		// Corrupt the first byte
		in.put(0, (byte) (in.get(0) ^ 0xFF));

		assertThrows(DataCorruptionException.class, () -> item.verify(in));
	}

	/*
	    Description: writeToSocketChannel writes up to maxCount and advances position.
	 */
	@Test
	public void writeToSocketChannelTest() throws Exception {
		final Config testConfig = TestConfigBuilder.config();
		testConfig.val("item-data-input-layer-size", "1MB");
		testConfig.val("item-data-input-file", null);
		testConfig.val("item-data-input-seed", "7a42d9c483244167");
		testConfig.val("item-data-input-layer-heap", false);
		testConfig.val("item-data-input-layer-cache", 25);

		final DataInput dataInput = generate_data_input(testConfig);
		final DataItemImpl item = new DataItemImpl("Z", 0L, 1_000_000L);
		item.dataInput(dataInput);

		final int maxCount = 4096;
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final var chan = Channels.newChannel(baos);

		final long written = item.writeToSocketChannel(chan, maxCount);
		assertEquals(maxCount, written);
		assertEquals(maxCount, item.position());
		assertEquals(maxCount, baos.size());

		// Verify content matches the data layer's first bytes
		final byte[] out = baos.toByteArray();
		final ByteBuffer layer = dataInput.getLayer(item.layer()).asReadOnlyBuffer();
		layer.position(0);
		layer.limit(maxCount);
		final byte[] expected = new byte[maxCount];
		layer.slice().get(expected);
		assertArrayEquals(expected, out);
	}

	/*
	    Description: writeToFileChannel writes up to maxCount in one operation and advances position.
	 */
	@Test
	public void writeToFileChannelTest() throws Exception {
		final Config testConfig = TestConfigBuilder.config();
		testConfig.val("item-data-input-layer-size", "1MB");
		testConfig.val("item-data-input-file", null);
		testConfig.val("item-data-input-seed", "7a42d9c483244167");
		testConfig.val("item-data-input-layer-heap", false);
		testConfig.val("item-data-input-layer-cache", 25);

		final DataInput dataInput = generate_data_input(testConfig);
		final DataItemImpl item = new DataItemImpl("W", 0L, 1_000_000L);
		item.dataInput(dataInput);

		final int maxCount = 2048;
		final Path tmp = Files.createTempFile("dataitemimpl", ".bin");
		try (final FileChannel fc = FileChannel.open(tmp, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
			final long n = item.writeToFileChannel(fc, maxCount);
			assertEquals(maxCount, n);
			assertEquals(maxCount, item.position());
		}

		final byte[] onDisk = Files.readAllBytes(tmp);
		assertEquals(maxCount, onDisk.length);

		final ByteBuffer layer = dataInput.getLayer(item.layer()).asReadOnlyBuffer();
		layer.position(0);
		layer.limit(maxCount);
		final byte[] expected = new byte[maxCount];
		layer.slice().get(expected);
		assertArrayEquals(expected, onDisk);
	}

	@Test
	public void writeToSocketChannelDedupableFalseTest() throws Exception {
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1MB"), 25, false, 0.0, false);
		final DataItemImpl item = new DataItemImpl("test-dedupe-item", 100L, 1_000_000L);
		item.dataInput(dataInput);

		final int maxCount = 8192;
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final var chan = Channels.newChannel(baos);

		final long written = item.writeToSocketChannel(chan, maxCount);
		assertEquals(maxCount, written);
		assertEquals(maxCount, item.position());
		assertEquals(maxCount, baos.size());

		final byte[] out = baos.toByteArray();
		final ByteBuffer outBuff = ByteBuffer.wrap(out);

		final long objectId = outBuff.getLong();
		assertEquals(fnv1a64("test-dedupe-item"), objectId);
		assertEquals(100L, outBuff.getLong());
		outBuff.position(4096);
		assertEquals(objectId, outBuff.getLong());
		assertEquals(100L + 4096L, outBuff.getLong());
	}

	@Test
	public void writeToSocketChannelDedupableFalseUses64BitObjectIdForJavaHashCollisionsTest() throws Exception {
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1MB"), 25, false, 0.0, false);
		final DataItemImpl itemAa = new DataItemImpl("Aa", 100L, 1_000_000L);
		final DataItemImpl itemBB = new DataItemImpl("BB", 100L, 1_000_000L);
		itemAa.dataInput(dataInput);
		itemBB.dataInput(dataInput);
		assertEquals(itemAa.name().hashCode(), itemBB.name().hashCode());

		final ByteArrayOutputStream aaOut = new ByteArrayOutputStream();
		final ByteArrayOutputStream bbOut = new ByteArrayOutputStream();
		final long aaWritten = itemAa.writeToSocketChannel(Channels.newChannel(aaOut), 16);
		final long bbWritten = itemBB.writeToSocketChannel(Channels.newChannel(bbOut), 16);
		assertEquals(16L, aaWritten);
		assertEquals(16L, bbWritten);

		final ByteBuffer aaBuff = ByteBuffer.wrap(aaOut.toByteArray());
		final ByteBuffer bbBuff = ByteBuffer.wrap(bbOut.toByteArray());
		final long aaObjectId = aaBuff.getLong();
		final long bbObjectId = bbBuff.getLong();
		assertNotEquals((long) itemAa.name().hashCode(), aaObjectId);
		assertNotEquals((long) itemBB.name().hashCode(), bbObjectId);
		assertEquals(fnv1a64(itemAa.name()), aaObjectId);
		assertEquals(fnv1a64(itemBB.name()), bbObjectId);
		assertNotEquals(aaObjectId, bbObjectId);
		assertEquals(100L, aaBuff.getLong());
		assertEquals(100L, bbBuff.getLong());
	}

	@Test
	public void writeToSocketChannelDedupableFalseZeroProgressDoesNotLoopTest() throws Exception {
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1MB"), 25, false, 0.0, false);
		final DataItemImpl item = new DataItemImpl("zero-progress-item", 0L, 1_000_000L);
		item.dataInput(dataInput);
		final WritableByteChannel zeroChan = new WritableByteChannel() {
			@Override
			public int write(final ByteBuffer src) {
				return 0;
			}

			@Override
			public boolean isOpen() {
				return true;
			}

			@Override
			public void close() {}
		};
		final long written = item.writeToSocketChannel(zeroChan, 4096);
		assertEquals(0L, written);
		assertEquals(0L, item.position());
	}

	@Test
	public void readDedupableFalseStampsEachChunkTest() throws Exception {
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1MB"), 25, false, 0.0, false);
		final DataItemImpl item = new DataItemImpl("read-dedupe-item", 200L, 1_000_000L);
		item.dataInput(dataInput);
		final ByteBuffer dst = ByteBuffer.allocate(8192);
		final int n = item.read(dst);
		assertEquals(8192, n);
		assertEquals(8192, item.position());
		dst.flip();
		final long objectId = dst.getLong();
		assertEquals(fnv1a64("read-dedupe-item"), objectId);
		assertEquals(200L, dst.getLong());
		dst.position(4096);
		assertEquals(objectId, dst.getLong());
		assertEquals(200L + 4096L, dst.getLong());
	}

	@Test
	public void writeToAsyncChannelDedupableFalseUsesDistinctStampBuffersTest() throws Exception {
		final DataInput dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("1MB"), 25, false, 0.0, false);
		final DataItemImpl item = new DataItemImpl("async-stamp-item", 100L, 1_000_000L);
		item.dataInput(dataInput);
		final CapturingAsyncChannel channel = new CapturingAsyncChannel();
		final CompletionHandler<Integer, Object> handler = new CompletionHandler<Integer, Object>() {
			@Override
			public void completed(final Integer result, final Object attachment) {}

			@Override
			public void failed(final Throwable exc, final Object attachment) {}
		};
		item.position(0);
		item.writeToAsyncChannel(channel, 0L, 16L, null, handler);
		item.position(4096L);
		item.writeToAsyncChannel(channel, 4096L, 16L, null, handler);
		assertEquals(2, channel.writtenBuffers.size());
		final ByteBuffer first = channel.writtenBuffers.get(0).duplicate();
		final ByteBuffer second = channel.writtenBuffers.get(1).duplicate();
		assertNotSame(channel.writtenBuffers.get(0), channel.writtenBuffers.get(1));
		assertEquals(fnv1a64("async-stamp-item"), first.getLong());
		assertEquals(100L, first.getLong());
		assertEquals(fnv1a64("async-stamp-item"), second.getLong());
		assertEquals(100L + 4096L, second.getLong());
	}

	private static final class CapturingAsyncChannel implements AsyncChannel {

		private final List<ByteBuffer> writtenBuffers = new ArrayList<>();

		@Override
		public boolean isFileChannel() {
			return false;
		}

		@Override
		public AsynchronousChannel wrapped() {
			return this;
		}

		@Override
		public <A> void read(
						final ByteBuffer dst, final long position, final A attach, final CompletionHandler<Integer, ? super A> handler) {
			throw new UnsupportedOperationException("read not used in this test");
		}

		@Override
		public <A> void write(
						final ByteBuffer src, final long position, final A attach, final CompletionHandler<Integer, ? super A> handler) {
			writtenBuffers.add(src);
		}

		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public void close() {}
	}
}
