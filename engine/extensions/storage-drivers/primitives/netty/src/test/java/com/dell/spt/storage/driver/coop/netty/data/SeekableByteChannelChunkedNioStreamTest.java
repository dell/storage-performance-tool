package com.dell.spt.storage.driver.coop.netty.data;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

/**
 * Tests for {@link SeekableByteChannelChunkedNioStream} — chunked reading logic
 * and end-of-input detection.
 */
class SeekableByteChannelChunkedNioStreamTest {

	/** In-memory SeekableByteChannel backed by a ByteBuffer. */
	private static final class InMemoryByteChannel implements SeekableByteChannel {
		private final ByteBuffer buffer;
		private boolean open = true;

		InMemoryByteChannel(final byte[] data) {
			this.buffer = ByteBuffer.wrap(data);
		}

		@Override
		public int read(final ByteBuffer dst) {
			if (!buffer.hasRemaining()) {
				return -1;
			}
			final int toRead = Math.min(dst.remaining(), buffer.remaining());
			final int oldLimit = buffer.limit();
			buffer.limit(buffer.position() + toRead);
			dst.put(buffer);
			buffer.limit(oldLimit);
			return toRead;
		}

		@Override
		public int write(final ByteBuffer src) {
			throw new UnsupportedOperationException();
		}

		@Override
		public long position() {
			return buffer.position();
		}

		@Override
		public SeekableByteChannel position(final long newPosition) {
			buffer.position((int) newPosition);
			return this;
		}

		@Override
		public long size() {
			return buffer.capacity();
		}

		@Override
		public SeekableByteChannel truncate(final long size) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean isOpen() {
			return open;
		}

		@Override
		public void close() {
			open = false;
		}
	}

	@Test
	void readsEntireContent_singleChunk() throws Exception {
		final byte[] data = new byte[64];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) i;

		final var stream = new SeekableByteChannelChunkedNioStream(new InMemoryByteChannel(data));

		assertFalse(stream.isEndOfInput());
		assertEquals(64, stream.length());
		assertEquals(0, stream.progress());

		final ByteBuf chunk = stream.readChunk(UnpooledByteBufAllocator.DEFAULT);
		assertNotNull(chunk);
		assertEquals(64, chunk.readableBytes());
		assertTrue(stream.isEndOfInput());
		assertEquals(64, stream.progress());

		// Verify content
		for (int i = 0; i < data.length; i++) {
			assertEquals(data[i], chunk.getByte(i));
		}

		chunk.release();
		stream.close();
	}

	@Test
	void readsInMultipleChunks_largeContent() throws Exception {
		// Create data larger than BUFF_SIZE_MAX to trigger chunking.
		// BUFF_SIZE_MAX is defined in StorageDriver, typically 1MB.
		// Use a more modest size that still triggers the partial-chunk path.
		final int totalSize = 4096;
		final byte[] data = new byte[totalSize];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) (i & 0xFF);

		final var stream = new SeekableByteChannelChunkedNioStream(new InMemoryByteChannel(data));

		long totalRead = 0;
		int chunks = 0;
		while (!stream.isEndOfInput()) {
			final ByteBuf chunk = stream.readChunk(UnpooledByteBufAllocator.DEFAULT);
			assertNotNull(chunk);
			totalRead += chunk.readableBytes();
			chunks++;
			chunk.release();
		}

		assertEquals(totalSize, totalRead, "All bytes should be read");
		assertEquals(totalSize, stream.progress());
		assertTrue(chunks >= 1, "Should read at least one chunk");

		stream.close();
	}

	@Test
	void readChunkAfterEnd_returnsNull() throws Exception {
		final byte[] data = new byte[16];
		final var stream = new SeekableByteChannelChunkedNioStream(new InMemoryByteChannel(data));

		// Read all content
		while (!stream.isEndOfInput()) {
			final ByteBuf chunk = stream.readChunk(UnpooledByteBufAllocator.DEFAULT);
			if (chunk != null)
				chunk.release();
		}

		// Next read after end should return null
		assertNull(stream.readChunk(UnpooledByteBufAllocator.DEFAULT));

		stream.close();
	}

	@Test
	void closeDelegates() throws Exception {
		final var channel = new InMemoryByteChannel(new byte[8]);
		final var stream = new SeekableByteChannelChunkedNioStream(channel);

		assertTrue(channel.isOpen());
		stream.close();
		assertFalse(channel.isOpen());
	}
}
