package com.dell.spt.storage.driver.coop.netty.data;

import static com.dell.spt.base.storage.driver.StorageDriver.BUFF_SIZE_MAX;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

/**
 * Modified version of io.netty.handler.stream.ChunkedNioStream.
 * Ensures that the final call to readChunk() writes only a partial chunk to the
 * given byte channel and isEndOfInput() causes the stream to end.
 */
public final class SeekableByteChannelChunkedNioStream implements ChunkedInput<ByteBuf> {
	private final ReadableByteChannel in;
	private final int chunkSize;
	private final ByteBuffer byteBuffer;
	private final long sizeToTransfer;

	private long bytesTransferred = 0;

	public SeekableByteChannelChunkedNioStream(final SeekableByteChannel sbc) throws IOException {
		this(sbc, sbc.size());
	}

	private SeekableByteChannelChunkedNioStream(final SeekableByteChannel sbc, final long sizeToTransfer) {
		this.in = sbc;
		this.chunkSize = (int) (sizeToTransfer > BUFF_SIZE_MAX ? BUFF_SIZE_MAX : sizeToTransfer);
		this.byteBuffer = ByteBuffer.allocate(chunkSize);
		this.sizeToTransfer = sizeToTransfer;
	}

	@Override
	public final boolean isEndOfInput() {
		return bytesTransferred == sizeToTransfer;
	}

	@Override
	public void close() throws Exception {
		in.close();
	}

	@Deprecated
	@Override
	public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
		return readChunk(ctx.alloc());
	}

	@Override
	public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
		if (isEndOfInput()) {
			return null;
		}

		int nextChunkSize = chunkSize;
		long bytesRemaining = length() - progress();

		// Is there less than a chunk size of data remaining?
		if (bytesRemaining < chunkSize) {
			// Limit the byte buffer and next chunk size
			byteBuffer.limit((int) bytesRemaining);
			nextChunkSize = (int) bytesRemaining;
		}

		// Read into the byte buffer
		int readBytes = byteBuffer.position();
		while (true) {
			int localReadBytes = in.read(byteBuffer);
			if (localReadBytes < 0) {
				break;
			}
			readBytes += localReadBytes;
			bytesTransferred += localReadBytes;
			if (readBytes == nextChunkSize) {
				break;
			}
		}

		// Write from the byte buffer
		byteBuffer.flip();
		boolean release = true;
		ByteBuf buffer = allocator.buffer(byteBuffer.remaining());
		try {
			buffer.writeBytes(byteBuffer);
			byteBuffer.clear();
			release = false;
			return buffer;
		} finally {
			if (release) {
				buffer.release();
			}
		}
	}

	@Override
	public long length() {
		return sizeToTransfer;
	}

	@Override
	public long progress() {
		return bytesTransferred;
	}
}
