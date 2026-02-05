package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A pool of pre-registered RDMA buffers.
 *
 * <p>Pre-registering memory with the RDMA NIC is an expensive operation.
 * This pool ensures that buffers are registered once during initialization
 * and reused for multiple S3-RDMA operations, significantly reducing latency.
 */
public class RdmaBufferPool implements AutoCloseable {

	private final RdmaTransport transport;
	private final int bufferSize;
	private final BlockingQueue<ByteBuffer> pool;
	private final int maxCount;

	public RdmaBufferPool(final RdmaTransport transport, final int bufferSize, final int count) {
		this.transport = transport;
		this.bufferSize = bufferSize;
		this.maxCount = count;
		this.pool = new LinkedBlockingQueue<>(count);
	}

	/**
	 * Pre-allocate and register all buffers in the pool.
	 */
	public void init() {
		Loggers.MSG.info("Initializing RDMA buffer pool: count={}, bufferSize={}", maxCount, bufferSize);
		int allocated = 0;
		for (int i = 0; i < maxCount; i++) {
			final ByteBuffer buf = transport.allocateNative(bufferSize);
			if (buf != null) {
				if (pool.offer(buf)) {
					allocated++;
				} else {
					transport.freeNative(buf);
				}
			} else {
				Loggers.MSG.error("Failed to allocate native RDMA buffer {} of {}", i + 1, maxCount);
				break;
			}
		}
		Loggers.MSG.info("RDMA buffer pool ready: {} of {} buffers allocated", allocated, maxCount);
	}

	/**
	 * Acquire a buffer from the pool.
	 *
	 * @param timeout timeout value
	 * @param unit    timeout unit
	 * @return a pre-registered ByteBuffer, or null if timeout reached
	 */
	public ByteBuffer acquire(final long timeout, final TimeUnit unit) throws InterruptedException {
		return pool.poll(timeout, unit);
	}

	/**
	 * Release a buffer back to the pool.
	 *
	 * @param buffer the buffer to return
	 */
	public void release(final ByteBuffer buffer) {
		if (buffer == null) {
			return;
		}
		if (buffer.capacity() != bufferSize) {
			// This buffer doesn't belong to the pool (probably allocated via fallback)
			transport.freeNative(buffer);
			return;
		}
		buffer.clear();
		if (!pool.offer(buffer)) {
			// Pool is full (should not happen if logic is correct), free it
			transport.freeNative(buffer);
		}
	}

	public int getBufferSize() {
		return bufferSize;
	}

	@Override
	public void close() {
		Loggers.MSG.info("Closing RDMA buffer pool");
		ByteBuffer buf;
		while ((buf = pool.poll()) != null) {
			transport.freeNative(buf);
		}
	}
}
