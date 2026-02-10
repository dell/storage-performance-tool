package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
	/** Identity-based set of all buffers owned by this pool (checked out or queued). */
	private final Set<ByteBuffer> owned = Collections.newSetFromMap(new java.util.IdentityHashMap<>());
	private final AtomicInteger outstanding = new AtomicInteger();

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
					synchronized (owned) {
						owned.add(buf);
					}
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
		final ByteBuffer buf = pool.poll(timeout, unit);
		if (buf != null) {
			outstanding.incrementAndGet();
		}
		return buf;
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
		final boolean mine;
		synchronized (owned) {
			mine = owned.contains(buffer);
		}
		if (!mine) {
			// This buffer doesn't belong to the pool (probably allocated via fallback)
			transport.freeNative(buffer);
			return;
		}
		outstanding.decrementAndGet();
		buffer.clear();
		if (!pool.offer(buffer)) {
			// Pool is full (should not happen if logic is correct), free it
			synchronized (owned) {
				owned.remove(buffer);
			}
			transport.freeNative(buffer);
		}
	}

	/**
	 * Check if a buffer belongs to this pool (by identity, not capacity).
	 */
	public boolean isPoolBuffer(final ByteBuffer buffer) {
		if (buffer == null) {
			return false;
		}
		synchronized (owned) {
			return owned.contains(buffer);
		}
	}

	public int getBufferSize() {
		return bufferSize;
	}

	@Override
	public void close() {
		final int out = outstanding.get();
		if (out > 0) {
			Loggers.MSG.warn("Closing RDMA buffer pool with {} buffers still checked out", out);
		}
		Loggers.MSG.info("Closing RDMA buffer pool");
		ByteBuffer buf;
		while ((buf = pool.poll()) != null) {
			transport.freeNative(buf);
		}
		synchronized (owned) {
			owned.clear();
		}
	}
}
