package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Registered direct buffers reused across RDMA operations.
 *
 * <p>Allocating, zero-filling, and registering a buffer per operation runs on the driver's
 * dispatch path and limits RDMA throughput. This pool creates buffers on demand per size class,
 * registers each once, and returns them for reuse.
 *
 * <p>Reuse safety: a buffer may be returned with {@link #release} only after the server's HTTP
 * response for its request was received, which signals that the server finished accessing the
 * memory. Any other ending (timeout, lost connection, failure after dispatch) must use
 * {@link #discard}, which deregisters the memory so the server can no longer reach it.
 *
 * <p>Each size class holds at most {@code maxBuffersPerClass} buffers; when they are all in use,
 * {@link #acquire} returns {@code null} and the caller uses a per-operation buffer instead.
 */
final class RdmaBufferPool {

	/** Smallest size class. */
	static final int MIN_CLASS_BYTES = 1 << 20;
	/** Largest power-of-two class; larger buffers are sized exactly. */
	static final int MAX_POW2_CLASS_BYTES = 1 << 30;

	/** A registered buffer owned by the pool. */
	record PooledBuffer(ByteBuffer buffer, long mrHandle, int capacity) {}

	private final RdmaTransport transport;
	private final int maxBuffersPerClass;
	private final Map<Integer, ConcurrentLinkedDeque<PooledBuffer>> free = new ConcurrentHashMap<>();
	private final Map<Integer, AtomicInteger> created = new ConcurrentHashMap<>();
	private volatile boolean closed;

	final LongAdder hits = new LongAdder();
	final LongAdder misses = new LongAdder();
	final LongAdder exhausted = new LongAdder();
	final LongAdder discarded = new LongAdder();

	RdmaBufferPool(final RdmaTransport transport, final int maxBuffersPerClass) {
		this.transport = transport;
		this.maxBuffersPerClass = Math.max(1, maxBuffersPerClass);
	}

	/** Size class for a transfer of {@code size} bytes. */
	static int capacityFor(final int size) {
		if (size <= MIN_CLASS_BYTES) {
			return MIN_CLASS_BYTES;
		}
		if (size > MAX_POW2_CLASS_BYTES) {
			return size;
		}
		return Integer.highestOneBit(size - 1) << 1;
	}

	/**
	 * Returns a registered buffer of at least {@code size} bytes, or {@code null} when the class
	 * is at its limit or registration fails.
	 */
	PooledBuffer acquire(final int size) {
		if (closed) {
			return null;
		}
		final int capacity = capacityFor(size);
		final PooledBuffer reused = freeList(capacity).pollFirst();
		if (reused != null) {
			hits.increment();
			return reused;
		}
		final AtomicInteger count = created.computeIfAbsent(capacity, ignored -> new AtomicInteger());
		if (count.incrementAndGet() > maxBuffersPerClass) {
			count.decrementAndGet();
			exhausted.increment();
			return null;
		}
		final ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		final long mrHandle = transport.registerBuffer(buffer, capacity);
		if (mrHandle == 0) {
			count.decrementAndGet();
			return null;
		}
		misses.increment();
		return new PooledBuffer(buffer, mrHandle, capacity);
	}

	/** Returns a buffer whose request received its HTTP response. */
	void release(final PooledBuffer pooled) {
		if (closed) {
			discard(pooled);
			return;
		}
		freeList(pooled.capacity()).offerFirst(pooled);
		if (closed) {
			// close() may have drained the list before this buffer was added.
			drain(freeList(pooled.capacity()));
		}
	}

	/** Deregisters a buffer the server might still access; it is never reused. */
	void discard(final PooledBuffer pooled) {
		transport.deregisterBuffer(pooled.buffer(), pooled.mrHandle());
		created.get(pooled.capacity()).decrementAndGet();
		discarded.increment();
	}

	/** Deregisters every idle buffer; buffers still in use are discarded by their owners. */
	void close() {
		closed = true;
		free.values().forEach(this::drain);
	}

	private void drain(final ConcurrentLinkedDeque<PooledBuffer> list) {
		PooledBuffer pooled;
		while ((pooled = list.pollFirst()) != null) {
			transport.deregisterBuffer(pooled.buffer(), pooled.mrHandle());
			created.get(pooled.capacity()).decrementAndGet();
		}
	}

	private ConcurrentLinkedDeque<PooledBuffer> freeList(final int capacity) {
		return free.computeIfAbsent(capacity, ignored -> new ConcurrentLinkedDeque<>());
	}

	/** Buffers created and not yet discarded or drained, across all classes. */
	int liveBuffers() {
		return created.values().stream().mapToInt(AtomicInteger::get).sum();
	}

	/** Idle buffers held by the pool, across all classes. */
	int idleBuffers() {
		return free.values().stream().mapToInt(ConcurrentLinkedDeque::size).sum();
	}

	String summary() {
		return "poolHits=" + hits.sum() + ", poolCreated=" + misses.sum() + ", poolExhausted=" + exhausted.sum()
						+ ", poolDiscarded=" + discarded.sum();
	}
}
