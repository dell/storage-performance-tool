package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.Loggers;
import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * Registered direct buffers reused across RDMA operations.
 *
 * <p>Allocating, zero-filling, and registering a buffer per operation runs on the driver's
 * dispatch path and limits RDMA throughput. This pool creates buffers on demand per size class,
 * registers each once, and returns them for reuse.
 *
 * <p>Reuse safety: a buffer may be returned with {@link #release} only when the server can no
 * longer access it: its request was never sent, or the server's final HTTP response for it was
 * received. Any other ending (timeout, lost connection, failure after dispatch) must use
 * {@link #discard}, which deregisters the memory so the server can no longer reach it.
 *
 * <p>Bounds: each size class holds at most {@code maxBuffersPerClass} buffers, and all pool
 * buffers together at most {@code maxPooledBytes}. A new buffer that would exceed the byte budget
 * first evicts idle buffers of other classes. Transfers larger than {@link #MAX_CLASS_BYTES}, and
 * requests that find their class or the budget exhausted, get {@code null} from {@link #acquire}
 * and use a per-operation buffer instead.
 */
final class RdmaBufferPool {

	/** Smallest size class. */
	static final int MIN_CLASS_BYTES = 1 << 20;
	/** Largest size class; larger transfers are not pooled. */
	static final int MAX_CLASS_BYTES = 1 << 30;
	/** The pool may hold this fraction (1/n) of the JVM's direct-memory limit. */
	static final int DIRECT_MEMORY_SHARE_DIVISOR = 2;
	private static final String MAX_DIRECT_MEMORY_OPTION = "MaxDirectMemorySize";

	/** A registered buffer owned by the pool; leased to at most one request at a time. */
	static final class PooledBuffer {
		private final ByteBuffer buffer;
		private final long mrHandle;
		private final int capacity;
		/** Guarded by the pool. */
		private boolean leased;

		private PooledBuffer(final ByteBuffer buffer, final long mrHandle, final int capacity) {
			this.buffer = buffer;
			this.mrHandle = mrHandle;
			this.capacity = capacity;
		}

		ByteBuffer buffer() {
			return buffer;
		}

		long mrHandle() {
			return mrHandle;
		}

		int capacity() {
			return capacity;
		}
	}

	private final RdmaTransport transport;
	private final int maxBuffersPerClass;
	private final long maxPooledBytes;
	/** Guarded by this. */
	private final Map<Integer, ArrayDeque<PooledBuffer>> idle = new HashMap<>();
	/** Live buffers (leased or idle) per class; guarded by this. */
	private final Map<Integer, Integer> live = new HashMap<>();
	/** Capacity of all live buffers; guarded by this. */
	private long liveBytes;
	/** Guarded by this. */
	private boolean closed;
	private final AtomicBoolean invalidReturnLogged = new AtomicBoolean();

	final LongAdder hits = new LongAdder();
	final LongAdder created = new LongAdder();
	final LongAdder exhausted = new LongAdder();
	final LongAdder unpooled = new LongAdder();
	final LongAdder evicted = new LongAdder();
	final LongAdder discarded = new LongAdder();
	final LongAdder invalidReturns = new LongAdder();

	RdmaBufferPool(final RdmaTransport transport, final int maxBuffersPerClass, final long maxPooledBytes) {
		this.transport = transport;
		this.maxBuffersPerClass = Math.max(1, maxBuffersPerClass);
		this.maxPooledBytes = Math.max(0, maxPooledBytes);
	}

	/** Byte budget for a pool in this JVM: a share of the direct-memory limit. */
	static long defaultMaxPooledBytes() {
		long limit = 0;
		try {
			limit = Long.parseLong(ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class)
							.getVMOption(MAX_DIRECT_MEMORY_OPTION).getValue());
		} catch (final RuntimeException e) {
			Loggers.MSG.debug("{} unavailable; using the heap limit: {}", MAX_DIRECT_MEMORY_OPTION, e.toString());
		}
		// Unset (0) means the JVM limits direct memory to the maximum heap size.
		if (limit <= 0) {
			limit = Runtime.getRuntime().maxMemory();
		}
		return limit / DIRECT_MEMORY_SHARE_DIVISOR;
	}

	/** Size class for a transfer of {@code size} bytes, at most {@link #MAX_CLASS_BYTES}. */
	static int capacityFor(final int size) {
		if (size <= MIN_CLASS_BYTES) {
			return MIN_CLASS_BYTES;
		}
		return Integer.highestOneBit(size - 1) << 1;
	}

	/**
	 * Leases a registered buffer of at least {@code size} bytes, cleared for use, or returns
	 * {@code null} when the transfer is not pooled.
	 */
	PooledBuffer acquire(final int size) {
		if (size > MAX_CLASS_BYTES) {
			unpooled.increment();
			return null;
		}
		final int capacity = capacityFor(size);
		final List<PooledBuffer> evictions = new ArrayList<>();
		final boolean reserved;
		synchronized (this) {
			if (closed) {
				return null;
			}
			final PooledBuffer reused = idleList(capacity).pollFirst();
			if (reused != null) {
				reused.leased = true;
				reused.buffer.clear();
				hits.increment();
				return reused;
			}
			reserved = live.getOrDefault(capacity, 0) < maxBuffersPerClass && reserve(capacity, evictions);
		}
		deregisterAfterUnlock(evictions);
		if (!reserved) {
			exhausted.increment();
			return null;
		}
		// Allocation and registration run outside the lock; the reservation holds the slot.
		ByteBuffer buffer = null;
		long mrHandle = 0;
		try {
			buffer = ByteBuffer.allocateDirect(capacity);
			mrHandle = transport.registerBuffer(buffer, capacity);
		} catch (final RuntimeException | OutOfMemoryError e) {
			Loggers.MSG.debug("RDMA pool buffer of {} bytes unavailable: {}", capacity, e.toString());
		}
		if (mrHandle == 0) {
			synchronized (this) {
				unreserve(capacity);
			}
			return null;
		}
		final PooledBuffer pooled = new PooledBuffer(buffer, mrHandle, capacity);
		pooled.leased = true;
		created.increment();
		return pooled;
	}

	/** Returns a leased buffer the server can no longer access. */
	void release(final PooledBuffer pooled) {
		synchronized (this) {
			if (!endLease(pooled)) {
				return;
			}
			if (!closed) {
				idleList(pooled.capacity).offerFirst(pooled);
				return;
			}
			unreserve(pooled.capacity);
		}
		transport.deregisterBuffer(pooled.buffer, pooled.mrHandle);
	}

	/** Deregisters a leased buffer the server might still access; it is never reused. */
	void discard(final PooledBuffer pooled) {
		synchronized (this) {
			if (!endLease(pooled)) {
				return;
			}
			unreserve(pooled.capacity);
		}
		discarded.increment();
		transport.deregisterBuffer(pooled.buffer, pooled.mrHandle);
	}

	/** Deregisters every idle buffer; leased buffers are discarded or released by their owners. */
	void close() {
		final List<PooledBuffer> drained = new ArrayList<>();
		synchronized (this) {
			closed = true;
			idle.values().forEach(list -> {
				PooledBuffer pooled;
				while ((pooled = list.pollFirst()) != null) {
					unreserve(pooled.capacity);
					drained.add(pooled);
				}
			});
		}
		deregisterAfterUnlock(drained);
	}

	/** Buffers created and not yet discarded, evicted, or drained, across all classes. */
	synchronized int liveBuffers() {
		return live.values().stream().mapToInt(Integer::intValue).sum();
	}

	/** Capacity of all live buffers. */
	synchronized long liveBytes() {
		return liveBytes;
	}

	/** Idle buffers held by the pool, across all classes. */
	synchronized int idleBuffers() {
		return idle.values().stream().mapToInt(ArrayDeque::size).sum();
	}

	String summary() {
		return "poolHits=" + hits.sum() + ", poolCreated=" + created.sum() + ", poolExhausted=" + exhausted.sum()
						+ ", poolUnpooled=" + unpooled.sum() + ", poolEvicted=" + evicted.sum()
						+ ", poolDiscarded=" + discarded.sum() + ", poolInvalidReturns=" + invalidReturns.sum();
	}

	/** Ends a lease; a buffer that is not leased was already returned and is left alone. */
	private boolean endLease(final PooledBuffer pooled) {
		if (!pooled.leased) {
			invalidReturns.increment();
			if (invalidReturnLogged.compareAndSet(false, true)) {
				Loggers.ERR.error("RDMA pool buffer returned twice (handle {}); ignored", pooled.mrHandle);
			}
			return false;
		}
		pooled.leased = false;
		return true;
	}

	/**
	 * Reserves room for a new buffer, evicting idle buffers of other classes if needed. Nothing is
	 * evicted unless the eviction makes room.
	 */
	private boolean reserve(final int capacity, final List<PooledBuffer> evictions) {
		long evictable = 0;
		for (final var entry : idle.entrySet()) {
			if (entry.getKey() != capacity) {
				evictable += (long) entry.getKey() * entry.getValue().size();
			}
		}
		if (liveBytes - evictable + capacity > maxPooledBytes) {
			return false;
		}
		for (final var entry : idle.entrySet()) {
			if (liveBytes + capacity <= maxPooledBytes) {
				break;
			}
			if (entry.getKey() == capacity) {
				continue;
			}
			PooledBuffer victim;
			while (liveBytes + capacity > maxPooledBytes && (victim = entry.getValue().pollLast()) != null) {
				unreserve(victim.capacity);
				evictions.add(victim);
				evicted.increment();
			}
		}
		live.merge(capacity, 1, Integer::sum);
		liveBytes += capacity;
		return true;
	}

	private void unreserve(final int capacity) {
		live.merge(capacity, -1, Integer::sum);
		liveBytes -= capacity;
	}

	private void deregisterAfterUnlock(final List<PooledBuffer> buffers) {
		for (final PooledBuffer pooled : buffers) {
			transport.deregisterBuffer(pooled.buffer, pooled.mrHandle);
		}
	}

	private ArrayDeque<PooledBuffer> idleList(final int capacity) {
		return idle.computeIfAbsent(capacity, ignored -> new ArrayDeque<>());
	}
}
