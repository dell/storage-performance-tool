package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import java.nio.ByteBuffer;

/**
 * Child-JVM program for {@link RdmaDirectMemoryCanaryTest}: run with a small
 * {@code -XX:MaxDirectMemorySize}, it fills the pool's budget with idle buffers and then requests a
 * per-operation buffer that only fits if the idle memory is reclaimed.
 */
public final class RdmaDirectMemoryCanary {

	static final int MIB = 1 << 20;
	static final String RESULT_PREFIX = "RESULT=";

	private RdmaDirectMemoryCanary() {}

	public static void main(final String[] args) {
		final long budget = Long.parseLong(args[1]) * MIB;
		final int request = Integer.parseInt(args[2]) * MIB;
		final var transport = new FakeRdmaTransport(new RdmaConfig(true, 0, false, "auto", "", "WARN"));
		transport.init("http://10.0.0.1:9020", "key", "secret");
		final var pool = new RdmaBufferPool(transport, 4, budget);
		fillWithIdleBuffers(pool, budget);
		final String result = switch (args[0]) {
		case "plain" -> plainAllocation(request);
		case "nics" -> repeatedUnpooled(pool, budget, request);
		case "reclaim" -> RdmaBufferPool.allocateUnpooled(request, pool, ByteBuffer::allocateDirect) == null
						? "exhausted"
						: "allocated idle=" + pool.idleBuffers();
		default -> throw new IllegalArgumentException(args[0]);
		};
		System.out.println(RESULT_PREFIX + result);
	}

	/**
	 * Leaves two idle classes, together exactly the budget. A separate frame, so that, as in the
	 * driver, nothing but the pool references the buffers.
	 */
	private static void fillWithIdleBuffers(final RdmaBufferPool pool, final long budget) {
		final var large = pool.acquire((int) (budget / 3 * 2));
		final var small = pool.acquire((int) (budget / 3));
		pool.release(large);
		pool.release(small);
		if (pool.idleBuffers() != 2 || pool.liveBytes() != budget) {
			throw new IllegalStateException("pool not filled: " + pool.summary());
		}
	}

	/**
	 * Two NICs' pools, the second also full of idle buffers, and more per-operation buffers than the
	 * limit holds, dropped without a collection between them: each attempt reclaims from its own NIC's
	 * pool only, and the JVM's limit holds throughout.
	 */
	private static String repeatedUnpooled(final RdmaBufferPool first, final long budget, final int request) {
		final var transport = new FakeRdmaTransport(new RdmaConfig(true, 0, false, "auto", "", "WARN"));
		transport.init("http://192.0.2.1:9020", "key", "secret");
		final var second = new RdmaBufferPool(transport, 4, MIB);
		second.release(second.acquire(MIB));
		// Well past the limit of 48 MiB in total, with no explicit collection.
		final int rounds = 8;
		for (int i = 0; i < rounds; i++) {
			final var buffer = RdmaBufferPool.allocateUnpooled(request, first, ByteBuffer::allocateDirect);
			if (buffer == null) {
				return "exhausted at " + i;
			}
		}
		return "allocated rounds=" + rounds + " idle=" + first.idleBuffers() + " other=" + second.idleBuffers();
	}

	private static String plainAllocation(final int request) {
		try {
			ByteBuffer.allocateDirect(request);
			return "allocated";
		} catch (final OutOfMemoryError e) {
			return "oom";
		}
	}
}
