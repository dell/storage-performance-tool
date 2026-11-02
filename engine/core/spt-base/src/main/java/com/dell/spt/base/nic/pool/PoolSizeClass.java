package com.dell.spt.base.nic.pool;

/**
 * The size classes of the RDMA buffer pools: powers of two from 1 MiB to 1 GiB. A larger transfer is
 * not pooled. {@code RdmaBufferPool} applies the same rule per operation, and a test of that module
 * pins the two together.
 */
public final class PoolSizeClass {

	public static final int MIN_BYTES = 1 << 20;
	public static final int MAX_BYTES = 1 << 30;

	private PoolSizeClass() {}

	/** The class that holds a transfer of {@code size} bytes, which is at most {@link #MAX_BYTES}. */
	public static int of(final long size) {
		if (size <= MIN_BYTES) {
			return MIN_BYTES;
		}
		return (int) (Long.highestOneBit(size - 1) << 1);
	}
}
