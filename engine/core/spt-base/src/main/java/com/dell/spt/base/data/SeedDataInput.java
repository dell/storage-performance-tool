package com.dell.spt.base.data;

import static com.dell.spt.base.data.DataInput.generateData;
import static java.nio.ByteBuffer.allocate;
import static java.nio.ByteBuffer.allocateDirect;

/**
 Created by kurila on 23.07.14. A uniform data input for producing uniform data items. Implemented
 as finite buffer of pseudo random bytes.
 */
public final class SeedDataInput
				extends CachedDataInput {

	private final double compressibility;
	private final boolean dedupable;

	public SeedDataInput() {
		super();
		this.compressibility = 0.0;
		this.dedupable = true;
	}

	public SeedDataInput(final long seed, final int size, final int cacheLimit, final boolean isInHeapMem) {
		this(seed, size, cacheLimit, isInHeapMem, 0.0, true);
	}

	public SeedDataInput(
					final long seed, final int size, final int cacheLimit, final boolean isInHeapMem,
					final double compressibility) {
		this(seed, size, cacheLimit, isInHeapMem, compressibility, true);
	}

	public SeedDataInput(
					final long seed, final int size, final int cacheLimit, final boolean isInHeapMem,
					final double compressibility, final boolean dedupable) {
		super(isInHeapMem ? allocate(size) : allocateDirect(size), cacheLimit, isInHeapMem);
		this.compressibility = compressibility;
		this.dedupable = dedupable;
		generateData(inputBuff, seed, compressibility);
	}

	public SeedDataInput(final SeedDataInput other) {
		super(other);
		this.compressibility = other.compressibility;
		this.dedupable = other.dedupable;
	}

	@Override
	public boolean isDedupable() {
		return dedupable;
	}

	@Override
	protected double getCompressibility() {
		return compressibility;
	}
}
