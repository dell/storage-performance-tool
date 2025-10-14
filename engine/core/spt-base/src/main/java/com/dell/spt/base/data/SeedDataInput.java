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

	public SeedDataInput() {
		super();
	}

	public SeedDataInput(final long seed, final int size, final int cacheLimit, final boolean isInHeapMem) {
		super(isInHeapMem ? allocate(size) : allocateDirect(size), cacheLimit, isInHeapMem);
		generateData(inputBuff, seed);
	}

	public SeedDataInput(final SeedDataInput other) {
		super(other);
	}
}
