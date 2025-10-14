package com.dell.spt.base.data;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Created by andrey on 24.07.17. */
public abstract class DataInputBase implements DataInput {

	protected ByteBuffer inputBuff;

	protected DataInputBase() {
		inputBuff = null;
	}

	protected DataInputBase(final ByteBuffer inputBuff) {
		this.inputBuff = inputBuff;
		inputBuff.clear();
	}

	protected DataInputBase(final DataInputBase other) {
		this.inputBuff = other.inputBuff;
	}

	@Override
	public final int getSize() {
		// NPE protection is necessary for the storage driver service
		return inputBuff == null ? 0 : inputBuff.capacity();
	}

	@Override
	public abstract ByteBuffer getLayer(final int layerIndex);

	@Override
	public void close() throws IOException {
		inputBuff = null;
	}
}
