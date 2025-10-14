package com.dell.spt.base.item.io;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.data.DataCorruptionException;
import com.dell.spt.base.item.DataItem;
import com.github.akurilov.commons.io.ByteCountOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class DataItemOutputStream extends ByteCountOutputStream {

	private final DataItem dataItem;

	private ByteBuffer buffWrapper = null;
	private byte[] buff = null;

	public DataItemOutputStream(final DataItem dataItem) {
		this.dataItem = dataItem;
	}

	@Override
	public final void write(final int b) throws DataCorruptionException {
		buffWrapper = ByteBuffer.allocate(1);
		try {
			dataItem.read(buffWrapper);
			buffWrapper.rewind();
			dataItem.verify(buffWrapper);
			dataItem.position(dataItem.position() + 1);
		} catch (final IOException e) {
			throwUnchecked(e);
		}
		byteCount.increment();
	}

	@Override
	@SuppressWarnings("ArrayEquality")
	public final void write(final byte buff[], final int offset, final int length)
					throws DataCorruptionException {
		if (buff != this.buff) {
			buffWrapper = ByteBuffer.wrap(buff);
		}
		buffWrapper.position(offset).limit(offset + length);
		dataItem.verify(buffWrapper);
		dataItem.position(dataItem.position() + length);
		byteCount.add(length);
	}

	@Override
	@SuppressWarnings("ArrayEquality")
	public final void write(final byte buff[]) throws DataCorruptionException {
		if (buff != this.buff) {
			buffWrapper = ByteBuffer.wrap(buff);
		}
		dataItem.verify(buffWrapper);
		dataItem.position(dataItem.position() + buff.length);
		byteCount.add(buff.length);
	}

	@Override
	public final void close() throws IOException {
		buffWrapper = null;
		buff = null;
	}
}
