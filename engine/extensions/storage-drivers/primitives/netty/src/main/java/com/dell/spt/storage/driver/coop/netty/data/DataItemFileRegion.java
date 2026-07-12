package com.dell.spt.storage.driver.coop.netty.data;

import com.dell.spt.base.item.DataItem;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class DataItemFileRegion
				extends AbstractReferenceCounted
				implements FileRegion {

	protected final DataItem dataItem;
	protected final long baseItemPosition;
	protected final long byteCount;
	protected long doneByteCount = 0;

	public DataItemFileRegion(final DataItem dataItem)
					throws IOException {
		this.dataItem = dataItem;
		this.baseItemPosition = dataItem.position();
		this.byteCount = dataItem.size() - baseItemPosition;
	}

	@Override
	public long position() {
		return baseItemPosition;
	}

	@Deprecated
	@Override
	public long transfered() {
		return doneByteCount;
	}

	@Override
	public long transferred() {
		return doneByteCount;
	}

	@Override
	public long count() {
		return byteCount;
	}

	@Override
	public long transferTo(final WritableByteChannel target, final long position)
					throws IOException {
		dataItem.position(baseItemPosition + position);
		final long bytesWritten = dataItem.writeToSocketChannel(target, byteCount - position);
		doneByteCount += bytesWritten;
		return bytesWritten;
	}

	@Override
	public FileRegion retain() {
		super.retain();
		return this;
	}

	@Override
	public FileRegion retain(int increment) {
		super.retain(increment);
		return this;
	}

	@Override
	public FileRegion touch() {
		return touch(this);
	}

	@Override
	public FileRegion touch(Object hint) {
		return this;
	}

	@Override
	protected void deallocate() {}
}
