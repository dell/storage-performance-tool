package com.dell.spt.base.item.op.data.range;

/** One explicit contiguous range; independent of the source item's whole-object metadata. */
public record ByteRange(long offset, long length) {

	public ByteRange {
		if (offset < 0 || length <= 0 || offset > Long.MAX_VALUE - (length - 1)) {
			throw new IllegalArgumentException("Range requires a nonnegative offset, positive length and signed-64-bit end");
		}
	}

	public long endInclusive() {
		return offset + (length - 1);
	}

	public String requestHeader() {
		return "bytes=" + offset + "-" + endInclusive();
	}
}
