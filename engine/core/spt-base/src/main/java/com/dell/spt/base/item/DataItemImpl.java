package com.dell.spt.base.item;

import static com.dell.spt.base.item.DataItem.rangeOffset;
import static java.lang.Math.min;

import com.dell.spt.base.data.DataCorruptionException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.data.DataSizeException;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import com.dell.spt.base.item.io.AsyncChannel;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

/**
* Created by kurila on 09.05.14. A data item which may produce uniformly distributed
* non-compressible content. Uses UniformDataSource as a ring buffer. Not thread safe. Note: the
* {@link java.nio.channels.ReadableByteChannel#read(ByteBuffer)} method implementation will not
* return 0 or -1 (endless)
*/
public class DataItemImpl extends ItemImpl implements DataItem {
	//
	private static final String STR_EMPTY_MASK = "0";
	private static final int CHUNK_SIZE = 4096;
	private static final int STAMP_SIZE = 16;
	private static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
	private static final long FNV64_PRIME = 0x100000001b3L;
	//
	private static final char LAYER_MASK_SEP = '/';
	//
	private volatile DataInput dataInput;
	private int dataInputSize;
	//
	protected int layerNum = 0;
	//
	protected long offset = 0;
	protected long position = 0;
	protected long size = 0;
	private transient String cachedObjectIdSourceName = null;
	private transient long cachedObjectId = 0;
	//
	protected final BitSet modifiedRangesMask = new BitSet(Long.SIZE);

	////////////////////////////////////////////////////////////////////////////////////////////////
	public DataItemImpl() {
		super();
	}

	//
	public DataItemImpl(final String value) throws IllegalArgumentException {
		this(value, value.indexOf(','));
	}

	//
	private DataItemImpl(final String value, final int firstCommaPos)
					throws IllegalArgumentException {

		super(value.substring(0, firstCommaPos));

		int prevCommaPos = firstCommaPos;
		int nextCommaPos = value.indexOf(',', prevCommaPos + 1);
		if (nextCommaPos < prevCommaPos) {
			throw new IllegalArgumentException("Invalid data item description: " + value);
		}
		final String offsetInfo = value.substring(prevCommaPos + 1, nextCommaPos);
		try {
			offset(Long.parseLong(offsetInfo, 0x10));
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException(
							String.format("Data item offset is not correct hexadecimal value: \"%s\"", offsetInfo));
		}

		prevCommaPos = nextCommaPos;
		nextCommaPos = value.indexOf(',', prevCommaPos + 1);
		if (nextCommaPos < prevCommaPos) {
			throw new IllegalArgumentException("Invalid data item description: " + value);
		}
		final String sizeInfo = value.substring(prevCommaPos + 1, nextCommaPos);
		try {
			truncate(Long.parseLong(sizeInfo, 10));
		} catch (final NumberFormatException e) {
			throw new IllegalArgumentException(
							String.format("Data item size is not correct decimal value: \"%s\"", sizeInfo));
		}

		prevCommaPos = nextCommaPos;
		final String rangesInfo = value.substring(prevCommaPos + 1);
		final int sepPos = rangesInfo.indexOf(LAYER_MASK_SEP, 0);
		try {
			// extract hexadecimal layer number
			layerNum = Integer.parseInt(rangesInfo.substring(0, sepPos), 0x10);
			// extract hexadecimal mask, convert into bit set and add to the existing mask
			final String rangesMask = rangesInfo.substring(sepPos + 1, rangesInfo.length());
			final char rangesMaskChars[];
			if (rangesMask.length() == 0) {
				rangesMaskChars = ("00" + rangesMask).toCharArray();
			} else if (rangesMask.length() % 2 == 1) {
				rangesMaskChars = ("0" + rangesMask).toCharArray();
			} else {
				rangesMaskChars = rangesMask.toCharArray();
			}
			// method "or" to merge w/ the existing mask
			modifiedRangesMask.or(BitSet.valueOf(Hex.decodeHex(rangesMaskChars)));
		} catch (final DecoderException | NumberFormatException e) {
			throw new IllegalArgumentException(
							String.format("Ranges mask is not correct hexadecimal value: %s", rangesInfo));
		}
	}

	//
	public DataItemImpl(final long offset, final long size) {
		this(Long.toString(offset, Character.MAX_RADIX), offset, size, 0);
	}

	//
	public DataItemImpl(final String name, final long offset, final long size) {
		this(name, offset, size, 0);
	}

	//
	public DataItemImpl(final long offset, final long size, final int layerNum) {
		this();
		this.layerNum = layerNum;
		this.offset = offset;
		this.size = size;
	}

	//
	public DataItemImpl(final String name, final long offset, final long size, final int layerNum) {
		super(name);
		this.layerNum = layerNum;
		this.offset = offset;
		this.size = size;
	}

	//
	public DataItemImpl(
					final DataItemImpl baseDataItem,
					final long internalOffset,
					final long size,
					final boolean nextLayer) {
		this.dataInput = baseDataItem.dataInput;
		this.dataInputSize = baseDataItem.dataInputSize;
		this.offset = baseDataItem.offset + internalOffset;
		this.size = size;
		this.layerNum = baseDataItem.layerNum + (nextLayer ? 1 : 0);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	// Human readable "serialization" implementation ///////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////
	private static final ThreadLocal<StringBuilder> STRB = ThreadLocal.withInitial(StringBuilder::new);

	@Override
	public String toString() {
		final StringBuilder strb = STRB.get();
		strb.setLength(0); // reset
		return strb.append(super.toString())
						.append(',')
						.append(Long.toString(offset, 0x10))
						.append(',')
						.append(size)
						.append(',')
						.append(Integer.toHexString(layerNum))
						.append('/')
						.append(
										modifiedRangesMask.isEmpty()
														? STR_EMPTY_MASK
														: Hex.encodeHexString(modifiedRangesMask.toByteArray()))
						.toString();
	}

	@Override
	public String toString(final String itemPath) {
		final StringBuilder strBuilder = STRB.get();
		strBuilder.setLength(0); // reset
		return strBuilder
						.append(super.toString(itemPath))
						.append(',')
						.append(Long.toString(offset, 0x10))
						.append(',')
						.append(size)
						.append(',')
						.append(Integer.toHexString(layerNum))
						.append('/')
						.append(
										modifiedRangesMask.isEmpty()
														? STR_EMPTY_MASK
														: Hex.encodeHexString(modifiedRangesMask.toByteArray()))
						.toString();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public final DataInput dataInput() {
		return dataInput;
	}

	//
	@Override
	public final void dataInput(final DataInput dataInput) {
		this.dataInput = dataInput;
		this.dataInputSize = dataInput.getSize();
	}

	//
	@Override
	public void reset() {
		super.reset();
		position = 0;
	}

	//
	@Override
	public final int layer() {
		return layerNum;
	}

	//
	@Override
	public final void layer(final int layerNum) {
		this.layerNum = layerNum;
	}

	//
	@Override
	public final void size(final long size) {
		this.size = size;
	}

	//
	@Override
	public final long offset() {
		return offset;
	}

	//
	@Override
	public final void offset(final long offset) {
		this.offset = offset < 0 ? Long.MAX_VALUE + offset + 1 : offset;
		position = 0;
	}

	//
	@Override
	public DataItemImpl slice(final long from, final long partSize) {
		if (from < 0) {
			throw new IllegalArgumentException();
		}
		if (partSize < 1) {
			throw new IllegalArgumentException();
		}
		final DataItemImpl dataItemSlice = new DataItemImpl(name(), offset + from, partSize, layerNum);
		final var dataInput = this.dataInput;
		if (dataInput != null) {
			dataItemSlice.dataInput(dataInput);
		}
		return dataItemSlice;
	}

	//
	@Override
	public long position() {
		return position;
	}

	//
	@Override
	public final DataItemImpl position(final long position) {
		this.position = position;
		return this;
	}

	//
	@Override
	public long size() {
		return size;
	}

	//
	@Override
	public DataItemImpl truncate(final long size) {
		this.size = size;
		return this;
	}

	//
	@Override
	public final long rangeSize(final int i) {
		return min(rangeOffset(i + 1), size) - rangeOffset(i);
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	// UPDATE //////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public final boolean isUpdated() {
		return layerNum > 0 || !modifiedRangesMask.isEmpty();
	}

	@Override
	public final void commitUpdatedRanges(final BitSet[] updatingRangesMaskPair) {
		if (updatingRangesMaskPair[1].isEmpty()) {
			modifiedRangesMask.or(updatingRangesMaskPair[0]);
		} else {
			modifiedRangesMask.clear();
			modifiedRangesMask.or(updatingRangesMaskPair[1]);
			layerNum++;
		}
	}

	@Override
	public final boolean isRangeUpdated(final int rangeIdx) {
		return modifiedRangesMask.get(rangeIdx);
	}

	@Override
	public final int updatedRangesCount() {
		return modifiedRangesMask.cardinality();
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	// ByteChannels implementation
	////////////////////////////////////////////////////////////////////////////////////////////////
	@Override
	public final void close() {}

	//
	@Override
	public final boolean isOpen() {
		return true;
	}

	private static final ThreadLocal<ByteBuffer> STAMP_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocate(STAMP_SIZE));

	private static long fnv1a64(final String value) {
		long hash = FNV64_OFFSET_BASIS;
		for (final byte b : value.getBytes(StandardCharsets.UTF_8)) {
			hash ^= b & 0xFFL;
			hash *= FNV64_PRIME;
		}
		return hash;
	}

	private long objectId() {
		final String itemName = name();
		if (itemName == null) {
			return 0;
		}
		if (!itemName.equals(cachedObjectIdSourceName)) {
			cachedObjectId = fnv1a64(itemName);
			cachedObjectIdSourceName = itemName;
		}
		return cachedObjectId;
	}

	private void writeStamp(final ByteBuffer stampBuffer) {
		stampBuffer.clear();
		stampBuffer.putLong(objectId());
		stampBuffer.putLong(offset + position);
		stampBuffer.flip();
	}

	//
	@Override
	public final int read(final ByteBuffer dst) {
		final boolean dedupable = dataInput.isDedupable();
		final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
		if (dedupable || dst.remaining() == 0) {
			final int ringPos = (int) ((offset + position) % dataInputSize);
			ringBuff.limit(dataInputSize);
			ringBuff.position(ringPos);
			final int n = Math.min(dst.remaining(), ringBuff.remaining());
			ringBuff.limit(ringPos + n);
			dst.put(ringBuff);
			position += n;
			return n;
		}
		final int startRemaining = dst.remaining();
		long doneCount = 0;
		final long maxCount = dst.remaining();
		while (doneCount < maxCount && dst.remaining() > 0) {
			final long chunkOffset = position % CHUNK_SIZE;
			if (chunkOffset < STAMP_SIZE) {
				if (maxCount - doneCount <= 0 || dst.remaining() == 0) {
					break;
				}
				final ByteBuffer stampBuff = STAMP_BUFFER.get();
				writeStamp(stampBuff);
				stampBuff.position((int) chunkOffset);
				final int stampRemaining = STAMP_SIZE - (int) chunkOffset;
				final int stampToPut = (int) Math.min(Math.min(stampRemaining, maxCount - doneCount), dst.remaining());
				stampBuff.limit(stampBuff.position() + stampToPut);
				dst.put(stampBuff);
				position += stampToPut;
				doneCount += stampToPut;
				if (stampToPut < stampRemaining) {
					break;
				}
				continue;
			}
			final long chunkRemaining = CHUNK_SIZE - Math.max(chunkOffset, STAMP_SIZE);
			final int ringPos = (int) ((offset + position) % dataInputSize);
			ringBuff.limit(dataInputSize);
			ringBuff.position(ringPos);
			final int n = (int) Math.min(Math.min(maxCount - doneCount, chunkRemaining), ringBuff.remaining());
			if (n <= 0) {
				break;
			}
			ringBuff.limit(ringPos + n);
			dst.put(ringBuff);
			position += n;
			doneCount += n;
		}
		return startRemaining - dst.remaining();
	}

	//
	@Override
	public final int write(final ByteBuffer src) throws DataCorruptionException, DataSizeException {
		if (src == null) {
			return 0;
		}
		int m;
		final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
		ringBuff.position((int) ((offset + position) % dataInputSize));
		final int n = Math.min(src.remaining(), ringBuff.remaining());
		if (n > 0) {
			byte bs, bi;
			for (m = 0; m < n; m++) {
				bs = ringBuff.get();
				bi = src.get();
				if (bs != bi) {
					throw new DataCorruptionException(m, bs, bi);
				}
			}
			position += n;
		} else {
			return n;
		}
		return m;
	}

	@Override
	public final long writeToSocketChannel(final WritableByteChannel chanDst, final long maxCount)
					throws IOException {
		if (maxCount <= 0) {
			return 0;
		}
		final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
		final boolean dedupable = dataInput.isDedupable();
		long doneCount = 0;
		int n, m;
		if (dedupable) {
			// spin while not done either destination channel consumes all the data
			while (doneCount < maxCount) {
				final int ringPos = (int) ((offset + position) % dataInputSize);
				ringBuff.limit(dataInputSize);
				ringBuff.position(ringPos);
				n = (int) Math.min(maxCount - doneCount, ringBuff.remaining());
				ringBuff.limit(ringPos + n);
				m = chanDst.write(ringBuff);
				doneCount += m;
				position += m;
				if (m < n) {
					break;
				}
			}
			return doneCount;
		}
		while (doneCount < maxCount) {
			final long chunkOffset = position % CHUNK_SIZE;
			if (chunkOffset < STAMP_SIZE) {
				final ByteBuffer stampBuff = STAMP_BUFFER.get();
				writeStamp(stampBuff);
				stampBuff.position((int) chunkOffset);
				final int stampRemaining = STAMP_SIZE - (int) chunkOffset;
				final int stampToWrite = (int) Math.min(stampRemaining, maxCount - doneCount);
				stampBuff.limit(stampBuff.position() + stampToWrite);
				m = chanDst.write(stampBuff);
				if (m <= 0) {
					break;
				}
				doneCount += m;
				position += m;
				if (m < stampToWrite) {
					break;
				}
				continue;
			}
			final long chunkRemaining = CHUNK_SIZE - Math.max(chunkOffset, STAMP_SIZE);
			final int ringPos = (int) ((offset + position) % dataInputSize);
			ringBuff.limit(dataInputSize);
			ringBuff.position(ringPos);
			n = (int) Math.min(Math.min(maxCount - doneCount, chunkRemaining), ringBuff.remaining());
			if (n <= 0) {
				break;
			}
			ringBuff.limit(ringPos + n);
			m = chanDst.write(ringBuff);
			if (m <= 0) {
				break;
			}
			doneCount += m;
			position += m;
			if (m < n) {
				break;
			}
		}
		return doneCount;
	}

	@Override
	public final long writeToFileChannel(final FileChannel chanDst, final long maxCount)
					throws IOException {
		if (maxCount <= 0) {
			return 0;
		}
		if (dataInput.isDedupable()) {
			final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
			int n = (int) ((offset + position) % dataInputSize);
			ringBuff.limit(dataInputSize);
			ringBuff.position(n);
			n = (int) Math.min(maxCount, ringBuff.remaining());
			ringBuff.limit(ringBuff.position() + n);
			n = chanDst.write(ringBuff);
			position += n;
			return n;
		}
		final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
		long doneCount = 0;
		while (doneCount < maxCount) {
			final long chunkOffset = position % CHUNK_SIZE;
			if (chunkOffset < STAMP_SIZE) {
				final ByteBuffer stampBuff = STAMP_BUFFER.get();
				writeStamp(stampBuff);
				stampBuff.position((int) chunkOffset);
				final int stampRemaining = STAMP_SIZE - (int) chunkOffset;
				final int stampToWrite = (int) Math.min(stampRemaining, maxCount - doneCount);
				stampBuff.limit(stampBuff.position() + stampToWrite);
				final int m = chanDst.write(stampBuff);
				if (m <= 0) {
					break;
				}
				doneCount += m;
				position += m;
				if (m < stampToWrite) {
					break;
				}
				continue;
			}
			final long chunkRemaining = CHUNK_SIZE - Math.max(chunkOffset, STAMP_SIZE);
			final int ringPos = (int) ((offset + position) % dataInputSize);
			ringBuff.limit(dataInputSize);
			ringBuff.position(ringPos);
			final int n = (int) Math.min(Math.min(maxCount - doneCount, chunkRemaining), ringBuff.remaining());
			if (n <= 0) {
				break;
			}
			ringBuff.limit(ringPos + n);
			final int m = chanDst.write(ringBuff);
			if (m <= 0) {
				break;
			}
			doneCount += m;
			position += m;
			if (m < n) {
				break;
			}
		}
		return doneCount;
	}

	@Override
	public final <A> void writeToAsyncChannel(
					final AsyncChannel dstChan, final long dstPos, final long maxCount, final A attach,
					final CompletionHandler<Integer, ? super A> handler) {
		if (maxCount <= 0) {
			handler.completed(0, attach);
			return;
		}
		if (!dataInput.isDedupable()) {
			final long chunkOffset = position % CHUNK_SIZE;
			if (chunkOffset < STAMP_SIZE) {
				final ByteBuffer stampBuff = ByteBuffer.allocate(STAMP_SIZE);
				writeStamp(stampBuff);
				stampBuff.position((int) chunkOffset);
				final int stampRemaining = STAMP_SIZE - (int) chunkOffset;
				final int stampToWrite = (int) Math.min(stampRemaining, maxCount);
				stampBuff.limit(stampBuff.position() + stampToWrite);
				dstChan.write(stampBuff, dstPos, attach, handler);
				return;
			}
			final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
			final long chunkRemaining = CHUNK_SIZE - Math.max(chunkOffset, STAMP_SIZE);
			final int ringPos = (int) ((offset + position) % dataInputSize);
			ringBuff.limit(dataInputSize);
			ringBuff.position(ringPos);
			final int n = (int) Math.min(Math.min(maxCount, chunkRemaining), ringBuff.remaining());
			ringBuff.limit(ringPos + n);
			dstChan.write(ringBuff, dstPos, attach, handler);
			return;
		}
		final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
		int n = (int) ((offset + position) % dataInputSize);
		ringBuff.limit(dataInputSize);
		ringBuff.position(n);
		n = (int) Math.min(maxCount, ringBuff.remaining());
		ringBuff.limit(ringBuff.position() + n);
		dstChan.write(ringBuff, dstPos, attach, handler);
	}

	@Override
	public final void verify(final ByteBuffer inBuff) throws DataCorruptionException {
		final ByteBuffer ringBuff = dataInput.getLayer(layerNum).asReadOnlyBuffer();
		ringBuff.position((int) ((offset + position) % dataInputSize));
		verify(inBuff, ringBuff);
	}

	private void verify(final ByteBuffer inBuff, final ByteBuffer ringBuff)
					throws DataCorruptionException {

		final int inputSize = inBuff.remaining();
		final int sizeToVerify = Math.min(ringBuff.remaining(), inputSize);

		// compare the 64 bit words 1st to make it faster
		final int wordCount = sizeToVerify >>> 3; // how many 64 bit words are there
		if (wordCount > 0) {
			long ws, wi;
			for (int k = 0; k < wordCount; k++) {
				ws = ringBuff.getLong();
				wi = inBuff.getLong();
				if (ws != wi) {
					// don't hurry more, find the exact non-matching byte
					final int wordPos = k << 3;
					byte bs, bi;
					for (int i = 0; i < 8; i++) {
						bs = (byte) ws;
						ws >>= 8;
						bi = (byte) wi;
						wi >>= 8;
						if (bs != bi) {
							throw new DataCorruptionException(wordPos + i, bs, bi);
						}
					}
				}
			}
		}

		// compare the remaining bytes if any
		final int tailByteCount = sizeToVerify & 7;
		if (tailByteCount > 0) {
			byte bs, bi;
			for (int m = 0; m < tailByteCount; m++) {
				bs = ringBuff.get();
				bi = inBuff.get();
				if (bs != bi) {
					throw new DataCorruptionException(m, bs, bi);
				}
			}
		}

		// ring buffer's remaining bytes count was less than input buffer's remaining bytes
		if (sizeToVerify < inputSize) {
			// try to verify again starting from the ring buffer's 0 position
			ringBuff.position(0);
			verify(inBuff, ringBuff);
		}
	}

	@Override
	public boolean equals(final Object o) {
		if (o == this) {
			return true;
		}
		if (!(o instanceof DataItemImpl)) {
			return false;
		}
		final DataItemImpl other = (DataItemImpl) o;
		return super.equals(other) && offset == other.offset;
	}

	//
	@Override
	public int hashCode() {
		return super.hashCode() ^ (int) offset;
	}

	@Override
	public void writeExternal(final ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeInt(layerNum);
		out.writeLong(offset);
		out.writeLong(position);
		out.writeLong(size);
		final byte buff[] = modifiedRangesMask.toByteArray();
		out.writeInt(buff.length);
		out.write(buff);
	}

	@Override
	public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		layerNum = in.readInt();
		offset = in.readLong();
		position = in.readLong();
		size = in.readLong();
		final int len = in.readInt();
		final byte buff[] = new byte[len];
		in.readFully(buff);
		modifiedRangesMask.or(BitSet.valueOf(buff));
	}
}
