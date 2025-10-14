package com.dell.spt.base.item;

import com.dell.spt.base.data.DataCorruptionException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.io.AsyncChannel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.BitSet;

/** Created by kurila on 11.07.16. */
public interface DataItem extends Item, SeekableByteChannel {

	double LOG2 = Math.log(2);

	DataInput dataInput();

	void dataInput(final DataInput dataInput);

	void reset();

	int layer();

	void layer(final int layerNum);

	void size(final long size);

	long offset();

	void offset(final long offset);

	@Override
	long position();

	@Override
	DataItem position(long position);

	<D extends DataItem> D slice(final long from, final long size);

	/**
	* @return The number of bytes written, possibly zero
	* @throws NonWritableChannelException If this channel was not opened for writing
	* @throws ClosedChannelException If this channel is closed
	* @throws AsynchronousCloseException If another thread closes this channel while the write
	*     operation is in progress
	* @throws ClosedByInterruptException If another thread interrupts the current thread while the
	*     write operation is in progress, thereby closing the channel and setting the current
	*     thread's interrupt status
	* @throws IOException If some other I/O error occurs
	*/
	long writeToSocketChannel(final WritableByteChannel chanDst, final long maxCount)
					throws IOException;

	long writeToFileChannel(final FileChannel chanDst, final long maxCount) throws IOException;

	/**
	 Warning: the data item's position should be updated by the handler
	 @param dstChan
	 @param dstPos
	 @param maxCount
	 @param attach
	 @param handler note that the handler should invoke {@link DataItem#position(long)} to set the new position for this
	 @param <A>
	 **/
	<A> void writeToAsyncChannel(
					final AsyncChannel dstChan, final long dstPos, final long maxCount, final A attach,
					final CompletionHandler<Integer, ? super A> handler);

	void verify(final ByteBuffer inBuff) throws DataCorruptionException;

	static int rangeCount(final long size) {
		return (int) Math.ceil(Math.log(size + 1) / LOG2);
	}

	static long rangeOffset(final int i) {
		return (1 << i) - 1;
	}

	long rangeSize(int rangeIdx);

	boolean isUpdated();

	boolean isRangeUpdated(final int rangeIdx);

	int updatedRangesCount();

	void commitUpdatedRanges(final BitSet[] updatingRangesMask);
}
