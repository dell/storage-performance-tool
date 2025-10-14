package com.dell.spt.base.item.io;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.CompletionHandler;

public interface AsyncChannel
				extends AsynchronousChannel {

	boolean isFileChannel();

	AsynchronousChannel wrapped();

	<A> void read(
					final ByteBuffer dst, final long position, final A attach, final CompletionHandler<Integer, ? super A> handler);

	<A> void write(
					final ByteBuffer src, final long position, final A attach, final CompletionHandler<Integer, ? super A> handler);

	static AsyncChannel wrap(final AsynchronousChannel channel) {
		return new AsyncChannelWrapper(channel);
	}
}
