package com.dell.spt.storage.driver.coop.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * The {@code storage.net.timeoutMilliSec} inactivity timer of one connection.
 *
 * <p>One all-idle timer: a read or a completed write restarts it, and it then fires once per
 * period {@code t} until the next one. If nothing is left to send when it fires, the response
 * is silent, and the request times out {@code t} after the last activity. While flushed request
 * bytes are still waiting for the socket (a single {@code FileRegion} body completes only at its
 * end, and a TLS body sits in flushed records), the event is a timeout only when the output made
 * no progress over the whole period before it. Each event samples the outbound buffer's current
 * message, pending bytes and progress together, compares the sample with the previous one and
 * keeps it. The first event after an activity only takes the sample. A stalled upload is therefore
 * reported at the second event after its last progress, within {@code 2t} of it.
 *
 * <p>All the work runs in the timer task, only after an idle period has expired: reads and writes
 * keep {@link IdleStateHandler}'s own bookkeeping, and Netty's output observation stays off.
 */
final class NetInactivityHandler extends IdleStateHandler {

	/** The outbound sample of the last event; meaningful only after a first event while sending. */
	private int lastMessage;
	private long lastPendingBytes;
	private long lastProgress;

	NetInactivityHandler(final long timeoutMillis) {
		super(false, 0, 0, timeoutMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void channelIdle(final ChannelHandlerContext ctx, final IdleStateEvent evt)
					throws Exception {
		final var buf = ctx.channel().unsafe().outboundBuffer();
		// Unflushed writes do not count: nothing sends them, as when TLS holds a request back
		// until a handshake that never completes.
		if (buf != null && !buf.isEmpty()) {
			final int message = System.identityHashCode(buf.current());
			final long pendingBytes = buf.totalPendingWriteBytes();
			final long progress = buf.currentProgress();
			final boolean moved = message != lastMessage || pendingBytes != lastPendingBytes
							|| progress != lastProgress;
			lastMessage = message;
			lastPendingBytes = pendingBytes;
			lastProgress = progress;
			if (evt.isFirst() || moved) {
				return;
			}
		}
		super.channelIdle(ctx, evt);
	}
}
