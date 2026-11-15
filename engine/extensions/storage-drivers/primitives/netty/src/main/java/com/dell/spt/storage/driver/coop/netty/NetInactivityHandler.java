package com.dell.spt.storage.driver.coop.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * The {@code storage.net.timeoutMilliSec} inactivity timer of one connection.
 *
 * <p>One all-idle timer with output observed: a read or a completed write restarts it, so a
 * transfer that keeps moving in either direction never times out. A response that stays silent
 * after its request was sent (nothing left to write) is reported {@code t} after the last
 * activity. While request bytes are still pending, the first event is held back: Netty takes its
 * output baseline on that event, then suppresses later events while the pending write progresses
 * (a single {@code FileRegion} body completes only at its end). A stalled upload is therefore
 * reported at the next event, at most {@code 2t} after its last activity.
 *
 * <p>All the work is in the timer task; reads and writes keep {@link IdleStateHandler}'s own
 * bookkeeping.
 */
final class NetInactivityHandler extends IdleStateHandler {

	NetInactivityHandler(final long timeoutMillis) {
		super(true, 0, 0, timeoutMillis, TimeUnit.MILLISECONDS);
	}

	@Override
	protected void channelIdle(final ChannelHandlerContext ctx, final IdleStateEvent evt)
					throws Exception {
		if (evt.isFirst() && hasPendingOutput(ctx)) {
			return;
		}
		super.channelIdle(ctx, evt);
	}

	private static boolean hasPendingOutput(final ChannelHandlerContext ctx) {
		final var buf = ctx.channel().unsafe().outboundBuffer();
		return buf != null && buf.totalPendingWriteBytes() > 0;
	}
}
