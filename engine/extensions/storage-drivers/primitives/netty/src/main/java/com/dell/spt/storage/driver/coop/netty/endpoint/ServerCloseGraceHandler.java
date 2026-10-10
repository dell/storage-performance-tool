package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Lets the server close a per-request connection first. Requests on these connections carry
 * {@code Connection: close}; once an exchange is complete and the connection is marked with
 * {@link #AWAIT_SERVER_CLOSE}, a close waits up to
 * {@link EndpointSelectionConstants#SERVER_CLOSE_GRACE_MILLIS} for the server's close, which keeps
 * TIME_WAIT off the client's ephemeral ports, and closes the connection itself otherwise. A further
 * close joins that wait rather than cutting it short. Unmarked closes (failures) proceed at once, as
 * does a close after the mark is cleared (shutdown).
 */
final class ServerCloseGraceHandler extends ChannelOutboundHandlerAdapter {

	/** TRUE once the connection's exchange is complete; FALSE (shutdown) forces an immediate close. */
	static final AttributeKey<Boolean> AWAIT_SERVER_CLOSE = AttributeKey.valueOf("endpointSelectionAwaitServerClose");

	// Confined to the channel's event loop, like every outbound event; set while the close waits.
	private ScheduledFuture<?> fallback;

	@Override
	public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
		final var channel = ctx.channel();
		if (!Boolean.TRUE.equals(channel.attr(AWAIT_SERVER_CLOSE).get()) || !channel.isActive()) {
			final var unusedClose = ctx.close(promise);
			return;
		}
		channel.closeFuture().addListener(closed -> promise.trySuccess());
		if (fallback != null) {
			return;
		}
		try {
			final var scheduled = ctx.executor().schedule(() -> {
				final var unusedClose = ctx.close();
			}, EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS, TimeUnit.MILLISECONDS);
			fallback = scheduled;
			channel.closeFuture().addListener(closed -> scheduled.cancel(false));
		} catch (final RejectedExecutionException e) {
			final var unusedClose = ctx.close(promise);
		}
	}
}
