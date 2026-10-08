package com.dell.spt.storage.driver.coop.netty.endpoint;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Lets the server close a per-request connection first. Requests on these connections carry
 * {@code Connection: close}; when a completed exchange is closed, this handler waits up to
 * {@link EndpointSelectionConstants#SERVER_CLOSE_GRACE_MILLIS} for the server's close, which keeps
 * TIME_WAIT off the client's ephemeral ports, and closes the connection itself otherwise. Only a
 * close marked with {@link #AWAIT_SERVER_CLOSE} is deferred; failure and shutdown closes proceed at once.
 */
final class ServerCloseGraceHandler extends ChannelOutboundHandlerAdapter {

	/** Set on a channel just before closing it after a completed exchange; consumed by the close. */
	static final AttributeKey<Boolean> AWAIT_SERVER_CLOSE = AttributeKey.valueOf("endpointSelectionAwaitServerClose");

	@Override
	public void close(final ChannelHandlerContext ctx, final ChannelPromise promise) throws Exception {
		final var channel = ctx.channel();
		if (!Boolean.TRUE.equals(channel.attr(AWAIT_SERVER_CLOSE).getAndSet(Boolean.FALSE)) || !channel.isActive()) {
			ctx.close(promise);
			return;
		}
		try {
			final var fallback = ctx.executor().schedule(() -> {
				ctx.close(promise);
			}, EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS, TimeUnit.MILLISECONDS);
			channel.closeFuture().addListener(closed -> {
				fallback.cancel(false);
				promise.trySuccess();
			});
		} catch (final RejectedExecutionException e) {
			ctx.close(promise);
		}
	}
}
