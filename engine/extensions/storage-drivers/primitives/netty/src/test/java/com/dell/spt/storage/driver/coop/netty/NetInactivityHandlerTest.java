package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The decision {@link NetInactivityHandler} adds to Netty's all-idle timer. The end-to-end
 * behaviour on loopback (steady PUT, TLS PUT, GET, range GET, pooled reuse, silent response,
 * stalled upload) is in the s3 driver's {@code S3NetInactivityTimeoutTest}.
 */
class NetInactivityHandlerTest {

	@Test
	void armsOneAllIdleTimerWithOutputObserved() {
		final IdleStateHandler handler = new NetInactivityHandler(1_500);
		assertEquals(0, handler.getReaderIdleTimeInMillis());
		assertEquals(0, handler.getWriterIdleTimeInMillis());
		assertEquals(1_500, handler.getAllIdleTimeInMillis());
	}

	@Test
	void aSilentResponseTimesOutOnTheFirstEvent() throws Exception {
		final var ctx = context(0);
		new NetInactivityHandler(1).channelIdle(ctx, IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
		verify(ctx).fireUserEventTriggered(IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
	}

	@Test
	void aPendingUploadWaitsForTheNextEvent() throws Exception {
		final var ctx = context(1);
		new NetInactivityHandler(1).channelIdle(ctx, IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
		verify(ctx, never()).fireUserEventTriggered(IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
	}

	@Test
	void aStalledUploadTimesOutOnTheNextEvent() throws Exception {
		// Netty fires a later event only when the pending write made no progress since the last.
		final var ctx = context(1);
		new NetInactivityHandler(1).channelIdle(ctx, IdleStateEvent.ALL_IDLE_STATE_EVENT);
		verify(ctx).fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
	}

	@Test
	void unflushedBytesDoNotHoldTheTimeoutBack() throws Exception {
		// TLS holds a request back until its handshake completes: nothing is sending it.
		final var ctx = context(0);
		when(ctx.channel().unsafe().outboundBuffer().totalPendingWriteBytes()).thenReturn(1387L);
		new NetInactivityHandler(1).channelIdle(ctx, IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
		verify(ctx).fireUserEventTriggered(IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
	}

	@Test
	void aClosedChannelWithoutAnOutboundBufferTimesOut() throws Exception {
		final var ctx = context(-1);
		new NetInactivityHandler(1).channelIdle(ctx, IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
		verify(ctx).fireUserEventTriggered(IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT);
	}

	@Test
	void aScheduledEventReachesTheNextHandler() {
		final var seen = new java.util.ArrayList<Object>();
		final var channel = new io.netty.channel.embedded.EmbeddedChannel(
						new NetInactivityHandler(1),
						new io.netty.channel.ChannelInboundHandlerAdapter() {
							@Override
							public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
								seen.add(evt);
							}
						});
		channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
		channel.runScheduledPendingTasks();
		assertTrue(seen.contains(IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT), seen::toString);
		channel.finishAndReleaseAll();
	}

	/** A context whose outbound buffer holds {@code flushed} entries, or has none when negative. */
	private static ChannelHandlerContext context(final int flushed) {
		final var ctx = mock(ChannelHandlerContext.class);
		final var channel = mock(Channel.class);
		final var unsafe = mock(Channel.Unsafe.class);
		when(ctx.channel()).thenReturn(channel);
		when(channel.unsafe()).thenReturn(unsafe);
		if (flushed >= 0) {
			final var buf = mock(ChannelOutboundBuffer.class);
			when(buf.size()).thenReturn(flushed);
			when(buf.isEmpty()).thenReturn(flushed == 0);
			when(unsafe.outboundBuffer()).thenReturn(buf);
		}
		return ctx;
	}
}
