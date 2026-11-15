package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The decision {@link NetInactivityHandler} adds to Netty's all-idle timer. The end-to-end
 * behaviour on loopback (steady PUT, TLS PUT, GET, range GET, pooled reuse, silent response,
 * stalled upload) is in the s3 driver's {@code S3NetInactivityTimeoutTest}.
 */
class NetInactivityHandlerTest {

	private static final IdleStateEvent FIRST = IdleStateEvent.FIRST_ALL_IDLE_STATE_EVENT;
	private static final IdleStateEvent NEXT = IdleStateEvent.ALL_IDLE_STATE_EVENT;

	@Test
	void armsOneAllIdleTimer() {
		final IdleStateHandler handler = new NetInactivityHandler(1_500);
		assertEquals(0, handler.getReaderIdleTimeInMillis());
		assertEquals(0, handler.getWriterIdleTimeInMillis());
		assertEquals(1_500, handler.getAllIdleTimeInMillis());
	}

	@Test
	void aSilentResponseTimesOutOnTheFirstEvent() throws Exception {
		final var out = new Output(0);
		new NetInactivityHandler(1).channelIdle(out.ctx, FIRST);
		verify(out.ctx).fireUserEventTriggered(FIRST);
	}

	@Test
	void aPendingUploadWaitsForTheNextEvent() throws Exception {
		final var out = new Output(1);
		new NetInactivityHandler(1).channelIdle(out.ctx, FIRST);
		verify(out.ctx, never()).fireUserEventTriggered(FIRST);
	}

	@Test
	void anUploadThatProgressedOverThePeriodWaitsAgain() throws Exception {
		final var out = new Output(1);
		final var handler = new NetInactivityHandler(1);
		handler.channelIdle(out.ctx, FIRST);
		out.progress = 200;
		handler.channelIdle(out.ctx, NEXT);
		verify(out.ctx, never()).fireUserEventTriggered(NEXT);
	}

	@Test
	void anUploadWithNoProgressOverThePeriodTimesOut() throws Exception {
		final var out = new Output(1);
		final var handler = new NetInactivityHandler(1);
		handler.channelIdle(out.ctx, FIRST);
		handler.channelIdle(out.ctx, NEXT);
		verify(out.ctx).fireUserEventTriggered(NEXT);
	}

	@Test
	void aRecordChangeThenAStallBuysOnlyOnePeriod() throws Exception {
		// The TLS sequence that cost Netty's own output observation an extra period: the message
		// and pending bytes change with the progress, then nothing moves. Every sample is taken
		// whole, so the event after the change already sees the stall.
		final var out = new Output(1);
		final var handler = new NetInactivityHandler(1);
		handler.channelIdle(out.ctx, FIRST);
		out.message = new Object();
		out.pending = 500;
		out.progress = 200;
		handler.channelIdle(out.ctx, NEXT);
		verify(out.ctx, never()).fireUserEventTriggered(NEXT);
		handler.channelIdle(out.ctx, NEXT);
		verify(out.ctx, times(1)).fireUserEventTriggered(NEXT);
	}

	@Test
	void aNewActivityTakesAFreshSample() throws Exception {
		// After a read or completed write the next event is a first one again: it only samples,
		// whatever the previous period's sample was.
		final var out = new Output(1);
		final var handler = new NetInactivityHandler(1);
		handler.channelIdle(out.ctx, FIRST);
		handler.channelIdle(out.ctx, NEXT);
		verify(out.ctx, times(1)).fireUserEventTriggered(NEXT);
		handler.channelIdle(out.ctx, FIRST);
		verify(out.ctx, never()).fireUserEventTriggered(FIRST);
	}

	@Test
	void unflushedBytesDoNotHoldTheTimeoutBack() throws Exception {
		// TLS holds a request back until its handshake completes: nothing is sending it.
		final var out = new Output(0);
		out.pending = 1387;
		new NetInactivityHandler(1).channelIdle(out.ctx, FIRST);
		verify(out.ctx).fireUserEventTriggered(FIRST);
	}

	@Test
	void aClosedChannelWithoutAnOutboundBufferTimesOut() throws Exception {
		final var out = new Output(-1);
		new NetInactivityHandler(1).channelIdle(out.ctx, FIRST);
		verify(out.ctx).fireUserEventTriggered(FIRST);
	}

	@Test
	void aScheduledEventReachesTheNextHandler() {
		final var seen = new ArrayList<Object>();
		final var channel = new EmbeddedChannel(
						new NetInactivityHandler(1),
						new ChannelInboundHandlerAdapter() {
							@Override
							public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) {
								seen.add(evt);
							}
						});
		channel.advanceTimeBy(1, TimeUnit.MILLISECONDS);
		channel.runScheduledPendingTasks();
		assertTrue(seen.contains(FIRST), seen::toString);
		channel.finishAndReleaseAll();
	}

	/**
	 * A context whose outbound buffer holds {@code flushed} entries (none when negative: no
	 * buffer); the current message, pending bytes and progress can change between events.
	 */
	private static final class Output {
		final ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
		Object message = new Object();
		long pending = 1000;
		long progress = 100;

		Output(final int flushed) {
			final var channel = mock(Channel.class);
			final var unsafe = mock(Channel.Unsafe.class);
			when(ctx.channel()).thenReturn(channel);
			when(channel.unsafe()).thenReturn(unsafe);
			if (flushed >= 0) {
				final var buf = mock(ChannelOutboundBuffer.class);
				when(buf.size()).thenReturn(flushed);
				when(buf.isEmpty()).thenReturn(flushed == 0);
				when(buf.current()).thenAnswer(call -> message);
				when(buf.totalPendingWriteBytes()).thenAnswer(call -> pending);
				when(buf.currentProgress()).thenAnswer(call -> progress);
				when(unsafe.outboundBuffer()).thenReturn(buf);
			}
		}
	}
}
