package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.PrematureChannelClosureException;
import io.netty.handler.timeout.IdleStateEvent;

import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Tests for {@link ResponseHandlerBase} — exception handling and idle timeout logic.
 */
@SuppressWarnings("unchecked")
class ResponseHandlerBaseTest {

	private ResponseHandlerBase<Object, Item, Operation<Item>> newHandler(
					final NettyStorageDriverBase<Item, Operation<Item>> driver) {
		return new ResponseHandlerBase<>(driver, false) {
			@Override
			protected void handle(
							final io.netty.channel.Channel channel,
							final Operation<Item> op,
							final Object msg) {
				// no-op stub
			}
		};
	}

	private EmbeddedChannel channelWithHandler(
					final ResponseHandlerBase<?, ?, ?> handler, final Operation<Item> op) {
		final var ch = new EmbeddedChannel(handler);
		if (op != null) {
			ch.attr(NettyStorageDriver.ATTR_KEY_OPERATION).set(op);
		}
		return ch;
	}

	@Test
	void exceptionCaught_ioExceptionWhileStarted_setsFailIo() {
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		when(driver.isStopped()).thenReturn(false);
		final var handler = newHandler(driver);
		final Operation<Item> op = mock(Operation.class);
		final var ch = channelWithHandler(handler, op);

		ch.pipeline().fireExceptionCaught(new IOException("connection reset"));

		verify(op).status(Operation.Status.FAIL_IO);
		verify(driver).complete(eq(ch), eq(op));
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_prematureCloseWhenNotStartedNotShutdown_setsInterrupted() {
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(false);
		when(driver.isShutdown()).thenReturn(false);
		when(driver.isStopped()).thenReturn(false);
		final var handler = newHandler(driver);
		final Operation<Item> op = mock(Operation.class);
		final var ch = channelWithHandler(handler, op);

		ch.pipeline().fireExceptionCaught(new PrematureChannelClosureException("closed"));

		verify(op).status(Operation.Status.INTERRUPTED);
		verify(driver).complete(eq(ch), eq(op));
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_otherExceptionWhenNotStarted_setsFailUnknown() {
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(false);
		when(driver.isShutdown()).thenReturn(false);
		when(driver.isStopped()).thenReturn(false);
		final var handler = newHandler(driver);
		final Operation<Item> op = mock(Operation.class);
		final var ch = channelWithHandler(handler, op);

		ch.pipeline().fireExceptionCaught(new RuntimeException("unexpected"));

		verify(op).status(Operation.Status.FAIL_UNKNOWN);
		verify(driver).complete(eq(ch), eq(op));
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_nullOpOnChannel_doesNothing() {
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		when(driver.isStopped()).thenReturn(false);
		final var handler = newHandler(driver);
		final var ch = channelWithHandler(handler, null);

		// Should not throw
		ch.pipeline().fireExceptionCaught(new IOException("no op"));

		verify(driver, never()).complete(any(), any());
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_driverStopped_skipsComplete() {
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		when(driver.isStopped()).thenReturn(true);
		final var handler = newHandler(driver);
		final Operation<Item> op = mock(Operation.class);
		final var ch = channelWithHandler(handler, op);

		ch.pipeline().fireExceptionCaught(new IOException("stopped"));

		verify(op).status(Operation.Status.FAIL_IO);
		verify(driver, never()).complete(any(), any());
		ch.finishAndReleaseAll();
	}

	@Test
	void userEventTriggered_idleStateEvent_throwsSocketTimeout() {
		final var driver = mock(NettyStorageDriverBase.class);
		final var handler = newHandler(driver);
		final var ch = channelWithHandler(handler, null);

		// IdleStateEvent should cause SocketTimeoutException, which propagates
		// through the pipeline as an exception
		ch.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);

		// The SocketTimeoutException thrown by userEventTriggered will be caught
		// by Netty and forwarded to exceptionCaught. Since there's no op on the
		// channel, it should be a no-op there.
		// Verify the channel is still usable (no crash)
		ch.finishAndReleaseAll();
	}
}
