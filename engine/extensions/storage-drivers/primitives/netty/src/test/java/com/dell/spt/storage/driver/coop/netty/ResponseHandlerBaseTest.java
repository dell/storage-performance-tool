package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;

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
		when(driver.shouldLogChannelFailureWarning()).thenReturn(true);
		final var handler = newHandler(driver);
		final Operation<Item> op = mock(Operation.class);
		when(op.toString()).thenReturn("READ,/bucket/b0000007/key,,");
		when(op.nodeAddr()).thenReturn("swfs");
		final var ch = channelWithHandler(handler, op);

		ch.pipeline().fireExceptionCaught(new IOException("connection reset"));

		verify(op).status(Operation.Status.FAIL_IO);
		verify(driver).shouldLogChannelFailureWarning();
		verify(driver).complete(eq(ch), eq(op));
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_deleteAfterHeadersClearsIncompleteTransportTimingBeforeCompletion() {
		final NettyStorageDriverBase<IntegrityManifestDataItem, DeleteRequestOperation> driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		when(driver.isStopped()).thenReturn(false);
		when(driver.shouldLogChannelFailureWarning()).thenReturn(false);
		final ResponseHandlerBase<Object, IntegrityManifestDataItem, DeleteRequestOperation> handler = new ResponseHandlerBase<>(driver, false) {
			@Override
			protected void handle(
							final io.netty.channel.Channel channel,
							final DeleteRequestOperation op,
							final Object msg) {}
		};
		final DeleteRequestOperation op = mock(DeleteRequestOperation.class);
		final var ch = new EmbeddedChannel(handler);
		ch.attr(NettyStorageDriver.ATTR_KEY_OPERATION).set(op);

		ch.pipeline().fireExceptionCaught(new PrematureChannelClosureException("closed after headers"));

		final var ordered = inOrder(op, driver);
		ordered.verify(op).status(Operation.Status.FAIL_IO);
		ordered.verify(op).failTransportAttempt();
		ordered.verify(driver).complete(ch, op);
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_repeatedIoFailureDoesNotBuildWarningContext() {
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		when(driver.isStopped()).thenReturn(false);
		when(driver.shouldLogChannelFailureWarning()).thenReturn(false);
		final var handler = newHandler(driver);
		final Operation<Item> op = mock(Operation.class);
		final var ch = channelWithHandler(handler, op);

		ch.pipeline().fireExceptionCaught(new IOException("connection reset"));

		verify(op).status(Operation.Status.FAIL_IO);
		verify(op, never()).nodeAddr();
		verify(driver).complete(eq(ch), eq(op));
		ch.finishAndReleaseAll();
	}

	@Test
	void failureContext_includesOperationKeyNodeAndChannel() {
		final Operation<Item> op = mock(Operation.class);
		when(op.toString()).thenReturn("READ,/bucket/b0000007/key,,");
		when(op.nodeAddr()).thenReturn("swfs");
		final var channel = new EmbeddedChannel();

		assertEquals(
						"op=READ,/bucket/b0000007/key,,, node=swfs, channel=embedded",
						ResponseHandlerBase.failureContext(op, channel));
		channel.finishAndReleaseAll();
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

	@Test
	void channelRead0_nonOperationAttribute_skipsWithoutCrash() {
		// When a non-Operation object (e.g., a plain String) is stored in ATTR_KEY_OPERATION,
		// channelRead0 should return early without calling handle() or throwing ClassCastException.
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		final var handler = newHandler(driver);
		final var ch = new EmbeddedChannel(handler);

		// Store a non-Operation object in the attribute (raw type bypasses generic check)
		@SuppressWarnings("rawtypes")
		final io.netty.util.Attribute rawAttr = ch.attr(NettyStorageDriver.ATTR_KEY_OPERATION);
		rawAttr.set("not-an-operation");

		// Write an inbound message — this triggers channelRead0
		ch.writeInbound(new Object());

		// The handler should have returned early — no crash, no driver interaction
		verify(driver, never()).complete(any(), any());
		ch.finishAndReleaseAll();
	}

	@Test
	void exceptionCaught_nonOperationAttribute_skipsWithoutCrash() {
		// When a non-Operation object is in ATTR_KEY_OPERATION, exceptionCaught
		// should return early — no status change, no driver.complete().
		final var driver = mock(NettyStorageDriverBase.class);
		when(driver.isStarted()).thenReturn(true);
		when(driver.isStopped()).thenReturn(false);
		final var handler = newHandler(driver);
		final var ch = new EmbeddedChannel(handler);

		// Store a non-Operation object (raw type bypasses generic check)
		@SuppressWarnings("rawtypes")
		final io.netty.util.Attribute rawAttr = ch.attr(NettyStorageDriver.ATTR_KEY_OPERATION);
		rawAttr.set("not-an-operation");

		// Fire an exception through the pipeline
		ch.pipeline().fireExceptionCaught(new IOException("test exception"));

		// Should NOT have called complete — the guard returned early
		verify(driver, never()).complete(any(), any());
		ch.finishAndReleaseAll();
	}
}
