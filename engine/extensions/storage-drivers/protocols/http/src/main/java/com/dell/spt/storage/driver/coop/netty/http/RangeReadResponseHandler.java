package com.dell.spt.storage.driver.coop.netty.http;

import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.IdleStateEvent;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;

/**
 * Range-only streaming pipeline handler. All binding, cancellation and inbound events are confined
 * to the channel event loop. Completion receives the captured attempt and whether framing permits
 * connection reuse; it must release transport custody before delivering a logical result.
 */
public final class RangeReadResponseHandler extends SimpleChannelInboundHandler<HttpObject> {
	private final BiConsumer<RangeReadAttempt, Boolean> completion;
	private ChannelHandlerContext context;
	private RangeReadAttempt active;
	private boolean informational;
	private boolean keepAlive;
	private boolean completionPending;
	private int inboundDepth;
	private RangeReadAttempt rejectedCompletion;

	public RangeReadResponseHandler(BiConsumer<RangeReadAttempt, Boolean> completion) {
		this.completion = Objects.requireNonNull(completion);
	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) {
		context = ctx;
	}

	/** Bind before request handoff, on the event loop, without replacing an outstanding attempt. */
	public void bind(RangeReadAttempt attempt) {
		requireEventLoop();
		Objects.requireNonNull(attempt);
		if (active != null || completionPending || context.isRemoved() || !context.channel().isActive() || attempt.outcome() != null)
			throw new IllegalStateException("Range response handler is not ready for this attempt");
		active = attempt;
		informational = false;
		keepAlive = false;
	}

	/** An old timeout/cancellation token must not affect a later request on a reused connection. */
	public boolean fail(RangeReadAttempt attempt, Operation.Status status) {
		requireEventLoop();
		Objects.requireNonNull(attempt);
		if (active != attempt)
			return false;
		attempt.transportFailure(status);
		complete(false);
		return true;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object message) throws Exception {
		inboundDepth++;
		try {
			super.channelRead(ctx, message);
		} finally {
			inboundDepth--;
			// SimpleChannelInboundHandler has now released the inbound reference.
			if (inboundDepth == 0)
				settleRejectedCompletion();
		}
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) {
		if (active == null) {
			ctx.close();
			return;
		}
		if (!message.decoderResult().isSuccess()) {
			active.finish(false);
			complete(false);
			return;
		}
		if (message instanceof HttpResponse response) {
			final int status = response.status().code();
			informational = status >= 100 && status < 200 && status != 101;
			if (!informational) {
				keepAlive = HttpUtil.isKeepAlive(response);
				final var headers = response.headers();
				if (!active.headers(status,
								HttpResponseHandlerBase.responseOperationStatus(response.status().codeClass(), response.status()),
								headers.getAll(HttpHeaderNames.CONTENT_RANGE), headers.getAll(HttpHeaderNames.CONTENT_LENGTH),
								headers.getAll(HttpHeaderNames.CONTENT_TYPE), headers.contains(HttpHeaderNames.TRANSFER_ENCODING))) {
					complete(false);
					return;
				}
			}
		}
		if (message instanceof HttpContent content) {
			if (informational) {
				if (content.content().isReadable()) {
					active.finish(false);
					complete(false);
				} else if (content instanceof LastHttpContent) {
					informational = false;
				}
				return;
			}
			if (!active.bodyBytes(content.content().readableBytes())) {
				complete(false);
				return;
			}
			if (content instanceof LastHttpContent last) {
				// Framing/representation metadata cannot be repaired or replaced by trailers.
				final var trailers = last.trailingHeaders();
				active.finish(!trailers.contains(HttpHeaderNames.CONTENT_RANGE)
								&& !trailers.contains(HttpHeaderNames.CONTENT_LENGTH)
								&& !trailers.contains(HttpHeaderNames.TRANSFER_ENCODING)
								&& !trailers.contains(HttpHeaderNames.CONTENT_TYPE));
				complete(keepAlive && active.outcome().category() == RangeReadAttempt.Category.SUCCESS);
			}
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		if (active != null) {
			if (cause instanceof DecoderException)
				active.finish(false);
			else
				active.transportFailure(Operation.Status.FAIL_IO);
			complete(false);
		} else {
			ctx.close();
		}
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
		if (event instanceof IdleStateEvent) {
			if (active != null)
				fail(active, Operation.Status.FAIL_TIMEOUT);
		} else {
			super.userEventTriggered(ctx, event);
		}
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (active != null) {
			active.transportFailure(Operation.Status.FAIL_IO);
			complete(false);
		}
		super.channelInactive(ctx);
	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) {
		if (active != null) {
			active.transportFailure(Operation.Status.FAIL_IO);
			complete(false);
		}
	}

	private void complete(boolean reusable) {
		final var completed = active;
		active = null;
		if (!reusable)
			context.close();
		completionPending = true;
		// Release the current inbound reference and finish the decoder batch before reuse.
		try {
			context.executor().execute(() -> {
				completionPending = false;
				completion.accept(completed, reusable && !context.isRemoved() && context.channel().isActive());
			});
		} catch (RejectedExecutionException rejected) {
			// A stopping executor cannot finish the decoder batch; retire this connection.
			rejectedCompletion = completed;
			context.close();
			if (inboundDepth == 0)
				settleRejectedCompletion();
		}
	}

	private void settleRejectedCompletion() {
		if (rejectedCompletion != null) {
			final var completed = rejectedCompletion;
			rejectedCompletion = null;
			completionPending = false;
			completion.accept(completed, false);
		}
	}

	private void requireEventLoop() {
		if (context == null || !context.executor().inEventLoop())
			throw new IllegalStateException("Range response state belongs to the channel event loop");
	}
}
