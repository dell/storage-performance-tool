package com.dell.spt.storage.driver.coop.netty.http;

import static org.junit.jupiter.api.Assertions.*;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.ByteRange;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeResponseValidator;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RangeReadResponseHandlerTest {
	private record Result(RangeReadAttempt attempt, boolean reusable) {}

	@ParameterizedTest
	@ValueSource(booleans = {true, false
	})
	void rejectedCompletionReleasesInboundBeforeSettlingOnce(boolean validBody) {
		var payload = Unpooled.buffer().writeZero(validBody ? 3 : 4);
		var results = new ArrayList<Result>();
		var handler = new RangeReadResponseHandler((attempt, reusable) -> {
			assertEquals(0, payload.refCnt());
			results.add(new Result(attempt, reusable));
		});
		var channel = new EmbeddedChannel(handler);
		try {
			var ctx = org.mockito.Mockito.spy(channel.pipeline().context(handler));
			var executor = org.mockito.Mockito.mock(io.netty.util.concurrent.EventExecutor.class);
			org.mockito.Mockito.when(executor.inEventLoop()).thenReturn(true);
			org.mockito.Mockito.doThrow(new java.util.concurrent.RejectedExecutionException("stopping"))
							.when(executor).execute(org.mockito.ArgumentMatchers.any(Runnable.class));
			org.mockito.Mockito.doReturn(executor).when(ctx).executor();
			handler.handlerAdded(ctx);
			var attempt = new RangeReadAttempt(new OperationLifecycle(), new ByteRange(2, 3));
			handler.bind(attempt);
			assertTrue(attempt.requestHandoff());
			var response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT);
			response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes 2-4/8");
			response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, 3);
			channel.writeInbound(response);
			channel.writeInbound(new DefaultLastHttpContent(payload));
			channel.runPendingTasks();
			assertEquals(List.of(new Result(attempt, false)), results);
			assertFalse(channel.isActive());
			handler.handlerRemoved(ctx);
			assertEquals(1, results.size());
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	private static class Connection implements AutoCloseable {
		final List<Result> results = new ArrayList<>();
		final RangeReadResponseHandler handler = new RangeReadResponseHandler((a, reuse) -> results.add(new Result(a, reuse)));
		final EmbeddedChannel channel = new EmbeddedChannel(new RangeReadResponseDecoder(8192), new HttpRequestEncoder(), handler);

		RangeReadAttempt begin() {
			var attempt = new RangeReadAttempt(new OperationLifecycle(), new ByteRange(2, 3));
			handler.bind(attempt);
			assertTrue(attempt.requestHandoff());
			channel.writeOutbound(new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/bucket/key"));
			Object outbound;
			while ((outbound = channel.readOutbound()) != null)
				ReferenceCountUtil.release(outbound);
			return attempt;
		}

		void receive(String wire) {
			channel.writeInbound(Unpooled.copiedBuffer(wire, StandardCharsets.US_ASCII));
			channel.runPendingTasks();
		}

		@Override
		public void close() {
			channel.finishAndReleaseAll();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {true, false
	})
	void decoderRejectsResponsesBeyondOrdinaryDriverLimits(boolean initialLine) {
		try (var c = new Connection()) {
			var attempt = c.begin();
			String wire = initialLine
							? "HTTP/1.1 206 " + "x".repeat(HttpStorageDriver.REQ_LINE_LEN) + "\r\n"
							: "HTTP/1.1 206 Partial Content\r\nX-Padding: " + "x".repeat(HttpStorageDriver.HEADERS_LEN) + "\r\n";
			c.receive(wire + "Content-Range: bytes 2-4/8\r\nContent-Length: 3\r\n\r\nabc");
			assertEquals(List.of(new Result(attempt, false)), c.results);
			assertNotEquals(Operation.Status.SUCC, attempt.outcome().status());
			assertFalse(c.channel.isActive());
		}
	}

	@Test
	void rejectedCompletionWithoutInboundSettlesCancellationOnce() {
		try (var c = new Connection()) {
			var attempt = c.begin();
			var ctx = org.mockito.Mockito.spy(c.channel.pipeline().context(c.handler));
			var executor = org.mockito.Mockito.mock(io.netty.util.concurrent.EventExecutor.class);
			org.mockito.Mockito.when(executor.inEventLoop()).thenReturn(true);
			org.mockito.Mockito.doThrow(new java.util.concurrent.RejectedExecutionException("stopping"))
							.when(executor).execute(org.mockito.ArgumentMatchers.any(Runnable.class));
			org.mockito.Mockito.doReturn(executor).when(ctx).executor();
			c.handler.handlerAdded(ctx);
			assertTrue(c.handler.fail(attempt, Operation.Status.FAIL_TIMEOUT));
			assertEquals(List.of(new Result(attempt, false)), c.results);
			assertEquals(Operation.Status.FAIL_TIMEOUT, attempt.outcome().status());
			assertFalse(c.handler.fail(attempt, Operation.Status.FAIL_IO));
			c.channel.runPendingTasks();
			assertEquals(1, c.results.size());
			assertFalse(c.channel.isActive());
		}
	}

	@Test
	void exactBodyWaitsForChunkedFramingThenAllowsReuse() {
		try (var c = new Connection()) {
			var first = c.begin();
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n");
			assertNull(first.outcome());
			assertTrue(c.results.isEmpty());
			c.receive("0\r\n\r\n");
			assertEquals(RangeReadAttempt.Category.SUCCESS, first.outcome().category());
			assertEquals(List.of(new Result(first, true)), c.results);
			var second = c.begin();
			assertFalse(c.handler.fail(first, Operation.Status.FAIL_TIMEOUT));
			assertTrue(c.channel.isActive());
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/*\r\nContent-Length: 3\r\n\r\nxyz");
			assertEquals(RangeReadAttempt.Category.SUCCESS, second.outcome().category());
			assertEquals(2, c.results.size());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"HTTP/1.1 200 OK\r\nContent-Length: 3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: +3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 3,3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 1-3/8\r\nContent-Length: 3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/4\r\nContent-Length: 3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 2\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 3\r\nContent-Length: 3\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n",
			"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Type: multipart/byteranges; boundary=x\r\nContent-Length: 3\r\n"
	})
	void rejectedHeadersCloseWithoutDrainingBody(String headers) {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive(headers + "\r\n");
			assertEquals(RangeReadAttempt.Category.VALIDATION, attempt.outcome().category());
			assertFalse(c.channel.isActive());
			assertEquals(List.of(new Result(attempt, false)), c.results);
			assertEquals(0, attempt.outcome().receivedBytes());
		}
	}

	@ParameterizedTest
	@ValueSource(ints = {403, 404, 416, 500, 503, 504
	})
	void httpErrorsKeepHttpCategory(int status) {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive("HTTP/1.1 " + status + " Error\r\nContent-Length: 1000000\r\n\r\n");
			assertEquals(RangeReadAttempt.Category.HTTP, attempt.outcome().category());
			assertEquals(status, attempt.outcome().httpStatus());
			assertEquals(1, c.results.size());
			assertFalse(c.channel.isActive());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"2\r\nab\r\n0\r\n\r\n", "4\r\nabcd\r\n0\r\n\r\n", "3\r\nabc\r\nz\r\n", "3\r\nabc\r\n0\r\nContent-Range: bytes 2-4/8\r\n\r\n"
	})
	void shortOversizedMalformedAndTrailerBodiesFail(String body) {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nTransfer-Encoding: chunked\r\n\r\n" + body);
			assertEquals(RangeReadAttempt.Category.VALIDATION, attempt.outcome().category());
			assertFalse(c.channel.isActive());
			assertEquals(1, c.results.size());
		}
	}

	@Test
	void informationalResponseDoesNotSettleAttempt() {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\n");
			assertNull(attempt.outcome());
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/80\r\nContent-Length: 3\r\n\r\nabc");
			assertEquals(RangeReadAttempt.Category.SUCCESS, attempt.outcome().category());
		}
	}

	@Test
	void timeoutAndLateCloseCompleteOnceWithoutSuccessAtExactBytes() {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n");
			c.channel.pipeline().fireUserEventTriggered(IdleStateEvent.FIRST_READER_IDLE_STATE_EVENT);
			c.channel.runPendingTasks();
			assertEquals(Operation.Status.FAIL_TIMEOUT, attempt.outcome().status());
			c.channel.close().syncUninterruptibly();
			c.channel.runPendingTasks();
			assertEquals(1, c.results.size());
			assertEquals(3, attempt.outcome().receivedBytes());
		}
	}

	@Test
	void connectionLossIsTransportFailure() {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 3\r\n\r\na");
			c.channel.close().syncUninterruptibly();
			c.channel.runPendingTasks();
			assertEquals(RangeReadAttempt.Category.TRANSPORT, attempt.outcome().category());
			assertEquals(1, c.results.size());
		}
	}

	@Test
	void referenceReleasedBeforeCompletionAndNoAggregation() {
		var calls = new ArrayList<Boolean>();
		var payload = Unpooled.buffer(4).writeInt(0);
		var handler = new RangeReadResponseHandler((a, reuse) -> {
			assertEquals(0, payload.refCnt());
			calls.add(reuse);
		});
		var channel = new EmbeddedChannel(handler);
		try {
			var attempt = new RangeReadAttempt(new OperationLifecycle(), new ByteRange(2, 3));
			handler.bind(attempt);
			attempt.requestHandoff();
			var response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT);
			response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes 2-4/8");
			channel.writeInbound(response);
			channel.writeInbound(new DefaultHttpContent(payload));
			channel.runPendingTasks();
			assertEquals(RangeResponseValidator.Failure.OVERSIZED_BODY, attempt.outcome().validationFailure());
			assertEquals(4, attempt.outcome().receivedBytes());
			assertEquals(List.of(false), calls);
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void closeDelimitedExactBodySucceedsOnlyAtEofAndCannotReuse() {
		try (var c = new Connection()) {
			var attempt = c.begin();
			c.receive("HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nConnection: close\r\n\r\nabc");
			assertNull(attempt.outcome());
			c.channel.close().syncUninterruptibly();
			c.channel.runPendingTasks();
			assertEquals(RangeReadAttempt.Category.SUCCESS, attempt.outcome().category());
			assertEquals(List.of(new Result(attempt, false)), c.results);
		}
	}

	@Test
	void bindingCannotReplaceAnOutstandingAttempt() {
		try (var c = new Connection()) {
			var first = c.begin();
			assertThrows(IllegalStateException.class, () -> c.handler.bind(
							new RangeReadAttempt(new OperationLifecycle(), new ByteRange(2, 3))));
			assertTrue(c.handler.fail(first, Operation.Status.FAIL_IO));
			assertFalse(c.handler.fail(first, Operation.Status.FAIL_TIMEOUT));
			c.channel.runPendingTasks();
			assertEquals(List.of(new Result(first, false)), c.results);
		}
	}

	@Test
	void largeBodyStreamsWithReleasedChunksAndNoEarlyCompletion() {
		final int chunks = 128;
		final int chunkSize = 1024;
		var results = new ArrayList<Result>();
		var handler = new RangeReadResponseHandler((a, reuse) -> results.add(new Result(a, reuse)));
		var channel = new EmbeddedChannel(handler);
		try {
			var attempt = new RangeReadAttempt(new OperationLifecycle(), new ByteRange(0, chunks * chunkSize));
			handler.bind(attempt);
			attempt.requestHandoff();
			var response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT);
			response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes 0-131071/200000");
			channel.writeInbound(response);
			for (int i = 0; i < chunks; i++) {
				var payload = Unpooled.buffer(chunkSize).writeZero(chunkSize);
				channel.writeInbound(new DefaultHttpContent(payload));
				assertEquals(0, payload.refCnt());
				assertNull(attempt.outcome());
			}
			assertTrue(results.isEmpty());
			channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT);
			channel.runPendingTasks();
			assertEquals(RangeReadAttempt.Category.SUCCESS, attempt.outcome().category());
			assertEquals(chunks * chunkSize, attempt.outcome().receivedBytes());
			assertEquals(List.of(new Result(attempt, true)), results);
		} finally {
			channel.finishAndReleaseAll();
		}
	}

	@Test
	void removedHandlerCannotAdvertiseConnectionReuse() {
		var results = new ArrayList<Result>();
		var handler = new RangeReadResponseHandler((a, reuse) -> results.add(new Result(a, reuse)));
		var channel = new EmbeddedChannel(new io.netty.channel.ChannelInboundHandlerAdapter() {
			@Override
			public void channelRead(io.netty.channel.ChannelHandlerContext ctx, Object message) {
				ctx.fireChannelRead(message);
				if (message instanceof LastHttpContent)
					ctx.pipeline().remove(handler);
			}
		}, handler);
		try {
			var attempt = new RangeReadAttempt(new OperationLifecycle(), new ByteRange(2, 3));
			handler.bind(attempt);
			attempt.requestHandoff();
			var response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.PARTIAL_CONTENT,
							Unpooled.wrappedBuffer(new byte[]{1, 2, 3
							}));
			response.headers().set(HttpHeaderNames.CONTENT_RANGE, "bytes 2-4/8");
			channel.writeInbound(response);
			channel.runPendingTasks();
			assertEquals(RangeReadAttempt.Category.SUCCESS, attempt.outcome().category());
			assertEquals(List.of(new Result(attempt, false)), results);
			assertThrows(IllegalStateException.class, () -> handler.bind(
							new RangeReadAttempt(new OperationLifecycle(), new ByteRange(2, 3))));
		} finally {
			channel.finishAndReleaseAll();
		}
	}

}
