package com.dell.spt.storage.driver.coop.netty.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.TokenItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.path.PathOperation;
import com.dell.spt.base.item.op.token.TokenOperation;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpStatusClass;
import io.netty.handler.codec.http.LastHttpContent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Tests for {@link HttpResponseHandlerBase#handleResponseStatus} — the HTTP status code
 * to operation status mapping logic.
 */
class HttpResponseHandlerBaseTest {

	@SuppressWarnings("unchecked")
	private final HttpStorageDriverBase<DataItemImpl, DataOperation<DataItemImpl>> driver = mock(HttpStorageDriverBase.class);

	private HttpResponseHandlerBase<DataItemImpl, DataOperation<DataItemImpl>> handler;

	@BeforeEach
	void setUp() {
		handler = new HttpResponseHandlerBase<>(driver, false) {
			@Override
			protected void handleResponseHeaders(
							final io.netty.channel.Channel channel,
							final DataOperation<DataItemImpl> op,
							final io.netty.handler.codec.http.HttpHeaders respHeaders) {
				// no-op stub
			}
		};
	}

	/** Expose the protected method for testing. */
	private boolean invokeHandleResponseStatus(
					DataOperation<DataItemImpl> op, HttpStatusClass statusClass, HttpResponseStatus status) {
		return handler.handleResponseStatus(op, statusClass, status);
	}

	@SuppressWarnings("unchecked")
	private <I extends Item, O extends Operation<I>> void invokeHandleResponseContentChunk(
					final O op, final ByteBuf contentChunk) throws Exception {
		final HttpStorageDriverBase<I, O> contentHandlerDriver = mock(HttpStorageDriverBase.class);
		final HttpResponseHandlerBase<I, O> contentHandler = new HttpResponseHandlerBase<>(contentHandlerDriver, false) {
			@Override
			protected void handleResponseHeaders(
							final Channel channel, final O op, final HttpHeaders respHeaders) {
				// no-op stub
			}
		};
		contentHandler.handleResponseContentChunk(mock(Channel.class), op, contentChunk);
	}

	private static ByteBuf contentChunk(final int size) {
		return Unpooled.wrappedBuffer(new byte[size]);
	}

	static Stream<Arguments> statusMappings() {
		return Stream.of(
						// SUCCESS → SUCC, returns true
						Arguments.of(HttpStatusClass.SUCCESS, HttpResponseStatus.OK, Operation.Status.SUCC, true),
						Arguments.of(HttpStatusClass.SUCCESS, HttpResponseStatus.NO_CONTENT, Operation.Status.SUCC, true),
						// INFORMATIONAL → RESP_FAIL_CLIENT
						Arguments.of(HttpStatusClass.INFORMATIONAL, HttpResponseStatus.CONTINUE, Operation.Status.RESP_FAIL_CLIENT, false),
						// REDIRECTION → RESP_FAIL_CLIENT
						Arguments.of(HttpStatusClass.REDIRECTION, HttpResponseStatus.MOVED_PERMANENTLY, Operation.Status.RESP_FAIL_CLIENT, false),
						// CLIENT_ERROR specifics
						Arguments.of(HttpStatusClass.CLIENT_ERROR, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE, Operation.Status.RESP_FAIL_SVC, false),
						Arguments.of(HttpStatusClass.CLIENT_ERROR, HttpResponseStatus.REQUEST_URI_TOO_LONG, Operation.Status.RESP_FAIL_SVC, false),
						Arguments.of(HttpStatusClass.CLIENT_ERROR, HttpResponseStatus.UNAUTHORIZED, Operation.Status.RESP_FAIL_AUTH, false),
						Arguments.of(HttpStatusClass.CLIENT_ERROR, HttpResponseStatus.FORBIDDEN, Operation.Status.RESP_FAIL_AUTH, false),
						Arguments.of(HttpStatusClass.CLIENT_ERROR, HttpResponseStatus.NOT_FOUND, Operation.Status.RESP_FAIL_NOT_FOUND, false),
						Arguments.of(HttpStatusClass.CLIENT_ERROR, HttpResponseStatus.BAD_REQUEST, Operation.Status.RESP_FAIL_CLIENT, false),
						// SERVER_ERROR specifics
						Arguments.of(HttpStatusClass.SERVER_ERROR, HttpResponseStatus.GATEWAY_TIMEOUT, Operation.Status.FAIL_TIMEOUT, false),
						Arguments.of(HttpStatusClass.SERVER_ERROR, HttpResponseStatus.INSUFFICIENT_STORAGE, Operation.Status.RESP_FAIL_SPACE, false),
						Arguments.of(HttpStatusClass.SERVER_ERROR, HttpResponseStatus.INTERNAL_SERVER_ERROR, Operation.Status.RESP_FAIL_SVC, false),
						// UNKNOWN → FAIL_UNKNOWN
						Arguments.of(HttpStatusClass.UNKNOWN, new HttpResponseStatus(999, "Custom"), Operation.Status.FAIL_UNKNOWN, false));
	}

	@ParameterizedTest(name = "{0} {1} → {2}")
	@MethodSource("statusMappings")
	void mapsHttpStatusToOperationStatus(
					HttpStatusClass statusClass,
					HttpResponseStatus httpStatus,
					Operation.Status expectedOpStatus,
					boolean expectedReturn) {

		@SuppressWarnings("unchecked")
		final DataOperation<DataItemImpl> op = mock(DataOperation.class);

		boolean result = invokeHandleResponseStatus(op, statusClass, httpStatus);

		assertEquals(expectedReturn, result);
		verify(op).status(expectedOpStatus);
	}

	@SuppressWarnings("unchecked")
	@Test
	void startsDataResponseOnlyForFirstNonEmptyReadChunk() throws Exception {
		final DataOperation<DataItemImpl> op = mock(DataOperation.class);
		when(op.type()).thenReturn(com.dell.spt.base.item.op.OpType.READ);
		when(op.respDataTimeStart()).thenReturn(0L);

		invokeHandleResponseContentChunk(op, Unpooled.EMPTY_BUFFER);
		verify(op, never()).startDataResponse();
		verify(op, never()).countBytesDone(anyLong());

		invokeHandleResponseContentChunk(op, contentChunk(3));

		verify(op).startDataResponse();
		verify(op).countBytesDone(3L);
	}

	@SuppressWarnings("unchecked")
	@Test
	void startsPathResponseOnlyForFirstNonEmptyReadChunk() throws Exception {
		final PathOperation<PathItem> op = mock(PathOperation.class);
		when(op.type()).thenReturn(com.dell.spt.base.item.op.OpType.READ);
		when(op.respDataTimeStart()).thenReturn(0L);

		invokeHandleResponseContentChunk(op, Unpooled.EMPTY_BUFFER);
		verify(op, never()).startDataResponse();
		verify(op, never()).countBytesDone(anyLong());

		invokeHandleResponseContentChunk(op, contentChunk(3));

		verify(op).startDataResponse();
		verify(op).countBytesDone(3L);
	}

	@SuppressWarnings("unchecked")
	@Test
	void ignoresPathStartDataResponseStateRace() throws Exception {
		final PathOperation<PathItem> op = mock(PathOperation.class);
		when(op.type()).thenReturn(com.dell.spt.base.item.op.OpType.READ);
		when(op.respDataTimeStart()).thenReturn(0L);
		doThrow(new IllegalStateException("already started")).when(op).startDataResponse();

		assertDoesNotThrow(() -> invokeHandleResponseContentChunk(op, contentChunk(1)));
		verify(op).countBytesDone(1L);
	}

	@SuppressWarnings("unchecked")
	@Test
	void startsTokenResponseOnlyForFirstNonEmptyReadChunk() throws Exception {
		final TokenOperation<TokenItem> op = mock(TokenOperation.class);
		when(op.type()).thenReturn(com.dell.spt.base.item.op.OpType.READ);
		when(op.respDataTimeStart()).thenReturn(0L);

		invokeHandleResponseContentChunk(op, Unpooled.EMPTY_BUFFER);
		verify(op, never()).startDataResponse();
		verify(op, never()).countBytesDone(anyLong());

		invokeHandleResponseContentChunk(op, contentChunk(3));

		verify(op).startDataResponse();
		verify(op).countBytesDone(3L);
	}

	@SuppressWarnings("unchecked")
	@Test
	void ignoresTokenStartDataResponseStateRace() throws Exception {
		final TokenOperation<TokenItem> op = mock(TokenOperation.class);
		when(op.type()).thenReturn(com.dell.spt.base.item.op.OpType.READ);
		when(op.respDataTimeStart()).thenReturn(0L);
		doThrow(new IllegalStateException("already started")).when(op).startDataResponse();

		assertDoesNotThrow(() -> invokeHandleResponseContentChunk(op, contentChunk(1)));
		verify(op).countBytesDone(1L);
	}

	@SuppressWarnings("unchecked")
	@Test
	void fullHttpResponseDeliversItsBodyExactlyOnce() throws Exception {
		final var chunkCalls = new AtomicInteger();
		final var finishCalls = new AtomicInteger();
		final DataOperation<DataItemImpl> op = mock(DataOperation.class);
		final HttpResponseHandlerBase<DataItemImpl, DataOperation<DataItemImpl>> countingHandler = new HttpResponseHandlerBase<>(driver, false) {
			@Override
			protected void handleResponseHeaders(
							final Channel channel,
							final DataOperation<DataItemImpl> operation,
							final HttpHeaders headers) {}

			@Override
			protected void handleResponseContentChunk(
							final Channel channel,
							final DataOperation<DataItemImpl> operation,
							final ByteBuf content) {
				chunkCalls.incrementAndGet();
			}

			@Override
			protected void handleResponseContentFinish(
							final Channel channel, final DataOperation<DataItemImpl> operation) {
				finishCalls.incrementAndGet();
			}
		};
		final var response = new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK, contentChunk(3));

		countingHandler.handle(mock(Channel.class), op, response);

		assertEquals(1, chunkCalls.get());
		assertEquals(1, finishCalls.get());
	}

	private static final String ERROR_BODY = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>NoSuchKey</Code>"
					+ "<Message>The specified key does not exist.</Message><Key>k</Key><RequestId>r-1</RequestId></Error>";
	private static final int LARGE_ITEM = 1024 * 1024;
	private static final int SMALL_ITEM = 16;

	/** A read's verified outcome: the status it ends with and the bytes it counted. */
	private record ReadOutcome(Operation.Status status, long countBytesDone) {}

	static Stream<Arguments> refusedVerifiedReads() {
		final var cases = new ArrayList<Arguments>();
		for (final var refusal : List.of(
						Arguments.of(HttpResponseStatus.NOT_FOUND, Operation.Status.RESP_FAIL_NOT_FOUND),
						Arguments.of(HttpResponseStatus.FORBIDDEN, Operation.Status.RESP_FAIL_AUTH),
						Arguments.of(HttpResponseStatus.SERVICE_UNAVAILABLE, Operation.Status.RESP_FAIL_SVC))) {
			// The larger item fails a verified body by its content, the smaller one by its size.
			for (final int itemSize : new int[]{LARGE_ITEM, SMALL_ITEM
			}) {
				for (final int chunks : new int[]{1, 2
				}) {
					cases.add(Arguments.of(refusal.get()[0], refusal.get()[1], itemSize, chunks));
				}
			}
		}
		return cases.stream();
	}

	/**
	 * With verification on, a refused READ's error body is not item data: the read keeps the status of
	 * its status line, not {@code RESP_FAIL_CORRUPT}, and counts its body as the unverified route does.
	 */
	@ParameterizedTest(name = "{0}, item {2} B, {3} chunk(s) → {1}")
	@MethodSource("refusedVerifiedReads")
	void aRefusedVerifiedReadKeepsItsHttpStatus(
					final HttpResponseStatus httpStatus, final Operation.Status expected, final int itemSize, final int chunks)
					throws Exception {
		final byte[] body = ERROR_BODY.getBytes(StandardCharsets.UTF_8);
		assertTrue(body.length > SMALL_ITEM && body.length < LARGE_ITEM);

		final ReadOutcome verified = read(true, itemSize, httpStatus, body, chunks);
		final ReadOutcome unverified = read(false, itemSize, httpStatus, body, chunks);

		assertEquals(new ReadOutcome(expected, body.length), verified);
		assertEquals(unverified, verified, "verify on and off agree for a refused read");
	}

	@Test
	void aCorrectVerifiedReadSucceeds() throws Exception {
		final byte[] body = itemContent(SMALL_ITEM * 256);
		assertEquals(new ReadOutcome(Operation.Status.SUCC, body.length),
						read(true, body.length, HttpResponseStatus.OK, body, 2));
	}

	/** A 2xx body with one byte flipped is corrupt; its later chunks are not verified or counted. */
	@Test
	void aVerifiedReadWithOneByteFlippedIsCorrupt() throws Exception {
		final byte[] body = itemContent(SMALL_ITEM * 256);
		final int flipped = 100;
		body[flipped] ^= 1;
		assertEquals(new ReadOutcome(Operation.Status.RESP_FAIL_CORRUPT, flipped),
						read(true, body.length, HttpResponseStatus.OK, body, 1));
		assertEquals(new ReadOutcome(Operation.Status.RESP_FAIL_CORRUPT, flipped),
						read(true, body.length, HttpResponseStatus.OK, body, 2));
	}

	@Test
	void aVerifiedReadRefusedWithoutABodyIsNotFound() throws Exception {
		assertEquals(new ReadOutcome(Operation.Status.RESP_FAIL_NOT_FOUND, 0),
						read(true, LARGE_ITEM, HttpResponseStatus.NOT_FOUND, new byte[0], 0));
	}

	/** Answers a READ of an item of {@code itemSize} bytes through {@link HttpResponseHandlerBase#handle}. */
	@SuppressWarnings("unchecked")
	private static ReadOutcome read(
					final boolean verify, final int itemSize, final HttpResponseStatus status, final byte[] body, final int chunks)
					throws Exception {
		try (final var input = dataInput()) {
			final var item = new DataItemImpl("k", 0, itemSize);
			item.dataInput(input);
			final var op = new DataOperationImpl<DataItemImpl>(
							0, OpType.READ, item, null, "/bucket/", Credential.NONE, null, 0);
			op.startRequest();
			op.finishRequest();
			final HttpStorageDriverBase<DataItemImpl, DataOperation<DataItemImpl>> readDriver = mock(HttpStorageDriverBase.class);
			final HttpResponseHandlerBase<DataItemImpl, DataOperation<DataItemImpl>> readHandler = new HttpResponseHandlerBase<>(readDriver, verify) {
				@Override
				protected void handleResponseHeaders(
								final Channel channel,
								final DataOperation<DataItemImpl> operation,
								final HttpHeaders headers) {}
			};
			final Channel channel = mock(Channel.class);
			readHandler.handle(channel, op, new DefaultHttpResponse(HttpVersion.HTTP_1_1, status));
			int offset = 0;
			for (int i = 0; i < chunks; i++) {
				final int end = i == chunks - 1 ? body.length : body.length * (i + 1) / chunks;
				readHandler.handle(channel, op, new DefaultHttpContent(Unpooled.wrappedBuffer(body, offset, end - offset)));
				offset = end;
			}
			readHandler.handle(channel, op, LastHttpContent.EMPTY_LAST_CONTENT);
			verify(readDriver).complete(channel, op);
			return new ReadOutcome(op.status(), op.countBytesDone());
		}
	}

	private static DataInput dataInput() throws IOException {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
	}

	/** The bytes a verified read of an item of {@code size} bytes expects. */
	private static byte[] itemContent(final int size) throws Exception {
		try (final var input = dataInput()) {
			final var item = new DataItemImpl("k", 0, size);
			item.dataInput(input);
			final ByteBuffer content = ByteBuffer.allocate(size);
			while (content.hasRemaining()) {
				item.read(content);
			}
			return content.array();
		}
	}

}
