package com.dell.spt.storage.driver.coop.netty.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.TokenItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.path.PathOperation;
import com.dell.spt.base.item.op.token.TokenOperation;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpStatusClass;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
}
