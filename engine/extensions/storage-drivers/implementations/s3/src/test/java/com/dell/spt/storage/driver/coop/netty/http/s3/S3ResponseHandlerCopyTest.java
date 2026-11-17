package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A copy that S3 answers {@code 200 OK} with an {@code <Error>} document failed, with the status a
 * multipart completion answered so gets: its status is set before the driver's {@code complete()}. A
 * {@code <CopyObjectResult>} stays a success, a refused copy keeps the status of its HTTP class, and
 * only a copy's 2xx body is kept, up to the scan bound, until its response finishes, even past its
 * channel's close.
 */
final class S3ResponseHandlerCopyTest {

	private static final String DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
	private static final String COPY_RESULT = DECLARATION
					+ "\n<CopyObjectResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
					+ "<LastModified>2026-11-16T00:00:00.000Z</LastModified>"
					+ "<ETag>\"9b2cf535f27731c974343645a3985328\"</ETag></CopyObjectResult>";

	@SuppressWarnings("unchecked")
	private final S3StorageDriver<DataItemImpl, Operation<DataItemImpl>> driver = mock(S3StorageDriver.class);
	private final S3ResponseHandler<DataItemImpl, Operation<DataItemImpl>> handler = new S3ResponseHandler<>(driver, false, false, null);
	private final EmbeddedChannel channel = new EmbeddedChannel();

	/** Each test is one step: its driver logs each copy error code once. */
	@BeforeEach
	void stepLog() {
		when(driver.copyErrorCodeLog()).thenReturn(S3ResponseHandler.CompletionErrorCodeLog.forCopies());
	}

	@Test
	void aTransientErrorBodyFailsTheCopyAsRetryable() throws Exception {
		for (final var code : S3ResponseHandler.TRANSIENT_COMPLETION_ERROR_CODES) {
			assertEquals(Operation.Status.RESP_FAIL_SVC,
							copyStatus("<Error><Code>" + code + "</Code><Message>m</Message></Error>"), code);
		}
	}

	@Test
	void aPermanentErrorBodyFailsTheCopyAsNotRetryable() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_CLIENT,
						copyStatus("<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"));
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, copyStatus(
						DECLARATION + "<Error xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Code>NoSuchKey</Code></Error>"));
	}

	@Test
	void anUnreadableCodeIsRetryable() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_SVC, copyStatus("<Error><Message>no code</Message></Error>"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, copyStatus("<Error/>"));
	}

	@Test
	void anErrorBodyAfterWhitespaceAndADeclarationFailsTheCopy() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_SVC, copyStatus(
						"\n  \r\n\t" + DECLARATION + "\n\n<Error><Code>InternalError</Code></Error>"));
	}

	@Test
	void anErrorBodySplitAcrossTwoChunksFailsTheCopy() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_CLIENT,
						copyStatus("<Error><Code>Access", "Denied</Code></Error>"));
		assertEquals(Operation.Status.RESP_FAIL_SVC,
						copyStatus(" ".repeat(300), "\n<Error><Code>SlowDown</Code></Error>"));
	}

	@Test
	void aCopyResultStaysSuccessful() throws Exception {
		assertEquals(Operation.Status.SUCC, copyStatus(COPY_RESULT));
		assertEquals(Operation.Status.SUCC, copyStatus(COPY_RESULT.substring(0, 60), COPY_RESULT.substring(60)));
	}

	@Test
	void anEmptyCopyResponseStaysSuccessful() throws Exception {
		assertEquals(Operation.Status.SUCC, copyStatus());
		assertEquals(Operation.Status.SUCC, copyStatus(""));
		assertFalse(S3ResponseHandler.keptCopyResponse(channel), "an empty body is not kept");
	}

	@Test
	void anErrorRootBeyondTheBoundIsNotRead() throws Exception {
		assertEquals(Operation.Status.SUCC, copyStatus(
						" ".repeat(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES), "<Error><Code>InternalError</Code></Error>"));
	}

	@Test
	void aRefusedCopyKeepsItsStatusAndKeepsNoCopyBuffer() throws Exception {
		final var op = copy();
		respond(op, HttpResponseStatus.SERVICE_UNAVAILABLE, "<Error><Code>AccessDenied</Code></Error>");
		assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
		final var refused = copy();
		respond(refused, HttpResponseStatus.FORBIDDEN, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.RESP_FAIL_AUTH, refused.status());
		assertFalse(S3ResponseHandler.keptCopyResponse(channel), "a refused copy's body goes to the failed-response log only");
	}

	@Test
	void aPlainCreateAndAReadKeepNoCopyBuffer() throws Exception {
		final var create = flat(OpType.CREATE, null);
		respond(create, HttpResponseStatus.OK, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.SUCC, create.status());
		final var emptySource = flat(OpType.CREATE, "");
		respond(emptySource, HttpResponseStatus.OK, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.SUCC, emptySource.status());
		final var read = flat(OpType.READ, "/src");
		respond(read, HttpResponseStatus.OK, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.SUCC, read.status());
		assertFalse(S3ResponseHandler.keptCopyResponse(channel), "only a copy keeps its body");
	}

	@Test
	void theNextResponseOnTheChannelIsNotTakenForACopy() throws Exception {
		final var error = "<Error><Code>InternalError</Code></Error>";
		final var first = copy();
		respond(first, HttpResponseStatus.OK, error);
		assertEquals(Operation.Status.RESP_FAIL_SVC, first.status());
		final var create = flat(OpType.CREATE, null);
		handler.handleResponseStatus(create, HttpResponseStatus.OK.codeClass(), HttpResponseStatus.OK);
		handler.handleResponseContentChunk(channel, create, chunk(error));
		assertNull(S3ResponseHandler.heldCopyResponse(channel), "a plain create after a copy keeps no body");
		handler.handleResponseContentFinish(channel, create);
		assertEquals(Operation.Status.SUCC, create.status());
		respond(copy(), HttpResponseStatus.OK, error);
		final var read = flat(OpType.READ, "/src");
		handler.handleResponseStatus(read, HttpResponseStatus.OK.codeClass(), HttpResponseStatus.OK);
		handler.handleResponseContentChunk(channel, read, chunk(error));
		assertNull(S3ResponseHandler.heldCopyResponse(channel), "a read after a copy keeps no body");
		handler.handleResponseContentFinish(channel, read);
		assertEquals(Operation.Status.SUCC, read.status());
	}

	@Test
	void theBufferStopsAtTheScanBound() throws Exception {
		final var op = copy();
		handler.handleResponseStatus(op, HttpResponseStatus.OK.codeClass(), HttpResponseStatus.OK);
		final var big = " ".repeat(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES - 10);
		final ByteBuf first = chunk(big);
		handler.handleResponseContentChunk(channel, op, first);
		assertEquals(big.length(), first.readableBytes(), "the chunk is not consumed");
		handler.handleResponseContentChunk(channel, op, chunk(" ".repeat(100)));
		handler.handleResponseContentChunk(channel, op, chunk(" ".repeat(10_000)));
		final ByteBuf body = keptBody();
		assertEquals(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES, body.readableBytes());
		assertEquals(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES, body.capacity(), "the buffer never grows past the bound");
		assertEquals(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES, body.maxCapacity());
		handler.handleResponseContentFinish(channel, op);
		assertEquals(Operation.Status.SUCC, op.status());
	}

	@Test
	void theBufferIsReleasedWhenTheResponseFinishes() throws Exception {
		final var op = copy();
		handler.handleResponseStatus(op, HttpResponseStatus.OK.codeClass(), HttpResponseStatus.OK);
		handler.handleResponseContentChunk(channel, op, chunk(COPY_RESULT));
		final ByteBuf body = keptBody();
		assertTrue(S3ResponseHandler.heldCopyResponse(channel) != null);
		handler.handleResponseContentFinish(channel, op);
		assertEquals(null, S3ResponseHandler.heldCopyResponse(channel));
		assertEquals(0, body.refCnt());
		// Many copies on one channel: each finish releases its own buffer, so none is left.
		for (int i = 0; i < 100; i++) {
			assertEquals(Operation.Status.SUCC, copyStatus(COPY_RESULT));
		}
		channel.close().sync();
		assertEquals(null, S3ResponseHandler.heldCopyResponse(channel));
	}

	/**
	 * A body ended by the connection's close: the HTTP codec emits its last content when the channel
	 * goes inactive, after the close, so the close must leave the kept body to the finish.
	 */
	@Test
	void aBodyThatEndsAfterTheChannelClosesIsStillScanned() throws Exception {
		final var op = copy();
		handler.handleResponseStatus(op, HttpResponseStatus.OK.codeClass(), HttpResponseStatus.OK);
		handler.handleResponseContentChunk(channel, op, chunk("<Error><Code>Internal"));
		final ByteBuf body = keptBody();
		channel.close().sync();
		assertEquals(1, body.refCnt(), "the close leaves the body to the finish");
		handler.handleResponseContentChunk(channel, op, chunk("Error</Code></Error>"));
		handler.handleResponseContentFinish(channel, op);
		assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
		assertEquals(null, S3ResponseHandler.heldCopyResponse(channel));
		assertEquals(0, body.refCnt());
	}

	@Test
	void aCopyResponseOnAClosedChannelIsScanned() throws Exception {
		channel.close().sync();
		final var op = copy();
		respond(op, HttpResponseStatus.OK, "<Error><Code>AccessDenied</Code></Error>");
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
		assertEquals(null, S3ResponseHandler.heldCopyResponse(channel));
	}

	@Test
	void theStatusIsSetBeforeTheDriverCompletesTheCopy() throws Exception {
		final var op = copy();
		final var statusSeenByComplete = new AtomicReference<Operation.Status>();
		doAnswer(invocation -> {
			statusSeenByComplete.set(invocation.<Operation<DataItemImpl>> getArgument(1).status());
			return null;
		}).when(driver).complete(any(), any());
		respond(op, HttpResponseStatus.OK, "<Error><Code>InternalError</Code></Error>");
		verify(driver).complete(channel, op);
		assertEquals(Operation.Status.RESP_FAIL_SVC, statusSeenByComplete.get());
	}

	@Test
	void eachCopyErrorCodeIsLoggedOncePerStep() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			copyStatus("<Error><Code>InternalError</Code></Error>");
			copyStatus("<Error><Code>InternalError</Code></Error>");
			copyStatus("<Error><Code>X\n2026-11-16 | ERROR | forged</Code></Error>");
			final var lines = errors.copyErrorLines();
			assertEquals(2, lines.size(), lines::toString);
			assertTrue(lines.get(0).message().contains("S3 error code InternalError;")
							&& lines.get(0).message().contains(Operation.Status.RESP_FAIL_SVC.toString()), lines::toString);
			assertTrue(lines.get(1).message().contains("S3 error code (unreadable);")
							&& !lines.get(1).message().contains("forged"), lines::toString);
			assertTrue(errors.completionErrorLines().isEmpty(), "a copy is not logged as a completion");
		}
	}

	private Operation.Status copyStatus(final String... chunks) throws Exception {
		final var op = copy();
		respond(op, HttpResponseStatus.OK, chunks);
		return op.status();
	}

	private void respond(final Operation<DataItemImpl> op, final HttpResponseStatus status, final String... chunks)
					throws Exception {
		handler.handleResponseStatus(op, status.codeClass(), status);
		for (final var text : chunks) {
			handler.handleResponseContentChunk(channel, op, chunk(text));
		}
		handler.handleResponseContentFinish(channel, op);
	}

	private ByteBuf keptBody() {
		final ByteBuf body = S3ResponseHandler.heldCopyResponse(channel);
		assertTrue(body != null && body.refCnt() == 1, "the copy's body is kept");
		return body;
	}

	private static ByteBuf chunk(final String text) {
		return Unpooled.wrappedBuffer(text.getBytes(StandardCharsets.UTF_8));
	}

	/** A copy of {@code /src/k0} into {@code /dst}, as {@code item.input.path} makes one. */
	private static Operation<DataItemImpl> copy() {
		return flat(OpType.CREATE, "/src");
	}

	private static Operation<DataItemImpl> flat(final OpType type, final String srcPath) {
		return new DataOperationImpl<DataItemImpl>(
						0, type, new DataItemImpl("k0", 0L, 2048), srcPath, "/dst/", Credential.NONE, null, 0);
	}
}
