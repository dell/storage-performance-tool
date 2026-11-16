package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.Credential;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A response the target refuses logs, after its status line, one {@code errors.log} line with the S3
 * {@code <Code>} and {@code <Message>} of its body, both capped and made safe for one log line. The
 * operation's status and byte count stay those of the response; a successful response keeps nothing
 * of its body and logs nothing new.
 */
final class S3ResponseHandlerFailedResponseTest {

	private static final long PART_SIZE = 1024;
	private static final String ENTITY_TOO_SMALL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
					+ "<Error><Code>EntityTooSmall</Code>"
					+ "<Message>Your proposed upload is smaller than the minimum allowed object size.</Message>"
					+ "<RequestId>r-1</RequestId></Error>";

	@SuppressWarnings("rawtypes")
	private final S3StorageDriver driver = mock(S3StorageDriver.class);
	@SuppressWarnings("unchecked")
	private final S3ResponseHandler<DataItemImpl, Operation<DataItemImpl>> handler = new S3ResponseHandler<>(driver, false, false, null);
	private final EmbeddedChannel channel = new EmbeddedChannel();

	@BeforeEach
	void stepLog() {
		when(driver.completionErrorCodeLog()).thenReturn(new S3ResponseHandler.CompletionErrorCodeLog());
	}

	@Test
	void aRefusedCompletionLogsItsCodeAndMessage() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = completion();
			respond(op, HttpResponseStatus.BAD_REQUEST, ENTITY_TOO_SMALL);
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
			verify(driver).complete(channel, op);
			final var all = errors.drained().stream().map(ErrorsLogCapture.Line::message).toList();
			assertTrue(all.contains(op + ": 400 Bad Request"), all::toString);
			assertEquals(List.of(op + ": S3 error code EntityTooSmall, message: Your proposed upload is smaller than"
							+ " the minimum allowed object size."), messages(errors), all::toString);
		}
	}

	@Test
	void aRefusedPutLogsItsCodeAndMessage() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			// The body arrives in two chunks that split the code.
			respond(op, HttpResponseStatus.FORBIDDEN, "<Error><Code>Access", "Denied</Code><Message>Access Denied</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_AUTH, op.status());
			assertEquals(0, bytes(op));
			verify(driver).complete(channel, op);
			assertEquals(List.of(op + ": S3 error code AccessDenied, message: Access Denied"), messages(errors));
		}
	}

	@Test
	void aRefusedGetLogsItsCodeAndCountsItsBodyAsBefore() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.READ);
			final var body = "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
			respond(op, HttpResponseStatus.NOT_FOUND, body);
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, op.status());
			assertEquals(body.length(), bytes(op), "the error body is counted as it was");
			assertEquals(List.of(op + ": S3 error code NoSuchKey, message: The specified key does not exist."), messages(errors));
		}
	}

	@Test
	void aLongMessageIsCut() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			respond(op, HttpResponseStatus.BAD_REQUEST, "<Error><Code>InvalidArgument</Code><Message>"
							+ "m".repeat(S3ResponseHandler.MAX_LOGGED_ERROR_MESSAGE_LENGTH + 1) + "</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
			assertEquals(List.of(op + ": S3 error code InvalidArgument, message: "
							+ "m".repeat(S3ResponseHandler.MAX_LOGGED_ERROR_MESSAGE_LENGTH) + S3ResponseHandler.CUT_MARK),
							messages(errors));
		}
	}

	/** Only the body's first bytes are kept: a message they cut ends with the cut mark, a code past them is missing. */
	@Test
	void aBodyBeyondTheBoundIsNotKept() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			final var prefix = "<Error><Message>";
			final var kept = S3ResponseHandler.FAILED_RESPONSE_SCAN_BYTES - prefix.length();
			respond(op, HttpResponseStatus.BAD_REQUEST, prefix + " ".repeat(kept - 5) + "short",
							" more text</Message><Code>InvalidArgument</Code></Error>", "x".repeat(64 * 1024));
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
			assertEquals(List.of(op + ": S3 error code (missing), message: short" + S3ResponseHandler.CUT_MARK), messages(errors));
		}
	}

	@Test
	void aBodyWithoutACodeSaysSo() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			respond(op, HttpResponseStatus.SERVICE_UNAVAILABLE, "<Error><Message>Please reduce your request rate.</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
			assertEquals(List.of(op + ": S3 error code (missing), message: Please reduce your request rate."), messages(errors));
		}
	}

	@Test
	void aBodyThatIsNotXmlSaysNeitherWasFound() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.READ);
			final var body = "<html><body>502 Bad Gateway\r\nfrom the proxy</body></html>";
			respond(op, HttpResponseStatus.BAD_GATEWAY, body);
			assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
			assertEquals(body.length(), bytes(op));
			assertEquals(List.of(op + ": S3 error code (missing), message: (missing)"), messages(errors));
		}
	}

	/** The target's text cannot break or forge a log line. */
	@Test
	void controlCharactersCannotBreakTheLine() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			respond(op, HttpResponseStatus.BAD_REQUEST, "<Error><Code>X\n2026-11-15 | ERROR | forged</Code>"
							+ "<Message>first\r\n2026-11-15 | ERROR | forged \u0085\tend</Message></Error>");
			final var lines = messages(errors);
			assertEquals(1, lines.size(), lines::toString);
			final var line = lines.get(0);
			assertTrue(line.startsWith(op + ": S3 error code (unreadable), message: first"), line);
			assertTrue(line.endsWith("end"), line);
			assertTrue(line.chars().noneMatch(c -> Character.isISOControl(c) || c == ' '), line);
		}
	}

	@Test
	void aFailedResponseWithoutABodyLogsNothingNew() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.READ);
			respond(op, HttpResponseStatus.NOT_FOUND);
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, op.status());
			assertEquals(List.of(), messages(errors));
			assertFalse(S3ResponseHandler.keptFailedResponse(channel), "no body is kept for a refusal without one");
		}
	}

	@Test
	void aRefusedPartLogsItsCode() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var parent = (CompositeDataOperationImpl<DataItemImpl>) completion();
			final Operation<DataItemImpl> part = new PartialDataOperationImpl<DataItemImpl>(
							0, OpType.CREATE, new DataItemImpl(0L, PART_SIZE), null, "/bucket/", Credential.NONE, 0, parent);
			respond(part, HttpResponseStatus.SERVICE_UNAVAILABLE,
							"<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_SVC, part.status());
			assertEquals(List.of(part + ": S3 error code SlowDown, message: Please reduce your request rate."), messages(errors));
		}
	}

	/**
	 * A refused initiation logs its status line and its code line only. A further line used to dump
	 * the raw body as "Upload id not found"; the code line now says all of it. The driver would grant
	 * the once-per-step warning, so a wrongly taken upload-ID branch shows at WARN.
	 */
	@Test
	void aRefusedInitiationLogsOnlyItsStatusAndCode() throws Exception {
		when(driver.claimUploadIdNotFoundWarning()).thenReturn(true);
		try (final var errors = new ErrorsLogCapture()) {
			final var op = upload();
			final var body = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
			respond(op, HttpResponseStatus.FORBIDDEN, body);
			assertEquals(Operation.Status.RESP_FAIL_AUTH, op.status());
			verify(driver).complete(channel, op);
			assertEquals(List.of(op + ": 403 Forbidden", op + ": S3 error code AccessDenied, message: Access Denied"),
							errors.drained().stream().map(ErrorsLogCapture.Line::message).toList());
		}
	}

	/** An abort sent before every part is done is not an initiation: refused, it logs no upload-id line. */
	@Test
	void aRefusedAbortBeforeItsPartsAreDoneLogsOnlyItsStatusAndCode() throws Exception {
		when(driver.claimUploadIdNotFoundWarning()).thenReturn(true);
		try (final var errors = new ErrorsLogCapture()) {
			final var op = upload();
			((CompositeDataOperationImpl<DataItemImpl>) op).put(S3Api.KEY_MPU_ABORT, "true");
			respond(op, HttpResponseStatus.NOT_FOUND,
							"<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, op.status());
			assertEquals(List.of(op + ": 404 Not Found",
							op + ": S3 error code NoSuchUpload, message: The specified upload does not exist."),
							errors.drained().stream().map(ErrorsLogCapture.Line::message).toList());
		}
	}

	/** An abort answered 2xx with a body before every part is done is not an initiation: no upload-ID line. */
	@Test
	void aSuccessfulAbortWithABodyBeforeItsPartsAreDoneLogsNoUploadIdLine() throws Exception {
		when(driver.claimUploadIdNotFoundWarning()).thenReturn(true);
		try (final var errors = new ErrorsLogCapture()) {
			final var op = upload();
			((CompositeDataOperationImpl<DataItemImpl>) op).put(S3Api.KEY_MPU_ABORT, "true");
			respond(op, HttpResponseStatus.OK, "<html>ok</html>");
			assertEquals(List.of(), uploadIdLines(errors));
		}
	}

	/** A ranged update of a large item is a composite operation too, but never an initiation: no upload-ID line. */
	@Test
	void aSuccessfulCompositeUpdateWithABodyLogsNoUploadIdLine() throws Exception {
		when(driver.claimUploadIdNotFoundWarning()).thenReturn(true);
		try (final var errors = new ErrorsLogCapture()) {
			final var op = new CompositeDataOperationImpl<DataItemImpl>(
							0, OpType.UPDATE, new DataItemImpl(0L, 4 * PART_SIZE), null, "/bucket/", Credential.NONE,
							null, 0, PART_SIZE);
			op.subOperations();
			respond(op, HttpResponseStatus.OK, "<html>ok</html>");
			assertEquals(List.of(), uploadIdLines(errors));
		}
	}

	@Test
	void aRefusedAbortLogsItsCode() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = completion();
			((CompositeDataOperationImpl<DataItemImpl>) op).put(S3Api.KEY_MPU_ABORT, "true");
			respond(op, HttpResponseStatus.NOT_FOUND,
							"<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, op.status());
			assertEquals(List.of(op + ": S3 error code NoSuchUpload, message: The specified upload does not exist."),
							messages(errors));
		}
	}

	/**
	 * With verification on, a refused read's body is not verified as data: the read keeps the status of
	 * its HTTP status line and counts no bytes, and its whole error body is still kept and its code logged.
	 */
	@Test
	void aRefusedVerifiedReadLogsTheCodeOfABodyInTwoChunks() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = smallRead();
			respondVerified(op, HttpResponseStatus.NOT_FOUND, "<Error><Code>NoSuch", "Key</Code><Message>gone</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, op.status());
			assertEquals(0, bytes(op), "a refused verified read counts no bytes");
			assertEquals(List.of(op + ": S3 error code NoSuchKey, message: gone"), messages(errors));
		}
	}

	/** A 2xx read whose data fails verification is not an S3 error: nothing is kept or logged as one. */
	@Test
	void aSuccessfulReadThatFailsVerificationLogsNoS3Error() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = smallRead();
			respondVerified(op, HttpResponseStatus.OK, "<Error><Code>First", "Chunk</Code><Message>m</Message></Error>");
			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
			assertEquals(List.of(), messages(errors));
			assertFalse(S3ResponseHandler.keptFailedResponse(channel), "no body is kept for a 2xx response");
		}
	}

	@Test
	void aMessageCutInsideASurrogatePairEndsWithTheCutMark() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			final var head = "m".repeat(S3ResponseHandler.MAX_LOGGED_ERROR_MESSAGE_LENGTH - 1);
			respond(op, HttpResponseStatus.BAD_REQUEST, "<Error><Code>InvalidArgument</Code><Message>" + head
							+ "\uD83D\uDE00tail</Message></Error>");
			assertEquals(List.of(op + ": S3 error code InvalidArgument, message: " + head + S3ResponseHandler.CUT_MARK),
							messages(errors));
		}
	}

	@Test
	void formatCharactersAreReplaced() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = flat(OpType.CREATE);
			respond(op, HttpResponseStatus.BAD_REQUEST,
							"<Error><Code>InvalidArgument</Code><Message>left\u202Eright\u200Bend</Message></Error>");
			assertEquals(List.of(op + ": S3 error code InvalidArgument, message: left right end"), messages(errors));
		}
	}

	/** Each failed response on a channel logs only its own body. */
	@Test
	void consecutiveFailuresOnOneChannelLogTheirOwnBodies() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var first = flat(OpType.CREATE);
			respond(first, HttpResponseStatus.BAD_REQUEST, "<Error><Code>First</Code><Message>one</Message></Error>");
			final var second = flat(OpType.CREATE);
			respond(second, HttpResponseStatus.BAD_REQUEST, "<Error><Code>Second</Code><Message>two</Message></Error>");
			assertEquals(List.of(first + ": S3 error code First, message: one", second + ": S3 error code Second, message: two"),
							messages(errors));
		}
	}

	/** A successful response keeps nothing of its body, leaves its chunks unread and logs nothing. */
	@Test
	void aSuccessKeepsNothingAndLogsNothing() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var put = flat(OpType.CREATE);
			final var read = flat(OpType.READ);
			final var chunk = Unpooled.wrappedBuffer(ENTITY_TOO_SMALL.getBytes(StandardCharsets.UTF_8));
			handler.handleResponseStatus(put, HttpResponseStatus.OK.codeClass(), HttpResponseStatus.OK);
			handler.handleResponseContentChunk(channel, put, chunk);
			assertEquals(0, chunk.readerIndex());
			handler.handleResponseContentFinish(channel, put);
			respond(read, HttpResponseStatus.OK, ENTITY_TOO_SMALL);
			assertEquals(Operation.Status.SUCC, put.status());
			assertEquals(Operation.Status.SUCC, read.status());
			assertEquals(ENTITY_TOO_SMALL.length(), bytes(read));
			assertFalse(S3ResponseHandler.keptFailedResponse(channel), "no body is kept for a successful response");
			assertEquals(List.of(), errors.drained());
		}
	}

	/** #329's check still fails a completion answered 200 with an error document, with its own line only. */
	@Test
	void aCompletionAnswered200WithAnErrorKeepsItsOwnHandling() throws Exception {
		try (final var errors = new ErrorsLogCapture()) {
			final var op = completion();
			respond(op, HttpResponseStatus.OK, "<Error><Code>InternalError</Code></Error>");
			assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
			assertEquals(1, errors.completionErrorLines().size());
			assertEquals(List.of(), messages(errors));
		}
	}

	private void respond(final Operation<DataItemImpl> op, final HttpResponseStatus status, final String... chunks)
					throws Exception {
		handler.handleResponseStatus(op, status.codeClass(), status);
		for (final var chunk : chunks) {
			handler.handleResponseContentChunk(channel, op, Unpooled.wrappedBuffer(chunk.getBytes(StandardCharsets.UTF_8)));
		}
		handler.handleResponseContentFinish(channel, op);
	}

	private static long bytes(final Operation<DataItemImpl> op) {
		return ((DataOperationImpl<DataItemImpl>) op).countBytesDone();
	}

	/** Answers {@code op} through a handler that verifies read data, as {@code item.data.verify} does. */
	private void respondVerified(final Operation<DataItemImpl> op, final HttpResponseStatus status, final String... chunks)
					throws Exception {
		final var verifying = new S3ResponseHandler<DataItemImpl, Operation<DataItemImpl>>(driver, true, false, null);
		verifying.handleResponseStatus(op, status.codeClass(), status);
		for (final var chunk : chunks) {
			verifying.handleResponseContentChunk(channel, op, Unpooled.wrappedBuffer(chunk.getBytes(StandardCharsets.UTF_8)));
		}
		verifying.handleResponseContentFinish(channel, op);
	}

	/** A read of a 4-byte item, whose verification fails on any longer body by its size. */
	private static Operation<DataItemImpl> smallRead() {
		return new DataOperationImpl<DataItemImpl>(
						0, OpType.READ, new DataItemImpl("k-small", 0L, 4), null, "/bucket/", Credential.NONE, null, 0);
	}

	/** Every upload-ID line, at WARN or at DEBUG. */
	private static List<String> uploadIdLines(final ErrorsLogCapture errors) throws InterruptedException {
		final var lines = new java.util.ArrayList<String>();
		errors.drained().stream().map(ErrorsLogCapture.Line::message).forEach(lines::add);
		lines.addAll(errors.drainedDebugMessages());
		return lines.stream().filter(message -> message.contains("Upload id not found")).toList();
	}

	private static List<String> messages(final ErrorsLogCapture errors) throws InterruptedException {
		return errors.failedResponseLines().stream().map(ErrorsLogCapture.Line::message).toList();
	}

	private static Operation<DataItemImpl> flat(final OpType type) {
		return new DataOperationImpl<DataItemImpl>(
						0, type, new DataItemImpl("k-" + type, 0L, 4 * PART_SIZE), null, "/bucket/", Credential.NONE, null, 0);
	}

	/** An upload whose parts are not done yet, so its next response is its initiation's. */
	private static Operation<DataItemImpl> upload() {
		final var op = new CompositeDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, new DataItemImpl(0L, 4 * PART_SIZE), null, "/bucket/", Credential.NONE,
						null, 0, PART_SIZE);
		op.subOperations();
		return op;
	}

	/** An upload whose parts are all done, so its next response is its completion's. */
	private static Operation<DataItemImpl> completion() {
		final var op = (CompositeDataOperationImpl<DataItemImpl>) upload();
		for (int i = 0; i < op.subOperations().size(); i++) {
			op.markSubTaskCompleted();
		}
		op.put(S3Api.KEY_UPLOAD_ID, "u-1");
		return op;
	}
}
