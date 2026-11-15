package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.storage.Credential;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A multipart completion that S3 answers {@code 200 OK} with an {@code <Error>} document failed: its
 * status is set before the driver's {@code complete()}, whose failure branch aborts the upload. Every
 * other multipart response keeps the status of its HTTP class.
 */
final class S3ResponseHandlerCompletionTest {

	private static final long PART_SIZE = 1024;
	private static final String DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

	@SuppressWarnings("unchecked")
	private final S3StorageDriver<DataItemImpl, CompositeDataOperationImpl<DataItemImpl>> driver = mock(S3StorageDriver.class);
	private final S3ResponseHandler<DataItemImpl, CompositeDataOperationImpl<DataItemImpl>> handler = new S3ResponseHandler<>(driver, false, false, null);
	private final EmbeddedChannel channel = new EmbeddedChannel();

	@BeforeEach
	@AfterEach
	void resetLog() {
		S3ResponseHandler.resetCompletionErrorCodeLogForTest();
	}

	@Test
	void anErrorBodyFailsTheCompletion() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_SVC,
						completionStatus("<Error><Code>InternalError</Code><Message>m</Message></Error>"));
	}

	@Test
	void anErrorBodyAfterADeclarationAndWhitespaceFailsTheCompletion() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_SVC, completionStatus(
						"\n  \r\n\t" + DECLARATION + "\n\n<Error><Code>InternalError</Code></Error>"));
	}

	@Test
	void aNamespacedErrorRootFailsTheCompletion() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, completionStatus(
						DECLARATION + "<Error xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Code>AccessDenied</Code></Error>"));
	}

	@Test
	void keepAliveWhitespaceInItsOwnChunkThenAnErrorFailsTheCompletion() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_SVC,
						completionStatus(" ".repeat(300), "\n", "<Error><Code>SlowDown</Code></Error>"));
	}

	@Test
	void eachTransientCodeIsRetryable() throws Exception {
		for (final var code : S3ResponseHandler.TRANSIENT_COMPLETION_ERROR_CODES) {
			assertEquals(Operation.Status.RESP_FAIL_SVC,
							completionStatus("<Error><Code>" + code + "</Code></Error>"), code);
		}
	}

	@Test
	void aPermanentCodeIsNotRetryable() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_CLIENT,
						completionStatus("<Error><Code>InvalidPart</Code><Message>m</Message></Error>"));
		assertEquals(Operation.Status.RESP_FAIL_CLIENT,
						completionStatus("<Error>\n  <Code> AccessDenied </Code>\n</Error>"));
	}

	@Test
	void anUnreadableCodeIsRetryable() throws Exception {
		assertEquals(Operation.Status.RESP_FAIL_SVC, completionStatus("<Error><Message>no code</Message></Error>"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, completionStatus("<Error><Code></Code></Error>"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, completionStatus("<Error><Code>InvalidPa"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, completionStatus("<Error/>"));
	}

	@Test
	void aCodeBeyondTheBoundIsUnreadable() throws Exception {
		final var padding = "<Message>" + "x".repeat(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES) + "</Message>";
		assertEquals(Operation.Status.RESP_FAIL_SVC,
						completionStatus("<Error>" + padding + "<Code>InvalidPart</Code></Error>"));
	}

	@Test
	void anErrorRootBeyondTheBoundIsNotRead() throws Exception {
		assertEquals(Operation.Status.SUCC, completionStatus(
						" ".repeat(S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES), "<Error><Code>InternalError</Code></Error>"));
	}

	@Test
	void aNormalResultStaysSuccessful() throws Exception {
		assertEquals(Operation.Status.SUCC, completionStatus(DECLARATION
						+ "\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
						+ "<Location>http://bucket.s3.amazonaws.com/Error</Location><Bucket>bucket</Bucket>"
						+ "<Key>&lt;Error&gt;Error</Key><ETag>\"3858f62230ac3c915f300c664312c11f-9\"</ETag>"
						+ "</CompleteMultipartUploadResult>"));
	}

	@Test
	void anotherRootWhoseNameStartsWithErrorStaysSuccessful() throws Exception {
		assertEquals(Operation.Status.SUCC, completionStatus("<ErrorReport><Code>InternalError</Code></ErrorReport>"));
	}

	/** Known limit: the check is negative, so an empty or whitespace-only 200 still counts as success. */
	@Test
	void aWhitespaceOnlyBodyStaysSuccessful() throws Exception {
		assertEquals(Operation.Status.SUCC, completionStatus("   \n  "));
		assertEquals(Operation.Status.SUCC, completionStatus(DECLARATION));
		assertEquals(Operation.Status.SUCC, completionStatus("<?xml version=\"1.0\" <Error>"));
	}

	@Test
	void aRefusedCompletionKeepsItsStatus() throws Exception {
		final var op = completion();
		op.status(Operation.Status.RESP_FAIL_AUTH);
		finish(op, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.RESP_FAIL_AUTH, op.status());
	}

	@Test
	void anInitResponseIsUnaffected() throws Exception {
		final var op = upload();
		op.status(Operation.Status.SUCC);
		finish(op, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.SUCC, op.status());
		assertNull(channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).get());

		final var initiated = upload();
		initiated.status(Operation.Status.SUCC);
		finish(initiated, "<InitiateMultipartUploadResult><UploadId>u-1</UploadId></InitiateMultipartUploadResult>");
		assertEquals(Operation.Status.SUCC, initiated.status());
		assertEquals("u-1", channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).get());
	}

	@Test
	void anAbortResponseIsUnaffected() throws Exception {
		final var op = completion();
		op.put(S3Api.KEY_MPU_ABORT, "true");
		op.status(Operation.Status.SUCC);
		finish(op, "<Error><Code>InternalError</Code></Error>");
		assertEquals(Operation.Status.SUCC, op.status());
	}

	@Test
	void theStatusIsSetBeforeTheDriverCompletesTheUpload() throws Exception {
		final var op = completion();
		op.status(Operation.Status.SUCC);
		finish(op, "<Error><Code>InternalError</Code></Error>");
		verify(driver).complete(channel, op);
		assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
	}

	private Operation.Status completionStatus(final String... chunks) throws Exception {
		final var op = completion();
		op.status(Operation.Status.SUCC);
		finish(op, chunks);
		return op.status();
	}

	private void finish(final CompositeDataOperationImpl<DataItemImpl> op, final String... chunks) throws Exception {
		for (final var chunk : chunks) {
			handler.handleResponseContentChunk(channel, op,
							Unpooled.wrappedBuffer(chunk.getBytes(StandardCharsets.UTF_8)));
		}
		handler.handleResponseContentFinish(channel, op);
	}

	/** An upload whose parts are not done yet, so its next response is its initiation's. */
	private static CompositeDataOperationImpl<DataItemImpl> upload() {
		final var op = new CompositeDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, new DataItemImpl(0L, 4 * PART_SIZE), null, "/bucket/", Credential.NONE,
						null, 0, PART_SIZE);
		op.subOperations();
		return op;
	}

	/** An upload whose parts are all done, so its next response is its completion's. */
	private static CompositeDataOperationImpl<DataItemImpl> completion() {
		final var op = upload();
		for (int i = 0; i < op.subOperations().size(); i++) {
			op.markSubTaskCompleted();
		}
		op.put(S3Api.KEY_UPLOAD_ID, "u-1");
		return op;
	}
}
