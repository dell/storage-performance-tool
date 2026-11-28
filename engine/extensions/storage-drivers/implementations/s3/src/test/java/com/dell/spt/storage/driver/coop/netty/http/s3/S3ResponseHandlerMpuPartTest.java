package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.Credential;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Regression tests covering response-header handling on error responses, where the
 * server omits headers the success path would normally provide (ETag for MPU parts,
 * x-amz-version-id when versioning is enabled). Both branches must tolerate the
 * missing headers without throwing or silently corrupting operation state.
 */
final class S3ResponseHandlerMpuPartTest {

	private static final long PART_SIZE = 10L * 1024 * 1024;
	private static final long ITEM_SIZE = 20L * 1024 * 1024;

	@Test
	void handleResponseHeadersDoesNotThrowWhenPartResponseHasNoETag() {
		final var handler = new S3ResponseHandler<DataItemImpl, PartialDataOperationImpl<DataItemImpl>>(
						null, false, false, null);

		final var partItem = new DataItemImpl(0L, PART_SIZE);
		final var parentItem = new DataItemImpl(0L, ITEM_SIZE);
		final var parent = new CompositeDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, parentItem, null, "/streaming/20MB/63/", Credential.NONE,
						null, 0, PART_SIZE);
		final var partOp = new PartialDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, partItem, null, "/streaming/20MB/63/", Credential.NONE,
						0, parent);

		final HttpHeaders emptyHeaders = new DefaultHttpHeaders();

		assertDoesNotThrow(() -> handler.handleResponseHeaders(null, partOp, emptyHeaders));
	}

	/** A 2xx part reply whose ETag header is missing, empty or blank fails the part as not found and stores nothing. */
	@Test
	void aSuccessfulPartReplyWithoutAnETagFailsThePartAsNotFound() throws Exception {
		for (final String eTag : new String[]{null, "", "   "
		}) {
			final var partOp = part();
			partOp.status(Operation.Status.SUCC);
			// unvalidated, so a blank value gets in as a decoder that did not trim it would pass it
			@SuppressWarnings("deprecation")
			final HttpHeaders headers = new DefaultHttpHeaders(false);
			if (eTag != null) {
				headers.set(HttpHeaderNames.ETAG, eTag);
			}
			final List<ErrorsLogCapture.Line> lines;
			try (final var errors = new ErrorsLogCapture()) {
				handler().handleResponseHeaders(null, partOp, headers);
				lines = errors.drained();
			}

			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, partOp.status(), "ETag " + eTag);
			assertNull(partOp.parent().get("1"), "ETag " + eTag);
			assertEquals(1, eTagLines(lines).size(), lines::toString);
			assertTrue(eTagLines(lines).get(0).contains("(part number 1, ETag header: " + (eTag == null ? "(missing)" : "(empty)") + ")"),
							lines::toString);
		}
	}

	/** Quoted ETags, even of nothing or of spaces, and weak ones are stored as sent, as before. */
	@Test
	void aQuotedOrWeakPartETagIsStoredAsSent() throws Exception {
		for (final String eTag : new String[]{"\"\"", "\"   \"", "W/\"\"", "\"abc\""
		}) {
			final var partOp = part();
			partOp.status(Operation.Status.SUCC);
			final List<ErrorsLogCapture.Line> lines;
			try (final var errors = new ErrorsLogCapture()) {
				handler().handleResponseHeaders(null, partOp, new DefaultHttpHeaders().set(HttpHeaderNames.ETAG, eTag));
				lines = errors.drained();
			}

			assertEquals(Operation.Status.SUCC, partOp.status(), eTag);
			assertEquals(eTag, partOp.parent().get("1"));
			assertEquals(List.of(), eTagLines(lines));
		}
	}

	/** A part not yet answered with a status is left as it is, and nothing is logged. */
	@Test
	void aPendingPartWithoutAnETagIsLeftAsItIs() throws Exception {
		final var partOp = part();
		final List<String> lines;
		try (final var errors = new ErrorsLogCapture()) {
			handler().handleResponseHeaders(null, partOp, new DefaultHttpHeaders());
			lines = errors.drained().stream().map(ErrorsLogCapture.Line::message).collect(Collectors.toList());
			lines.addAll(errors.drainedDebugMessages());
		}

		assertEquals(Operation.Status.PENDING, partOp.status());
		assertEquals(List.of(), lines);
	}

	/** A refused part reply without an ETag keeps the status of its status line, so the part is retried as before. */
	@Test
	void aRefusedPartReplyWithoutAnETagKeepsItsStatus() throws Exception {
		for (final var status : new HttpResponseStatus[]{HttpResponseStatus.SERVICE_UNAVAILABLE, HttpResponseStatus.FORBIDDEN
		}) {
			final var handler = handler();
			final var partOp = part();
			final List<ErrorsLogCapture.Line> lines;
			try (final var errors = new ErrorsLogCapture()) {
				handler.handleResponseStatus(partOp, status.codeClass(), status);
				handler.handleResponseHeaders(null, partOp, new DefaultHttpHeaders());
				lines = errors.drained();
			}

			assertEquals(status == HttpResponseStatus.FORBIDDEN ? Operation.Status.RESP_FAIL_AUTH : Operation.Status.RESP_FAIL_SVC,
							partOp.status());
			assertEquals(List.of(), eTagLines(lines));
		}
	}

	private static S3ResponseHandler<DataItemImpl, PartialDataOperationImpl<DataItemImpl>> handler() {
		return new S3ResponseHandler<>(null, false, false, null);
	}

	private static PartialDataOperationImpl<DataItemImpl> part() {
		final var parent = new CompositeDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, new DataItemImpl(0L, ITEM_SIZE), null, "/bucket/", Credential.NONE, null, 0, PART_SIZE);
		return new PartialDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, new DataItemImpl(0L, PART_SIZE), null, "/bucket/", Credential.NONE, 0, parent);
	}

	private static List<String> eTagLines(final List<ErrorsLogCapture.Line> lines) {
		return lines.stream().map(ErrorsLogCapture.Line::message)
						.filter(message -> message.contains("ETag not found in the multipart part upload response")).toList();
	}

	@Test
	void handleResponseHeadersDoesNotCorruptItemNameWhenVersionHeaderMissing() {
		final var handler = new S3ResponseHandler<DataItemImpl, DataOperationImpl<DataItemImpl>>(
						null, false, true, null);

		final var item = new DataItemImpl("foo", 0L, ITEM_SIZE);
		final var op = new DataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, item, null, "/bucket/", Credential.NONE, null, 0);

		final HttpHeaders emptyHeaders = new DefaultHttpHeaders();

		handler.handleResponseHeaders(null, op, emptyHeaders);

		assertEquals("foo", item.name());
	}
}
