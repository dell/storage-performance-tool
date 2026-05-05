package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.Credential;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

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
