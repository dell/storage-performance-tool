package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that the S3 response handler propagates LIST metrics onto the operation prior to
 * completion.
 */
final class S3ResponseHandlerListTest {

	private S3ResponseHandler<PathItemImpl, Operation<PathItemImpl>> handler;
	private ByteBuf content;

	@BeforeEach
	void setUp() {
		handler = new S3ResponseHandler<>(null, false, false);
		content = Unpooled.copiedBuffer(LIST_V2_RESPONSE, StandardCharsets.UTF_8);
	}

	@AfterEach
	void tearDown() {
		content.release();
	}

	@Test
	void parseListResponsePopulatesOperationMetrics() throws Exception {
		final var op = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).build());

		final Method parseListResponse = S3ResponseHandler.class.getDeclaredMethod(
						"parseListResponse", ByteBuf.class, com.dell.spt.base.item.op.list.ListOperation.class);
		parseListResponse.setAccessible(true);
		final ByteBuf payload = content.retainedDuplicate();
		try {
			parseListResponse.invoke(handler, payload, op);
		} finally {
			payload.release();
		}

		assertEquals(2, op.objectsListed());
		assertEquals(579L, op.bytesListed());
		assertTrue(op.truncated());
		assertEquals("token-123", op.options().continuationToken());
		assertEquals("token-123", op.continuationToken());
	}

	private static final String LIST_V2_RESPONSE = "<ListBucketResult>"
					+ "<IsTruncated>true</IsTruncated>"
					+ "<Contents><Key>a.txt</Key><Size>123</Size></Contents>"
					+ "<Contents><Key>b.txt</Key><Size>456</Size></Contents>"
					+ "<NextContinuationToken>token-123</NextContinuationToken>"
					+ "</ListBucketResult>";
}
