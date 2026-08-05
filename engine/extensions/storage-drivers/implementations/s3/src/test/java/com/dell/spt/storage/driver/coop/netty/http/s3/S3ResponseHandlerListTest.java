package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Exercises the production LIST chunk/finish path with a concrete, metadata-enabled driver. */
final class S3ResponseHandlerListTest {

	private S3StorageDriverTest.TestS3Driver driver;
	private S3ResponseHandler<Item, Operation<Item>> handler;
	private ByteBuf content;

	@BeforeEach
	void setUp() throws Exception {
		driver = new S3StorageDriverTest.TestS3Driver(
						S3StorageDriverTest.metadataConfig(false));
		driver.suppressCompletionForResponseTest();
		handler = new S3ResponseHandler<>(driver, false, false, null);
		content = Unpooled.copiedBuffer(LIST_V2_RESPONSE, StandardCharsets.UTF_8);
	}

	@AfterEach
	void tearDown() throws Exception {
		content.release();
		driver.close();
	}

	@Test
	void productionPathPopulatesOperationMetrics() throws Exception {
		final var op = newOperation();
		final var channel = new EmbeddedChannel();
		final ByteBuf payload = content.retainedDuplicate();
		try {
			handler.handleResponseContentChunk(channel, asItemOperation(op), payload);
			op.status(Operation.Status.SUCC);
			handler.handleResponseContentFinish(channel, asItemOperation(op));

			assertEquals(2, op.objectsListed());
			assertEquals(579L, op.bytesListed());
			assertTrue(op.truncated());
			assertEquals("token-123", op.options().continuationToken());
			assertEquals("token-123", op.continuationToken());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
			payload.release();
		}
	}

	@Test
	void firstListContentChunkStartsDataResponseTiming() throws Exception {
		final var op = newOperation();
		final ByteBuf payload = content.retainedDuplicate();
		final var channel = new EmbeddedChannel();
		try {
			assertEquals(0L, op.respDataTimeStart());
			handler.handleResponseContentChunk(channel, asItemOperation(op), payload);
			assertTrue(op.respDataTimeStart() > 0, "LIST first-byte timestamp should be captured");
			op.status(Operation.Status.SUCC);
			handler.handleResponseContentFinish(channel, asItemOperation(op));
		} finally {
			channel.close();
			payload.release();
		}
	}

	@Test
	void malformedResponseIsStickyTerminalWithoutPartialMutation() throws Exception {
		final var op = newOperation();
		final ByteBuf malformed = Unpooled.copiedBuffer(
						"<ListBucketResult><Contents><Key>a</Key><Size>1</Size></Contents>",
						StandardCharsets.UTF_8);
		final var channel = new EmbeddedChannel();
		try {
			handler.handleResponseContentChunk(channel, asItemOperation(op), malformed);
			op.status(Operation.Status.SUCC);
			final IntegrityTerminalException failure = assertThrows(
							IntegrityTerminalException.class,
							() -> handler.handleResponseContentFinish(channel, asItemOperation(op)));
			assertRecordedTerminalFailure(failure);
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
			assertEquals(0, op.objectsListed());
			assertTrue(op.listedObjects() == null || op.listedObjects().isEmpty());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
			malformed.release();
		}
	}

	@Test
	void ordinaryModeRetainsWarningOnlyParserCompatibilityWithoutSpool() throws Exception {
		final var ordinaryDriver = new S3StorageDriverTest.TestS3Driver(
						S3StorageDriverTest.baseConfig(
										false, 4, false, null, "s3.us-east-1.amazonaws.com:443"));
		ordinaryDriver.suppressCompletionForResponseTest();
		final var ordinaryHandler = new S3ResponseHandler<Item, Operation<Item>>(
						ordinaryDriver, false, false, null);
		final var op = newOperation();
		final ByteBuf malformed = Unpooled.copiedBuffer(
						"<ListBucketResult><Contents><Key>a</Key></Contents></ListBucketResult>",
						StandardCharsets.UTF_8);
		final var channel = new EmbeddedChannel();
		try {
			ordinaryHandler.handleResponseContentChunk(channel, asItemOperation(op), malformed);
			op.status(Operation.Status.SUCC);
			assertDoesNotThrow(() -> ordinaryHandler.handleResponseContentFinish(
							channel, asItemOperation(op)));
			assertNull(ordinaryDriver.terminalFailure());
			assertEquals(Operation.Status.SUCC, op.status());
			assertEquals(0, op.objectsListed());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
			malformed.release();
			ordinaryDriver.close();
		}
	}

	@Test
	void buffersLargeIntegrityListAcrossArbitraryChunkBoundaries() throws Exception {
		final var xml = new StringBuilder("<ListBucketResult><IsTruncated>false</IsTruncated>");
		for (int i = 0; i < 2000; i++) {
			xml.append("<Contents><Key>prefix/key-").append(i)
							.append("</Key><Size>1</Size></Contents>");
		}
		xml.append("</ListBucketResult>");
		final byte[] bytes = xml.toString().getBytes(StandardCharsets.UTF_8);
		final var op = newOperation();
		final var channel = new EmbeddedChannel();
		try {
			for (int start = 0; start < bytes.length; start += 997) {
				final int length = Math.min(997, bytes.length - start);
				final ByteBuf chunk = Unpooled.wrappedBuffer(bytes, start, length);
				try {
					handler.handleResponseContentChunk(channel, asItemOperation(op), chunk);
				} finally {
					chunk.release();
				}
			}
			op.status(Operation.Status.SUCC);
			handler.handleResponseContentFinish(channel, asItemOperation(op));
			assertEquals(2000, op.objectsListed());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
		}
	}

	@Test
	void successfulEmptyIntegrityBodyIsStickyTerminalAndDoesNotLeakSpool() {
		final var op = newOperation();
		op.status(Operation.Status.SUCC);
		final var channel = new EmbeddedChannel();
		try {
			final IntegrityTerminalException failure = assertThrows(
							IntegrityTerminalException.class,
							() -> handler.handleResponseContentFinish(channel, asItemOperation(op)));
			assertRecordedTerminalFailure(failure);
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
		}
	}

	@Test
	void oversizedIntegrityListIsStickyTerminalAndDeletesSpool() throws Exception {
		final var op = newOperation();
		final byte[] mebibyte = new byte[1024 * 1024];
		final var channel = new EmbeddedChannel();
		try {
			for (int i = 0; i < 16; i++) {
				final ByteBuf chunk = Unpooled.wrappedBuffer(mebibyte);
				try {
					handler.handleResponseContentChunk(channel, asItemOperation(op), chunk);
				} finally {
					chunk.release();
				}
			}
			final ByteBuf overflow = Unpooled.wrappedBuffer(new byte[]{1
			});
			try {
				final IntegrityTerminalException failure = assertThrows(
								IntegrityTerminalException.class,
								() -> handler.handleResponseContentChunk(
												channel, asItemOperation(op), overflow));
				assertRecordedTerminalFailure(failure);
				assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
				assertEquals(0, S3ResponseHandler.activeListSpoolCount());
			} finally {
				overflow.release();
			}
		} finally {
			channel.close();
		}
	}

	private static ListOperationImpl<PathItemImpl> newOperation() {
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).build());
		return op;
	}

	private void assertRecordedTerminalFailure(final IntegrityTerminalException thrown) {
		final IntegrityTerminalException recorded = driver.terminalFailure();
		assertTrue(recorded != null);
		assertEquals(thrown.category(), recorded.category());
		assertEquals(thrown.getMessage(), recorded.getMessage());
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> asItemOperation(
					final ListOperationImpl<PathItemImpl> operation) {
		return (Operation<Item>) (Operation<?>) operation;
	}

	private static final String LIST_V2_RESPONSE = "<ListBucketResult>"
					+ "<IsTruncated>true</IsTruncated>"
					+ "<Contents><Key>a.txt</Key><Size>123</Size></Contents>"
					+ "<Contents><Key>b.txt</Key><Size>456</Size></Contents>"
					+ "<NextContinuationToken>token-123</NextContinuationToken>"
					+ "</ListBucketResult>";
}
