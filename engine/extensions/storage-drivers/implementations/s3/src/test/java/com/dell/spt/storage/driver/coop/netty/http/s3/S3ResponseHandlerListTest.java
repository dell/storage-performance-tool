package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
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
		handler = new S3ResponseHandler<>(null, false, false, null);
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

	@Test
	void firstListContentChunkStartsDataResponseTiming() throws Exception {
		final var op = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);

		final ByteBuf payload = content.retainedDuplicate();
		final var channel = new EmbeddedChannel();
		try {
			assertEquals(0L, op.respDataTimeStart());

			handler.handleResponseContentChunk(channel, op, payload);

			assertTrue(op.respDataTimeStart() > 0, "LIST first-byte timestamp should be captured");
			op.options(ListOptions.builder().fetchMetadata(true).build());
			op.status(Operation.Status.SUCC);
			final Method finish = S3ResponseHandler.class.getDeclaredMethod(
							"finishListResponse", io.netty.channel.Channel.class,
							com.dell.spt.base.item.op.list.ListOperation.class);
			finish.setAccessible(true);
			finish.invoke(handler, channel, op);
		} finally {
			channel.close();
			payload.release();
		}
	}

	@Test
	void malformedResponseAfterPartialObjectIsTerminal() throws Exception {
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).build());
		final Method parse = S3ResponseHandler.class.getDeclaredMethod(
						"parseListResponse", ByteBuf.class, com.dell.spt.base.item.op.list.ListOperation.class);
		parse.setAccessible(true);
		final ByteBuf malformed = Unpooled.copiedBuffer(
						"<ListBucketResult><Contents><Key>a</Key><Size>1</Size></Contents>",
						StandardCharsets.UTF_8);
		try {
			final var wrapped = assertThrows(
							InvocationTargetException.class, () -> parse.invoke(handler, malformed, op));
			assertInstanceOf(IntegrityTerminalException.class, wrapped.getCause());
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
		} finally {
			malformed.release();
		}
	}

	@Test
	void buffersLargeListAcrossArbitraryChunkBoundaries() throws Exception {
		final var xml = new StringBuilder("<ListBucketResult><IsTruncated>false</IsTruncated>");
		for (int i = 0; i < 2000; i++) {
			xml.append("<Contents><Key>prefix/key-").append(i)
							.append("</Key><Size>1</Size></Contents>");
		}
		xml.append("</ListBucketResult>");
		final byte[] bytes = xml.toString().getBytes(StandardCharsets.UTF_8);
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).build());
		final Method buffer = S3ResponseHandler.class.getDeclaredMethod(
						"bufferListContent", io.netty.channel.Channel.class,
						com.dell.spt.base.item.op.list.ListOperation.class, ByteBuf.class);
		buffer.setAccessible(true);
		final var channel = new EmbeddedChannel();
		for (int start = 0; start < bytes.length; start += 997) {
			final int length = Math.min(997, bytes.length - start);
			final ByteBuf chunk = Unpooled.wrappedBuffer(bytes, start, length);
			try {
				buffer.invoke(handler, channel, op, chunk);
			} finally {
				chunk.release();
			}
		}
		op.status(Operation.Status.SUCC);
		final Method finish = S3ResponseHandler.class.getDeclaredMethod(
						"finishListResponse", io.netty.channel.Channel.class,
						com.dell.spt.base.item.op.list.ListOperation.class);
		finish.setAccessible(true);
		try {
			finish.invoke(handler, channel, op);
			assertEquals(2000, op.objectsListed());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
		}
	}

	@Test
	void successfulEmptyHttpBodyIsTerminalAndDoesNotLeakSpool() throws Exception {
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.status(Operation.Status.SUCC);
		final Method finish = S3ResponseHandler.class.getDeclaredMethod(
						"finishListResponse", io.netty.channel.Channel.class,
						com.dell.spt.base.item.op.list.ListOperation.class);
		finish.setAccessible(true);
		final var channel = new EmbeddedChannel();
		try {
			final var wrapped = assertThrows(
							InvocationTargetException.class, () -> finish.invoke(handler, channel, op));
			assertInstanceOf(IntegrityTerminalException.class, wrapped.getCause());
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
			assertEquals(0, S3ResponseHandler.activeListSpoolCount());
		} finally {
			channel.close();
		}
	}

	@Test
	void oversizedListResponseFailsTerminallyAndDeletesSpool() throws Exception {
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		final Method buffer = S3ResponseHandler.class.getDeclaredMethod(
						"bufferListContent", io.netty.channel.Channel.class,
						com.dell.spt.base.item.op.list.ListOperation.class, ByteBuf.class);
		buffer.setAccessible(true);
		final byte[] mebibyte = new byte[1024 * 1024];
		final var channel = new EmbeddedChannel();
		try {
			for (int i = 0; i < 16; i++) {
				final ByteBuf chunk = Unpooled.wrappedBuffer(mebibyte);
				try {
					buffer.invoke(handler, channel, op, chunk);
				} finally {
					chunk.release();
				}
			}
			final ByteBuf overflow = Unpooled.wrappedBuffer(new byte[]{1
			});
			try {
				final var wrapped = assertThrows(
								InvocationTargetException.class, () -> buffer.invoke(handler, channel, op, overflow));
				assertInstanceOf(IntegrityTerminalException.class, wrapped.getCause());
				assertEquals(Operation.Status.RESP_FAIL_CLIENT, op.status());
				assertEquals(0, S3ResponseHandler.activeListSpoolCount());
			} finally {
				overflow.release();
			}
		} finally {
			channel.close();
		}
	}

	private static final String LIST_V2_RESPONSE = "<ListBucketResult>"
					+ "<IsTruncated>true</IsTruncated>"
					+ "<Contents><Key>a.txt</Key><Size>123</Size></Contents>"
					+ "<Contents><Key>b.txt</Key><Size>456</Size></Contents>"
					+ "<NextContinuationToken>token-123</NextContinuationToken>"
					+ "</ListBucketResult>";
}
