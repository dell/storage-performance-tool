package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;

import com.github.akurilov.commons.io.Output;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.AttributeKey;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * A multipart (composite) response's body is kept in a buffer of its own, capped at {@link
 * S3ResponseHandler#MAX_MULTIPART_RESPONSE_BYTES}: a broken target's long answer cannot grow client
 * memory without bound, the body kept is always a prefix, the overflow is warned about once per driver,
 * and a chunk on a channel that has its buffer allocates nothing. Legitimate answers, and the ordinary
 * LIST page, which has its own uncapped buffer, are read whole.
 */
final class S3ResponseHandlerBufferTest {

	private static final long PART_SIZE = 1024;
	private static final int CHUNK = 8 * 1024;
	private static final String DECLARATION = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
	private static final String OVERFLOW_WARNING = "multipart response body is longer than";

	private S3StorageDriverTest.TestS3Driver driver;
	private S3ResponseHandler<Item, Operation<Item>> handler;
	private EmbeddedChannel channel;

	@BeforeEach
	void setUp() throws Exception {
		driver = newDriver();
		handler = new S3ResponseHandler<>(driver, false, false, null);
		channel = new EmbeddedChannel();
	}

	@AfterEach
	void tearDown() throws Exception {
		channel.close().syncUninterruptibly();
		driver.close();
	}

	@Test
	void theCapLeavesRoomForTheCompletionErrorScan() {
		assertTrue(S3ResponseHandler.MAX_MULTIPART_RESPONSE_BYTES >= S3ResponseHandler.COMPLETION_ERROR_SCAN_BYTES);
	}

	/** Guard: a legal long key makes the initiation answer longer than 1 KiB; its upload ID is still found. */
	@Test
	void anInitiationWithAnEightKibKeyFindsItsUploadId() throws Exception {
		driver.suppressCompletionForResponseTest();
		final var op = upload();
		op.status(Operation.Status.SUCC);
		// small chunks, so a buffer too small for the answer is full before the upload ID arrives
		respond(op, DECLARATION + "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>"
						+ "k".repeat(8 * 1024) + "</Key><UploadId>u-long-key</UploadId></InitiateMultipartUploadResult>", 512);
		assertEquals(Operation.Status.SUCC, op.status());
		assertEquals("u-long-key", channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).get());
	}

	/** Guard: an ordinary LIST page (integrity metadata off) of about 300 KiB is parsed whole. */
	@Test
	void anOrdinaryListPageOf300KibIsParsedWhole() throws Exception {
		driver.suppressCompletionForResponseTest();
		final int keys = 1000;
		final var xml = new StringBuilder("<ListBucketResult><IsTruncated>false</IsTruncated>");
		for (int i = 0; i < keys; i++) {
			xml.append("<Contents><Key>prefix/").append("p".repeat(260)).append("-").append(i)
							.append("</Key><Size>1</Size></Contents>");
		}
		xml.append("</ListBucketResult>");
		assertTrue(xml.length() >= 300 * 1024, "the page is about 300 KiB");
		final var op = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.options(ListOptions.builder().fetchMetadata(true).build());
		op.status(Operation.Status.SUCC);
		respond(op, xml.toString());
		assertEquals(Operation.Status.SUCC, op.status());
		assertEquals(keys, op.objectsListed());
	}

	/**
	 * An initiation answered with 1 MiB of filler before its upload ID keeps at most the cap, finds no
	 * ID, and completes not-found through the driver; the driver warns once, and not again for a second
	 * such answer.
	 */
	@Test
	void anOversizedInitiationIsCappedAndWarnedAboutOncePerDriver() throws Exception {
		setOpResultOut(driver);
		final var body = DECLARATION + "<InitiateMultipartUploadResult><Bucket>"
						+ " ".repeat(1024 * 1024) + "</Bucket><UploadId>u-hidden</UploadId></InitiateMultipartUploadResult>";
		try (final var errors = new ErrorsLogCapture()) {
			for (int i = 0; i < 2; i++) {
				final var op = upload();
				op.status(Operation.Status.SUCC);
				channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.TRUE); // no connection pool here
				respond(op, body);
				assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, op.status());
				assertNull(op.get(S3Api.KEY_UPLOAD_ID));
				final ByteBuf buffer = multipartBuffer(channel);
				assertTrue(buffer.capacity() <= S3ResponseHandler.MAX_MULTIPART_RESPONSE_BYTES,
								() -> "capacity " + buffer.capacity());
			}
			final List<String> warnings = errors.drained().stream()
							.map(ErrorsLogCapture.Line::message)
							.filter(message -> message.contains(OVERFLOW_WARNING))
							.toList();
			assertEquals(1, warnings.size(), warnings::toString);
			assertTrue(warnings.get(0).contains(Integer.toString(S3ResponseHandler.MAX_MULTIPART_RESPONSE_BYTES)), warnings::toString);
		}
	}

	/** A later chunk smaller than the room left is not spliced onto a body whose middle was dropped. */
	@Test
	void anOversizedBodyKeepsAPrefixAndNeverASplice() throws Exception {
		driver.suppressCompletionForResponseTest();
		final var op = upload();
		op.status(Operation.Status.SUCC);
		final int max = S3ResponseHandler.MAX_MULTIPART_RESPONSE_BYTES;
		final byte[] first = filled(max - 4 * 1024, 'a');
		final byte[] second = filled(8 * 1024, 'b');
		final byte[] third = filled(100, 'c');
		for (final byte[] chunk : List.of(first, second, third)) {
			handler.handleResponseContentChunk(channel, asItemOperation(op), Unpooled.wrappedBuffer(chunk));
		}
		final ByteBuf buffer = multipartBuffer(channel);
		final byte[] kept = new byte[buffer.readableBytes()];
		buffer.getBytes(buffer.readerIndex(), kept);
		final byte[] sent = new byte[first.length + second.length + third.length];
		System.arraycopy(first, 0, sent, 0, first.length);
		System.arraycopy(second, 0, sent, first.length, second.length);
		System.arraycopy(third, 0, sent, first.length + second.length, third.length);
		assertEquals(max, kept.length);
		assertTrue(Arrays.equals(kept, Arrays.copyOf(sent, kept.length)), "the body kept is a prefix of the body sent");
		handler.handleResponseContentFinish(channel, asItemOperation(op));
	}

	/** Once the channel has its buffer, a further chunk allocates about nothing. */
	@Test
	void aChunkOnAChannelWithItsBufferAllocatesAboutNothing() throws Exception {
		final var bean = ManagementFactory.getThreadMXBean();
		assumeTrue(bean instanceof com.sun.management.ThreadMXBean);
		final var allocation = (com.sun.management.ThreadMXBean) bean;
		assumeTrue(allocation.isThreadAllocatedMemorySupported() && allocation.isThreadAllocatedMemoryEnabled());
		driver.suppressCompletionForResponseTest();
		final var op = upload();
		op.status(Operation.Status.SUCC);
		respond(op, "<InitiateMultipartUploadResult><UploadId>u-1</UploadId></InitiateMultipartUploadResult>");
		assertEquals("u-1", channel.attr(S3Api.KEY_ATTR_UPLOAD_ID).get());
		final ByteBuf buffer = multipartBuffer(channel);
		final ByteBuf chunk = Unpooled.wrappedBuffer(filled(64, ' '));
		final int warmUp = 50_000;
		final int calls = 200_000;
		chunks(op, buffer, chunk, warmUp);
		final long thread = Thread.currentThread().threadId();
		final long before = allocation.getThreadAllocatedBytes(thread);
		chunks(op, buffer, chunk, calls);
		final long perCall = (allocation.getThreadAllocatedBytes(thread) - before) / calls;
		assertTrue(perCall < 64, () -> perCall + " bytes allocated per chunk");
	}

	/** #329's completion error scan still sees an error after 3 KiB of keep-alive whitespace. */
	@Test
	void aCompletionErrorAfterThreeKibOfWhitespaceStillFails() throws Exception {
		driver.suppressCompletionForResponseTest();
		final var op = upload();
		for (int i = 0; i < op.subOperations().size(); i++) {
			op.markSubTaskCompleted();
		}
		op.put(S3Api.KEY_UPLOAD_ID, "u-1");
		op.status(Operation.Status.SUCC);
		respond(op, " ".repeat(3 * 1024) + "<Error><Code>InternalError</Code><Message>m</Message></Error>");
		assertEquals(Operation.Status.RESP_FAIL_SVC, op.status());
	}

	private void chunks(final Operation<?> operation, final ByteBuf buffer, final ByteBuf chunk, final int count) throws Exception {
		final Operation<Item> op = asItemOperation(operation);
		for (int i = 0; i < count; i++) {
			if (buffer.writableBytes() < chunk.capacity()) {
				buffer.clear();
			}
			chunk.readerIndex(0);
			handler.handleResponseContentChunk(channel, op, chunk);
		}
	}

	private void respond(final Operation<?> operation, final String body) throws Exception {
		respond(operation, body, CHUNK);
	}

	private void respond(final Operation<?> operation, final String body, final int chunkSize) throws Exception {
		final Operation<Item> op = asItemOperation(operation);
		final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		for (int start = 0; start < bytes.length; start += chunkSize) {
			final ByteBuf chunk = Unpooled.wrappedBuffer(bytes, start, Math.min(chunkSize, bytes.length - start));
			try {
				handler.handleResponseContentChunk(channel, op, chunk);
			} finally {
				chunk.release();
			}
		}
		handler.handleResponseContentFinish(channel, op);
	}

	private static S3StorageDriverTest.TestS3Driver newDriver() throws Exception {
		return new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(
						false, 4, false, null, "s3.us-east-1.amazonaws.com:443"));
	}

	@SuppressWarnings("unchecked")
	private static void setOpResultOut(final S3StorageDriverTest.TestS3Driver drv) throws Exception {
		final Output<Operation<Item>> output = Mockito.mock(Output.class);
		Mockito.when(output.put(Mockito.<Operation<Item>> any())).thenReturn(true);
		final Field field = StorageDriverBase.class.getDeclaredField("opResultOut");
		field.setAccessible(true);
		field.set(drv, output);
	}

	@SuppressWarnings("unchecked")
	private static ByteBuf multipartBuffer(final Channel channel) throws Exception {
		final Field field = S3ResponseHandler.class.getDeclaredField("MULTIPART_RESPONSE_ATTR_KEY");
		field.setAccessible(true);
		final ByteBuf buffer = channel.attr((AttributeKey<ByteBuf>) field.get(null)).get();
		assertTrue(buffer != null, "the channel has a multipart response buffer");
		return buffer;
	}

	private static byte[] filled(final int length, final char c) {
		final byte[] bytes = new byte[length];
		Arrays.fill(bytes, (byte) c);
		return bytes;
	}

	/** An upload whose parts are not done yet, so its next response is its initiation's. */
	private static CompositeDataOperationImpl<DataItemImpl> upload() {
		final var op = new CompositeDataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, new DataItemImpl("/bucket/obj", 0L, 4 * PART_SIZE), null, "/bucket/", Credential.NONE,
						null, 0, PART_SIZE);
		op.subOperations();
		return op;
	}

	@SuppressWarnings("unchecked")
	private static <O extends Operation<?>> Operation<Item> asItemOperation(final O operation) {
		return (Operation<Item>) (Operation<?>) operation;
	}
}
