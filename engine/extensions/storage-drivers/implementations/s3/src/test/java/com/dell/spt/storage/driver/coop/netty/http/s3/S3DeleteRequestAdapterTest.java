package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.CREDENTIAL;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.operation;
import static com.dell.spt.storage.driver.coop.netty.http.s3.S3DeleteRequestTestFixture.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.deletion.DeleteTargetOutcome;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.AttributeKey;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

final class S3DeleteRequestAdapterTest {

	@Test
	void oneCurrentTargetBuildsOrdinarySignedObjectDelete() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			final HttpRequest request = driver.build(operation(target("current key", null)));

			assertEquals(HttpMethod.DELETE, request.method());
			assertEquals("/bucket/current%20key", request.uri());
			assertEquals("0", request.headers().get(HttpHeaderNames.CONTENT_LENGTH));
			assertFalse(request.uri().contains("versionId="));
			assertNotNull(request.headers().get(HttpHeaderNames.AUTHORIZATION));
			assertTrue(request.headers().get(HttpHeaderNames.AUTHORIZATION)
							.startsWith(S3Api.AUTH_V4_PREFIX));
		}
	}

	@Test
	void oneExactTargetUsesStandardVersionQuerySelector() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			final HttpRequest request = driver.build(operation(target("exact/key", "v+1/=")));

			assertEquals(HttpMethod.DELETE, request.method());
			assertEquals("/bucket/exact/key?versionId=v%2B1%2F%3D", request.uri());
			assertFalse(request.headers().contains("x-amz-version-id"));
		}
	}

	@Test
	void multipleTargetsBuildOneSignedNonQuietMultiDeleteRequest() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			final FullHttpRequest request = (FullHttpRequest) driver.build(operation(
							target("folder/a & <snowman ☃>\r.txt", null),
							target("exact>key", "v&<\"'")));
			final byte[] content = new byte[request.content().readableBytes()];
			request.content().getBytes(request.content().readerIndex(), content);
			final String body = new String(content, StandardCharsets.UTF_8);

			assertEquals(HttpMethod.POST, request.method());
			assertEquals("/bucket?delete", request.uri());
			assertTrue(body.contains("<Key>folder/a &amp; &lt;snowman ☃&gt;&#13;.txt</Key>"));
			assertTrue(body.contains("<Key>exact&gt;key</Key><VersionId>v&amp;&lt;&quot;&apos;</VersionId>"));
			assertTrue(body.contains("<Quiet>false</Quiet>"));
			assertEquals(Integer.toString(content.length),
							request.headers().get(HttpHeaderNames.CONTENT_LENGTH));
			assertEquals("application/xml", request.headers().get(HttpHeaderNames.CONTENT_TYPE));
			assertEquals(
							Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(content)),
							request.headers().get(HttpHeaderNames.CONTENT_MD5));
			assertTrue(request.headers().get(HttpHeaderNames.AUTHORIZATION)
							.startsWith(S3Api.AUTH_V4_PREFIX));
		}
	}

	@Test
	void maximumSizeRequestKeepsAllThousandTargetsInOneBatch() throws Exception {
		final DeleteTarget[] targets = IntStream.range(0, DeleteRequest.MAX_TARGET_COUNT)
						.mapToObj(index -> target("key-" + index, index % 2 == 0 ? null : "version-" + index))
						.toArray(DeleteTarget[]::new);
		try (final var driver = new DeleteDriver(config(false))) {
			final FullHttpRequest request = (FullHttpRequest) driver.build(operation(targets));
			final String body = request.content().toString(StandardCharsets.UTF_8);

			assertEquals(HttpMethod.POST, request.method());
			assertEquals(1_000, body.split("<Object>", -1).length - 1);
			assertTrue(body.contains("<Key>key-999</Key><VersionId>version-999</VersionId>"));
		}
	}

	@Test
	void defaultSignatureV2AlsoSignsMultiDelete() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(
						false, 2, false, null, "127.0.0.1:9024");
		try (final var driver = new DeleteDriver(config)) {
			final HttpRequest request = driver.build(
							operation(target("one", null), target("two", null)));

			assertTrue(request.headers().get(HttpHeaderNames.AUTHORIZATION)
							.startsWith(S3Api.AUTH_PREFIX + CREDENTIAL.getUid() + ':'));
		}
	}

	@Test
	void batchSuccessReconcilesEveryReturnedIdentity() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			driver.suppressCompletion = true;
			final var operation = operation(
							target("current", null), target("exact", "version-2"));
			final String xml = "<DeleteResult>"
							+ "<Deleted><Key>exact</Key><VersionId>version-2</VersionId></Deleted>"
							+ "<Deleted><Key>current</Key></Deleted>"
							+ "</DeleteResult>";
			final var handler = new S3ResponseHandler<IntegrityManifestDataItem, DeleteRequestOperation>(
							driver, false, false, null);
			final var channel = new EmbeddedChannel();
			final var content = Unpooled.copiedBuffer(xml, StandardCharsets.UTF_8);
			try {
				operation.status(com.dell.spt.base.item.op.Operation.Status.SUCC);
				handler.handleResponseContentChunk(channel, operation, content);
				final ByteBuf bufferedContent = bufferedDeleteContent(channel);
				handler.handleResponseContentFinish(channel, operation);

				assertEquals(DeleteRequestOutcome.FULL_SUCCESS, operation.deleteResult().outcome());
				assertEquals(
								List.of(DeleteTargetOutcome.ACCEPTED, DeleteTargetOutcome.ACCEPTED),
								operation.deleteResult().targetResults().stream()
												.map(result -> result.outcome())
												.toList());
				assertEquals(0, bufferedContent.refCnt());
				assertNull(deleteResponseState(channel));
			} finally {
				content.release();
				channel.close();
			}
		}
	}

	@Test
	void oversizedBatchResponseFailsClosedReleasesBufferAndClearsChannelState() throws Exception {
		final int maxDeleteResponseBytes = 16 * 1024 * 1024;
		try (final var driver = new DeleteDriver(config(false))) {
			driver.suppressCompletion = true;
			final var handler = new S3ResponseHandler<IntegrityManifestDataItem, DeleteRequestOperation>(
							driver, false, false, null);
			final var channel = new EmbeddedChannel();
			final var prefix = Unpooled.copiedBuffer("<DeleteResult>", StandardCharsets.UTF_8);
			final var overflow = Unpooled.buffer(maxDeleteResponseBytes).writerIndex(maxDeleteResponseBytes);
			try {
				final var oversized = operation(target("one", null), target("two", null));
				oversized.status(com.dell.spt.base.item.op.Operation.Status.SUCC);
				handler.handleResponseContentChunk(channel, oversized, prefix);
				final ByteBuf bufferedContent = bufferedDeleteContent(channel);
				handler.handleResponseContentChunk(channel, oversized, overflow);
				handler.handleResponseContentFinish(channel, oversized);

				assertEquals(DeleteRequestOutcome.FAILED, oversized.deleteResult().outcome());
				assertEquals(DeleteFailureClassification.PROTOCOL,
								oversized.deleteResult().failureClassification());
				assertEquals(0, bufferedContent.refCnt());
				assertNull(deleteResponseState(channel));

				final var reused = operation(target("one", null), target("two", null));
				final var validResponse = Unpooled.copiedBuffer(
								"<DeleteResult><Deleted><Key>one</Key></Deleted>"
												+ "<Deleted><Key>two</Key></Deleted></DeleteResult>",
								StandardCharsets.UTF_8);
				try {
					reused.status(com.dell.spt.base.item.op.Operation.Status.SUCC);
					handler.handleResponseContentChunk(channel, reused, validResponse);
					handler.handleResponseContentFinish(channel, reused);
					assertEquals(DeleteRequestOutcome.FULL_SUCCESS, reused.deleteResult().outcome());
				} finally {
					validResponse.release();
				}
			} finally {
				prefix.release();
				overflow.release();
				channel.close();
			}
		}
	}

	@Test
	void validPartialResponsePreservesPerTargetOutcomes() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			final var operation = operation(target("accepted", null), target("denied", "version-3"));
			reconcile(
							driver,
							operation,
							"<DeleteResult>"
											+ "<Deleted><Key>accepted</Key></Deleted>"
											+ "<Error><Key>denied</Key><VersionId>version-3</VersionId>"
											+ "<Code>AccessDenied</Code><Message>denied</Message></Error>"
											+ "</DeleteResult>");

			assertEquals(DeleteRequestOutcome.PARTIAL, operation.deleteResult().outcome());
			assertEquals(1, operation.deleteResult().acceptedObjectCount());
			assertEquals(1, operation.deleteResult().failedObjectCount());
		}
	}

	@Test
	void malformedIncompleteDuplicateAndUnexpectedResponsesFailClosed() throws Exception {
		final List<String> defectiveResponses = List.of(
						"<DeleteResult><Deleted><Key>current</Key></Deleted>",
						"<DeleteResult></DeleteResult>",
						"<DeleteResult><Deleted><Key>current</Key></Deleted></DeleteResult>",
						"<DeleteResult><Deleted><Key>current</Key></Deleted>"
										+ "<Deleted><Key>current</Key></Deleted>"
										+ "<Deleted><Key>exact</Key><VersionId>version-2</VersionId></Deleted>"
										+ "</DeleteResult>",
						"<DeleteResult><Deleted><Key>current</Key></Deleted>"
										+ "<Deleted><Key>unexpected</Key></Deleted>"
										+ "</DeleteResult>");
		for (final String response : defectiveResponses) {
			try (final var driver = new DeleteDriver(config(false))) {
				final var operation = operation(
								target("current", null), target("exact", "version-2"));
				reconcile(driver, operation, response);

				assertEquals(DeleteRequestOutcome.FAILED, operation.deleteResult().outcome());
				assertEquals(DeleteFailureClassification.PROTOCOL,
								operation.deleteResult().failureClassification());
				assertEquals(2, operation.deleteResult().failedObjectCount());
			}
		}
	}

	@Test
	void standaloneDeleteBypassesObjectTaggingMode() throws Exception {
		try (final var driver = new DeleteDriver(config(true))) {
			final HttpRequest request = driver.build(operation(target("key", null)));

			assertEquals(HttpMethod.DELETE, request.method());
			assertEquals("/bucket/key", request.uri());
			assertFalse(request.uri().contains("tagging"));
		}
	}

	@Test
	void driverAdvertisesStandaloneDeleteCapability() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			assertTrue(driver.supportsStandaloneDeleteRequests());
		}
	}

	@Test
	void transportFailureConservativelyFailsEveryTarget() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			final var operation = operation(target("one", null), target("two", "version-2"));
			operation.status(com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC);

			driver.finishTransportFailure(operation);

			assertEquals(DeleteRequestOutcome.FAILED, operation.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.OPERATIONAL,
							operation.deleteResult().failureClassification());
			assertEquals(2, operation.deleteResult().failedObjectCount());
		}
	}

	@Test
	void ordinarySingleObjectBuilderRejectsStandaloneBatchFallthrough() throws Exception {
		try (final var driver = new DeleteDriver(config(false))) {
			final var operation = operation(target("one", null), target("two", null));

			assertThrows(IllegalArgumentException.class, () -> driver.buildOrdinary(operation));
		}
	}

	private static void reconcile(
					final DeleteDriver driver,
					final DeleteRequestOperation operation,
					final String xml) throws Exception {
		driver.suppressCompletion = true;
		final var handler = new S3ResponseHandler<IntegrityManifestDataItem, DeleteRequestOperation>(
						driver, false, false, null);
		final var channel = new EmbeddedChannel();
		final var content = Unpooled.copiedBuffer(xml, StandardCharsets.UTF_8);
		try {
			operation.status(com.dell.spt.base.item.op.Operation.Status.SUCC);
			handler.handleResponseContentChunk(channel, operation, content);
			handler.handleResponseContentFinish(channel, operation);
		} finally {
			content.release();
			channel.close();
		}
	}

	private static Config config(final boolean taggingEnabled) {
		final Config config = S3StorageDriverTest.baseConfig(
						false, 4, false, null, "127.0.0.1:9024");
		config.val("storage-object-tagging-enabled", taggingEnabled);
		return config;
	}

	private static ByteBuf bufferedDeleteContent(final Channel channel) throws Exception {
		final Object state = deleteResponseState(channel);
		final Field contentField = state.getClass().getDeclaredField("content");
		contentField.setAccessible(true);
		return (ByteBuf) contentField.get(state);
	}

	@SuppressWarnings("unchecked")
	private static Object deleteResponseState(final Channel channel) throws Exception {
		final Field keyField = S3ResponseHandler.class.getDeclaredField("DELETE_RESPONSE_ATTR_KEY");
		keyField.setAccessible(true);
		final var key = (AttributeKey<Object>) keyField.get(null);
		return channel.attr(key).get();
	}

	private static final class DeleteDriver
					extends S3StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> {
		private boolean suppressCompletion;

		private DeleteDriver(final Config config) throws Exception {
			super(
							"delete-adapter-test",
							DataInput.instance(
											null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
							config.configVal("storage"),
							false,
							config.intVal("load-batch-size"));
			operationResultOutput(new NoopOutput());
		}

		private HttpRequest build(final DeleteRequestOperation operation) throws Exception {
			return httpRequest(operation, "127.0.0.1:9024");
		}

		private HttpRequest buildOrdinary(final DeleteRequestOperation operation) throws Exception {
			return ordinaryObjectRequest(operation, "127.0.0.1:9024");
		}

		private void finishTransportFailure(final DeleteRequestOperation operation) {
			complete(null, operation);
		}

		@Override
		public void complete(final Channel channel, final DeleteRequestOperation operation) {
			if (!suppressCompletion) {
				super.complete(channel, operation);
			}
		}
	}

	private static final class NoopOutput implements Output<DeleteRequestOperation> {

		@Override
		public boolean put(final DeleteRequestOperation value) {
			return true;
		}

		@Override
		public int put(
						final List<DeleteRequestOperation> values, final int from, final int to) {
			return to - from;
		}

		@Override
		public int put(final List<DeleteRequestOperation> values) {
			return values.size();
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}
}
