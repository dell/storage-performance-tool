package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.driver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.fallthroughDriver;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.operation;
import static com.dell.spt.storage.driver.coop.aws.s3.S3AwsDeleteRequestTestFixture.target;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.deletion.DeleteTargetOutcome;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.DeletedObject;
import software.amazon.awssdk.services.s3.model.S3Error;

final class S3AwsDeleteRequestAdapterTest {
	private static final long RESULT_TIMEOUT_SECONDS = 5;

	@Test
	void oneCurrentTargetUsesOnePrimaryDeleteObjectRequest() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		when(primary.deleteObject(any(DeleteObjectRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
		try (final var driver = driver(primary, exact)) {
			final DeleteRequestOperation operation = operation(target("current-key", null));

			driver.execute(operation).join();

			final ArgumentCaptor<DeleteObjectRequest> request = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(primary).deleteObject(request.capture());
			verifyNoInteractions(exact);
			assertEquals("bucket", request.getValue().bucket());
			assertEquals("current-key", request.getValue().key());
			assertNull(request.getValue().versionId());
			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, operation.deleteResult().outcome());
		}
	}

	@Test
	void oneExactTargetUsesOneExactVersionDeleteObjectRequest() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		when(exact.deleteObject(any(DeleteObjectRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
		try (final var driver = driver(primary, exact)) {
			final DeleteRequestOperation operation = operation(target("exact-key", "version-1"));

			driver.execute(operation).join();

			final ArgumentCaptor<DeleteObjectRequest> request = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(exact).deleteObject(request.capture());
			verifyNoInteractions(primary);
			assertEquals("version-1", request.getValue().versionId());
			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, operation.deleteResult().outcome());
		}
	}

	@Test
	void batchUsesOneNonQuietExactVersionRequestAndReconcilesFullSuccess() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		when(exact.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder()
										.deleted(
														DeletedObject.builder().key("current-key").build(),
														DeletedObject.builder().key("exact-key").versionId("version-2").build())
										.build()));
		try (final var driver = driver(primary, exact)) {
			final DeleteRequestOperation operation = operation(
							target("current-key", null), target("exact-key", "version-2"));

			driver.execute(operation).join();

			final ArgumentCaptor<DeleteObjectsRequest> request = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
			verify(exact).deleteObjects(request.capture());
			verifyNoInteractions(primary);
			assertFalse(request.getValue().delete().quiet());
			assertEquals(2, request.getValue().delete().objects().size());
			assertNull(request.getValue().delete().objects().get(0).versionId());
			assertEquals("version-2", request.getValue().delete().objects().get(1).versionId());
			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, operation.deleteResult().outcome());
			assertEquals(2, operation.deleteResult().acceptedObjectCount());
		}
	}

	@Test
	void currentOnlyBatchUsesPrimaryClient() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		when(primary.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder()
										.deleted(
														DeletedObject.builder().key("one").build(),
														DeletedObject.builder().key("two").build())
										.build()));
		try (final var driver = driver(primary, exact)) {
			final DeleteRequestOperation operation = operation(target("one", null), target("two", null));

			driver.execute(operation).join();

			verify(primary).deleteObjects(any(DeleteObjectsRequest.class));
			verifyNoInteractions(exact);
			assertEquals(DeleteRequestOutcome.FULL_SUCCESS, operation.deleteResult().outcome());
		}
	}

	@Test
	void validPartialResponsePreservesEveryTargetOutcome() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		when(exact.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder()
										.deleted(DeletedObject.builder().key("accepted").build())
										.errors(S3Error.builder()
														.key("denied").versionId("version-3")
														.code("AccessDenied").message("denied").build())
										.build()));
		try (final var driver = driver(primary, exact)) {
			final DeleteRequestOperation operation = operation(
							target("accepted", null), target("denied", "version-3"));

			driver.execute(operation).join();

			assertEquals(DeleteRequestOutcome.PARTIAL, operation.deleteResult().outcome());
			assertEquals(1, operation.deleteResult().acceptedObjectCount());
			assertEquals(1, operation.deleteResult().failedObjectCount());
			assertTrue(operation.deleteResult().targetResults().get(1).errorMessage()
							.contains("AccessDenied"));
		}
	}

	@Test
	void malformedIncompleteDuplicateAndUnexpectedResponsesFailConservatively() throws Exception {
		final List<DeleteObjectsResponse> malformedResponses = List.of(
						DeleteObjectsResponse.builder()
										.deleted(DeletedObject.builder().key((String) null).build())
										.build(),
						DeleteObjectsResponse.builder()
										.deleted(DeletedObject.builder().key("one").build())
										.build(),
						DeleteObjectsResponse.builder()
										.deleted(
														DeletedObject.builder().key("one").build(),
														DeletedObject.builder().key("one").build(),
														DeletedObject.builder().key("two").build())
										.build(),
						DeleteObjectsResponse.builder()
										.deleted(
														DeletedObject.builder().key("one").build(),
														DeletedObject.builder().key("unexpected").build())
										.build());
		for (final DeleteObjectsResponse response : malformedResponses) {
			final S3AsyncClient primary = mock(S3AsyncClient.class);
			when(primary.deleteObjects(any(DeleteObjectsRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(response));
			try (final var driver = driver(primary, mock(S3AsyncClient.class))) {
				final DeleteRequestOperation operation = operation(target("one", null), target("two", null));

				driver.execute(operation).join();

				assertEquals(DeleteRequestOutcome.FAILED, operation.deleteResult().outcome());
				assertEquals(DeleteFailureClassification.PROTOCOL,
								operation.deleteResult().failureClassification());
				assertEquals(2, operation.deleteResult().failedObjectCount());
			}
		}
	}

	@Test
	void sdkFailureBecomesOneConservativeLogicalRequestOutcome() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		when(primary.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.failedFuture(
										SdkClientException.builder().message("transport unavailable").build()));
		try (final var driver = driver(primary, mock(S3AsyncClient.class))) {
			final DeleteRequestOperation operation = operation(target("one", null), target("two", null));

			driver.execute(operation).join();

			verify(primary).deleteObjects(any(DeleteObjectsRequest.class));
			assertEquals(DeleteRequestOutcome.FAILED, operation.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.OPERATIONAL,
							operation.deleteResult().failureClassification());
			assertEquals(2, operation.deleteResult().failedObjectCount());
			assertEquals(0, operation.responseFirstByteTime(),
							"transport failures must not fabricate a response-start marker");
			assertEquals(0, operation.respTimeDone(),
							"transport failures without a response must not fabricate a last-byte marker");
		}
	}

	@Test
	void invokeNioTransportFailureDoesNotFabricateResponseTiming() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		when(primary.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.failedFuture(
										SdkClientException.builder().message("transport unavailable").build()));
		try (final var driver = driver(primary, mock(S3AsyncClient.class))) {
			final DeleteRequestOperation operation = operation(target("one", null), target("two", null));

			driver.invokeNio(operation);

			assertEquals(0, operation.responseFirstByteTime());
			assertEquals(0, operation.respTimeDone(),
							"invokeNio failure cleanup must not invent a completed transport response");
		}
	}

	@Test
	void upperProtocolBatchLimitStillUsesOneSdkRequest() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final List<DeletedObject> deleted = IntStream.range(0, 1_000)
						.mapToObj(index -> DeletedObject.builder().key("key-" + index).build())
						.toList();
		when(primary.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(
										DeleteObjectsResponse.builder().deleted(deleted).build()));
		final List<DeleteTarget> targets = new ArrayList<>(1_000);
		IntStream.range(0, 1_000).forEach(index -> targets.add(target("key-" + index, null)));
		try (final var driver = driver(primary, mock(S3AsyncClient.class))) {
			final DeleteRequestOperation operation = operation(targets.toArray(DeleteTarget[]::new));

			driver.execute(operation).join();

			final ArgumentCaptor<DeleteObjectsRequest> request = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
			verify(primary).deleteObjects(request.capture());
			assertEquals(1_000, request.getValue().delete().objects().size());
			assertEquals(1_000, operation.deleteResult().acceptedObjectCount());
		}
	}

	@Test
	void advertisesStandaloneSupportAndNeverUsesLegacySingleDeleteForBatch() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		when(primary.deleteObjects(any(DeleteObjectsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectsResponse.builder()
										.deleted(
														DeletedObject.builder().key("one").build(),
														DeletedObject.builder().key("two").build())
										.build()));
		try (final var driver = driver(primary, mock(S3AsyncClient.class))) {
			final DeleteRequestOperation operation = operation(target("one", null), target("two", null));

			driver.execute(operation).join();

			assertTrue(driver.supportsStandaloneDeleteRequests());
			verify(primary, never()).deleteObject(any(DeleteObjectRequest.class));
			verify(primary).deleteObjects(any(DeleteObjectsRequest.class));
			assertEquals(Operation.Status.SUCC, operation.status());
		}
	}

	@Test
	void legacySingleDeleteStillPropagatesAnExactRequestedVersion() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		when(exact.deleteObject(any(DeleteObjectRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
		try (final var typedDriver = driver(primary, exact)) {
			@SuppressWarnings("rawtypes")
			final S3AwsStorageDriver rawDriver = typedDriver;
			final var item = new com.dell.spt.base.item.IntegrityManifestDataItem(
							"bucket", "legacy-key", 0, "legacy-version");
			final var operation = new DataOperationImpl<>(
							0, OpType.DELETE, item, null, "/bucket",
							S3AwsDeleteRequestTestFixture.CREDENTIAL, null, 0);

			@SuppressWarnings("unchecked")
			final CompletableFuture<Void> result = rawDriver.execute(operation);
			result.join();

			final ArgumentCaptor<DeleteObjectRequest> request = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(exact).deleteObject(request.capture());
			verifyNoInteractions(primary);
			assertEquals("legacy-key", request.getValue().key());
			assertEquals("legacy-version", request.getValue().versionId());
		}
	}

	@Test
	void legacySingleDeleteBuilderRejectsStandaloneRepresentativeItemFallThrough() throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		try (final var driver = driver(primary, exact)) {
			final DeleteRequestOperation operation = operation(target("one", null), target("two", null));

			final CompletionException failure = assertThrows(
							CompletionException.class, () -> driver.deleteObject(operation).join());

			assertTrue(failure.getCause() instanceof IllegalArgumentException);
			verifyNoInteractions(primary, exact);
			assertEquals(DeleteRequestOutcome.FAILED, operation.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.PROTOCOL,
							operation.deleteResult().failureClassification());
			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, operation.status());
			assertEquals(2, operation.deleteResult().targetResults().size());
			assertTrue(operation.deleteResult().targetResults().stream()
							.allMatch(result -> result.outcome() == DeleteTargetOutcome.FAILED
											&& result.failureClassification() == DeleteFailureClassification.PROTOCOL));
		}
	}

	@Test
	void representativeItemFallthroughCompletesThroughRealNioLifecycleWithoutSdkEmission()
					throws Exception {
		final S3AsyncClient primary = mock(S3AsyncClient.class);
		final S3AsyncClient exact = mock(S3AsyncClient.class);
		try (final var driver = fallthroughDriver(primary, exact)) {
			final ResultOutput output = new ResultOutput();
			driver.operationResultOutput(output);
			driver.start();
			assertTrue(driver.put(operation(target("one", null), target("two", null))));

			final DeleteRequestOperation result = output.await();
			assertNotNull(result, "AWS fallthrough did not publish a terminal result");
			awaitCompletionAccounting(driver);

			verify(primary, never()).deleteObject(any(DeleteObjectRequest.class));
			verify(primary, never()).deleteObjects(any(DeleteObjectsRequest.class));
			verify(exact, never()).deleteObject(any(DeleteObjectRequest.class));
			verify(exact, never()).deleteObjects(any(DeleteObjectsRequest.class));
			assertEquals(DeleteRequestOutcome.FAILED, result.deleteResult().outcome());
			assertEquals(DeleteFailureClassification.PROTOCOL,
							result.deleteResult().failureClassification());
			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(2, result.deleteResult().targetResults().size());
			assertTrue(result.deleteResult().targetResults().stream()
							.allMatch(targetResult -> targetResult.outcome() == DeleteTargetOutcome.FAILED
											&& targetResult.failureClassification() == DeleteFailureClassification.PROTOCOL));
			assertEquals(1, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount());
		}
	}

	private static void awaitCompletionAccounting(
					final S3AwsStorageDriver<?, ?> driver) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while ((driver.completedOpCount() != 1 || driver.activeOpCount() != 0)
						&& System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
	}

	private static final class ResultOutput implements Output<DeleteRequestOperation> {
		private final LinkedBlockingQueue<DeleteRequestOperation> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final DeleteRequestOperation value) {
			return results.offer(value);
		}

		@Override
		public int put(
						final List<DeleteRequestOperation> values, final int from, final int to) {
			int count = 0;
			for (int i = from; i < to; i++) {
				if (!results.offer(values.get(i))) {
					break;
				}
				count++;
			}
			return count;
		}

		@Override
		public int put(final List<DeleteRequestOperation> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			return null;
		}

		@Override
		public void close() {
			results.clear();
		}

		private DeleteRequestOperation await() throws InterruptedException {
			return results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		}
	}
}
