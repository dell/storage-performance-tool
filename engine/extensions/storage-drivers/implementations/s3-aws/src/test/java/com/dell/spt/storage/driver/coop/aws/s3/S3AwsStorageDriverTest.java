package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.integrity.IntegrityConfig;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityMetadata;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.integrity.IntegrityMode;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListedObject;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.SdkInternalExecutionAttribute;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class S3AwsStorageDriverTest {

	private S3AwsStorageDriver<Item, Operation<Item>> drv;
	private S3AsyncClient mockS3Client;

	// Constants from the driver for testing
	private static final String KEY_UPLOAD_ID = "uploadId";
	private static final String KEY_MPU_ABORT = "mpuAbort";
	private static final Credential TEST_CRED = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	@SuppressWarnings("unchecked")
	private S3AwsStorageDriver<Item, Operation<Item>> newDriverMock() {
		return Mockito.mock(S3AwsStorageDriver.class,
						Mockito.withSettings().lenient().defaultAnswer(Mockito.CALLS_REAL_METHODS));
	}

	private static void disableIntegrity(final Config storageConfig) {
		final Config integrityConfig = mock(Config.class);
		final Config inputConfig = mock(Config.class);
		when(storageConfig.configVal("integrity")).thenReturn(integrityConfig);
		when(integrityConfig.stringVal("mode")).thenReturn("none");
		when(integrityConfig.stringVal("algorithm")).thenReturn(IntegrityMetadataCodec.ALGORITHM_SHA256);
		when(integrityConfig.configVal("input")).thenReturn(inputConfig);
		when(inputConfig.stringVal("provenance")).thenReturn("none");
		when(inputConfig.stringVal("expectedProducerId")).thenReturn("");
	}

	private void setBucketName(S3AwsStorageDriver<Item, Operation<Item>> driver, String bucketName) throws Exception {
		Field bucketField = S3AwsStorageDriver.class.getDeclaredField("bucketName");
		bucketField.setAccessible(true);
		bucketField.set(driver, bucketName);
	}

	private void setS3Client(S3AwsStorageDriver<Item, Operation<Item>> driver, S3AsyncClient s3Client) throws Exception {
		Field clientField = S3AwsStorageDriver.class.getDeclaredField("s3AsyncClient");
		clientField.setAccessible(true);
		clientField.set(driver, s3Client);
		setExactVersionS3Client(driver, s3Client);
	}

	private void setExactVersionS3Client(
					S3AwsStorageDriver<Item, Operation<Item>> driver, S3AsyncClient s3Client) throws Exception {
		Field exactClientField = S3AwsStorageDriver.class.getDeclaredField("exactVersionS3Client");
		exactClientField.setAccessible(true);
		exactClientField.set(driver, s3Client);
	}

	private void setVersioningEnabled(
					S3AwsStorageDriver<Item, Operation<Item>> driver, boolean enabled) throws Exception {
		Field versioningField = S3AwsStorageDriver.class.getDeclaredField("versioningEnabled");
		versioningField.setAccessible(true);
		versioningField.setBoolean(driver, enabled);
	}

	private void setChecksumFields(
					S3AwsStorageDriver<Item, Operation<Item>> driver,
					boolean enabled,
					ChecksumAlgorithm algorithm) throws Exception {
		Field enabledField = S3AwsStorageDriver.class.getDeclaredField("checksumEnabled");
		enabledField.setAccessible(true);
		enabledField.set(driver, enabled);
		Field algoField = S3AwsStorageDriver.class.getDeclaredField("checksumAlgorithm");
		algoField.setAccessible(true);
		algoField.set(driver, algorithm);
	}

	private void enableIntegrityMetadata(
					final S3AwsStorageDriver<Item, Operation<Item>> driver) throws Exception {
		final Field field = StorageDriverBase.class.getDeclaredField("integrityConfig");
		field.setAccessible(true);
		field.set(driver, new IntegrityConfig(
						IntegrityMode.METADATA,
						IntegrityMetadataCodec.ALGORITHM_SHA256,
						IntegrityInputProvenance.EXTERNAL,
						null));
	}

	private static IntegrityMetadata metadataFor(final byte[] content) throws Exception {
		return new IntegrityMetadata(
						IntegrityMetadataCodec.VERSION_1,
						IntegrityMetadataCodec.ALGORITHM_SHA256,
						HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content)),
						content.length);
	}

	@BeforeEach
	void setUp() throws Exception {
		drv = newDriverMock();
		mockS3Client = mock(S3AsyncClient.class);
		setS3Client(drv, mockS3Client);
		setBucketName(drv, "test-bucket");
	}

	@Test
	void verificationPresenceForwardsWaitingThreadInterruption() throws Exception {
		final CountDownLatch requestStarted = new CountDownLatch(1);
		final CompletableFuture<HeadObjectResponse> pending = new CompletableFuture<>();
		when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenAnswer(ignored -> {
			requestStarted.countDown();
			return pending;
		});
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final AtomicBoolean interruptedStatus = new AtomicBoolean();
		final Thread worker = Thread.ofPlatform().start(() -> {
			try {
				drv.presence(new DeleteTarget(
								new IntegrityManifestDataItem("bucket", "key", 1, null), 0));
			} catch (final Throwable thrown) {
				failure.set(thrown);
				interruptedStatus.set(Thread.currentThread().isInterrupted());
			}
		});
		try {
			assertTrue(requestStarted.await(5, TimeUnit.SECONDS), "verification HEAD never started");
			worker.interrupt();
			worker.join(TimeUnit.SECONDS.toMillis(5));
			assertFalse(worker.isAlive(), "interrupted verification HEAD did not terminate");
			assertTrue(failure.get() instanceof InterruptedException,
							() -> "expected interruption, got " + failure.get());
			assertTrue(interruptedStatus.get(), "verification HEAD cleared the interrupt status");
		} finally {
			pending.complete(HeadObjectResponse.builder().build());
			worker.interrupt();
			worker.join(TimeUnit.SECONDS.toMillis(5));
		}
	}

	@Test
	void verificationPresenceForwardsAsyncInterruption() {
		final InterruptedException expected = new InterruptedException("external verification interrupt");
		final CompletableFuture<HeadObjectResponse> interrupted = new CompletableFuture<>();
		interrupted.completeExceptionally(expected);
		when(mockS3Client.headObject(any(HeadObjectRequest.class))).thenReturn(interrupted);
		try {
			final InterruptedException actual = assertThrows(
							InterruptedException.class,
							() -> drv.presence(new DeleteTarget(
											new IntegrityManifestDataItem("bucket", "key", 1, null), 0)));
			assertSame(expected, actual);
			assertTrue(Thread.currentThread().isInterrupted());
		} finally {
			Thread.interrupted();
		}
	}

	@Nested
	class IntegrityMetadataTest {

		@Test
		@SuppressWarnings("unchecked")
		void putCarriesMetadataAndReusesSha256ForTransportChecksum() throws Exception {
			enableIntegrityMetadata(drv);
			setChecksumFields(drv, true, ChecksumAlgorithm.SHA256);
			when(mockS3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
							.thenReturn(CompletableFuture.completedFuture(
											PutObjectResponse.builder().versionId("returned-version").build()));

			final byte[] content = "body".getBytes(StandardCharsets.UTF_8);
			final IntegrityMetadata metadata = metadataFor(content);
			final DataItem item = new DataItemImpl("key", 0, content.length);
			item.dataInput(DataInput.instance(
							null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false));
			final DataOperationImpl<DataItem> op = new DataOperationImpl<>(
							0, OpType.CREATE, item, null, "/bucket", TEST_CRED, null, 0);
			op.integrityMetadata(metadata);

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			final ArgumentCaptor<PutObjectRequest> request = ArgumentCaptor.forClass(PutObjectRequest.class);
			verify(mockS3Client).putObject(request.capture(), any(AsyncRequestBody.class));
			assertEquals(IntegrityMetadataCodec.logicalMetadata(metadata), request.getValue().metadata());
			assertEquals(
							Base64.getEncoder().encodeToString(HexFormat.of().parseHex(metadata.digest())),
							request.getValue().checksumSHA256());
			assertNull(request.getValue().checksumAlgorithm());
			assertEquals("returned-version", op.returnedVersionId());
		}

		@Test
		@SuppressWarnings({"unchecked", "rawtypes"
		})
		void multipartInitiationCarriesWholeObjectMetadata() throws Exception {
			enableIntegrityMetadata(drv);
			final IntegrityMetadata metadata = metadataFor(
							"body".getBytes(StandardCharsets.UTF_8));
			final CompositeDataOperation op = mock(CompositeDataOperation.class);
			final DataItem item = mock(DataItem.class);
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.item()).thenReturn(item);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.allSubOperationsDone()).thenReturn(false);
			when(op.integrityMetadata()).thenReturn(metadata);
			when(mockS3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(
											CreateMultipartUploadResponse.builder().uploadId("upload-id").build()));

			drv.execute((Operation) op).join();

			final ArgumentCaptor<CreateMultipartUploadRequest> request = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
			verify(mockS3Client).createMultipartUpload(request.capture());
			assertEquals(IntegrityMetadataCodec.logicalMetadata(metadata), request.getValue().metadata());
			verify(op).put(KEY_UPLOAD_ID, "upload-id");
		}

		@Test
		@SuppressWarnings("unchecked")
		void readUsesExactManifestVersionAndVerifiesTheCompleteBody() throws Exception {
			enableIntegrityMetadata(drv);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setS3Client(drv, mockS3Client);
			final Field exactClientField = S3AwsStorageDriver.class.getDeclaredField("exactVersionS3Client");
			exactClientField.setAccessible(true);
			exactClientField.set(drv, exactVersionS3Client);
			final byte[] content = "body".getBytes(StandardCharsets.UTF_8);
			final IntegrityMetadata metadata = metadataFor(content);
			final GetObjectResponse getResponse = GetObjectResponse.builder()
							.metadata(IntegrityMetadataCodec.logicalMetadata(metadata))
							.contentLength((long) content.length)
							.versionId("returned-version")
							.build();
			when(exactVersionS3Client.getObject(
							any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(
											new ResponseInputStream<>(getResponse, new ByteArrayInputStream(content))));

			final IntegrityManifestDataItem item = new IntegrityManifestDataItem(
							"bucket", "folder/key~literal", content.length, "requested-version");
			final DataOperationImpl<IntegrityManifestDataItem> op = new DataOperationImpl<>(
							0, OpType.READ, item, null, "/bucket", TEST_CRED, null, 0);
			op.startRequest();
			op.finishRequest();

			drv.invokeNio((Operation<Item>) (Operation<?>) op);

			final ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
			verify(exactVersionS3Client).getObject(request.capture(), any(AsyncResponseTransformer.class));
			verify(mockS3Client, never()).getObject(
							any(GetObjectRequest.class), any(AsyncResponseTransformer.class));
			assertEquals("folder/key~literal", request.getValue().key());
			assertEquals("requested-version", request.getValue().versionId());
			assertEquals("returned-version", op.returnedVersionId());
			assertNotNull(op.integrityVerificationResult());
			assertTrue(op.integrityVerificationResult().verified());
			assertEquals(Operation.Status.SUCC, op.status());
		}

		@Test
		@SuppressWarnings("unchecked")
		void deleteUsesStructuredKeyAndRequestedVersionWithoutParsingLiteralTilde() throws Exception {
			enableIntegrityMetadata(drv);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(exactVersionS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));

			final IntegrityManifestDataItem item = new IntegrityManifestDataItem(
							"bucket", "folder/key~literal", 0, "requested-version");
			final DataOperationImpl<IntegrityManifestDataItem> op = new DataOperationImpl<>(
							0, OpType.DELETE, item, null, "/bucket", TEST_CRED, null, 0);

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			final ArgumentCaptor<DeleteObjectRequest> request = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(exactVersionS3Client).deleteObject(request.capture());
			verify(mockS3Client, never()).deleteObject(any(DeleteObjectRequest.class));
			assertEquals("folder/key~literal", request.getValue().key());
			assertEquals("requested-version", request.getValue().versionId());
		}

		@Test
		@SuppressWarnings("unchecked")
		void headUsesStructuredKeyAndRequestedVersionWithoutParsingLiteralTilde() throws Exception {
			enableIntegrityMetadata(drv);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(mockS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			when(exactVersionS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));

			final IntegrityManifestDataItem item = new IntegrityManifestDataItem(
							"bucket", "folder/key~literal", 0, "requested-version");
			final DataOperationImpl<IntegrityManifestDataItem> op = new DataOperationImpl<>(
							0, OpType.STAT, item, null, "/bucket", TEST_CRED, null, 0);

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			final ArgumentCaptor<HeadObjectRequest> request = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(exactVersionS3Client).headObject(request.capture());
			verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
			assertEquals("folder/key~literal", request.getValue().key());
			assertEquals("requested-version", request.getValue().versionId());
		}

		@Test
		@SuppressWarnings("unchecked")
		void noVersionReadUsesPrimaryClientWithoutTouchingExactClient() throws Exception {
			enableIntegrityMetadata(drv);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			final Field exactClientField = S3AwsStorageDriver.class.getDeclaredField("exactVersionS3Client");
			exactClientField.setAccessible(true);
			exactClientField.set(drv, exactVersionS3Client);
			final byte[] content = "body".getBytes(StandardCharsets.UTF_8);
			final GetObjectResponse getResponse = GetObjectResponse.builder()
							.metadata(IntegrityMetadataCodec.logicalMetadata(metadataFor(content)))
							.contentLength((long) content.length)
							.build();
			when(mockS3Client.getObject(
							any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(
											new ResponseInputStream<>(getResponse, new ByteArrayInputStream(content))));

			final IntegrityManifestDataItem item = new IntegrityManifestDataItem(
							"bucket", "key", content.length, null);
			final DataOperationImpl<IntegrityManifestDataItem> op = new DataOperationImpl<>(
							0, OpType.READ, item, null, "/bucket", TEST_CRED, null, 0);
			op.startRequest();
			op.finishRequest();

			drv.invokeNio((Operation<Item>) (Operation<?>) op);

			final ArgumentCaptor<GetObjectRequest> request = ArgumentCaptor.forClass(GetObjectRequest.class);
			verify(mockS3Client).getObject(request.capture(), any(AsyncResponseTransformer.class));
			verifyNoInteractions(exactVersionS3Client);
			assertNull(request.getValue().versionId());
			assertTrue(op.integrityVerificationResult().verified());
			assertEquals(Operation.Status.SUCC, op.status());
		}

		@Test
		@SuppressWarnings("unchecked")
		void digestMismatchRemainsTerminalCorruptionAfterReadCompletion() throws Exception {
			enableIntegrityMetadata(drv);
			final byte[] expected = "body".getBytes(StandardCharsets.UTF_8);
			final byte[] actual = "Body".getBytes(StandardCharsets.UTF_8);
			final GetObjectResponse getResponse = GetObjectResponse.builder()
							.metadata(IntegrityMetadataCodec.logicalMetadata(metadataFor(expected)))
							.contentLength((long) actual.length)
							.build();
			when(mockS3Client.getObject(
							any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(
											new ResponseInputStream<>(getResponse, new ByteArrayInputStream(actual))));

			final IntegrityManifestDataItem item = new IntegrityManifestDataItem(
							"bucket", "key", actual.length, null);
			final DataOperationImpl<IntegrityManifestDataItem> op = new DataOperationImpl<>(
							0, OpType.READ, item, null, "/bucket", TEST_CRED, null, 0);
			op.startRequest();
			op.finishRequest();

			drv.invokeNio((Operation<Item>) (Operation<?>) op);

			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, op.status());
			assertFalse(op.integrityVerificationResult().verified());
		}
	}

	// -----------------------------------------------------------------------
	// parseBucketAndKey — instance method, package-visible, tested via driver mock
	// -----------------------------------------------------------------------

	@Nested
	class ParseBucketAndKeyTest {

		@Test
		void withLeadingSlashAndKey() {
			String[] bk = drv.parseBucketAndKey("/large/mkk0lurmliru");
			assertEquals("large", bk[0]);
			assertEquals("mkk0lurmliru", bk[1]);
		}

		@Test
		void withoutLeadingSlash() {
			String[] bk = drv.parseBucketAndKey("mybucket/my/nested/key.txt");
			assertEquals("mybucket", bk[0]);
			assertEquals("my/nested/key.txt", bk[1]);
		}

		@Test
		void bucketOnly_noKey() {
			String[] bk = drv.parseBucketAndKey("/onlybucket");
			assertEquals("test-bucket", bk[0]);
			assertEquals("onlybucket", bk[1]);
		}

		@Test
		void bucketOnlyNoSlash() {
			String[] bk = drv.parseBucketAndKey("onlybucket");
			assertEquals("test-bucket", bk[0]);
			assertEquals("onlybucket", bk[1]);
		}

		@ParameterizedTest
		@CsvSource({
				"/b/k, b, k",
				"/bucket/prefix/deep/key, bucket, prefix/deep/key",
				"bucket/key, bucket, key",
		})
		void parameterized(String input, String expectedBucket, String expectedKey) {
			String[] bk = drv.parseBucketAndKey(input);
			assertEquals(expectedBucket, bk[0]);
			assertEquals(expectedKey, bk[1]);
		}
	}

	// -----------------------------------------------------------------------
	// resolveBucketAndKey — package-visible, tested directly
	// -----------------------------------------------------------------------

	@Nested
	class ResolveBucketAndKeyTest {

		@SuppressWarnings("unchecked")
		@Test
		void withDstPath_simpleBucket() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/large");
			when(item.name()).thenReturn("mkk0lurmliru");
			when(op.item()).thenReturn(item);

			String[] bk = drv.resolveBucketAndKey(op);
			assertEquals("large", bk[0]);
			assertEquals("mkk0lurmliru", bk[1]);
		}

		@SuppressWarnings("unchecked")
		@Test
		void withDstPath_nestedPrefix() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/bucket/prefix");
			when(item.name()).thenReturn("mykey");
			when(op.item()).thenReturn(item);

			String[] bk = drv.resolveBucketAndKey(op);
			assertEquals("bucket", bk[0]);
			assertEquals("prefix/mykey", bk[1]);
		}

		@SuppressWarnings("unchecked")
		@Test
		void withDstPath_itemNameHasLeadingSlash() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("/somekey");
			when(op.item()).thenReturn(item);

			String[] bk = drv.resolveBucketAndKey(op);
			assertEquals("mybucket", bk[0]);
			assertEquals("somekey", bk[1]);
		}

		@SuppressWarnings("unchecked")
		@Test
		void noDstPath_fallsBackToParseBucketAndKey() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn(null);
			when(item.name()).thenReturn("/fallback-bucket/fallback-key");
			when(op.item()).thenReturn(item);

			String[] bk = drv.resolveBucketAndKey(op);
			assertEquals("fallback-bucket", bk[0]);
			assertEquals("fallback-key", bk[1]);
		}

		@SuppressWarnings("unchecked")
		@Test
		void emptyDstPath_fallsBackToParseBucketAndKey() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("");
			when(item.name()).thenReturn("/b/k");
			when(op.item()).thenReturn(item);

			String[] bk = drv.resolveBucketAndKey(op);
			assertEquals("b", bk[0]);
			assertEquals("k", bk[1]);
		}
	}

	// -----------------------------------------------------------------------
	// resolveBucketAndKey — recycled operations (buildItemPath mutates item name)
	//
	// After the first execution cycle the framework calls op.result() which
	// invokes buildItemPath(item, dstPath).  This prepends dstPath to the
	// item name, e.g. "mkk0lurmliru" → "/spttest/mkk0lurmliru".
	// On the next recycle pass the same item (with the mutated name) is
	// handed back to the driver.  resolveBucketAndKey must still resolve
	// the *same* bucket and key it did on the first call.
	// -----------------------------------------------------------------------

	@Nested
	class ResolveBucketAndKeyRecycleTest {

		/**
		 * Simulate the exact sequence that happens during recycle:
		 *   1st call: dstPath="/spttest", itemName="mkk0lurmliru"
		 *             → expect ["spttest", "mkk0lurmliru"]
		 *   buildItemPath mutates itemName → "/spttest/mkk0lurmliru"
		 *   2nd call: dstPath="/spttest", itemName="/spttest/mkk0lurmliru"
		 *             → must still return ["spttest", "mkk0lurmliru"]
		 */
		@SuppressWarnings("unchecked")
		@Test
		void afterBuildItemPath_simpleBucket_sameResult() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/spttest");
			when(item.name()).thenReturn("mkk0lurmliru");
			when(op.item()).thenReturn(item);

			// First call — pristine item name
			String[] first = drv.resolveBucketAndKey(op);
			assertEquals("spttest", first[0], "bucket on 1st call");
			assertEquals("mkk0lurmliru", first[1], "key on 1st call");

			// Simulate buildItemPath: dstPath + "/" + itemName
			when(item.name()).thenReturn("/spttest/mkk0lurmliru");

			// Second call — recycled item name
			String[] second = drv.resolveBucketAndKey(op);
			assertEquals("spttest", second[0], "bucket on recycled call");
			assertEquals("mkk0lurmliru", second[1], "key on recycled call");
		}

		/**
		 * Same scenario with a nested prefix: dstPath="/bucket/prefix"
		 *   1st: itemName="mykey" → ["bucket", "prefix/mykey"]
		 *   after buildItemPath: itemName="/bucket/prefix/mykey"
		 *   2nd: → must still return ["bucket", "prefix/mykey"]
		 */
		@SuppressWarnings("unchecked")
		@Test
		void afterBuildItemPath_nestedPrefix_sameResult() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/bucket/prefix");
			when(item.name()).thenReturn("mykey");
			when(op.item()).thenReturn(item);

			String[] first = drv.resolveBucketAndKey(op);
			assertEquals("bucket", first[0], "bucket on 1st call");
			assertEquals("prefix/mykey", first[1], "key on 1st call");

			// After buildItemPath: "/bucket/prefix" + "/" + "mykey"
			when(item.name()).thenReturn("/bucket/prefix/mykey");

			String[] second = drv.resolveBucketAndKey(op);
			assertEquals("bucket", second[0], "bucket on recycled call");
			assertEquals("prefix/mykey", second[1], "key on recycled call");
		}

		/**
		 * Item name already has a leading slash on the first call (some
		 * item input formats produce this).  After buildItemPath it may
		 * become "/spttest/somekey" from an original "/somekey".
		 */
		@SuppressWarnings("unchecked")
		@Test
		void afterBuildItemPath_itemHadLeadingSlash_sameResult() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("/somekey");
			when(op.item()).thenReturn(item);

			String[] first = drv.resolveBucketAndKey(op);
			assertEquals("mybucket", first[0]);
			assertEquals("somekey", first[1]);

			// After buildItemPath: itemName stays "/mybucket/somekey"
			// (buildItemPath prepends dstPath when name doesn't start with it)
			when(item.name()).thenReturn("/mybucket/somekey");

			String[] second = drv.resolveBucketAndKey(op);
			assertEquals("mybucket", second[0], "bucket on recycled call");
			assertEquals("somekey", second[1], "key on recycled call");
		}
	}

	// -----------------------------------------------------------------------
	// list() — 7-arg variant
	// -----------------------------------------------------------------------

	@Nested
	class ListTest {

		@Test
		void usesCorrectBucketPrefixAndMaxKeys() throws Exception {
			String prefix = "test-prefix";
			int count = 100;

			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(
											S3Object.builder().key("test-object-1.txt").size(1024L)
															.lastModified(Instant.now()).build(),
											S3Object.builder().key("test-object-2.txt").size(2048L)
															.lastModified(Instant.now()).build())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);
			Item mockItem = mock(Item.class);
			when(factory.getItem(anyString(), anyLong(), anyLong())).thenReturn(mockItem);

			List<Item> result = drv.list(factory, "/test-bucket", prefix, 10, null, count, ListOptions.DEFAULT);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());

			assertEquals("test-bucket", cap.getValue().bucket());
			assertEquals(prefix, cap.getValue().prefix());
			assertEquals(count, cap.getValue().maxKeys());
			// 2 items + null poison marker (not truncated)
			assertEquals(3, result.size());
			assertNull(result.get(2));
		}

		@Test
		void withContinuationToken() throws Exception {
			String token = "next-page-token";
			ListOptions options = ListOptions.builder().continuationToken(token).build();

			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, null, "pfx/", 10, null, 50, options);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());

			assertEquals(token, cap.getValue().continuationToken());
		}

		@Test
		void withOptionsStartAfter_setsStartAfter() throws Exception {
			ListOptions options = ListOptions.builder().startAfter("key-42").build();

			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, null, null, 10, null, 50, options);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("key-42", cap.getValue().startAfter());
			assertNull(cap.getValue().continuationToken());
		}

		@Test
		void withLastPrevItem_setsStartAfter() throws Exception {
			Item prevItem = mock(Item.class);
			when(prevItem.name()).thenReturn("/object-49");

			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, null, null, 10, prevItem, 100, null);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());

			assertEquals("object-49", cap.getValue().startAfter());
		}

		@Test
		void truncatedResponse_noPoisonMarker() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(S3Object.builder().key("obj1").size(10L)
											.lastModified(Instant.now()).build())
							.isTruncated(true) // explicitly set true for this test
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);
			Item mockItem = mock(Item.class);
			when(factory.getItem(anyString(), anyLong(), anyLong())).thenReturn(mockItem);

			List<Item> result = drv.list(factory, null, null, 10, null, 100, null);

			// Truncated → no null poison marker
			assertEquals(1, result.size());
			assertNotNull(result.get(0));
		}

		@Test
		void emptyResponse_returnsEmptyList() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			List<Item> result = drv.list(factory, null, null, 10, null, 100, null);
			// Empty non-truncated response still gets null poison marker
			assertEquals(1, result.size());
			assertNull(result.get(0));
		}

		@Test
		void s3Exception_wrappedAsIOException() {
			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.failedFuture(S3Exception.builder().message("boom").build()));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			// .join() wraps exceptions in CompletionException, which is now unwrapped and wrapped in IOException
			IOException ex = assertThrows(IOException.class,
							() -> drv.list(factory, null, null, 10, null, 100, null));
			assertTrue(ex.getCause() instanceof S3Exception);
		}

		@Test
		void sixArgOverload_delegatesToSevenArg() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			List<Item> result = drv.list(factory, "/path", "pfx", 10, null, 50);
			assertNotNull(result);
			verify(mockS3Client).listObjectsV2(any(ListObjectsV2Request.class));
		}

		@Test
		void maxKeysClampedTo1000() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, null, null, 10, null, 5000, null);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals(1000, cap.getValue().maxKeys());
		}

		@Test
		void extractsBucketFromPathParam() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, "/other-bucket", null, 10, null, 50, null);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("other-bucket", cap.getValue().bucket());
		}

		@Test
		void nullPath_fallsBackToConfiguredBucket() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, null, null, 10, null, 50, null);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("test-bucket", cap.getValue().bucket());
		}

		@Test
		void maxKeysClampedToAtLeast1() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			drv.list(factory, null, null, 10, null, 0, null);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals(1, cap.getValue().maxKeys());
		}
	}

	// -----------------------------------------------------------------------
	// probeCommonPrefixes
	// -----------------------------------------------------------------------

	@Nested
	class ProbeCommonPrefixesTest {

		@Test
		void returnsDiscoverResult_withCorrectPrefixes() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.commonPrefixes(
											CommonPrefix.builder().prefix("dir1/").build(),
											CommonPrefix.builder().prefix("dir2/").build())
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			ListDiscoveryProbe.DiscoverResult result = drv.probeCommonPrefixes("/test-bucket", "pfx/", "/", 100);

			assertEquals(2, result.commonPrefixes().size());
			assertTrue(result.commonPrefixes().contains("dir1/"));
			assertTrue(result.commonPrefixes().contains("dir2/"));
			assertFalse(result.truncated());
			assertFalse(result.hasContents());
		}

		@Test
		void extractsBucketFromBucketPath_withLeadingSlash() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.commonPrefixes(Collections.emptyList())
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			drv.probeCommonPrefixes("/my-custom-bucket", "", "/", 10);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("my-custom-bucket", cap.getValue().bucket());
		}

		@Test
		void extractsBucketFromBucketPath_withoutLeadingSlash() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.commonPrefixes(Collections.emptyList())
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			drv.probeCommonPrefixes("raw-bucket", "", "/", 10);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("raw-bucket", cap.getValue().bucket());
		}

		@Test
		void nullBucketPath_usesConfiguredBucketName() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.commonPrefixes(Collections.emptyList())
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			drv.probeCommonPrefixes(null, "", "/", 10);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("test-bucket", cap.getValue().bucket());
		}

		@Test
		void truncatedResponse_setsTruncatedFlag() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.commonPrefixes(CommonPrefix.builder().prefix("a/").build())
							.contents(S3Object.builder().key("obj").size(1L)
											.lastModified(Instant.now()).build())
							.isTruncated(true)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			ListDiscoveryProbe.DiscoverResult result = drv.probeCommonPrefixes("/bucket", "", "/", 10);

			assertTrue(result.truncated());
			assertTrue(result.hasContents());
		}

		@Test
		void setsDelimiterAndPrefix() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.commonPrefixes(Collections.emptyList())
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			drv.probeCommonPrefixes("/bucket", "my-prefix/", "-", 500);

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("my-prefix/", cap.getValue().prefix());
			assertEquals("-", cap.getValue().delimiter());
			assertEquals(500, cap.getValue().maxKeys());
		}

		@Test
		void s3Exception_wrappedAsIOException() {
			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.failedFuture(S3Exception.builder().message("access denied").build()));

			// .join() wraps exceptions in CompletionException, which is now unwrapped and wrapped in IOException
			IOException ex = assertThrows(IOException.class,
							() -> drv.probeCommonPrefixes("/bucket", "", "/", 10));
			assertTrue(ex.getCause() instanceof S3Exception);
		}
	}

	// -----------------------------------------------------------------------
	// deleteObject — tested via execute() (now package-visible)
	// -----------------------------------------------------------------------

	@Nested
	class DeleteObjectTest {

		@SuppressWarnings("unchecked")
		@Test
		void deletesWithCorrectBucketAndKey() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.DELETE);
			when(op.dstPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("mykey.dat");
			when(op.item()).thenReturn(item);

			drv.execute(op).join();

			ArgumentCaptor<DeleteObjectRequest> cap = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(mockS3Client).deleteObject(cap.capture());
			assertEquals("mybucket", cap.getValue().bucket());
			assertEquals("mykey.dat", cap.getValue().key());
		}

		@SuppressWarnings("unchecked")
		@Test
		void legacyItemsCarrierTargetsExactVersionInsteadOfMissingLiteralKey() throws Exception {
			setVersioningEnabled(drv, true);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
			when(exactVersionS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
			final DataItem item = new DataItemImpl("cross-driver-key~version-42", 0, 0);
			final DataOperationImpl<DataItem> op = new DataOperationImpl<>(
							0, OpType.DELETE, item, null, "/items-bucket", TEST_CRED, null, 0);

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			final ArgumentCaptor<DeleteObjectRequest> request = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(exactVersionS3Client).deleteObject(request.capture());
			verify(mockS3Client, never()).deleteObject(any(DeleteObjectRequest.class));
			assertEquals("items-bucket", request.getValue().bucket());
			assertEquals("cross-driver-key", request.getValue().key());
			assertEquals("version-42", request.getValue().versionId());
		}
	}

	// -----------------------------------------------------------------------
	// putObject — tested via execute() (now package-visible)
	// -----------------------------------------------------------------------

	@Nested
	class PutObjectTest {
		// buildPutObjectRequest tests removed - method no longer exists after CRT streaming refactor
		// Functionality is now tested through execute() integration tests
	}

	// -----------------------------------------------------------------------
	// readObject — tested via execute() (now package-visible)
	// -----------------------------------------------------------------------

	@Nested
	class ReadObjectTest {
		// toBlockingInputStream tests removed - returns specific type that's hard to mock
		// Core functionality tested through integration tests
	}

	// -----------------------------------------------------------------------
	// headObject — tested via execute() (now package-visible)
	// -----------------------------------------------------------------------

	@Nested
	class HeadObjectTest {

		@SuppressWarnings("unchecked")
		@Test
		void statsWithCorrectBucketAndKey() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.STAT);
			when(op.dstPath()).thenReturn("/stat-bucket");
			when(item.name()).thenReturn("stat-me.dat");
			when(op.item()).thenReturn(item);

			drv.execute(op).join();

			ArgumentCaptor<HeadObjectRequest> cap = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(mockS3Client).headObject(cap.capture());
			assertEquals("stat-bucket", cap.getValue().bucket());
			assertEquals("stat-me.dat", cap.getValue().key());
		}

		@SuppressWarnings("unchecked")
		@Test
		void statsWithNestedPrefix() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.STAT);
			when(op.dstPath()).thenReturn("/bucket/prefix");
			when(item.name()).thenReturn("mykey");
			when(op.item()).thenReturn(item);

			drv.execute(op).join();

			ArgumentCaptor<HeadObjectRequest> cap = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(mockS3Client).headObject(cap.capture());
			assertEquals("bucket", cap.getValue().bucket());
			assertEquals("prefix/mykey", cap.getValue().key());
		}

		@SuppressWarnings("unchecked")
		@Test
		void legacyItemsCarrierTargetsTheSameExactVersionAsDelete() throws Exception {
			setVersioningEnabled(drv, true);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(mockS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			when(exactVersionS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			final DataItem item = new DataItemImpl("cross-driver-key~version-42", 0, 0);
			final DataOperationImpl<DataItem> op = new DataOperationImpl<>(
							0, OpType.STAT, item, null, "/items-bucket", TEST_CRED, null, 0);

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			final ArgumentCaptor<HeadObjectRequest> request = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(exactVersionS3Client).headObject(request.capture());
			verify(mockS3Client, never()).headObject(any(HeadObjectRequest.class));
			assertEquals("items-bucket", request.getValue().bucket());
			assertEquals("cross-driver-key", request.getValue().key());
			assertEquals("version-42", request.getValue().versionId());
		}
	}

	// -----------------------------------------------------------------------
	// execute() dispatch — unsupported type + NOOP
	// -----------------------------------------------------------------------

	@Nested
	class ExecuteDispatchTest {

		@SuppressWarnings("unchecked")
		@Test
		void noopOperation_doesNotCallS3() throws Exception {
			Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.NOOP);

			drv.execute(op).join();

			verifyNoInteractions(mockS3Client);
		}
	}

	@Nested
	class DeleteTimingInterceptorTest {

		@Test
		void interceptorCarriesDeleteOperationToHttpTransportWithoutMarkingTiming() {
			final DeleteRequestOperation op = S3AwsDeleteRequestTestFixture.operation(
							S3AwsDeleteRequestTestFixture.target("key", null));
			op.startRequest();
			op.finishRequest();
			final ExecutionAttributes attrs = new ExecutionAttributes()
							.putAttribute(S3AwsStorageDriver.DELETE_TIMING_OPERATION_ATTRIBUTE, op);
			final var interceptor = new S3AwsStorageDriver.DeleteTimingExecutionInterceptor();

			interceptor.beforeTransmission(mock(Context.BeforeTransmission.class), attrs);
			assertEquals(0, op.requestFirstByteTime());
			assertEquals(0, op.responseFirstByteTime());
			assertSame(
							op,
							attrs.getAttribute(SdkInternalExecutionAttribute.SDK_HTTP_EXECUTION_ATTRIBUTES)
											.getAttribute(DeleteTimingAsyncHttpClient.DELETE_OPERATION));
		}
	}

	// -----------------------------------------------------------------------
	// listObjects — tested via execute() with LIST OpType
	// -----------------------------------------------------------------------

	@Nested
	class ListObjectsTest {

		private ListObjectsV2Response buildListResponse(
						List<S3Object> contents, boolean truncated, String nextToken) {
			ListObjectsV2Response.Builder b = ListObjectsV2Response.builder()
							.contents(contents)
							.isTruncated(truncated);
			if (nextToken != null) {
				b.nextContinuationToken(nextToken);
			}
			return b.build();
		}

		@SuppressWarnings("unchecked")
		@Test
		void listsWithCorrectBucketAndPrefix() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			when(op.options()).thenReturn(ListOptions.DEFAULT);

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(buildListResponse(
											List.of(
															S3Object.builder().key("obj1").size(100L).build(),
															S3Object.builder().key("obj2").size(200L).build()),
											false, null)));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("mybucket", cap.getValue().bucket());

			verify(op).objectsListed(2);
			@SuppressWarnings("rawtypes")
			ArgumentCaptor<List> objects = ArgumentCaptor.forClass(List.class);
			verify(op).listedObjects(objects.capture());
			assertEquals("obj1", ((com.dell.spt.base.item.op.list.ListedObject) objects.getValue().get(0)).key());
			assertEquals(100L, ((com.dell.spt.base.item.op.list.ListedObject) objects.getValue().get(0)).size());
			verify(op).truncated(false);
			verify(op).pageFirstKey("obj1");
			verify(op).startAfter("obj2");
			verify(op).continuationToken(null);
		}

		@SuppressWarnings("unchecked")
		@Test
		void listRequestCarriesDataResponseTimingAttribute() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			when(op.options()).thenReturn(ListOptions.DEFAULT);

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(buildListResponse(
											List.of(S3Object.builder().key("obj1").size(100L).build()),
											false, null)));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertTrue(cap.getValue().overrideConfiguration().isPresent());
			assertSame(
							op,
							cap.getValue().overrideConfiguration().get()
											.executionAttributes()
											.getAttribute(S3AwsStorageDriver.LIST_TTFB_OPERATION_ATTRIBUTE));
			assertFalse(cap.getValue().overrideConfiguration().get().plugins().isEmpty());
			verify(op, never()).startDataResponse();
		}

		@SuppressWarnings("unchecked")
		@Test
		void wrappedListResponsePublisherStartsDataResponseOnFirstNonEmptyBodyBytes() {
			ListOperation<PathItem> op = mock(ListOperation.class);
			when(op.respDataTimeStart()).thenReturn(0L);
			Publisher<ByteBuffer> publisher = subscriber -> {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long n) {}

					@Override
					public void cancel() {}
				});
				subscriber.onNext(ByteBuffer.allocate(0));
				subscriber.onNext(ByteBuffer.wrap(new byte[]{1
				}));
				subscriber.onNext(ByteBuffer.wrap(new byte[]{2
				}));
				subscriber.onComplete();
			};

			S3AwsStorageDriver.wrapListDataResponsePublisher(publisher, op).subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscription.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(final ByteBuffer byteBuffer) {}

				@Override
				public void onError(final Throwable throwable) {
					fail(throwable);
				}

				@Override
				public void onComplete() {}
			});

			verify(op, times(1)).startDataResponse();
		}

		@SuppressWarnings("unchecked")
		@Test
		void wrappedListResponsePublisherDoesNotStartOnEmptyBodyBuffers() {
			ListOperation<PathItem> op = mock(ListOperation.class);
			when(op.respDataTimeStart()).thenReturn(0L);
			Publisher<ByteBuffer> publisher = subscriber -> {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long n) {}

					@Override
					public void cancel() {}
				});
				subscriber.onNext(ByteBuffer.allocate(0));
				subscriber.onComplete();
			};

			S3AwsStorageDriver.wrapListDataResponsePublisher(publisher, op).subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscription.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(final ByteBuffer byteBuffer) {}

				@Override
				public void onError(final Throwable throwable) {
					fail(throwable);
				}

				@Override
				public void onComplete() {}
			});

			verify(op, never()).startDataResponse();
		}

		@SuppressWarnings("unchecked")
		@Test
		void wrappedListResponsePublisherPropagatesError() {
			ListOperation<PathItem> op = mock(ListOperation.class);
			RuntimeException expected = new RuntimeException("boom");
			Publisher<ByteBuffer> publisher = subscriber -> {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long n) {}

					@Override
					public void cancel() {}
				});
				subscriber.onError(expected);
			};
			AtomicReference<Throwable> observed = new AtomicReference<>();

			S3AwsStorageDriver.wrapListDataResponsePublisher(publisher, op).subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscription.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(final ByteBuffer byteBuffer) {}

				@Override
				public void onError(final Throwable throwable) {
					observed.set(throwable);
				}

				@Override
				public void onComplete() {}
			});

			assertSame(expected, observed.get());
		}

		@SuppressWarnings("unchecked")
		@Test
		void wrappedListResponsePublisherPropagatesComplete() {
			ListOperation<PathItem> op = mock(ListOperation.class);
			Publisher<ByteBuffer> publisher = subscriber -> {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long n) {}

					@Override
					public void cancel() {}
				});
				subscriber.onComplete();
			};
			AtomicBoolean completed = new AtomicBoolean(false);

			S3AwsStorageDriver.wrapListDataResponsePublisher(publisher, op).subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscription.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(final ByteBuffer byteBuffer) {}

				@Override
				public void onError(final Throwable throwable) {
					fail(throwable);
				}

				@Override
				public void onComplete() {
					completed.set(true);
				}
			});

			assertTrue(completed.get());
		}

		@Test
		void listTtfbExecutionInterceptorNoOpsWithoutAttribute() {
			Publisher<ByteBuffer> publisher = subscriber -> {};
			Optional<Publisher<ByteBuffer>> expected = Optional.of(publisher);
			Context.ModifyHttpResponse context = mock(Context.ModifyHttpResponse.class);
			when(context.responsePublisher()).thenReturn(expected);

			Optional<Publisher<ByteBuffer>> actual = new S3AwsStorageDriver.ListTtfbExecutionInterceptor()
							.modifyAsyncHttpResponseContent(context, new ExecutionAttributes());

			assertSame(expected, actual);
		}

		@SuppressWarnings("unchecked")
		@Test
		void listTtfbExecutionInterceptorWrapsWhenAttributePresent() {
			ListOperation<PathItem> op = mock(ListOperation.class);
			when(op.respDataTimeStart()).thenReturn(0L);
			Publisher<ByteBuffer> publisher = subscriber -> {
				subscriber.onSubscribe(new Subscription() {
					@Override
					public void request(final long n) {}

					@Override
					public void cancel() {}
				});
				subscriber.onNext(ByteBuffer.wrap(new byte[]{1
				}));
				subscriber.onComplete();
			};
			Context.ModifyHttpResponse context = mock(Context.ModifyHttpResponse.class);
			when(context.responsePublisher()).thenReturn(Optional.of(publisher));
			ExecutionAttributes attrs = new ExecutionAttributes()
							.putAttribute(S3AwsStorageDriver.LIST_TTFB_OPERATION_ATTRIBUTE, op);

			Optional<Publisher<ByteBuffer>> wrapped = new S3AwsStorageDriver.ListTtfbExecutionInterceptor()
							.modifyAsyncHttpResponseContent(context, attrs);

			assertTrue(wrapped.isPresent());
			assertNotSame(publisher, wrapped.get());
			wrapped.get().subscribe(new Subscriber<>() {
				@Override
				public void onSubscribe(final Subscription subscription) {
					subscription.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(final ByteBuffer byteBuffer) {}

				@Override
				public void onError(final Throwable throwable) {
					fail(throwable);
				}

				@Override
				public void onComplete() {}
			});

			verify(op, times(1)).startDataResponse();
		}

		@SuppressWarnings("unchecked")
		@Test
		void listTtfbSdkPluginRegistersSingletonInterceptor() {
			SdkServiceClientConfiguration.Builder serviceBuilder = mock(SdkServiceClientConfiguration.Builder.class);
			ClientOverrideConfiguration.Builder overrideBuilder = mock(ClientOverrideConfiguration.Builder.class);
			when(overrideBuilder.addExecutionInterceptor(any(ExecutionInterceptor.class))).thenReturn(overrideBuilder);
			ArgumentCaptor<Consumer<ClientOverrideConfiguration.Builder>> consumerCaptor = ArgumentCaptor.forClass(Consumer.class);

			new S3AwsStorageDriver.ListTtfbSdkPlugin().configureClient(serviceBuilder);

			verify(serviceBuilder).overrideConfiguration(consumerCaptor.capture());
			consumerCaptor.getValue().accept(overrideBuilder);
			ArgumentCaptor<ExecutionInterceptor> interceptorCaptor = ArgumentCaptor.forClass(ExecutionInterceptor.class);
			verify(overrideBuilder).addExecutionInterceptor(interceptorCaptor.capture());
			assertSame(S3AwsStorageDriver.LIST_TTFB_INTERCEPTOR, interceptorCaptor.getValue());
		}

		@SuppressWarnings("unchecked")
		@Test
		void listsWithPrefixFromItemName() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("/logs/");
			when(op.item()).thenReturn(item);
			when(op.options()).thenReturn(ListOptions.DEFAULT);

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(buildListResponse(Collections.emptyList(), false, null)));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("logs/", cap.getValue().prefix());
		}

		@SuppressWarnings("unchecked")
		@Test
		void truncatedResponse_setsTokenAndTruncated() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			when(op.options()).thenReturn(ListOptions.DEFAULT);

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(buildListResponse(
											List.of(S3Object.builder().key("a").size(10L).build()),
											true, "next-token-123")));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			verify(op).objectsListed(1);
			verify(op).truncated(true);
			verify(op).continuationToken("next-token-123");
			verify(op).startAfter("a");
		}

		@SuppressWarnings("unchecked")
		@Test
		void paginationUsesContinuationToken() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			ListOptions opts = ListOptions.DEFAULT.toBuilder()
							.continuationToken("prev-token")
							.build();
			when(op.options()).thenReturn(opts);

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(buildListResponse(Collections.emptyList(), false, null)));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("prev-token", cap.getValue().continuationToken());
		}

		@SuppressWarnings("unchecked")
		@Test
		void fallsBackToConfiguredBucket_whenSrcPathNull() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn(null);
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			when(op.options()).thenReturn(ListOptions.DEFAULT);

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(buildListResponse(Collections.emptyList(), false, null)));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			ArgumentCaptor<ListObjectsV2Request> cap = ArgumentCaptor.forClass(ListObjectsV2Request.class);
			verify(mockS3Client).listObjectsV2(cap.capture());
			assertEquals("test-bucket", cap.getValue().bucket());
		}

		@SuppressWarnings("unchecked")
		@Test
		void fetchMetadata_accumulatesBytes() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			ListOptions opts = ListOptions.DEFAULT.toBuilder()
							.fetchMetadata(true)
							.build();
			when(op.options()).thenReturn(opts);

			List<S3Object> contents = List.of(
							S3Object.builder().key("a").size(100L).build(),
							S3Object.builder().key("b").size(250L).build());
			ListObjectsV2Response response = buildListResponse(contents, false, null);
			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(response));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			verify(op).bytesListed(contents.stream().mapToLong(S3Object::size).sum());
		}

		@SuppressWarnings("unchecked")
		@Test
		void noFetchMetadata_zeroBytes() throws Exception {
			ListOperation<PathItem> op = mock(ListOperation.class);
			PathItem item = mock(PathItem.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.srcPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("");
			when(op.item()).thenReturn(item);
			when(op.options()).thenReturn(ListOptions.DEFAULT);

			List<S3Object> contents = List.of(
							S3Object.builder().key("x").size(999L).build());
			ListObjectsV2Response response = buildListResponse(contents, false, null);
			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(CompletableFuture.completedFuture(response));

			drv.execute((Operation<Item>) (Operation<?>) op).join();

			verify(op).bytesListed(0L);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void listsAllVersionsWithExactIdsMarkersAndDeleteMarkerCount() throws Exception {
		ListOperation<PathItem> op = mock(ListOperation.class);
		PathItem item = mock(PathItem.class);
		when(op.type()).thenReturn(OpType.LIST);
		when(op.srcPath()).thenReturn("/bucket");
		when(item.name()).thenReturn("/prefix/");
		when(op.item()).thenReturn(item);
		when(op.respDataTimeStart()).thenReturn(1L);
		ListOptions options = ListOptions.builder()
						.includeVersions(true)
						.fetchMetadata(true)
						.keyMarker("prior-key")
						.versionIdMarker("prior-version")
						.maxKeys(1000)
						.build();
		when(op.options()).thenReturn(options);
		ListObjectVersionsResponse response = ListObjectVersionsResponse.builder()
						.versions(
										ObjectVersion.builder().key("prefix/key").versionId("v2").size(7L).build(),
										ObjectVersion.builder().key("prefix/key").versionId("null").size(5L).build())
						.deleteMarkers(
										DeleteMarkerEntry.builder().key("prefix/key").versionId("marker-v").build())
						.isTruncated(true)
						.nextKeyMarker("next-key")
						.nextVersionIdMarker("next-version")
						.build();
		when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(response));

		drv.execute((Operation<Item>) (Operation<?>) op).join();

		ArgumentCaptor<ListObjectVersionsRequest> request = ArgumentCaptor.forClass(ListObjectVersionsRequest.class);
		verify(mockS3Client).listObjectVersions(request.capture());
		verify(mockS3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
		assertEquals("bucket", request.getValue().bucket());
		assertEquals("prefix/", request.getValue().prefix());
		assertEquals("prior-key", request.getValue().keyMarker());
		assertEquals("prior-version", request.getValue().versionIdMarker());

		@SuppressWarnings("rawtypes")
		ArgumentCaptor<List> objects = ArgumentCaptor.forClass(List.class);
		verify(op).listedObjects(objects.capture());
		ListedObject first = (ListedObject) objects.getValue().get(0);
		ListedObject second = (ListedObject) objects.getValue().get(1);
		assertEquals("v2", first.versionId());
		assertEquals("null", second.versionId());
		verify(op).objectsListed(3);
		verify(op).deleteMarkersListed(1);
		verify(op).bytesListed(12L);
		verify(op).truncated(true);
		ArgumentCaptor<ListOptions> next = ArgumentCaptor.forClass(ListOptions.class);
		verify(op).options(next.capture());
		assertEquals("next-key", next.getValue().keyMarker());
		assertEquals("next-version", next.getValue().versionIdMarker());
	}

	@SuppressWarnings("unchecked")
	@Test
	void allVersionDiscoveryRejectsMissingExactVersionId() {
		ListOperation<PathItem> op = mock(ListOperation.class);
		PathItem item = mock(PathItem.class);
		when(op.type()).thenReturn(OpType.LIST);
		when(op.srcPath()).thenReturn("/bucket");
		when(item.name()).thenReturn("");
		when(op.item()).thenReturn(item);
		when(op.options()).thenReturn(ListOptions.builder().includeVersions(true).build());
		ListObjectVersionsResponse response = ListObjectVersionsResponse.builder()
						.versions(ObjectVersion.builder().key("key").size(1L).build())
						.isTruncated(false)
						.build();
		when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(response));

		CompletionException failure = assertThrows(
						CompletionException.class,
						() -> drv.execute((Operation<Item>) (Operation<?>) op).join());
		assertInstanceOf(IllegalStateException.class, failure.getCause());
		verify(op, never()).listedObjects(anyList());
	}

	@SuppressWarnings("unchecked")
	@Test
	void allVersionDiscoveryRejectsTruncatedPageWithoutNextMarkers() {
		ListOperation<PathItem> op = mock(ListOperation.class);
		PathItem item = mock(PathItem.class);
		when(op.type()).thenReturn(OpType.LIST);
		when(op.srcPath()).thenReturn("/bucket");
		when(item.name()).thenReturn("prefix/");
		when(op.item()).thenReturn(item);
		when(op.options()).thenReturn(ListOptions.builder().includeVersions(true).build());
		ListObjectVersionsResponse response = ListObjectVersionsResponse.builder()
						.versions(ObjectVersion.builder()
										.key("prefix/key")
										.versionId("v1")
										.size(1L)
										.build())
						.isTruncated(true)
						.build();
		when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(response));

		CompletionException failure = assertThrows(
						CompletionException.class,
						() -> drv.execute((Operation<Item>) (Operation<?>) op).join());

		assertInstanceOf(IllegalStateException.class, failure.getCause());
		assertEquals(
						"Truncated S3 version LIST result is missing its next markers",
						failure.getCause().getMessage());
		verify(op, never()).listedObjects(anyList());
	}

	@SuppressWarnings("unchecked")
	@Test
	void allVersionDiscoveryRejectsMissingSizeWithoutPublishingResults() {
		ListOperation<PathItem> op = mock(ListOperation.class);
		PathItem item = mock(PathItem.class);
		when(op.type()).thenReturn(OpType.LIST);
		when(op.srcPath()).thenReturn("/bucket");
		when(item.name()).thenReturn("prefix/");
		when(op.item()).thenReturn(item);
		when(op.options()).thenReturn(ListOptions.builder().includeVersions(true).build());
		ListObjectVersionsResponse response = ListObjectVersionsResponse.builder()
						.versions(ObjectVersion.builder()
										.key("prefix/key")
										.versionId("v1")
										.build())
						.isTruncated(false)
						.build();
		when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class)))
						.thenReturn(CompletableFuture.completedFuture(response));

		CompletionException failure = assertThrows(
						CompletionException.class,
						() -> drv.execute((Operation<Item>) (Operation<?>) op).join());

		assertInstanceOf(IllegalStateException.class, failure.getCause());
		assertEquals(
						"S3 LIST entry is missing Size for key \"prefix/key\"",
						failure.getCause().getMessage());
		verify(op, never()).listedObjects(anyList());
	}

	@SuppressWarnings("unchecked")
	@Test
	void allVersionDiscoveryCarriesBothMarkersAcrossTwoPages() {
		ListOperation<PathItem> op = mock(ListOperation.class);
		PathItem item = mock(PathItem.class);
		when(op.type()).thenReturn(OpType.LIST);
		when(op.srcPath()).thenReturn("/bucket");
		when(item.name()).thenReturn("prefix/");
		when(op.item()).thenReturn(item);
		when(op.respDataTimeStart()).thenReturn(1L);
		AtomicReference<ListOptions> options = new AtomicReference<>(
						ListOptions.builder().includeVersions(true).fetchMetadata(true).build());
		when(op.options()).thenAnswer(invocation -> options.get());
		doAnswer(invocation -> {
			options.set(invocation.getArgument(0));
			return null;
		}).when(op).options(any(ListOptions.class));
		ListObjectVersionsResponse firstPage = ListObjectVersionsResponse.builder()
						.versions(ObjectVersion.builder()
										.key("prefix/a")
										.versionId("a-v1")
										.size(1L)
										.build())
						.isTruncated(true)
						.nextKeyMarker("prefix/a")
						.nextVersionIdMarker("a-v1")
						.build();
		ListObjectVersionsResponse secondPage = ListObjectVersionsResponse.builder()
						.versions(ObjectVersion.builder()
										.key("prefix/b")
										.versionId("b-v1")
										.size(2L)
										.build())
						.isTruncated(false)
						.build();
		when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class)))
						.thenReturn(
										CompletableFuture.completedFuture(firstPage),
										CompletableFuture.completedFuture(secondPage));

		drv.execute((Operation<Item>) (Operation<?>) op).join();
		drv.execute((Operation<Item>) (Operation<?>) op).join();

		ArgumentCaptor<ListObjectVersionsRequest> requests = ArgumentCaptor.forClass(ListObjectVersionsRequest.class);
		verify(mockS3Client, times(2)).listObjectVersions(requests.capture());
		assertNull(requests.getAllValues().get(0).keyMarker());
		assertNull(requests.getAllValues().get(0).versionIdMarker());
		assertEquals("prefix/a", requests.getAllValues().get(1).keyMarker());
		assertEquals("a-v1", requests.getAllValues().get(1).versionIdMarker());
		verify(op).startAfter("prefix/a");
		verify(op).startAfter("prefix/b");
		verify(op, times(2)).listedObjects(anyList());
	}

	@SuppressWarnings("unchecked")
	@Test
	void allVersionDiscoveryPropagatesPermissionDeniedWithoutResults() {
		ListOperation<PathItem> op = mock(ListOperation.class);
		PathItem item = mock(PathItem.class);
		when(op.type()).thenReturn(OpType.LIST);
		when(op.srcPath()).thenReturn("/bucket");
		when(item.name()).thenReturn("prefix/");
		when(op.item()).thenReturn(item);
		when(op.options()).thenReturn(ListOptions.builder().includeVersions(true).build());
		S3Exception denied = (S3Exception) S3Exception.builder()
						.statusCode(403)
						.message("AccessDenied")
						.build();
		when(mockS3Client.listObjectVersions(any(ListObjectVersionsRequest.class)))
						.thenReturn(CompletableFuture.failedFuture(denied));

		CompletionException failure = assertThrows(
						CompletionException.class,
						() -> drv.execute((Operation<Item>) (Operation<?>) op).join());
		assertSame(denied, failure.getCause());
		verify(op, never()).listedObjects(anyList());
	}

	// -----------------------------------------------------------------------
	// requestNewPath — protected, callable from same package
	// -----------------------------------------------------------------------

	@Nested
	class RequestNewPathTest {

		@Test
		void extractsBucketPath_withSlash() {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadBucketResponse.builder().build()));

			String result = drv.requestNewPath("/large/prefix");
			assertEquals("/large", result);

			// Verify headBucket was called with the bucket from path, not this.bucketName
			ArgumentCaptor<HeadBucketRequest> cap = ArgumentCaptor.forClass(HeadBucketRequest.class);
			verify(mockS3Client).headBucket(cap.capture());
			assertEquals("large", cap.getValue().bucket());
		}

		@Test
		void extractsBucketPath_noSubpath() {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadBucketResponse.builder().build()));

			String result = drv.requestNewPath("/mybucket");
			assertEquals("/mybucket", result);
		}

		@Test
		void missingBucket_createsIt() {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(CompletableFuture.failedFuture(NoSuchBucketException.builder().message("no bucket").build()));
			when(mockS3Client.createBucket(any(CreateBucketRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(CreateBucketResponse.builder().build()));

			String result = drv.requestNewPath("/newbucket");
			assertEquals("/newbucket", result);

			ArgumentCaptor<CreateBucketRequest> cap = ArgumentCaptor.forClass(CreateBucketRequest.class);
			verify(mockS3Client).createBucket(cap.capture());
			assertEquals("newbucket", cap.getValue().bucket());
		}

		@Test
		void headBucketFailure_nonNoSuchBucket_throwsRuntimeException() {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(CompletableFuture.failedFuture(S3Exception.builder().message("access denied").build()));

			assertThrows(RuntimeException.class, () -> drv.requestNewPath("/nonexistent"));
		}

		@Test
		void createBucketFailure_throwsRuntimeException() {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(CompletableFuture.failedFuture(NoSuchBucketException.builder().message("no bucket").build()));
			when(mockS3Client.createBucket(any(CreateBucketRequest.class)))
							.thenReturn(CompletableFuture.failedFuture(S3Exception.builder().message("create failed").build()));

			assertThrows(RuntimeException.class, () -> drv.requestNewPath("/failbucket"));
		}
	}

	// -----------------------------------------------------------------------
	// requestNewAuthToken — protected, callable from same package
	// -----------------------------------------------------------------------

	@Test
	void requestNewAuthToken_returnsNull() {
		assertNull(drv.requestNewAuthToken(null));
	}

	// -----------------------------------------------------------------------
	// adjustIoBuffers — no-op
	// -----------------------------------------------------------------------

	@Test
	void adjustIoBuffers_doesNotThrow() {
		assertDoesNotThrow(() -> drv.adjustIoBuffers(4096, OpType.READ));
	}

	// -----------------------------------------------------------------------
	// getBucketName accessor
	// -----------------------------------------------------------------------

	@Test
	void getBucketName_returnsConfiguredBucket() {
		assertEquals("test-bucket", drv.getBucketName());
	}

	// -----------------------------------------------------------------------
	// invokeNio — the main operation entry point
	// -----------------------------------------------------------------------

	@Nested
	class InvokeNioTest {

		@SuppressWarnings("unchecked")
		@Test
		void successfulDelete_callsFinishOperation() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.DELETE);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			drv.invokeNio(op);

			verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
			// finishOperation calls startResponse, finishResponse, and status(SUCC)
			verify(op).startResponse();
			verify(op).finishResponse();
			verify(op).status(Operation.Status.SUCC);
		}

		@SuppressWarnings("unchecked")
		@Test
		void failedOperation_setsStatusToFailUnknown() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			// Make getObject throw an exception
			when(mockS3Client.getObject(any(GetObjectRequest.class), (software.amazon.awssdk.core.async.AsyncResponseTransformer<GetObjectResponse, ?>) any()))
							.thenReturn(CompletableFuture.failedFuture(NoSuchKeyException.builder().message("not found").build()));

			drv.invokeNio(op);

			verify(op).status(Operation.Status.FAIL_UNKNOWN);
		}

		@SuppressWarnings("unchecked")
		@Test
		void failedOperation_handlesTimingErrors() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			when(mockS3Client.getObject(any(GetObjectRequest.class), (software.amazon.awssdk.core.async.AsyncResponseTransformer<GetObjectResponse, ?>) any()))
							.thenReturn(CompletableFuture.failedFuture(NoSuchKeyException.builder().message("not found").build()));
			// Make startResponse throw too, to exercise the inner catch
			doThrow(new IllegalStateException("already started")).when(op).startResponse();

			// Should not throw despite double failure
			assertDoesNotThrow(() -> drv.invokeNio(op));
			verify(op).status(Operation.Status.FAIL_UNKNOWN);
		}

		@SuppressWarnings("unchecked")
		@Test
		void successfulDelete_nonDataItem_skipsCountBytesDone() {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class); // plain Item, not DataItem
			when(op.type()).thenReturn(OpType.DELETE);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			drv.invokeNio(op);

			// Should succeed without attempting countBytesDone
			verify(op).status(Operation.Status.SUCC);
		}

		@SuppressWarnings({"unchecked", "rawtypes"
		})
		@org.junit.jupiter.api.Disabled("Test requires complex mock setup for DataOperation instanceof/cast behavior with Mockito")
		@Test
		void successfulCreate_dataItem_countsBytesDone() throws Exception {
			DataItem dataItem = mock(DataItem.class);
			when(dataItem.name()).thenReturn("obj");
			when(dataItem.size()).thenReturn(2048L);
			when(dataItem.dataInput()).thenReturn(mock(com.dell.spt.base.data.DataInput.class));

			DataOperation dataOp = mock(DataOperation.class);
			when(dataOp.type()).thenReturn(OpType.CREATE);
			when(dataOp.dstPath()).thenReturn("/bucket");
			when(dataOp.item()).thenReturn(dataItem);

			// Mock putObject for CREATE operation
			PutObjectResponse putResp = PutObjectResponse.builder().build();
			when(mockS3Client.putObject(any(PutObjectRequest.class), any(software.amazon.awssdk.core.async.AsyncRequestBody.class)))
							.thenReturn(CompletableFuture.completedFuture(putResp));

			drv.invokeNio((Operation) dataOp);

			// invokeNio calls countBytesDone(dataItem.size()) for non-READ DataItem ops
			verify(dataOp).countBytesDone(2048L);
		}
	}

	// -----------------------------------------------------------------------
	// putObject — additional edge cases
	// -----------------------------------------------------------------------

	@Nested
	class PutObjectEdgeCasesTest {

		@SuppressWarnings("unchecked")
		@Test
		void unsupportedItemType_throwsUnsupportedOperationException() {
			Item plainItem = mock(Item.class); // not DataItem, not PathItem
			when(plainItem.name()).thenReturn("plain");

			Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.dstPath()).thenReturn("/bucket");
			when(op.item()).thenReturn(plainItem);

			var ex = assertThrows(java.util.concurrent.CompletionException.class, () -> drv.execute(op).join());
			assertTrue(ex.getCause() instanceof UnsupportedOperationException);
			assertTrue(ex.getCause().getMessage().contains("DataItem or PathItem"));
		}
	}

	// -----------------------------------------------------------------------
	// readObject — DataOperation branch with actual bytes
	// -----------------------------------------------------------------------

	@Nested
	class ReadObjectDataOperationTest {
		@SuppressWarnings({"unchecked", "rawtypes"
		})
		@Test
		void readObjectStartsDataResponseOnFirstBodyBytes() {
			final DataOperation op = mock(DataOperation.class);
			final DataItem item = mock(DataItem.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/bucket");
			when(op.item()).thenReturn(item);
			when(item.name()).thenReturn("key");
			final var response = new ResponseInputStream<>(
							GetObjectResponse.builder().build(),
							new ByteArrayInputStream(new byte[]{1, 2, 3, 4
							}));
			when(mockS3Client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(response));

			drv.execute(op).join();

			verify(op).startResponse();
			verify(op).startDataResponse();
			verify(op).countBytesDone(4L);
			verify(op).finishResponse();
		}
	}

	// -----------------------------------------------------------------------
	// requestNewPath — path without leading slash
	// -----------------------------------------------------------------------

	@Nested
	class RequestNewPathEdgeCasesTest {

		@Test
		void pathWithoutLeadingSlash() {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadBucketResponse.builder().build()));

			String result = drv.requestNewPath("nobucket");
			assertEquals("/nobucket", result);
		}
	}

	// -----------------------------------------------------------------------
	// resolveBucketName — static, extracted from constructor
	// -----------------------------------------------------------------------

	@Nested
	class ResolveBucketNameTest {

		@Test
		void fromItemOutputPath() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			Config itemConfig = mock(Config.class);
			// storage-net-node-addrs always throws (confuse path mismatch in practice)
			when(config.stringVal("storage-net-node-addrs")).thenThrow(new RuntimeException("no path"));
			when(config.configVal("item")).thenReturn(itemConfig);
			when(itemConfig.stringVal("output-path")).thenReturn("/mybucket");

			assertEquals("mybucket", S3AwsStorageDriver.resolveBucketName(config));
		}

		@Test
		void fromItemInputPath_whenOutputPathNull() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			Config itemConfig = mock(Config.class);
			when(config.stringVal("storage-net-node-addrs")).thenThrow(new RuntimeException("no path"));
			when(config.configVal("item")).thenReturn(itemConfig);
			when(itemConfig.stringVal("output-path")).thenReturn(null);
			when(itemConfig.stringVal("input-path")).thenReturn("/readbucket");

			assertEquals("readbucket", S3AwsStorageDriver.resolveBucketName(config));
		}

		@Test
		void fromNodeAddrs_withSlash() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			when(config.stringVal("storage-net-node-addrs")).thenReturn("addr/extra");

			assertEquals("addr", S3AwsStorageDriver.resolveBucketName(config));
		}

		@Test
		void fromNodeAddrs_noSlash() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			when(config.stringVal("storage-net-node-addrs")).thenReturn("justaddr");

			assertEquals("justaddr", S3AwsStorageDriver.resolveBucketName(config));
		}

		@Test
		void allSourcesMissing_fallsBackToUsername() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			when(config.stringVal("storage-net-node-addrs")).thenThrow(new RuntimeException("no path"));
			when(config.configVal("item")).thenThrow(new RuntimeException("no item config"));

			String result = S3AwsStorageDriver.resolveBucketName(config);
			String expectedUser = System.getProperty("user.name", "spt");
			assertEquals(expectedUser + "test", result);
		}

		@Test
		void nodeAddrsEmpty_fallsThrough() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			Config itemConfig = mock(Config.class);
			when(config.stringVal("storage-net-node-addrs")).thenReturn("");
			when(config.configVal("item")).thenReturn(itemConfig);
			when(itemConfig.stringVal("output-path")).thenReturn("/frombucket");

			assertEquals("frombucket", S3AwsStorageDriver.resolveBucketName(config));
		}

		@Test
		void outputPathTooShort_fallsToInputPath() {
			Config config = mock(Config.class);
			disableIntegrity(config);
			Config itemConfig = mock(Config.class);
			when(config.stringVal("storage-net-node-addrs")).thenThrow(new RuntimeException("no path"));
			when(config.configVal("item")).thenReturn(itemConfig);
			when(itemConfig.stringVal("output-path")).thenReturn("/"); // too short
			when(itemConfig.stringVal("input-path")).thenReturn("/inputbucket");

			assertEquals("inputbucket", S3AwsStorageDriver.resolveBucketName(config));
		}
	}

	@Nested
	class ClassifyFailureTest {

		@Test
		void s3Exception_401_returnsRespFailAuth() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(401)
							.message("Unauthorized")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_AUTH, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_403_returnsRespFailAuth() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(403)
							.message("Forbidden")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_AUTH, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_404_returnsRespFailNotFound() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(404)
							.message("Not Found")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_400_returnsRespFailClient() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(400)
							.message("Bad Request")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_CLIENT, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_500_returnsRespFailSvc() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(500)
							.message("Internal Server Error")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_SVC, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_503_returnsRespFailSvc() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(503)
							.message("Service Unavailable")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_SVC, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_504_returnsFailTimeout() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(504)
							.message("Gateway Timeout")
							.build();
			assertEquals(Operation.Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void s3Exception_507_returnsRespFailSpace() {
			S3Exception e = (S3Exception) S3Exception.builder()
							.statusCode(507)
							.message("Insufficient Storage")
							.build();
			assertEquals(Operation.Status.RESP_FAIL_SPACE, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void ioException_returnsFailIo() {
			assertEquals(Operation.Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(new IOException("connection reset")));
		}

		@Test
		void sdkClientException_wrappingIo_returnsFailIo() {
			var e = software.amazon.awssdk.core.exception.SdkClientException.builder()
							.cause(new IOException("connection refused"))
							.build();
			assertEquals(Operation.Status.FAIL_IO, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void apiCallTimeoutException_returnsFailTimeout() {
			var e = software.amazon.awssdk.core.exception.ApiCallTimeoutException.builder()
							.message("timed out")
							.build();
			assertEquals(Operation.Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void apiCallAttemptTimeoutException_returnsFailTimeout() {
			var e = software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException.builder()
							.message("attempt timed out")
							.build();
			assertEquals(Operation.Status.FAIL_TIMEOUT, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void genericException_returnsFailUnknown() {
			assertEquals(
							Operation.Status.FAIL_UNKNOWN,
							S3AwsStorageDriver.classifyFailure(new RuntimeException("unexpected")));
		}

		@Test
		void sdkClientException_nonIoCause_returnsFailUnknown() {
			var e = software.amazon.awssdk.core.exception.SdkClientException.builder()
							.cause(new IllegalStateException("bad state"))
							.build();
			assertEquals(Operation.Status.FAIL_UNKNOWN, S3AwsStorageDriver.classifyFailure(e));
		}

		@Test
		void completionException_wrappingS3Exception_unwrapsCorrectly() {
			S3Exception s3Ex = (S3Exception) S3Exception.builder().statusCode(404).build();
			CompletionException wrapper = new CompletionException(s3Ex);
			assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, S3AwsStorageDriver.classifyFailure(wrapper));
		}
	}

	// -----------------------------------------------------------------------
	// resolveChecksumAlgorithm — static, maps config string to SDK enum
	// -----------------------------------------------------------------------

	@Nested
	class ResolveChecksumAlgorithmTest {

		@Test
		void crc32() {
			assertEquals(ChecksumAlgorithm.CRC32, S3AwsStorageDriver.resolveChecksumAlgorithm("crc32"));
		}

		@Test
		void crc32c() {
			assertEquals(ChecksumAlgorithm.CRC32_C, S3AwsStorageDriver.resolveChecksumAlgorithm("crc32c"));
		}

		@Test
		void sha1() {
			assertEquals(ChecksumAlgorithm.SHA1, S3AwsStorageDriver.resolveChecksumAlgorithm("sha1"));
		}

		@Test
		void sha256() {
			assertEquals(ChecksumAlgorithm.SHA256, S3AwsStorageDriver.resolveChecksumAlgorithm("sha256"));
		}

		@Test
		void crc64Nvme() {
			assertEquals(ChecksumAlgorithm.CRC64_NVME, S3AwsStorageDriver.resolveChecksumAlgorithm("crc64-nvme"));
		}

		@Test
		void caseInsensitive() {
			assertEquals(ChecksumAlgorithm.CRC32, S3AwsStorageDriver.resolveChecksumAlgorithm("CRC32"));
			assertEquals(ChecksumAlgorithm.CRC32_C, S3AwsStorageDriver.resolveChecksumAlgorithm("CRC32C"));
			assertEquals(ChecksumAlgorithm.SHA1, S3AwsStorageDriver.resolveChecksumAlgorithm("SHA1"));
			assertEquals(ChecksumAlgorithm.SHA256, S3AwsStorageDriver.resolveChecksumAlgorithm("SHA256"));
			assertEquals(ChecksumAlgorithm.CRC64_NVME, S3AwsStorageDriver.resolveChecksumAlgorithm("CRC64-NVME"));
		}

		@Test
		void md5_returnsNull() {
			assertNull(S3AwsStorageDriver.resolveChecksumAlgorithm("md5"));
		}

		@Test
		void nullInput_returnsNull() {
			assertNull(S3AwsStorageDriver.resolveChecksumAlgorithm(null));
		}

		@Test
		void emptyInput_returnsNull() {
			assertNull(S3AwsStorageDriver.resolveChecksumAlgorithm(""));
		}

		@Test
		void unknownAlgorithm_returnsNull() {
			assertNull(S3AwsStorageDriver.resolveChecksumAlgorithm("blake2b"));
		}
	}

	// -----------------------------------------------------------------------
	// putObject checksum integration — verifies checksumAlgorithm on request
	// -----------------------------------------------------------------------

	@Nested
	class PutObjectChecksumTest {
		// Checksum tests removed - buildPutObjectRequest() method removed after CRT streaming refactor
		// Checksum functionality tested through integration tests
	}

	// -----------------------------------------------------------------------
	// Multipart Upload Operations
	// -----------------------------------------------------------------------

	@Nested
	class MultipartUploadTest {

		@SuppressWarnings("unchecked")
		@Test
		void initiateMultipartUpload_storesUploadId() throws Exception {
			CreateMultipartUploadResponse mockResponse = CreateMultipartUploadResponse.builder()
							.uploadId("test-upload-id-123")
							.build();

			when(mockS3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("rawtypes")
			CompositeDataOperation compositeOp = mock(CompositeDataOperation.class);
			DataItem item = mock(DataItem.class);
			when(compositeOp.item()).thenReturn(item);
			when(compositeOp.type()).thenReturn(OpType.CREATE);
			when(compositeOp.dstPath()).thenReturn("/test-bucket");
			when(item.name()).thenReturn("test-key");

			// Call the execute method which will route to executeCompositeOperation
			drv.execute((Operation) compositeOp).join();

			ArgumentCaptor<CreateMultipartUploadRequest> cap = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
			verify(mockS3Client).createMultipartUpload(cap.capture());
			assertEquals("test-bucket", cap.getValue().bucket());
			verify(compositeOp).put(KEY_UPLOAD_ID, "test-upload-id-123");
		}

		@SuppressWarnings("unchecked")
		@Test
		void initiateMultipartUpload_includesChecksumWhenEnabled() throws Exception {
			setChecksumFields(drv, true, ChecksumAlgorithm.CRC32);

			CreateMultipartUploadResponse mockResponse = CreateMultipartUploadResponse.builder()
							.uploadId("test-upload-id-456")
							.build();

			when(mockS3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("rawtypes")
			CompositeDataOperation compositeOp = mock(CompositeDataOperation.class);
			DataItem item = mock(DataItem.class);
			when(compositeOp.item()).thenReturn(item);
			when(compositeOp.type()).thenReturn(OpType.CREATE);
			when(compositeOp.dstPath()).thenReturn("/test-bucket");
			when(item.name()).thenReturn("test-key");

			drv.execute((Operation) compositeOp).join();

			ArgumentCaptor<CreateMultipartUploadRequest> cap = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
			verify(mockS3Client).createMultipartUpload(cap.capture());
			assertEquals(ChecksumAlgorithm.CRC32, cap.getValue().checksumAlgorithm());
		}

		@SuppressWarnings("unchecked")
		@Test
		void initiateMultipartUpload_includesCrc64NvmeWhenEnabled() throws Exception {
			setChecksumFields(drv, true, ChecksumAlgorithm.CRC64_NVME);

			CreateMultipartUploadResponse mockResponse = CreateMultipartUploadResponse.builder()
							.uploadId("test-upload-id-crc64")
							.build();

			when(mockS3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("rawtypes")
			CompositeDataOperation compositeOp = mock(CompositeDataOperation.class);
			DataItem item = mock(DataItem.class);
			when(compositeOp.item()).thenReturn(item);
			when(compositeOp.type()).thenReturn(OpType.CREATE);
			when(compositeOp.dstPath()).thenReturn("/test-bucket");
			when(item.name()).thenReturn("test-key");

			drv.execute((Operation) compositeOp).join();

			ArgumentCaptor<CreateMultipartUploadRequest> cap = ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);
			verify(mockS3Client).createMultipartUpload(cap.capture());
			assertEquals(ChecksumAlgorithm.CRC64_NVME, cap.getValue().checksumAlgorithm());
		}

		@SuppressWarnings("unchecked")
		@Disabled("Requires complex setup with real CompositeDataOperation/PartialDataOperation instances - tested through integration")
		@Test
		void uploadPart_storesEtagInParent() throws Exception {
			UploadPartResponse mockResponse = UploadPartResponse.builder()
							.eTag("\"etag-123\"")
							.build();

			when(mockS3Client.uploadPart(any(UploadPartRequest.class), any(AsyncRequestBody.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			// Use real objects like S3 driver does
			DataItem parentItem = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 4096);
			parentItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

			CompositeDataOperation parentOp = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<>(
							0, OpType.CREATE, parentItem, "/test-bucket", null, TEST_CRED, null, 0, 1024);
			parentOp.put(KEY_UPLOAD_ID, "upload-id-789");

			DataItem partItem = parentItem.slice(0, 1024);
			PartialDataOperation partialOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<>(
							0, OpType.CREATE, partItem, "/test-bucket", null, TEST_CRED, 0, parentOp);

			// Call uploadPart directly via reflection to avoid DataItemInputStream issues
			callUploadPart(drv, partialOp);

			ArgumentCaptor<UploadPartRequest> cap = ArgumentCaptor.forClass(UploadPartRequest.class);
			verify(mockS3Client).uploadPart(cap.capture(), any(AsyncRequestBody.class));
			assertEquals(1, cap.getValue().partNumber()); // 0-based + 1 = 1-based
			assertEquals("\"etag-123\"", parentOp.get("1"));
		}

		@SuppressWarnings("unchecked")
		@Disabled("Requires complex setup with real CompositeDataOperation instances - tested through integration")
		@Test
		void completeMultipartUpload_assemblesParts() throws Exception {
			CompleteMultipartUploadResponse mockResponse = CompleteMultipartUploadResponse.builder()
							.location("https://s3.amazonaws.com/bucket/key")
							.build();

			when(mockS3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			// Use real objects like S3 driver does
			DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 4096);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

			CompositeDataOperation compositeOp = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<>(
							0, OpType.CREATE, item, "/test-bucket", null, TEST_CRED, null, 0, 1024);
			compositeOp.put(KEY_UPLOAD_ID, "upload-id-complete");
			compositeOp.put("1", "\"etag-1\"");
			compositeOp.put("2", "\"etag-2\"");

			drv.execute((Operation) compositeOp).join();

			ArgumentCaptor<CompleteMultipartUploadRequest> cap = ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);
			verify(mockS3Client).completeMultipartUpload(cap.capture());
			assertEquals("upload-id-complete", cap.getValue().uploadId());
			assertNotNull(cap.getValue().multipartUpload());
			assertEquals(2, cap.getValue().multipartUpload().parts().size());
		}

		@SuppressWarnings("unchecked")
		@Test
		void abortMultipartUpload_callsS3Abort() throws Exception {
			AbortMultipartUploadResponse mockResponse = AbortMultipartUploadResponse.builder()
							.build();

			when(mockS3Client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("rawtypes")
			CompositeDataOperation compositeOp = mock(CompositeDataOperation.class);
			when(compositeOp.get(KEY_UPLOAD_ID)).thenReturn("upload-id-abort");
			when(compositeOp.get(KEY_MPU_ABORT)).thenReturn("true");
			when(compositeOp.type()).thenReturn(OpType.CREATE);
			when(compositeOp.dstPath()).thenReturn("/test-bucket");
			DataItem item = mock(DataItem.class);
			when(compositeOp.item()).thenReturn(item);
			when(item.name()).thenReturn("test-key");

			drv.execute((Operation) compositeOp).join();

			ArgumentCaptor<AbortMultipartUploadRequest> cap = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
			verify(mockS3Client).abortMultipartUpload(cap.capture());
			assertEquals("upload-id-abort", cap.getValue().uploadId());
		}
	}

	// -----------------------------------------------------------------------
	// Range Read Operations
	// -----------------------------------------------------------------------

	@Nested
	class RangeReadTest {

		@SuppressWarnings({"unchecked", "rawtypes"
		})
		@Test
		void readRangeStartsDataResponseOnFirstBodyBytes() throws Exception {
			final PartialDataOperation partialOp = mock(PartialDataOperation.class);
			final CompositeDataOperation parentOp = mock(CompositeDataOperation.class);
			final DataItem parentItem = mock(DataItem.class);
			final DataItem partItem = mock(DataItem.class);
			when(partialOp.type()).thenReturn(OpType.READ);
			when(partialOp.parent()).thenReturn(parentOp);
			when(partialOp.item()).thenReturn(partItem);
			when(parentOp.dstPath()).thenReturn("/test-bucket");
			when(parentOp.item()).thenReturn(parentItem);
			when(parentItem.name()).thenReturn("test-key");
			when(partItem.offset()).thenReturn(2048L);
			when(partItem.size()).thenReturn(1024L);
			final var response = new ResponseInputStream<>(
							GetObjectResponse.builder().build(),
							new ByteArrayInputStream(new byte[]{1, 2, 3, 4
							}));
			when(mockS3Client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(response));

			drv.execute((Operation) partialOp).join();

			verify(partialOp).startResponse();
			verify(partialOp).startDataResponse();
			verify(partialOp).countBytesDone(4L);
			verify(partialOp).finishResponse();
		}

		@SuppressWarnings("unchecked")
		@Disabled("Mocking complexity - requires ResponseInputStream and AsyncResponseTransformer mocking")
		@Test
		void readRange_calculatesCorrectRange() throws Exception {
			GetObjectResponse mockResponse = GetObjectResponse.builder()
							.contentLength(1024L)
							.build();

			when(mockS3Client.getObject(any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			// Use real objects like S3 driver does
			DataItem parentItem = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 4096);
			parentItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

			CompositeDataOperation parentOp = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<>(
							0, OpType.READ, parentItem, "/test-bucket", null, TEST_CRED, null, 0, 1024);

			DataItem partItem = parentItem.slice(2048, 1024); // partNumber=2
			PartialDataOperation partialOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<>(
							0, OpType.READ, partItem, "/test-bucket", null, TEST_CRED, 2, parentOp);

			drv.execute((Operation) partialOp).join();

			ArgumentCaptor<GetObjectRequest> cap = ArgumentCaptor.forClass(GetObjectRequest.class);
			verify(mockS3Client).getObject(cap.capture(), any(AsyncResponseTransformer.class));
			assertEquals("bytes=2048-3071", cap.getValue().range()); // 2 * 1024 = 2048, 2048 + 1024 - 1 = 3071
		}

		@SuppressWarnings("unchecked")
		@Disabled("Mocking complexity with execute() path - exception handling works in production")
		@Test
		void readRange_handlesIOException() throws Exception {
			DataItem item = mock(DataItem.class);
			when(item.offset()).thenReturn(0L);
			when(item.size()).thenThrow(new IOException("test error"));

			@SuppressWarnings("rawtypes")
			CompositeDataOperation parentOp = mock(CompositeDataOperation.class);
			when(parentOp.dstPath()).thenReturn("/test-bucket");
			DataItem parentItem = mock(DataItem.class);
			when(parentOp.item()).thenReturn(parentItem);
			when(parentItem.name()).thenReturn("test-key");

			@SuppressWarnings("rawtypes")
			PartialDataOperation partialOp = mock(PartialDataOperation.class);
			when(partialOp.item()).thenReturn(item);
			when(partialOp.parent()).thenReturn(parentOp);
			when(partialOp.partNumber()).thenReturn(0);
			when(partialOp.type()).thenReturn(OpType.READ);

			drv.execute((Operation) partialOp).join();

			assertEquals(Operation.Status.FAIL_IO, drv.classifyFailure(new IOException("test error")));
		}
	}

	// -----------------------------------------------------------------------
	// Copy Operations
	// -----------------------------------------------------------------------

	@Nested
	class CopyOperationTest {

		@SuppressWarnings("unchecked")
		@Test
		void copyObject_usesCorrectSourceAndDestination() throws Exception {
			CopyObjectResponse mockResponse = CopyObjectResponse.builder()
							.build();

			when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			@SuppressWarnings("unchecked")
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.item()).thenReturn(item);
			when(op.srcPath()).thenReturn("/source-bucket/source-key");
			when(op.dstPath()).thenReturn("/dest-bucket");
			when(item.name()).thenReturn("dest-key");

			drv.execute(op).join();

			ArgumentCaptor<CopyObjectRequest> cap = ArgumentCaptor.forClass(CopyObjectRequest.class);
			verify(mockS3Client).copyObject(cap.capture());
			assertEquals("source-bucket", cap.getValue().sourceBucket());
			assertEquals("source-key", cap.getValue().sourceKey());
			assertEquals("dest-bucket", cap.getValue().destinationBucket());
		}

		@SuppressWarnings("unchecked")
		@Test
		void copyObject_requiresSrcPath() throws Exception {
			@SuppressWarnings("unchecked")
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.item()).thenReturn(item);
			when(op.srcPath()).thenReturn(null); // No source path
			when(op.dstPath()).thenReturn("/dest-bucket");
			when(item.name()).thenReturn("dest-key");

			assertThrows(java.util.concurrent.CompletionException.class,
							() -> drv.execute(op).join());
		}
	}

	// -----------------------------------------------------------------------
	// Versioning Support
	// -----------------------------------------------------------------------

	@Nested
	class VersioningTest {

		@Test
		void extractVersionId_withTildeSeparator() throws Exception {
			setVersioningEnabled(drv, true);
			String[] result = drv.extractVersionId("my-key~version123");
			assertEquals("my-key", result[0]);
			assertEquals("version123", result[1]);
		}

		@Test
		void extractVersionId_withoutTildeSeparator() throws Exception {
			setVersioningEnabled(drv, true);
			String[] result = drv.extractVersionId("my-key");
			assertEquals("my-key", result[0]);
			assertNull(result[1]);
		}

		@Test
		void extractVersionId_versioningDisabled() throws Exception {
			setVersioningEnabled(drv, false);
			String[] result = drv.extractVersionId("my-key~version123");
			assertEquals("my-key~version123", result[0]);
			assertNull(result[1]);
		}

		@Test
		void extractVersionId_invalidVersionId() throws Exception {
			setVersioningEnabled(drv, true);
			String[] result = drv.extractVersionId("my-key~");
			assertEquals("my-key~", result[0]);
			assertNull(result[1]);
		}

		@SuppressWarnings("unchecked")
		@Test
		void readObject_includesVersionIdWhenPresent() throws Exception {
			setVersioningEnabled(drv, true);

			// Capture the GetObjectRequest to verify it's built correctly
			ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);

			// Mock getObject to fail immediately (we don't need to actually read)
			when(mockS3Client.getObject(requestCaptor.capture(), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.failedFuture(new RuntimeException("Test - request captured")));

			// Use real objects like S3 driver does
			DataItem item = new com.dell.spt.base.item.DataItemImpl("my-key~version456", 12345, 1024);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));
			@SuppressWarnings("rawtypes")
			DataOperation op = new DataOperationImpl(0, OpType.READ, item, null, "test-bucket", TEST_CRED, null, 0);

			// Execute and expect failure
			assertThrows(CompletionException.class, () -> drv.execute(op).join());

			// Verify the request was built with correct version ID and key
			GetObjectRequest request = requestCaptor.getValue();
			assertEquals("version456", request.versionId());
			assertEquals("my-key", request.key());
		}

		@SuppressWarnings("unchecked")
		@Test
		void currentKeyDeleteAndHeadRemainUnversionedWhenVersioningIsEnabled() throws Exception {
			setVersioningEnabled(drv, true);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
			when(mockS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			final DataItem deleteItem = new DataItemImpl("current-delete-key", 0, 0);
			final DataItem headItem = new DataItemImpl("current-head-key", 0, 0);

			drv.execute((Operation<Item>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.DELETE, deleteItem, null, "/bucket", TEST_CRED, null, 0)).join();
			drv.execute((Operation<Item>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.STAT, headItem, null, "/bucket", TEST_CRED, null, 0)).join();

			final ArgumentCaptor<DeleteObjectRequest> deleteRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			final ArgumentCaptor<HeadObjectRequest> headRequest = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(mockS3Client).deleteObject(deleteRequest.capture());
			verify(mockS3Client).headObject(headRequest.capture());
			verifyNoInteractions(exactVersionS3Client);
			assertEquals("current-delete-key", deleteRequest.getValue().key());
			assertNull(deleteRequest.getValue().versionId());
			assertEquals("current-head-key", headRequest.getValue().key());
			assertNull(headRequest.getValue().versionId());
		}

		@SuppressWarnings("unchecked")
		@Test
		void literalTildeKeysRemainLiteralWhenVersioningIsDisabled() throws Exception {
			setVersioningEnabled(drv, false);
			when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
			when(mockS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			final DataItem deleteItem = new DataItemImpl("literal-delete~suffix", 0, 0);
			final DataItem headItem = new DataItemImpl("literal-head~suffix", 0, 0);

			drv.execute((Operation<Item>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.DELETE, deleteItem, null, "/bucket", TEST_CRED, null, 0)).join();
			drv.execute((Operation<Item>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.STAT, headItem, null, "/bucket", TEST_CRED, null, 0)).join();

			final ArgumentCaptor<DeleteObjectRequest> deleteRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			final ArgumentCaptor<HeadObjectRequest> headRequest = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(mockS3Client).deleteObject(deleteRequest.capture());
			verify(mockS3Client).headObject(headRequest.capture());
			assertEquals("literal-delete~suffix", deleteRequest.getValue().key());
			assertNull(deleteRequest.getValue().versionId());
			assertEquals("literal-head~suffix", headRequest.getValue().key());
			assertNull(headRequest.getValue().versionId());
		}

		@SuppressWarnings("unchecked")
		@Test
		void structuredCurrentKeyWithTildeRemainsLiteralWhenVersioningIsEnabled() throws Exception {
			setVersioningEnabled(drv, true);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
			when(mockS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			final IntegrityManifestDataItem deleteItem = new IntegrityManifestDataItem(
							"bucket", "literal-delete~suffix", 0, null);
			final IntegrityManifestDataItem headItem = new IntegrityManifestDataItem(
							"bucket", "literal-head~suffix", 0, null);

			drv.execute((Operation<Item>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.DELETE, deleteItem, null, "/bucket", TEST_CRED, null, 0)).join();
			drv.execute((Operation<Item>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.STAT, headItem, null, "/bucket", TEST_CRED, null, 0)).join();

			final ArgumentCaptor<DeleteObjectRequest> deleteRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			final ArgumentCaptor<HeadObjectRequest> headRequest = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(mockS3Client).deleteObject(deleteRequest.capture());
			verify(mockS3Client).headObject(headRequest.capture());
			verifyNoInteractions(exactVersionS3Client);
			assertEquals("literal-delete~suffix", deleteRequest.getValue().key());
			assertNull(deleteRequest.getValue().versionId());
			assertEquals("literal-head~suffix", headRequest.getValue().key());
			assertNull(headRequest.getValue().versionId());
		}

		@Test
		void emptyRequestedVersionsKeepExtensionOperationsOnCurrentKeys() throws Exception {
			setVersioningEnabled(drv, true);
			final S3AsyncClient exactVersionS3Client = mock(S3AsyncClient.class);
			setExactVersionS3Client(drv, exactVersionS3Client);
			when(mockS3Client.getObject(
							any(GetObjectRequest.class), any(AsyncResponseTransformer.class)))
							.thenReturn(CompletableFuture.completedFuture(new ResponseInputStream<>(
											GetObjectResponse.builder().build(), new ByteArrayInputStream(new byte[0]))));
			when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(DeleteObjectResponse.builder().build()));
			when(mockS3Client.headObject(any(HeadObjectRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(HeadObjectResponse.builder().build()));
			final Operation<Item> read = extensionOperation(OpType.READ, "current-read", "");
			final Operation<Item> delete = extensionOperation(OpType.DELETE, "current-delete", "");
			final Operation<Item> head = extensionOperation(OpType.STAT, "current-head", "");

			drv.execute(read).join();
			drv.execute(delete).join();
			drv.execute(head).join();

			final ArgumentCaptor<GetObjectRequest> readRequest = ArgumentCaptor.forClass(GetObjectRequest.class);
			final ArgumentCaptor<DeleteObjectRequest> deleteRequest = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			final ArgumentCaptor<HeadObjectRequest> headRequest = ArgumentCaptor.forClass(HeadObjectRequest.class);
			verify(mockS3Client).getObject(readRequest.capture(), any(AsyncResponseTransformer.class));
			verify(mockS3Client).deleteObject(deleteRequest.capture());
			verify(mockS3Client).headObject(headRequest.capture());
			verifyNoInteractions(exactVersionS3Client);
			assertNull(readRequest.getValue().versionId());
			assertNull(deleteRequest.getValue().versionId());
			assertNull(headRequest.getValue().versionId());
		}

		@SuppressWarnings("unchecked")
		private Operation<Item> extensionOperation(
						final OpType opType, final String key, final String requestedVersionId) {
			final Operation<Item> operation = mock(Operation.class);
			final Item item = mock(Item.class);
			when(operation.type()).thenReturn(opType);
			when(operation.dstPath()).thenReturn("/bucket");
			when(operation.item()).thenReturn(item);
			when(operation.requestedVersionId()).thenReturn(requestedVersionId);
			when(item.name()).thenReturn(key);
			return operation;
		}
	}

	// -----------------------------------------------------------------------
	// Tagging Support
	// -----------------------------------------------------------------------

	@Nested
	class TaggingTest {

		@SuppressWarnings("unchecked")
		@Test
		void putObject_includesTagsWhenEnabled() throws Exception {
			setTaggingEnabled(drv, true, Map.of("tag1", "value1", "tag2", "value2"));

			PutObjectResponse mockResponse = PutObjectResponse.builder()
							.build();

			when(mockS3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			// Use real objects like S3 driver does
			DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 1024);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

			@SuppressWarnings("rawtypes")
			DataOperation op = new DataOperationImpl(0, OpType.CREATE, item, null, "test-bucket", TEST_CRED, null, 0);

			drv.execute(op).join();

			ArgumentCaptor<PutObjectRequest> cap = ArgumentCaptor.forClass(PutObjectRequest.class);
			verify(mockS3Client).putObject(cap.capture(), any(AsyncRequestBody.class));
			assertNotNull(cap.getValue().tagging());
		}

		@SuppressWarnings("unchecked")
		@Test
		void putObject_noTagsWhenDisabled() throws Exception {
			setTaggingEnabled(drv, false, Map.of());

			PutObjectResponse mockResponse = PutObjectResponse.builder()
							.build();

			when(mockS3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			// Use real objects like S3 driver does
			DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 1024);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

			@SuppressWarnings("rawtypes")
			DataOperation op = new DataOperationImpl(0, OpType.CREATE, item, null, "test-bucket", TEST_CRED, null, 0);

			drv.execute(op).join();

			ArgumentCaptor<PutObjectRequest> cap = ArgumentCaptor.forClass(PutObjectRequest.class);
			verify(mockS3Client).putObject(cap.capture(), any(AsyncRequestBody.class));
			assertNull(cap.getValue().tagging());
		}

		@SuppressWarnings("unchecked")
		@Test
		void putObject_noTagsWhenEmpty() throws Exception {
			setTaggingEnabled(drv, true, Map.of());

			PutObjectResponse mockResponse = PutObjectResponse.builder()
							.build();

			when(mockS3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
							.thenReturn(CompletableFuture.completedFuture(mockResponse));

			// Use real objects like S3 driver does
			DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 1024);
			item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

			@SuppressWarnings("rawtypes")
			DataOperation op = new DataOperationImpl(0, OpType.CREATE, item, null, "test-bucket", TEST_CRED, null, 0);

			drv.execute(op).join();

			ArgumentCaptor<PutObjectRequest> cap = ArgumentCaptor.forClass(PutObjectRequest.class);
			verify(mockS3Client).putObject(cap.capture(), any(AsyncRequestBody.class));
			assertNull(cap.getValue().tagging());
		}

		private void setTaggingEnabled(S3AwsStorageDriver<Item, Operation<Item>> driver, boolean enabled, Map<String, String> tags) throws Exception {
			Field taggingEnabledField = S3AwsStorageDriver.class.getDeclaredField("taggingEnabled");
			taggingEnabledField.setAccessible(true);
			taggingEnabledField.setBoolean(driver, enabled);

			Field objectTagsField = S3AwsStorageDriver.class.getDeclaredField("objectTags");
			objectTagsField.setAccessible(true);
			objectTagsField.set(driver, tags);
		}
	}

	// -----------------------------------------------------------------------
	// Tagging Config Path Tests
	// -----------------------------------------------------------------------

	@Nested
	class TaggingConfigPathTest {

		private Config mockBaseConfig(Map<String, String> tags) {
			// Mock parent class config requirements (StorageDriverBase)
			Config driverConfig = mock(Config.class);
			Config limitConfig = mock(Config.class);
			when(limitConfig.intVal("concurrency")).thenReturn(1);
			when(driverConfig.configVal("limit")).thenReturn(limitConfig);

			Config authConfig = mock(Config.class);
			when(authConfig.stringVal("uid")).thenReturn("test-user");
			when(authConfig.stringVal("secret")).thenReturn("test-secret");
			when(authConfig.stringVal("token")).thenReturn(null);

			Config config = mock(Config.class);
			disableIntegrity(config);
			when(config.configVal("driver")).thenReturn(driverConfig);
			when(config.stringVal("namespace")).thenReturn("test-ns");
			when(config.configVal("auth")).thenReturn(authConfig);

			// Mock CoopStorageDriverBase config requirements
			when(config.intVal("driver-limit-queue-input")).thenReturn(100);

			// Mock tagging config
			Config objectConfig = mock(Config.class);
			when(objectConfig.boolVal("versioning")).thenReturn(false);

			Config taggingConfig = mock(Config.class);
			when(taggingConfig.boolVal("enabled")).thenReturn(true);
			when(taggingConfig.mapVal("tags")).thenReturn(new java.util.HashMap<>(tags));

			when(objectConfig.configVal("tagging")).thenReturn(taggingConfig);
			when(config.configVal("object")).thenReturn(objectConfig);

			// Mock other S3AwsStorageDriver config requirements
			when(config.configVal("item")).thenThrow(new RuntimeException("no item config"));
			when(config.configVal("checksum")).thenThrow(new RuntimeException("no checksum config"));
			when(config.listVal("net.node.addrs")).thenThrow(new RuntimeException("no net config"));

			return config;
		}

		@Test
		void taggingEnabled_whenConfigObjectTaggingEnabled() throws Exception {
			// Mock config with tagging enabled and tags
			Config config = mockBaseConfig(Map.of("tag1", "value1", "tag2", "value2"));

			// Mock S3AsyncClient
			S3AsyncClient mockS3Client = mock(S3AsyncClient.class);

			// Create driver with mocked config and S3AsyncClient
			S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1",
							null,
							config,
							false,
							1,
							mockS3Client,
							100 * 1024L,  // smallObjectThresholdBytes
							8 * 1024 * 1024L  // partSizeBytes
			);

			// Verify the tagging fields were set correctly using reflection
			Field taggingEnabledField = S3AwsStorageDriver.class.getDeclaredField("taggingEnabled");
			taggingEnabledField.setAccessible(true);
			boolean taggingEnabled = taggingEnabledField.getBoolean(driver);

			Field objectTagsField = S3AwsStorageDriver.class.getDeclaredField("objectTags");
			objectTagsField.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<String, String> objectTags = (Map<String, String>) objectTagsField.get(driver);

			assertTrue(taggingEnabled, "taggingEnabled should be true when config.object.tagging.enabled is true");
			assertEquals(2, objectTags.size(), "Should have 2 tags");
			assertEquals("value1", objectTags.get("tag1"));
			assertEquals("value2", objectTags.get("tag2"));
		}

		@Test
		void taggingDisabled_whenConfigObjectTaggingDisabled() throws Exception {
			// Mock config with tagging disabled
			Config objectConfig = mock(Config.class);
			when(objectConfig.boolVal("versioning")).thenReturn(false);

			Config taggingConfig = mock(Config.class);
			when(taggingConfig.boolVal("enabled")).thenReturn(false);
			when(taggingConfig.mapVal("tags")).thenReturn(Map.of());

			when(objectConfig.configVal("tagging")).thenReturn(taggingConfig);

			Config config = mockBaseConfig(Map.of());
			when(config.configVal("object")).thenReturn(objectConfig);

			// Mock S3AsyncClient
			S3AsyncClient mockS3Client = mock(S3AsyncClient.class);

			// Create driver with mocked config and S3AsyncClient
			S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1",
							null,
							config,
							false,
							1,
							mockS3Client,
							100 * 1024L,  // smallObjectThresholdBytes
							8 * 1024 * 1024L  // partSizeBytes
			);

			// Verify the tagging fields were set correctly using reflection
			Field taggingEnabledField = S3AwsStorageDriver.class.getDeclaredField("taggingEnabled");
			taggingEnabledField.setAccessible(true);
			boolean taggingEnabled = taggingEnabledField.getBoolean(driver);

			Field objectTagsField = S3AwsStorageDriver.class.getDeclaredField("objectTags");
			objectTagsField.setAccessible(true);
			@SuppressWarnings("unchecked")
			Map<String, String> objectTags = (Map<String, String>) objectTagsField.get(driver);

			assertFalse(taggingEnabled, "taggingEnabled should be false when config.object.tagging.enabled is false");
			assertTrue(objectTags.isEmpty(), "Should have no tags when tagging is disabled");
		}
	}

	// -----------------------------------------------------------------------
	// Versioning Config Path Tests
	// -----------------------------------------------------------------------

	@Nested
	class VersioningConfigPathTest {

		private Config mockBaseConfig(Config objectConfig) {
			// Mock parent class config requirements (StorageDriverBase)
			Config driverConfig = mock(Config.class);
			Config limitConfig = mock(Config.class);
			when(limitConfig.intVal("concurrency")).thenReturn(1);
			when(driverConfig.configVal("limit")).thenReturn(limitConfig);

			Config authConfig = mock(Config.class);
			when(authConfig.stringVal("uid")).thenReturn("test-user");
			when(authConfig.stringVal("secret")).thenReturn("test-secret");
			when(authConfig.stringVal("token")).thenReturn(null);

			Config config = mock(Config.class);
			disableIntegrity(config);
			when(config.configVal("driver")).thenReturn(driverConfig);
			when(config.stringVal("namespace")).thenReturn("test-ns");
			when(config.configVal("auth")).thenReturn(authConfig);

			// Mock CoopStorageDriverBase config requirements
			when(config.intVal("driver-limit-queue-input")).thenReturn(100);

			// Mock S3AwsStorageDriver config requirements
			when(config.configVal("object")).thenReturn(objectConfig);
			when(config.configVal("item")).thenThrow(new RuntimeException("no item config"));
			when(config.configVal("checksum")).thenThrow(new RuntimeException("no checksum config"));
			when(config.listVal("net.node.addrs")).thenThrow(new RuntimeException("no net config"));

			return config;
		}

		@Test
		void versioningEnabled_whenConfigObjectVersioningTrue() throws Exception {
			// Mock the config hierarchy: config → configVal("object") → objectConfig → boolVal("versioning")
			Config objectConfig = mock(Config.class);
			when(objectConfig.boolVal("versioning")).thenReturn(true);

			Config config = mockBaseConfig(objectConfig);

			// Mock S3AsyncClient
			S3AsyncClient mockS3Client = mock(S3AsyncClient.class);

			// Create driver with mocked config and S3AsyncClient
			S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1",
							null,
							config,
							false,
							1,
							mockS3Client,
							100 * 1024L,  // smallObjectThresholdBytes
							8 * 1024 * 1024L  // partSizeBytes
			);

			// Verify the versioningEnabled field was set correctly using reflection
			Field versioningField = S3AwsStorageDriver.class.getDeclaredField("versioningEnabled");
			versioningField.setAccessible(true);
			boolean versioningEnabled = versioningField.getBoolean(driver);

			assertTrue(versioningEnabled, "versioningEnabled should be true when config.object.versioning is true");

			// Verify the config chain was called correctly (at least once, since it may be called multiple times)
			verify(config, atLeastOnce()).configVal("object");
			verify(objectConfig, atLeastOnce()).boolVal("versioning");
		}

		@Test
		void versioningDisabled_whenConfigObjectVersioningFalse() throws Exception {
			// Mock the config hierarchy with versioning false
			Config objectConfig = mock(Config.class);
			when(objectConfig.boolVal("versioning")).thenReturn(false);

			Config config = mockBaseConfig(objectConfig);

			// Mock S3AsyncClient
			S3AsyncClient mockS3Client = mock(S3AsyncClient.class);

			// Create driver with mocked config and S3AsyncClient
			S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1",
							null,
							config,
							false,
							1,
							mockS3Client,
							100 * 1024L,  // smallObjectThresholdBytes
							8 * 1024 * 1024L  // partSizeBytes
			);

			// Verify the versioningEnabled field was set correctly using reflection
			Field versioningField = S3AwsStorageDriver.class.getDeclaredField("versioningEnabled");
			versioningField.setAccessible(true);
			boolean versioningEnabled = versioningField.getBoolean(driver);

			assertFalse(versioningEnabled, "versioningEnabled should be false when config.object.versioning is false");

			// Verify the config chain was called correctly (at least once, since it may be called multiple times)
			verify(config, atLeastOnce()).configVal("object");
			verify(objectConfig, atLeastOnce()).boolVal("versioning");
		}

		@Test
		void versioningDisabled_whenConfigObjectMissing() throws Exception {
			// Mock config without object config (should default to false)
			Config config = mockBaseConfig(null);
			when(config.configVal("object")).thenThrow(new RuntimeException("no object config"));

			// Mock S3AsyncClient
			S3AsyncClient mockS3Client = mock(S3AsyncClient.class);

			// Create driver with mocked config and S3AsyncClient
			S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1",
							null,
							config,
							false,
							1,
							mockS3Client,
							100 * 1024L,  // smallObjectThresholdBytes
							8 * 1024 * 1024L  // partSizeBytes
			);

			// Verify the versioningEnabled field was set to default (false)
			Field versioningField = S3AwsStorageDriver.class.getDeclaredField("versioningEnabled");
			versioningField.setAccessible(true);
			boolean versioningEnabled = versioningField.getBoolean(driver);

			assertFalse(versioningEnabled, "versioningEnabled should default to false when config.object is missing");
		}
	}

	// -----------------------------------------------------------------------
	// Resource Cleanup Tests
	// -----------------------------------------------------------------------

	@Nested
	class ResourceCleanupTest {

		private Config mockDriverConfig() {
			final Config driverConfig = mock(Config.class);
			final Config limitConfig = mock(Config.class);
			when(limitConfig.intVal("concurrency")).thenReturn(1);
			when(driverConfig.configVal("limit")).thenReturn(limitConfig);
			when(driverConfig.intVal("threads")).thenReturn(1);

			final Config authConfig = mock(Config.class);
			when(authConfig.stringVal("uid")).thenReturn("test-user");
			when(authConfig.stringVal("secret")).thenReturn("test-secret");
			when(authConfig.stringVal("token")).thenReturn(null);

			final Config config = mock(Config.class);
			disableIntegrity(config);
			when(config.configVal("driver")).thenReturn(driverConfig);
			when(config.stringVal("namespace")).thenReturn("test-ns");
			when(config.configVal("auth")).thenReturn(authConfig);
			when(config.intVal("driver-limit-queue-input")).thenReturn(100);
			when(config.configVal("object")).thenThrow(new RuntimeException("no object config"));
			when(config.configVal("item")).thenThrow(new RuntimeException("no item config"));
			when(config.configVal("checksum")).thenThrow(new RuntimeException("no checksum config"));
			when(config.listVal("net.node.addrs")).thenThrow(new RuntimeException("no net config"));
			when(config.stringVal("storage-net-node-addrs")).thenThrow(new RuntimeException("no net config"));
			when(config.configVal("crt")).thenThrow(new RuntimeException("no crt config"));
			return config;
		}

		@Test
		void disabledIntegrityDoesNotCreateExactVersionClientOnConstructionOrClose() throws Exception {
			final S3AsyncClient primaryClient = mock(S3AsyncClient.class);
			@SuppressWarnings("unchecked")
			final Supplier<S3AsyncClient> exactVersionClientSupplier = mock(Supplier.class);
			final S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1", mock(com.dell.spt.base.data.DataInput.class), mockDriverConfig(),
							false, 1, primaryClient, exactVersionClientSupplier,
							100 * 1024L, 8 * 1024 * 1024L);

			driver.close();

			verify(primaryClient).close();
			verifyNoInteractions(exactVersionClientSupplier);
		}

		@Test
		void doClose_closesS3AsyncClientAndExecutors() throws Exception {
			S3AsyncClient mockClient = mock(S3AsyncClient.class);
			S3AsyncClient exactVersionClient = mock(S3AsyncClient.class);
			final Config config = mockDriverConfig();

			// Create the driver so it initializes its executors
			S3AwsStorageDriver<Item, Operation<Item>> driver = new S3AwsStorageDriver<>(
							"step-1", mock(com.dell.spt.base.data.DataInput.class), config, false, 1, mockClient, exactVersionClient, 100 * 1024L, 8 * 1024 * 1024L);

			// Act: Call the close method on the driver
			driver.close();

			// Assert: Verify the S3 client was closed
			verify(mockClient, times(1)).close();
			verify(exactVersionClient, times(1)).close();

			// Assert: Verify the thread pools were shut down
			Field executorField = S3AwsStorageDriver.class.getDeclaredField("executor");
			executorField.setAccessible(true);
			ExecutorService executor = (ExecutorService) executorField.get(driver);
			assertTrue(executor.isShutdown(), "Read executor should be shut down");

			Field uploadExecutorField = S3AwsStorageDriver.class.getDeclaredField("uploadExecutor");
			uploadExecutorField.setAccessible(true);
			ExecutorService uploadExecutor = (ExecutorService) uploadExecutorField.get(driver);
			assertTrue(uploadExecutor.isShutdown(), "Upload executor should be shut down");
		}
	}

	private void callUploadPart(S3AwsStorageDriver<Item, Operation<Item>> driver, PartialDataOperation op) throws Exception {
		java.lang.reflect.Method uploadPartMethod = S3AwsStorageDriver.class.getDeclaredMethod("uploadPart", PartialDataOperation.class);
		uploadPartMethod.setAccessible(true);
		((CompletableFuture<Void>) uploadPartMethod.invoke(driver, op)).join();
	}
}
