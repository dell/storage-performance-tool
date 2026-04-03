package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class S3AwsStorageDriverTest {

	private S3AwsStorageDriver<Item, Operation<Item>> drv;
	private S3Client mockS3Client;

	@SuppressWarnings("unchecked")
	private S3AwsStorageDriver<Item, Operation<Item>> newDriverMock() {
		return Mockito.mock(S3AwsStorageDriver.class,
						Mockito.withSettings().lenient().defaultAnswer(Mockito.CALLS_REAL_METHODS));
	}

	private void setBucketName(S3AwsStorageDriver<Item, Operation<Item>> driver, String bucketName) throws Exception {
		Field bucketField = S3AwsStorageDriver.class.getDeclaredField("bucketName");
		bucketField.setAccessible(true);
		bucketField.set(driver, bucketName);
	}

	private void setS3Client(S3AwsStorageDriver<Item, Operation<Item>> driver, S3Client s3Client) throws Exception {
		Field clientField = S3AwsStorageDriver.class.getDeclaredField("s3Client");
		clientField.setAccessible(true);
		clientField.set(driver, s3Client);
	}

	@BeforeEach
	void setUp() throws Exception {
		drv = newDriverMock();
		mockS3Client = mock(S3Client.class);
		setS3Client(drv, mockS3Client);
		setBucketName(drv, "test-bucket");
	}

	// -----------------------------------------------------------------------
	// parseBucketAndKey — static, package-visible, easy to test directly
	// -----------------------------------------------------------------------

	@Nested
	class ParseBucketAndKeyTest {

		@Test
		void withLeadingSlashAndKey() {
			String[] bk = S3AwsStorageDriver.parseBucketAndKey("/large/mkk0lurmliru");
			assertEquals("large", bk[0]);
			assertEquals("mkk0lurmliru", bk[1]);
		}

		@Test
		void withoutLeadingSlash() {
			String[] bk = S3AwsStorageDriver.parseBucketAndKey("mybucket/my/nested/key.txt");
			assertEquals("mybucket", bk[0]);
			assertEquals("my/nested/key.txt", bk[1]);
		}

		@Test
		void bucketOnly_noKey() {
			String[] bk = S3AwsStorageDriver.parseBucketAndKey("/onlybucket");
			assertEquals("onlybucket", bk[0]);
			assertNull(bk[1]);
		}

		@Test
		void bucketOnlyNoSlash() {
			String[] bk = S3AwsStorageDriver.parseBucketAndKey("onlybucket");
			assertEquals("onlybucket", bk[0]);
			assertNull(bk[1]);
		}

		@ParameterizedTest
		@CsvSource({
				"/b/k, b, k",
				"/bucket/prefix/deep/key, bucket, prefix/deep/key",
				"bucket/key, bucket, key",
		})
		void parameterized(String input, String expectedBucket, String expectedKey) {
			String[] bk = S3AwsStorageDriver.parseBucketAndKey(input);
			assertEquals(expectedBucket, bk[0]);
			assertEquals(expectedKey, bk[1]);
		}
	}

	// -----------------------------------------------------------------------
	// resolveBucketAndKey — private, tested via reflection
	// -----------------------------------------------------------------------

	@Nested
	class ResolveBucketAndKeyTest {

		@SuppressWarnings("unchecked")
		private String[] invokeResolve(Operation<Item> op) throws Exception {
			Method m = S3AwsStorageDriver.class.getDeclaredMethod("resolveBucketAndKey", Operation.class);
			m.setAccessible(true);
			return (String[]) m.invoke(drv, op);
		}

		@Test
		void withDstPath_simpleBucket() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/large");
			when(item.name()).thenReturn("mkk0lurmliru");
			when(op.item()).thenReturn(item);

			String[] bk = invokeResolve(op);
			assertEquals("large", bk[0]);
			assertEquals("mkk0lurmliru", bk[1]);
		}

		@Test
		void withDstPath_nestedPrefix() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/bucket/prefix");
			when(item.name()).thenReturn("mykey");
			when(op.item()).thenReturn(item);

			String[] bk = invokeResolve(op);
			assertEquals("bucket", bk[0]);
			assertEquals("prefix/mykey", bk[1]);
		}

		@Test
		void withDstPath_itemNameHasLeadingSlash() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("/mybucket");
			when(item.name()).thenReturn("/somekey");
			when(op.item()).thenReturn(item);

			String[] bk = invokeResolve(op);
			assertEquals("mybucket", bk[0]);
			assertEquals("somekey", bk[1]);
		}

		@Test
		void noDstPath_fallsBackToParseBucketAndKey() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn(null);
			when(item.name()).thenReturn("/fallback-bucket/fallback-key");
			when(op.item()).thenReturn(item);

			String[] bk = invokeResolve(op);
			assertEquals("fallback-bucket", bk[0]);
			assertEquals("fallback-key", bk[1]);
		}

		@Test
		void emptyDstPath_fallsBackToParseBucketAndKey() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.dstPath()).thenReturn("");
			when(item.name()).thenReturn("/b/k");
			when(op.item()).thenReturn(item);

			String[] bk = invokeResolve(op);
			assertEquals("b", bk[0]);
			assertEquals("k", bk[1]);
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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenThrow(S3Exception.builder().message("boom").build());

			@SuppressWarnings("unchecked")
			ItemFactory<Item> factory = mock(ItemFactory.class);

			assertThrows(IOException.class,
							() -> drv.list(factory, null, null, 10, null, 100, null));
		}

		@Test
		void sixArgOverload_delegatesToSevenArg() throws Exception {
			ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
							.contents(Collections.emptyList())
							.isTruncated(false)
							.build();

			when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenReturn(mockResponse);

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
							.thenThrow(S3Exception.builder().message("access denied").build());

			assertThrows(IOException.class,
							() -> drv.probeCommonPrefixes("/bucket", "", "/", 10));
		}
	}

	// -----------------------------------------------------------------------
	// deleteObject — tested via reflection of execute()
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

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);
			exec.invoke(drv, op);

			ArgumentCaptor<DeleteObjectRequest> cap = ArgumentCaptor.forClass(DeleteObjectRequest.class);
			verify(mockS3Client).deleteObject(cap.capture());
			assertEquals("mybucket", cap.getValue().bucket());
			assertEquals("mykey.dat", cap.getValue().key());
		}
	}

	// -----------------------------------------------------------------------
	// putObject — tested via reflection of execute()
	// -----------------------------------------------------------------------

	@Nested
	class PutObjectTest {

		@SuppressWarnings("unchecked")
		@Test
		void putsDataItemWithCorrectBucketAndKey() throws Exception {
			DataItem dataItem = mock(DataItem.class);
			when(dataItem.name()).thenReturn("upload.bin");
			when(dataItem.size()).thenReturn(1024L);

			Operation<Item> op = mock(Operation.class, Mockito.withSettings().extraInterfaces(DataOperation.class));
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.dstPath()).thenReturn("/upload-bucket");
			when(op.item()).thenReturn((Item) dataItem);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);
			exec.invoke(drv, op);

			ArgumentCaptor<PutObjectRequest> cap = ArgumentCaptor.forClass(PutObjectRequest.class);
			verify(mockS3Client).putObject(cap.capture(), any(RequestBody.class));
			assertEquals("upload-bucket", cap.getValue().bucket());
			assertEquals("upload.bin", cap.getValue().key());
		}

		@SuppressWarnings("unchecked")
		@Test
		void updateRoutesToPut() throws Exception {
			DataItem dataItem = mock(DataItem.class);
			when(dataItem.name()).thenReturn("update.bin");
			when(dataItem.size()).thenReturn(512L);

			Operation<Item> op = mock(Operation.class, Mockito.withSettings().extraInterfaces(DataOperation.class));
			when(op.type()).thenReturn(OpType.UPDATE);
			when(op.dstPath()).thenReturn("/bucket");
			when(op.item()).thenReturn((Item) dataItem);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);
			exec.invoke(drv, op);

			verify(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
		}
	}

	// -----------------------------------------------------------------------
	// readObject — tested via reflection of execute()
	// -----------------------------------------------------------------------

	@Nested
	class ReadObjectTest {

		@SuppressWarnings("unchecked")
		@Test
		void readsWithCorrectBucketAndKey() throws Exception {
			Item item = mock(Item.class);
			when(item.name()).thenReturn("read-me.dat");

			Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/read-bucket");
			when(op.item()).thenReturn(item);

			// Mock getObject to return an input stream
			GetObjectResponse getResp = GetObjectResponse.builder().build();
			ResponseInputStream<GetObjectResponse> ris = new ResponseInputStream<>(
							getResp, new ByteArrayInputStream(new byte[0]));
			when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(ris);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);
			exec.invoke(drv, op);

			ArgumentCaptor<GetObjectRequest> cap = ArgumentCaptor.forClass(GetObjectRequest.class);
			verify(mockS3Client).getObject(cap.capture());
			assertEquals("read-bucket", cap.getValue().bucket());
			assertEquals("read-me.dat", cap.getValue().key());
		}
	}

	// -----------------------------------------------------------------------
	// execute() dispatch — unsupported type
	// -----------------------------------------------------------------------

	@Nested
	class ExecuteDispatchTest {

		@SuppressWarnings("unchecked")
		@Test
		void unsupportedOpType_throwsUnsupportedOperationException() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.LIST);
			when(op.item()).thenReturn(item);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);

			var ex = assertThrows(java.lang.reflect.InvocationTargetException.class,
							() -> exec.invoke(drv, op));
			assertInstanceOf(UnsupportedOperationException.class, ex.getCause());
		}
	}

	// -----------------------------------------------------------------------
	// requestNewPath
	// -----------------------------------------------------------------------

	@Nested
	class RequestNewPathTest {

		@Test
		void extractsBucketPath_withSlash() throws Exception {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(HeadBucketResponse.builder().build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewPath", String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(drv, "/large/prefix");
			assertEquals("/large", result);

			// Verify headBucket was called with the bucket from path, not this.bucketName
			ArgumentCaptor<HeadBucketRequest> cap = ArgumentCaptor.forClass(HeadBucketRequest.class);
			verify(mockS3Client).headBucket(cap.capture());
			assertEquals("large", cap.getValue().bucket());
		}

		@Test
		void extractsBucketPath_noSubpath() throws Exception {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(HeadBucketResponse.builder().build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewPath", String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(drv, "/mybucket");
			assertEquals("/mybucket", result);
		}

		@Test
		void missingBucket_createsIt() throws Exception {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenThrow(NoSuchBucketException.builder().message("no bucket").build());
			when(mockS3Client.createBucket(any(CreateBucketRequest.class)))
							.thenReturn(CreateBucketResponse.builder().build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewPath", String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(drv, "/newbucket");
			assertEquals("/newbucket", result);

			ArgumentCaptor<CreateBucketRequest> cap = ArgumentCaptor.forClass(CreateBucketRequest.class);
			verify(mockS3Client).createBucket(cap.capture());
			assertEquals("newbucket", cap.getValue().bucket());
		}

		@Test
		void headBucketFailure_nonNoSuchBucket_throwsRuntimeException() throws Exception {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenThrow(S3Exception.builder().message("access denied").build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewPath", String.class);
			m.setAccessible(true);

			var ex = assertThrows(java.lang.reflect.InvocationTargetException.class,
							() -> m.invoke(drv, "/nonexistent"));
			assertInstanceOf(RuntimeException.class, ex.getCause());
		}

		@Test
		void createBucketFailure_throwsRuntimeException() throws Exception {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenThrow(NoSuchBucketException.builder().message("no bucket").build());
			when(mockS3Client.createBucket(any(CreateBucketRequest.class)))
							.thenThrow(S3Exception.builder().message("create failed").build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewPath", String.class);
			m.setAccessible(true);

			var ex = assertThrows(java.lang.reflect.InvocationTargetException.class,
							() -> m.invoke(drv, "/failbucket"));
			assertInstanceOf(RuntimeException.class, ex.getCause());
		}
	}

	// -----------------------------------------------------------------------
	// requestNewAuthToken
	// -----------------------------------------------------------------------

	@Test
	void requestNewAuthToken_returnsNull() throws Exception {
		Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewAuthToken",
						com.dell.spt.base.storage.Credential.class);
		m.setAccessible(true);
		Object result = m.invoke(drv, (Object) null);
		assertNull(result);
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
		void successfulDelete_callsFinishOperation() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.DELETE);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("invokeNio", Operation.class);
			m.setAccessible(true);
			m.invoke(drv, op);

			verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
			// finishOperation calls startResponse, finishResponse, and status(SUCC)
			verify(op).startResponse();
			verify(op).finishResponse();
			verify(op).status(Operation.Status.SUCC);
		}

		@SuppressWarnings("unchecked")
		@Test
		void successfulRead_withDataItem_countsBytesDone() throws Exception {
			DataItem dataItem = mock(DataItem.class);
			when(dataItem.name()).thenReturn("obj");
			when(dataItem.size()).thenReturn(2048L);

			// Create op that is both Operation and DataOperation
			DataOperation<DataItem> dataOp = mock(DataOperation.class);
			when(dataOp.type()).thenReturn(OpType.READ);
			when(dataOp.dstPath()).thenReturn("/bucket");
			when(dataOp.item()).thenReturn(dataItem);

			GetObjectResponse getResp = GetObjectResponse.builder().build();
			ResponseInputStream<GetObjectResponse> ris = new ResponseInputStream<>(
							getResp, new ByteArrayInputStream(new byte[0]));
			when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(ris);

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("invokeNio", Operation.class);
			m.setAccessible(true);
			m.invoke(drv, dataOp);

			// readObject calls countBytesDone, then invokeNio calls it again for metrics
			verify(dataOp, atLeast(1)).countBytesDone(anyLong());
			verify(dataOp).status(Operation.Status.SUCC);
		}

		@SuppressWarnings("unchecked")
		@Test
		void failedOperation_setsStatusToFailUnknown() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			// Make getObject throw an exception
			when(mockS3Client.getObject(any(GetObjectRequest.class)))
							.thenThrow(NoSuchKeyException.builder().message("not found").build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("invokeNio", Operation.class);
			m.setAccessible(true);
			m.invoke(drv, op);

			verify(op).status(Operation.Status.FAIL_UNKNOWN);
		}

		@SuppressWarnings("unchecked")
		@Test
		void failedOperation_handlesTimingErrors() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			when(mockS3Client.getObject(any(GetObjectRequest.class)))
							.thenThrow(NoSuchKeyException.builder().message("not found").build());
			// Make startResponse throw too, to exercise the inner catch
			doThrow(new IllegalStateException("already started")).when(op).startResponse();

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("invokeNio", Operation.class);
			m.setAccessible(true);
			// Should not throw despite double failure
			assertDoesNotThrow(() -> m.invoke(drv, op));
			verify(op).status(Operation.Status.FAIL_UNKNOWN);
		}

		@SuppressWarnings("unchecked")
		@Test
		void successfulDelete_nonDataItem_skipsCountBytesDone() throws Exception {
			Operation<Item> op = mock(Operation.class);
			Item item = mock(Item.class); // plain Item, not DataItem
			when(op.type()).thenReturn(OpType.DELETE);
			when(op.dstPath()).thenReturn("/bucket");
			when(item.name()).thenReturn("key");
			when(op.item()).thenReturn(item);

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("invokeNio", Operation.class);
			m.setAccessible(true);
			m.invoke(drv, op);

			// Should succeed without attempting countBytesDone
			verify(op).status(Operation.Status.SUCC);
		}
	}

	// -----------------------------------------------------------------------
	// putObject — additional edge cases
	// -----------------------------------------------------------------------

	@Nested
	class PutObjectEdgeCasesTest {

		@SuppressWarnings("unchecked")
		@Test
		void unsupportedItemType_throwsUnsupportedOperationException() throws Exception {
			Item plainItem = mock(Item.class); // not DataItem, not PathItem
			when(plainItem.name()).thenReturn("plain");

			Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.CREATE);
			when(op.dstPath()).thenReturn("/bucket");
			when(op.item()).thenReturn(plainItem);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);

			var ex = assertThrows(java.lang.reflect.InvocationTargetException.class,
							() -> exec.invoke(drv, op));
			assertInstanceOf(UnsupportedOperationException.class, ex.getCause());
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
		void readsDataAndCountsBytes() throws Exception {
			DataItem dataItem = mock(DataItem.class);
			when(dataItem.name()).thenReturn("data.bin");

			DataOperation dataOp = mock(DataOperation.class);
			when(dataOp.type()).thenReturn(OpType.READ);
			when(dataOp.dstPath()).thenReturn("/bucket");
			when(dataOp.item()).thenReturn(dataItem);

			byte[] content = new byte[4096];
			GetObjectResponse getResp = GetObjectResponse.builder().build();
			ResponseInputStream<GetObjectResponse> ris = new ResponseInputStream<>(
							getResp, new ByteArrayInputStream(content));
			when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(ris);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);
			exec.invoke(drv, dataOp);

			// readObject should count the 4096 bytes read
			verify(dataOp).countBytesDone(4096L);
		}

		@SuppressWarnings("unchecked")
		@Test
		void nonDataOperation_doesNotCountBytes() throws Exception {
			Item item = mock(Item.class);
			when(item.name()).thenReturn("obj");

			Operation<Item> op = mock(Operation.class);
			when(op.type()).thenReturn(OpType.READ);
			when(op.dstPath()).thenReturn("/bucket");
			when(op.item()).thenReturn(item);

			GetObjectResponse getResp = GetObjectResponse.builder().build();
			ResponseInputStream<GetObjectResponse> ris = new ResponseInputStream<>(
							getResp, new ByteArrayInputStream(new byte[100]));
			when(mockS3Client.getObject(any(GetObjectRequest.class))).thenReturn(ris);

			Method exec = S3AwsStorageDriver.class.getDeclaredMethod("execute", Operation.class);
			exec.setAccessible(true);
			// Should not throw - just doesn't call countBytesDone
			assertDoesNotThrow(() -> exec.invoke(drv, op));
		}
	}

	// -----------------------------------------------------------------------
	// requestNewPath — path without leading slash
	// -----------------------------------------------------------------------

	@Nested
	class RequestNewPathEdgeCasesTest {

		@Test
		void pathWithoutLeadingSlash() throws Exception {
			when(mockS3Client.headBucket(any(HeadBucketRequest.class)))
							.thenReturn(HeadBucketResponse.builder().build());

			Method m = S3AwsStorageDriver.class.getDeclaredMethod("requestNewPath", String.class);
			m.setAccessible(true);
			String result = (String) m.invoke(drv, "nobucket");
			assertEquals("/nobucket", result);
		}
	}
}
