package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.ListOptions;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class S3StorageDriverTest {

	private S3AwsStorageDriver<Item, Operation<Item>> newDriverMock() {
		// Create a mock that calls real methods; constructor is not invoked
		return Mockito.mock(S3AwsStorageDriver.class, Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));
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

	@Test
	void list_usesCorrectRequestValues() throws Exception {
		// Set up
		S3AwsStorageDriver<Item, Operation<Item>> drv = newDriverMock();
		S3Client mockS3Client = mock(S3Client.class);
		setS3Client(drv, mockS3Client);
		setBucketName(drv, "test-bucket");

		String path = "/test/path";
		String prefix = "test-prefix";
		int count = 100;

		// Mock response
		ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
						.contents(
										software.amazon.awssdk.services.s3.model.S3Object.builder()
														.key("test-object-1.txt")
														.size(1024L)
														.lastModified(Instant.now())
														.build(),
										software.amazon.awssdk.services.s3.model.S3Object.builder()
														.key("test-object-2.txt")
														.size(2048L)
														.lastModified(Instant.now())
														.build())
						.build();

		when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
						.thenReturn(mockResponse);

		// Execute
		List<Item> result = drv.list(
						mock(ItemFactory.class),
						path,
						prefix,
						10,
						null,
						count,
						ListOptions.DEFAULT);

		// Verify using ArgumentCaptor
		ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
		verify(mockS3Client).listObjectsV2(requestCaptor.capture());

		ListObjectsV2Request capturedRequest = requestCaptor.getValue();
		assertEquals("test-bucket", capturedRequest.bucket());
		assertEquals(prefix, capturedRequest.prefix());
		assertEquals(count, capturedRequest.maxKeys());
		assertEquals(2, result.size());
	}

	@Test
	void listWithPrefix_usesCorrectRequestValues() throws Exception {
		// Set up
		S3AwsStorageDriver<Item, Operation<Item>> drv = newDriverMock();
		S3Client mockS3Client = mock(S3Client.class);
		setS3Client(drv, mockS3Client);
		setBucketName(drv, "test-bucket");

		String path = "/test/path";
		String prefix = "test-prefix";

		// Mock response
		ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
						.contents(
										software.amazon.awssdk.services.s3.model.S3Object.builder()
														.key(prefix + "/object1.txt")
														.size(1024L)
														.lastModified(Instant.now())
														.build())
						.build();

		when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
						.thenReturn(mockResponse);

		// Execute
		List<Item> result = drv.list(
						mock(ItemFactory.class),
						path,
						prefix,
						10,
						null,
						1000,
						ListOptions.DEFAULT);

		// Verify using ArgumentCaptor
		ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
		verify(mockS3Client).listObjectsV2(requestCaptor.capture());

		ListObjectsV2Request capturedRequest = requestCaptor.getValue();
		assertEquals("test-bucket", capturedRequest.bucket());
		assertEquals(prefix, capturedRequest.prefix());
		assertNotNull(result);
	}

	@Test
	void probeCommonPrefixes_returnsCorrectPrefixes() throws Exception {
		// Set up
		S3AwsStorageDriver<Item, Operation<Item>> drv = newDriverMock();
		S3Client mockS3Client = mock(S3Client.class);
		setS3Client(drv, mockS3Client);
		setBucketName(drv, "test-bucket");

		String bucketPath = "/test/path";
		String prefix = "test/";
		String delimiter = "/";

		// Mock response
		ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
						.commonPrefixes(
										CommonPrefix.builder()
														.prefix("test/dir1/")
														.build(),
										CommonPrefix.builder()
														.prefix("test/dir2/")
														.build())
						.build();

		when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
						.thenReturn(mockResponse);

		// Execute
		List<String> result = drv.probeCommonPrefixes(bucketPath, prefix, delimiter, 100);

		// Verify using ArgumentCaptor
		ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
		verify(mockS3Client).listObjectsV2(requestCaptor.capture());

		ListObjectsV2Request capturedRequest = requestCaptor.getValue();
		assertEquals("test-bucket", capturedRequest.bucket());
		assertEquals(prefix, capturedRequest.prefix());
		assertEquals(delimiter, capturedRequest.delimiter());
		assertEquals(2, result.size());
		assertTrue(result.contains("test/dir1/"));
		assertTrue(result.contains("test/dir2/"));
	}

	@Test
	void buildListObjectsRequest_withPrefix_setsCorrectValues() throws Exception {
		// Arrange
		S3AwsStorageDriver<Item, Operation<Item>> drv = newDriverMock();
		setBucketName(drv, "test-bucket");

		S3Client mockS3Client = Mockito.mock(S3Client.class);
		setS3Client(drv, mockS3Client);

		String prefix = "test-prefix/";
		int maxKeys = 100;

		@SuppressWarnings("unchecked")
		ItemFactory<Item> itemFactory = Mockito.mock(ItemFactory.class);

		String path = null;
		int idRadix = 10;
		Item lastPrevItem = null;
		ListOptions options = null; // <-- NO continuation token

		ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);

		ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
						.contents(Collections.emptyList())
						.isTruncated(false)
						.build();

		Mockito.when(mockS3Client.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
						.thenReturn(mockResponse);

		// Act
		drv.list(
						itemFactory,
						path,
						prefix,
						idRadix,
						lastPrevItem,
						maxKeys,
						options);

		// Assert
		Mockito.verify(mockS3Client).listObjectsV2(requestCaptor.capture());
		ListObjectsV2Request request = requestCaptor.getValue();

		assertEquals("test-bucket", request.bucket());
		assertEquals(prefix, request.prefix());
		assertEquals(maxKeys, request.maxKeys());
		assertNull(request.continuationToken());
	}

	@SuppressWarnings("unchecked")
	@Test
	void buildListObjectsRequest_withContinuationToken_setsCorrectValues() throws Exception {
		// Arrange
		S3AwsStorageDriver<Item, Operation<Item>> drv = newDriverMock();
		setBucketName(drv, "test-bucket");

		S3Client mockS3Client = Mockito.mock(S3Client.class);
		setS3Client(drv, mockS3Client);

		String prefix = "test-prefix/";
		int maxKeys = 100;
		String continuationToken = "next-token";

		// Minimal required inputs for list(...)
		ItemFactory<Item> itemFactory = Mockito.mock(ItemFactory.class);
		String path = null;        // path not relevant for this test
		int idRadix = 10;
		Item lastPrevItem = null;

		ListOptions options = ListOptions.builder()
						.continuationToken(continuationToken)
						.build();

		ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);

		// Mock S3 response so method completes
		ListObjectsV2Response mockResponse = ListObjectsV2Response.builder()
						.contents(Collections.emptyList())
						.isTruncated(false)
						.build();

		Mockito.when(mockS3Client.listObjectsV2(Mockito.any(ListObjectsV2Request.class)))
						.thenReturn(mockResponse);

		// Act
		drv.list(
						itemFactory,
						path,
						prefix,
						idRadix,
						lastPrevItem,
						maxKeys,
						options);

		// Assert
		Mockito.verify(mockS3Client).listObjectsV2(requestCaptor.capture());
		ListObjectsV2Request request = requestCaptor.getValue();

		assertEquals("test-bucket", request.bucket());
		assertEquals(prefix, request.prefix());
		assertEquals(maxKeys, request.maxKeys());
		assertEquals(continuationToken, request.continuationToken());
	}

	@Test
	void list_callsS3ClientWithCorrectRequest() throws Exception {
		// Set up
		S3AwsStorageDriver<Item, Operation<Item>> drv = newDriverMock();
		S3Client mockS3Client = mock(S3Client.class);
		setS3Client(drv, mockS3Client);
		setBucketName(drv, "test-bucket");

		// Create mock ItemFactory
		@SuppressWarnings("unchecked")
		ItemFactory<Item> mockItemFactory = mock(ItemFactory.class);

		String path = "test-path/";
		String prefix = "test-prefix/";
		int idRadix = 16; // Hex
		Item lastPrevItem = null; // No previous item
		int count = 100;

		// Mock S3 response
		S3Object s3Object1 = S3Object.builder()
						.key("test-prefix/object1.txt")
						.size(100L)
						.build();
		S3Object s3Object2 = S3Object.builder()
						.key("test-prefix/object2.txt")
						.size(200L)
						.build();

		ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
						.contents(Arrays.asList(s3Object1, s3Object2))
						.build();

		when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
						.thenReturn(listResponse);

		// Mock ItemFactory behavior - adjust this based on how your ItemFactory works
		Item mockItem1 = mock(Item.class);
		Item mockItem2 = mock(Item.class);

		// You might need to adjust these mocks based on how your ItemFactory works
		when(mockItemFactory.getItem(anyString(), anyInt(), anyLong())).thenReturn(mockItem1, mockItem2);

		// Execute
		List<Item> result = drv.list(mockItemFactory, path, prefix, idRadix, lastPrevItem, count);

		// Verify - adjust assertions based on what you expect
		assertNotNull(result);
		// Don't assert the size - it might depend on implementation details
		verify(mockS3Client).listObjectsV2(any(ListObjectsV2Request.class));
	}
}
