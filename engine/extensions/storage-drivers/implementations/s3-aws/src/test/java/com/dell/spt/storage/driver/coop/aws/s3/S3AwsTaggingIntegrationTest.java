package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Integration-style test for S3 object tagging.
 * Uses a mock S3AsyncClient to capture requests for verification,
 * and real DataItem/DataOperation instances to exercise the full code path.
 */
public class S3AwsTaggingIntegrationTest {

	private static final Credential CREDENTIAL = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	private static Config getConfig(boolean taggingEnabled, Map<String, Object> tags) {
		Config config = Mockito.mock(Config.class);

		// Mock parent class config requirements
		Config driverConfig = Mockito.mock(Config.class);
		Config limitConfig = Mockito.mock(Config.class);
		when(limitConfig.intVal("concurrency")).thenReturn(1);
		when(driverConfig.configVal("limit")).thenReturn(limitConfig);
		when(driverConfig.intVal("threads")).thenReturn(1);
		when(config.configVal("driver")).thenReturn(driverConfig);
		when(config.stringVal("namespace")).thenReturn("test-ns");

		Config authConfig = Mockito.mock(Config.class);
		when(authConfig.stringVal("uid")).thenReturn(CREDENTIAL.getUid());
		when(authConfig.stringVal("secret")).thenReturn(CREDENTIAL.getSecret());
		when(authConfig.stringVal("token")).thenReturn(null);
		when(config.configVal("auth")).thenReturn(authConfig);

		// Mock CoopStorageDriverBase config requirements
		when(config.intVal("driver-limit-queue-input")).thenReturn(100);

		// Mock S3AwsStorageDriver config requirements
		Config objectConfig = Mockito.mock(Config.class);
		when(objectConfig.boolVal("tagging")).thenReturn(taggingEnabled);
		when(objectConfig.boolVal("tagging.enabled")).thenReturn(taggingEnabled);
		when(objectConfig.mapVal("tagging.tags")).thenReturn(tags);
		when(objectConfig.boolVal("versioning")).thenReturn(false);
		when(config.configVal("object")).thenReturn(objectConfig);

		when(config.configVal("item")).thenThrow(new RuntimeException("no item config"));
		when(config.configVal("checksum")).thenThrow(new RuntimeException("no checksum config"));
		when(config.listVal("net.node.addrs")).thenThrow(new RuntimeException("no net config"));
		when(config.stringVal("storage-net-node-addrs")).thenThrow(new RuntimeException("no net config"));

		return config;
	}

	private void setTaggingFields(S3AwsStorageDriver<?, ?> driver, boolean enabled, Map<String, String> tags) throws Exception {
		Field taggingEnabledField = S3AwsStorageDriver.class.getDeclaredField("taggingEnabled");
		taggingEnabledField.setAccessible(true);
		taggingEnabledField.set(driver, enabled);

		Field objectTagsField = S3AwsStorageDriver.class.getDeclaredField("objectTags");
		objectTagsField.setAccessible(true);
		objectTagsField.set(driver, tags != null ? tags : Map.of());
	}

	private S3AsyncClient mockS3Client;
	private S3AwsStorageDriver<DataItem, DataOperation<DataItem>> driver;
	private final Queue<PutObjectRequest> putObjectRequests = new ArrayDeque<>();

	@BeforeEach
	public void setUp() throws Exception {
		putObjectRequests.clear();
		mockS3Client = Mockito.mock(S3AsyncClient.class);

		// Mock putObject to capture requests
		when(mockS3Client.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
						.thenAnswer(invocation -> {
							PutObjectRequest request = invocation.getArgument(0);
							putObjectRequests.add(request);
							return CompletableFuture.completedFuture(PutObjectResponse.builder().build());
						});

		Config config = getConfig(true, Map.of("tag1", "value1", "tag2", "value2"));
		driver = new S3AwsStorageDriver<>(
						"test-storage-driver-s3-aws",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false),
						config,
						false,
						4096,
						mockS3Client);

		// Set bucket name and tagging fields via reflection (bypasses config resolution)
		Field bucketField = S3AwsStorageDriver.class.getDeclaredField("bucketName");
		bucketField.setAccessible(true);
		bucketField.set(driver, "test-bucket");

		setTaggingFields(driver, true, Map.of("tag1", "value1", "tag2", "value2"));
	}

	@AfterEach
	public void tearDown() throws Exception {
		putObjectRequests.clear();
		if (driver != null) {
			driver.close();
		}
	}

	@Test
	public void testPutObjectIncludesTagsWhenEnabled() throws Exception {
		DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

		DataOperation<DataItem> op = new DataOperationImpl<>(
						0, OpType.CREATE, item, null, "/test-bucket",
						CREDENTIAL, null, 0);

		driver.execute(op).join();

		assertEquals(1, putObjectRequests.size());
		PutObjectRequest request = putObjectRequests.poll();
		assertNotNull(request.tagging());
		// Tagging is stored as a string in the request
		assertTrue(request.tagging().contains("tag1"));
		assertTrue(request.tagging().contains("value1"));
	}

	@Test
	public void testPutObjectNoTagsWhenDisabled() throws Exception {
		// Disable tagging via reflection
		setTaggingFields(driver, false, Map.of());

		DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

		DataOperation<DataItem> op = new DataOperationImpl<>(
						0, OpType.CREATE, item, null, "/test-bucket",
						CREDENTIAL, null, 0);

		driver.execute(op).join();

		assertEquals(1, putObjectRequests.size());
		PutObjectRequest request = putObjectRequests.poll();
		assertNull(request.tagging());
	}

	@Test
	public void testPutObjectNoTagsWhenEmpty() throws Exception {
		// Enable tagging but with empty tags via reflection
		setTaggingFields(driver, true, Map.of());

		DataItem item = new com.dell.spt.base.item.DataItemImpl("test-key", 12345, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false));

		DataOperation<DataItem> op = new DataOperationImpl<>(
						0, OpType.CREATE, item, null, "/test-bucket",
						CREDENTIAL, null, 0);

		driver.execute(op).join();

		assertEquals(1, putObjectRequests.size());
		PutObjectRequest request = putObjectRequests.poll();
		assertNull(request.tagging());
	}
}
