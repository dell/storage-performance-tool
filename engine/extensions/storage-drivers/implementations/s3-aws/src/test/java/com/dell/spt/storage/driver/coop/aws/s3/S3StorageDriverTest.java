package com.dell.spt.storage.driver.coop.aws.s3;

import java.io.IOException;
import com.dell.spt.storage.driver.api.StorageMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for S3StorageDriver
 */
@RunWith(MockitoJUnitRunner.class)
public class S3StorageDriverTest {

    @Mock
    private S3Client mockS3Client;

    private S3StorageDriver s3StorageDriver;
    private final String bucketName = "test-bucket";
    private final String testKey = "test-object.txt";
    private final String testContent = "Hello, S3!";

    @Before
    public void setUp() {
        s3StorageDriver = new S3StorageDriver(mockS3Client, bucketName);
    }

    @Test
    public void testStoreObject() throws IOException {
        // Given
        InputStream data = new ByteArrayInputStream(testContent.getBytes());
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");

        // When
        s3StorageDriver.storeObject(testKey, data, testContent.length(), metadata);

        // Then
        verify(mockS3Client).putObject(any(PutObjectRequest.class), any());
    }

    @Test
    public void testGetObject() throws IOException {
        // Given
        InputStream expectedStream = new ByteArrayInputStream(testContent.getBytes());
        when(mockS3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(expectedStream);

        // When
        InputStream result = s3StorageDriver.getObject(testKey);

        // Then
        assertNotNull(result);
        verify(mockS3Client).getObject(any(GetObjectRequest.class));
    }

    @Test(expected = IOException.class)
    public void testGetObjectNotFound() throws IOException {
        // Given
        when(mockS3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().build());

        // When
        s3StorageDriver.getObject(testKey);
    }

    @Test
    public void testDeleteObject() throws IOException {
        // When
        s3StorageDriver.deleteObject(testKey);

        // Then
        verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    public void testObjectExists() throws IOException {
        // Given
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .contentLength(testContent.length())
                .build();
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headResponse);

        // When
        boolean exists = s3StorageDriver.objectExists(testKey);

        // Then
        assertTrue(exists);
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }

    @Test
    public void testObjectNotExists() throws IOException {
        // Given
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().build());

        // When
        boolean exists = s3StorageDriver.objectExists(testKey);

        // Then
        assertFalse(exists);
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }

    @Test
    public void testListObjects() throws IOException {
        // Given
        ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
                .contents(S3Object.builder()
                        .key(testKey)
                        .size(testContent.length())
                        .eTag("test-etag")
                        .storageClass(StorageClass.STANDARD)
                        .build())
                .build();
        when(mockS3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(listResponse);

        // When
        List<StorageMetadata> result = s3StorageDriver.listObjects("test-");

        // Then
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(testKey, result.get(0).getKey());
        verify(mockS3Client).listObjectsV2(any(ListObjectsV2Request.class));
    }

    @Test
    public void testCopyObject() throws IOException {
        // Given
        String destinationKey = "copied-object.txt";

        // When
        s3StorageDriver.copyObject(testKey, destinationKey);

        // Then
        verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
    }

    @Test
    public void testGetObjectMetadata() throws IOException {
        // Given
        Map<String, String> metadata = new HashMap<>();
        metadata.put("x-amz-meta-custom-key", "custom-value");
        
        HeadObjectResponse headResponse = HeadObjectResponse.builder()
                .contentLength(testContent.length())
                .eTag("test-etag")
                .contentType("text/plain")
                .metadata(metadata)
                .storageClass(StorageClass.STANDARD)
                .build();
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headResponse);

        // When
        StorageMetadata result = s3StorageDriver.getObjectMetadata(testKey);

        // Then
        assertNotNull(result);
        assertEquals(testKey, result.getKey());
        assertEquals(testContent.length(), result.getContentLength());
        assertEquals("test-etag", result.getETag());
        assertEquals("text/plain", result.getContentType());
        assertEquals("custom-value", result.getMetadata().get("custom-key"));
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }

    @Test(expected = IOException.class)
    public void testGetObjectMetadataNotFound() throws IOException {
        // Given
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().build());

        // When
        s3StorageDriver.getObjectMetadata(testKey);
    }

    @Test
    public void testGetBucketName() {
        assertEquals(bucketName, s3StorageDriver.getBucketName());
    }

    @Test
    public void testClose() {
        // When
        s3StorageDriver.close();

        // Then
        verify(mockS3Client).close();
    }
}
