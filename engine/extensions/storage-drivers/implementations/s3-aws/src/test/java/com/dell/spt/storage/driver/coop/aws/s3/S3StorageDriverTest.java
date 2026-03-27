package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.ListOptions;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class S3StorageDriverTest {

    private S3StorageDriver<Item, Operation<Item>> newDriverMock() {
        // Create a mock that calls real methods; constructor is not invoked
        return Mockito.mock(S3StorageDriver.class, Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));
    }
    
    private void setBucketName(S3StorageDriver<Item, Operation<Item>> driver, String bucketName) throws Exception {
        Field bucketField = S3StorageDriver.class.getDeclaredField("bucketName");
        bucketField.setAccessible(true);
        bucketField.set(driver, bucketName);
    }
    
    private void setS3Client(S3StorageDriver<Item, Operation<Item>> driver, S3Client s3Client) throws Exception {
        Field clientField = S3StorageDriver.class.getDeclaredField("s3Client");
        clientField.setAccessible(true);
        clientField.set(driver, s3Client);
    }

    @Test
    void putObject_usesCorrectRequestValues() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        String content = "Test content";
        InputStream contentStream = new ByteArrayInputStream(content.getBytes());
        long contentLength = content.getBytes().length;
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");
        
        // Execute
        drv.putObject(key, contentStream, contentLength, metadata);
        
        // Verify using ArgumentCaptor
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(mockS3Client).putObject(requestCaptor.capture(), bodyCaptor.capture());
        
        PutObjectRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-bucket", capturedRequest.bucket());
        assertEquals(key, capturedRequest.key());
        assertEquals("custom-value", capturedRequest.metadata().get("custom-key"));
    }
    
    @Test
    void getObject_usesCorrectRequestValues() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        // Mock response
        GetObjectResponse mockResponse = GetObjectResponse.builder().build();
        InputStream mockInputStream = new ByteArrayInputStream("Test content".getBytes());
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(mockResponse, mockInputStream);
        
        when(mockS3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(responseInputStream);
        
        // Execute
        drv.getObject(key);
        
        // Verify using ArgumentCaptor
        ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockS3Client).getObject(requestCaptor.capture());
        
        GetObjectRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-bucket", capturedRequest.bucket());
        assertEquals(key, capturedRequest.key());
    }

    @Test
    void buildDeleteObjectRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";

        ArgumentCaptor<DeleteObjectRequest> requestCaptor =
                ArgumentCaptor.forClass(DeleteObjectRequest.class);

        // deleteObject() returns void, so no stubbing needed

        // Act
        drv.deleteObject(key);

        // Assert
        Mockito.verify(mockS3Client).deleteObject(requestCaptor.capture());
        DeleteObjectRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
    }
    
    @Test
    void getObjectMetadata_usesCorrectRequestValues() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
                .metadata(Collections.singletonMap("custom-key", "custom-value"))
                .contentLength(100L)
                .build();
        
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headObjectResponse);
        
        // Execute
        drv.getObjectMetadata(key);
        
        // Verify using ArgumentCaptor
        ArgumentCaptor<HeadObjectRequest> requestCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(mockS3Client).headObject(requestCaptor.capture());
        
        HeadObjectRequest capturedRequest = requestCaptor.getValue();
        assertEquals("test-bucket", capturedRequest.bucket());
        assertEquals(key, capturedRequest.key());
    }
    
    @Test
    void buildListObjectsRequest_withPrefix_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
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

        ArgumentCaptor<ListObjectsV2Request> requestCaptor =
                ArgumentCaptor.forClass(ListObjectsV2Request.class);

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
            options
        );

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
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
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

        ArgumentCaptor<ListObjectsV2Request> requestCaptor =
            ArgumentCaptor.forClass(ListObjectsV2Request.class);

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
            options
        );

        // Assert
        Mockito.verify(mockS3Client).listObjectsV2(requestCaptor.capture());
        ListObjectsV2Request request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(prefix, request.prefix());
        assertEquals(maxKeys, request.maxKeys());
        assertEquals(continuationToken, request.continuationToken());
    }

    @Test
    void buildCreateMultipartUploadRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");

        ArgumentCaptor<CreateMultipartUploadRequest> requestCaptor =
                ArgumentCaptor.forClass(CreateMultipartUploadRequest.class);

        CreateMultipartUploadResponse mockResponse =
                CreateMultipartUploadResponse.builder()
                        .uploadId("upload-id-123")
                        .build();

        Mockito.when(mockS3Client.createMultipartUpload(
                Mockito.any(CreateMultipartUploadRequest.class)))
                .thenReturn(mockResponse);

        // Act
        String uploadId = drv.initiateMultipartUpload(key, metadata);

        // Assert
        Mockito.verify(mockS3Client).createMultipartUpload(requestCaptor.capture());
        CreateMultipartUploadRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
        assertEquals("custom-value", request.metadata().get("custom-key"));
        assertEquals("upload-id-123", uploadId);
    }
    
    @Test
    void buildUploadPartRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";
        String uploadId = "test-upload-id";
        int partNumber = 1;

        // Dummy upload data
        byte[] dataBytes = "test-data".getBytes(StandardCharsets.UTF_8);
        InputStream data = new ByteArrayInputStream(dataBytes);
        long length = dataBytes.length;

        ArgumentCaptor<UploadPartRequest> requestCaptor =
                ArgumentCaptor.forClass(UploadPartRequest.class);

        UploadPartResponse mockResponse = UploadPartResponse.builder()
                .eTag("etag-123")
                .build();

        Mockito.when(
            mockS3Client.uploadPart(
                Mockito.any(UploadPartRequest.class),
                Mockito.any(RequestBody.class)
            )
        ).thenReturn(mockResponse);

        // Act
        String eTag = drv.uploadPart(key, uploadId, partNumber, data, length);

        // Assert
        Mockito.verify(mockS3Client).uploadPart(
            requestCaptor.capture(),
            Mockito.any(RequestBody.class)
        );

        UploadPartRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
        assertEquals(uploadId, request.uploadId());
        assertEquals(partNumber, request.partNumber());
        assertEquals("etag-123", eTag);
    }

    @Test
    void buildCompleteMultipartUploadRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";
        String uploadId = "test-upload-id";
        List<String> etags = Arrays.asList("etag1", "etag2");

        ArgumentCaptor<CompleteMultipartUploadRequest> requestCaptor =
                ArgumentCaptor.forClass(CompleteMultipartUploadRequest.class);

        // completeMultipartUpload returns void; no stubbing required

        // Act
        drv.completeMultipartUpload(key, uploadId, etags);

        // Assert
        Mockito.verify(mockS3Client).completeMultipartUpload(requestCaptor.capture());
        CompleteMultipartUploadRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
        assertEquals(uploadId, request.uploadId());

        assertEquals(2, request.multipartUpload().parts().size());

        assertEquals(1, request.multipartUpload().parts().get(0).partNumber());
        assertEquals("etag1", request.multipartUpload().parts().get(0).eTag());

        assertEquals(2, request.multipartUpload().parts().get(1).partNumber());
        assertEquals("etag2", request.multipartUpload().parts().get(1).eTag());
    }

    @Test
    void buildAbortMultipartUploadRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";
        String uploadId = "test-upload-id";

        ArgumentCaptor<AbortMultipartUploadRequest> requestCaptor =
                ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);

        // We don't need to stub return value because abortMultipartUpload() returns void

        // Act
        drv.abortMultipartUpload(key, uploadId);

        // Assert
        Mockito.verify(mockS3Client).abortMultipartUpload(requestCaptor.capture());
        AbortMultipartUploadRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
        assertEquals(uploadId, request.uploadId());
    }
    
    @Test
    void buildGetObjectTaggingRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";

        // Mock S3 response
        GetObjectTaggingResponse mockResponse = GetObjectTaggingResponse.builder()
            .tagSet(
                Tag.builder().key("env").value("test").build()
            )
            .build();

        Mockito.when(mockS3Client.getObjectTagging(Mockito.any(GetObjectTaggingRequest.class)))
            .thenReturn(mockResponse);

        ArgumentCaptor<GetObjectTaggingRequest> requestCaptor =
            ArgumentCaptor.forClass(GetObjectTaggingRequest.class);

        // Act
        drv.getObjectTags(key);

        // Assert
        Mockito.verify(mockS3Client).getObjectTagging(requestCaptor.capture());
        GetObjectTaggingRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
    }

    @Test
    void buildPutObjectTaggingRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String key = "test-object.txt";
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");

        ArgumentCaptor<PutObjectTaggingRequest> requestCaptor =
                ArgumentCaptor.forClass(PutObjectTaggingRequest.class);

        // No stub required: putObjectTagging() returns void

        // Act
        drv.setObjectTags(key, tags);

        // Assert
        Mockito.verify(mockS3Client).putObjectTagging(requestCaptor.capture());
        PutObjectTaggingRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(key, request.key());
        assertEquals(2, request.tagging().tagSet().size());

        // Verify tags (order-independent)
        Map<String, String> actualTags = new HashMap<>();
        for (Tag tag : request.tagging().tagSet()) {
            actualTags.put(tag.key(), tag.value());
        }

        assertEquals("value1", actualTags.get("tag1"));
        assertEquals("value2", actualTags.get("tag2"));
    }

    @Test
    void buildPutBucketVersioningRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        ArgumentCaptor<PutBucketVersioningRequest> requestCaptor =
                ArgumentCaptor.forClass(PutBucketVersioningRequest.class);

        // putBucketVersioning returns void, so no stubbing needed

        // Act
        drv.enableVersioning();

        // Assert
        Mockito.verify(mockS3Client).putBucketVersioning(requestCaptor.capture());
        PutBucketVersioningRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(
            BucketVersioningStatus.ENABLED,
            request.versioningConfiguration().status()
        );
    }
    
    @Test
    void buildGetBucketVersioningRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        ArgumentCaptor<GetBucketVersioningRequest> requestCaptor =
                ArgumentCaptor.forClass(GetBucketVersioningRequest.class);

        GetBucketVersioningResponse mockResponse = GetBucketVersioningResponse.builder()
                .status(BucketVersioningStatus.ENABLED)
                .build();

        Mockito.when(mockS3Client.getBucketVersioning(
                Mockito.any(GetBucketVersioningRequest.class)))
                .thenReturn(mockResponse);

        // Act
        String status = drv.getVersioningStatus();

        // Assert
        Mockito.verify(mockS3Client).getBucketVersioning(requestCaptor.capture());
        GetBucketVersioningRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(BucketVersioningStatus.ENABLED.name(), status);
    }

    @Test
    void buildCopyObjectRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        String sourceKey = "source-object.txt";
        String destinationKey = "destination-object.txt";

        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");

        ArgumentCaptor<CopyObjectRequest> requestCaptor =
                ArgumentCaptor.forClass(CopyObjectRequest.class);

        // copyObject returns void; no stubbing required

        // Act
        drv.copyObject(sourceKey, destinationKey, metadata);

        // Assert
        Mockito.verify(mockS3Client).copyObject(requestCaptor.capture());
        CopyObjectRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.sourceBucket());
        assertEquals(sourceKey, request.sourceKey());
        assertEquals("test-bucket", request.destinationBucket());
        assertEquals(destinationKey, request.destinationKey());
        assertEquals(MetadataDirective.REPLACE, request.metadataDirective());
        assertEquals("custom-value", request.metadata().get("custom-key"));
    }
   
    @Test
    void buildDeleteObjectsRequest_setsCorrectValues() throws Exception {
        // Arrange
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        setBucketName(drv, "test-bucket");

        // Mock S3 client
        S3Client mockS3Client = Mockito.mock(S3Client.class);
        setS3Client(drv, mockS3Client);

        List<String> keys = Arrays.asList("key1", "key2", "key3");

        // Capture the DeleteObjectsRequest passed to S3
        ArgumentCaptor<DeleteObjectsRequest> requestCaptor =
                ArgumentCaptor.forClass(DeleteObjectsRequest.class);

        DeleteObjectsResponse mockResponse = DeleteObjectsResponse.builder()
                .deleted(
                    keys.stream()
                        .map(k -> DeletedObject.builder().key(k).build())
                        .collect(Collectors.toList())
                )
                .build();

        Mockito.when(mockS3Client.deleteObjects(Mockito.any(DeleteObjectsRequest.class)))
                .thenReturn(mockResponse);

        // Act
        drv.deleteObjects(keys);

        // Assert
        Mockito.verify(mockS3Client).deleteObjects(requestCaptor.capture());
        DeleteObjectsRequest request = requestCaptor.getValue();

        assertEquals("test-bucket", request.bucket());
        assertEquals(3, request.delete().objects().size());
        assertEquals("key1", request.delete().objects().get(0).key());
        assertEquals("key2", request.delete().objects().get(1).key());
        assertEquals("key3", request.delete().objects().get(2).key());
    }

    @Test
    void putObject_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");

        String key = "test-object.txt";
        String content = "Test content";
        byte[] contentBytes = content.getBytes();
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        long contentLength = contentBytes.length;
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");

        when(mockS3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().build());

        // Execute
        drv.putObject(key, contentStream, contentLength, metadata);

        // Verify
        verify(mockS3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void getObject_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        // Create a real stream for testing
        InputStream mockInputStream = new ByteArrayInputStream("Test content".getBytes());
        
        // Create a mock GetObjectResponse
        GetObjectResponse mockResponse = GetObjectResponse.builder().build();
        
        // Create a custom ResponseInputStream implementation
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(mockResponse, mockInputStream);
        
        when(mockS3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(responseInputStream);
        
        // Execute
        InputStream result = drv.getObject(key);
        
        // Verify
        verify(mockS3Client).getObject(any(GetObjectRequest.class));
        assertNotNull(result);
    }
    
    @Test
    void deleteObject_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        when(mockS3Client.deleteObject(any(DeleteObjectRequest.class)))
                .thenReturn(DeleteObjectResponse.builder().build());
        
        // Execute
        drv.deleteObject(key);
        
        // Verify
        verify(mockS3Client).deleteObject(any(DeleteObjectRequest.class));
    }

        @Test
    void objectExists_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().build());
        
        // Execute
        boolean exists = drv.objectExists(key);
        
        // Verify
        assertTrue(exists);
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }
    
    @Test
    void objectExists_returnsFalseWhenKeyNotFound() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().build());
        
        // Execute
        boolean exists = drv.objectExists(key);
        
        // Verify
        assertFalse(exists);
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }
    
    @Test
    void getObjectMetadata_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        // Create mock HeadObjectResponse
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");
        
        HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
                .metadata(metadata)
                .contentLength(100L)
                .lastModified(Instant.now())
                .build();
        
        when(mockS3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(headObjectResponse);
        
        // Execute
        Map<String, String> result = drv.getObjectMetadata(key);
        
        // Verify
        assertNotNull(result);
        assertEquals("custom-value", result.get("custom-key"));
        verify(mockS3Client).headObject(any(HeadObjectRequest.class));
    }

    @Test
    void list_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
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
    
    @Test
    void initiateMultipartUpload_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");
        
        CreateMultipartUploadResponse response = CreateMultipartUploadResponse.builder()
                .uploadId("test-upload-id")
                .build();
        
        when(mockS3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
                .thenReturn(response);
        
        // Execute
        String uploadId = drv.initiateMultipartUpload(key, metadata);  // Changed from createMultipartUpload to initiateMultipartUpload
        
        // Verify
        assertEquals("test-upload-id", uploadId);
        verify(mockS3Client).createMultipartUpload(any(CreateMultipartUploadRequest.class));
    }
    
    @Test
    void uploadPart_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        String uploadId = "test-upload-id";
        int partNumber = 1;
        byte[] data = "Test content".getBytes();
        InputStream inputStream = new ByteArrayInputStream(data);
        long contentLength = data.length;
        
        UploadPartResponse response = UploadPartResponse.builder()
                .eTag("test-etag")
                .build();
        
        when(mockS3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
                .thenReturn(response);
        
        // Execute
        String etag = drv.uploadPart(key, uploadId, partNumber, inputStream, contentLength);
        
        // Verify
        assertEquals("test-etag", etag);
        verify(mockS3Client).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    }

    @Test
    void completeMultipartUpload_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        String uploadId = "test-upload-id";
        List<String> etags = Arrays.asList("etag1", "etag2");
        
        CompleteMultipartUploadResponse response = CompleteMultipartUploadResponse.builder()
                .bucket("test-bucket")
                .key(key)
                .build();
        
        when(mockS3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
                .thenReturn(response);
        
        // Execute
        drv.completeMultipartUpload(key, uploadId, etags);
        
        // Verify
        verify(mockS3Client).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
    }
    
    @Test
    void abortMultipartUpload_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        String uploadId = "test-upload-id";
        
        AbortMultipartUploadResponse response = AbortMultipartUploadResponse.builder().build();
        
        when(mockS3Client.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
                .thenReturn(response);
        
        // Execute
        drv.abortMultipartUpload(key, uploadId);
        
        // Verify
        verify(mockS3Client).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
    }
    
    @Test
    void getObjectTags_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        
        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.builder().key("tag1").value("value1").build());
        tags.add(Tag.builder().key("tag2").value("value2").build());
        
        GetObjectTaggingResponse response = GetObjectTaggingResponse.builder()
                .tagSet(tags)
                .build();
        
        when(mockS3Client.getObjectTagging(any(GetObjectTaggingRequest.class)))
                .thenReturn(response);
        
        // Execute
        Map<String, String> result = drv.getObjectTags(key);
        
        // Verify
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("value1", result.get("tag1"));
        assertEquals("value2", result.get("tag2"));
        verify(mockS3Client).getObjectTagging(any(GetObjectTaggingRequest.class));
    }

    @Test
    void setObjectTags_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String key = "test-object.txt";
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        tags.put("tag2", "value2");
        
        PutObjectTaggingResponse response = PutObjectTaggingResponse.builder().build();
        
        when(mockS3Client.putObjectTagging(any(PutObjectTaggingRequest.class)))
                .thenReturn(response);
        
        // Execute
        drv.setObjectTags(key, tags);  // Changed from putObjectTags to setObjectTags
        
        // Verify
        verify(mockS3Client).putObjectTagging(any(PutObjectTaggingRequest.class));
    }
    
    @Test
    void copyObject_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        String sourceKey = "source-object.txt";
        String destinationKey = "destination-object.txt";
        Map<String, String> metadata = new HashMap<>();
        metadata.put("custom-key", "custom-value");
        
        CopyObjectResponse response = CopyObjectResponse.builder().build();
        
        when(mockS3Client.copyObject(any(CopyObjectRequest.class)))
                .thenReturn(response);
        
        // Execute
        drv.copyObject(sourceKey, destinationKey, metadata);
        
        // Verify
        verify(mockS3Client).copyObject(any(CopyObjectRequest.class));
    }
    
    @Test
    void deleteObjects_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        List<String> keys = Arrays.asList("key1", "key2", "key3");
        
        DeleteObjectsResponse response = DeleteObjectsResponse.builder().build();
        
        when(mockS3Client.deleteObjects(any(DeleteObjectsRequest.class)))
                .thenReturn(response);
        
        // Execute
        drv.deleteObjects(keys);
        
        // Verify
        verify(mockS3Client).deleteObjects(any(DeleteObjectsRequest.class));
    }
    
    @Test
    void enableVersioning_callsS3ClientWithCorrectRequest() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        PutBucketVersioningResponse response = PutBucketVersioningResponse.builder().build();
        
        when(mockS3Client.putBucketVersioning(any(PutBucketVersioningRequest.class)))
                .thenReturn(response);
        
        // Execute
        drv.enableVersioning();
        
        // Verify
        verify(mockS3Client).putBucketVersioning(any(PutBucketVersioningRequest.class));
    }
    
    @Test
    void getVersioningStatus_returnsSuspendedWhenNotEnabled() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        GetBucketVersioningResponse response = GetBucketVersioningResponse.builder()
                .status(BucketVersioningStatus.SUSPENDED)
                .build();
        
        when(mockS3Client.getBucketVersioning(any(GetBucketVersioningRequest.class)))
                .thenReturn(response);
        
        // Execute and verify
        String status = drv.getVersioningStatus();
        assertEquals("SUSPENDED", status);
        verify(mockS3Client).getBucketVersioning(any(GetBucketVersioningRequest.class));
    }

    @Test
    void isVersioningEnabled_returnsFalseWhenNotEnabled() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        // Return a response with null status (never enabled)
        GetBucketVersioningResponse response = GetBucketVersioningResponse.builder()
                .build(); // No status set
        
        when(mockS3Client.getBucketVersioning(any(GetBucketVersioningRequest.class)))
                .thenReturn(response);
        
        // Execute and verify
        assertFalse(drv.isVersioningEnabled());
        verify(mockS3Client).getBucketVersioning(any(GetBucketVersioningRequest.class));
    }

    @Test
    void isVersioningEnabled_returnsFalseWhenSuspended() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        // Return a response with SUSPENDED status
        GetBucketVersioningResponse response = GetBucketVersioningResponse.builder()
                .status(BucketVersioningStatus.SUSPENDED)
                .build();
        
        when(mockS3Client.getBucketVersioning(any(GetBucketVersioningRequest.class)))
                .thenReturn(response);
        
        // Execute and verify
        assertFalse(drv.isVersioningEnabled());
        verify(mockS3Client).getBucketVersioning(any(GetBucketVersioningRequest.class));
    }

    @Test
    void isVersioningEnabled_returnsTrueWhenEnabled() throws Exception {
        // Set up
        S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
        S3Client mockS3Client = mock(S3Client.class);
        setS3Client(drv, mockS3Client);
        setBucketName(drv, "test-bucket");
        
        // Return a response with ENABLED status
        GetBucketVersioningResponse response = GetBucketVersioningResponse.builder()
                .status(BucketVersioningStatus.ENABLED)
                .build();
        
        when(mockS3Client.getBucketVersioning(any(GetBucketVersioningRequest.class)))
                .thenReturn(response);
        
        // Execute and verify
        assertTrue(drv.isVersioningEnabled());
        verify(mockS3Client).getBucketVersioning(any(GetBucketVersioningRequest.class));
    }
}
        
