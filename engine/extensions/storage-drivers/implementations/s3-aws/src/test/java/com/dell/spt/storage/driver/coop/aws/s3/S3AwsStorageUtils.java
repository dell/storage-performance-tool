package com.dell.spt.storage.driver.coop.aws.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Utility class providing S3 convenience methods for external use.
 * These methods provide direct S3 operations without using the SPT operation lifecycle.
 */
public class S3AwsStorageUtils {

	private final S3Client s3Client;
	private final String bucketName;

	public S3AwsStorageUtils(S3Client s3Client, String bucketName) {
		this.s3Client = s3Client;
		this.bucketName = bucketName;
	}

	/**
	 * Store (put) an object in S3.
	 */
	public void putObject(String key, InputStream data, long length, Map<String, String> metadata) throws IOException {
		try {
			PutObjectRequest.Builder reqBuilder = PutObjectRequest.builder()
							.bucket(bucketName)
							.key(key);
			if (metadata != null && !metadata.isEmpty()) {
				reqBuilder.metadata(metadata);
			}
			PutObjectRequest req = reqBuilder.build();
			s3Client.putObject(req, RequestBody.fromInputStream(data, length));
		} catch (Exception e) {
			throw new IOException("Failed to put object to S3", e);
		}
	}

	/**
	 * Get (download) an object from S3.
	 */
	public InputStream getObject(String key) throws IOException {
		try {
			GetObjectRequest req = GetObjectRequest.builder()
							.bucket(bucketName)
							.key(key)
							.build();
			return s3Client.getObject(req);
		} catch (software.amazon.awssdk.services.s3.model.NoSuchKeyException e) {
			throw new IOException("Object not found: " + key, e);
		} catch (Exception e) {
			throw new IOException("Failed to get object from S3", e);
		}
	}

	/**
	 * Delete an object from S3.
	 */
	public void deleteObject(String key) throws IOException {
		try {
			DeleteObjectRequest req = DeleteObjectRequest.builder()
							.bucket(bucketName)
							.key(key)
							.build();
			s3Client.deleteObject(req);
		} catch (Exception e) {
			throw new IOException("Failed to delete object from S3", e);
		}
	}

	/**
	 * Check if an object exists in S3.
	 */
	public boolean objectExists(String key) throws IOException {
		try {
			HeadObjectRequest req = HeadObjectRequest.builder()
							.bucket(bucketName)
							.key(key)
							.build();
			s3Client.headObject(req);
			return true;
		} catch (software.amazon.awssdk.services.s3.model.NoSuchKeyException e) {
			return false;
		} catch (Exception e) {
			throw new IOException("Failed to check if object exists in S3", e);
		}
	}

	/**
	 * Initiate multipart upload.
	 */
	public String initiateMultipartUpload(String key, Map<String, String> metadata) throws IOException {
		try {
			CreateMultipartUploadRequest.Builder reqBuilder = CreateMultipartUploadRequest.builder()
							.bucket(bucketName)
							.key(key);
			if (metadata != null && !metadata.isEmpty()) {
				reqBuilder.metadata(metadata);
			}
			CreateMultipartUploadResponse resp = s3Client.createMultipartUpload(reqBuilder.build());
			return resp.uploadId();
		} catch (Exception e) {
			throw new IOException("Failed to initiate multipart upload", e);
		}
	}

	/**
	 * Upload a part in multipart upload.
	 */
	public String uploadPart(String key, String uploadId, int partNumber, InputStream data, long length) throws IOException {
		try {
			UploadPartRequest req = UploadPartRequest.builder()
							.bucket(bucketName)
							.key(key)
							.uploadId(uploadId)
							.partNumber(partNumber)
							.build();
			UploadPartResponse resp = s3Client.uploadPart(req, RequestBody.fromInputStream(data, length));
			return resp.eTag();
		} catch (Exception e) {
			throw new IOException("Failed to upload part", e);
		}
	}

	/**
	 * Complete multipart upload.
	 */
	public void completeMultipartUpload(String key, String uploadId, List<String> etags) throws IOException {
		try {
			List<CompletedPart> completedParts = new ArrayList<>();
			for (int i = 0; i < etags.size(); i++) {
				completedParts.add(CompletedPart.builder()
								.partNumber(i + 1)
								.eTag(etags.get(i))
								.build());
			}
			CompleteMultipartUploadRequest req = CompleteMultipartUploadRequest.builder()
							.bucket(bucketName)
							.key(key)
							.uploadId(uploadId)
							.multipartUpload(CompletedMultipartUpload.builder()
											.parts(completedParts)
											.build())
							.build();
			s3Client.completeMultipartUpload(req);
		} catch (Exception e) {
			throw new IOException("Failed to complete multipart upload", e);
		}
	}

	/**
	 * Abort multipart upload.
	 */
	public void abortMultipartUpload(String key, String uploadId) throws IOException {
		try {
			AbortMultipartUploadRequest req = AbortMultipartUploadRequest.builder()
							.bucket(bucketName)
							.key(key)
							.uploadId(uploadId)
							.build();
			s3Client.abortMultipartUpload(req);
		} catch (Exception e) {
			throw new IOException("Failed to abort multipart upload", e);
		}
	}

	/**
	 * Get object tags.
	 */
	public Map<String, String> getObjectTags(String key) throws IOException {
		try {
			GetObjectTaggingRequest req = GetObjectTaggingRequest.builder()
							.bucket(bucketName)
							.key(key)
							.build();
			GetObjectTaggingResponse resp = s3Client.getObjectTagging(req);
			Map<String, String> tags = new HashMap<>();
			for (Tag tag : resp.tagSet()) {
				tags.put(tag.key(), tag.value());
			}
			return tags;
		} catch (Exception e) {
			throw new IOException("Failed to get object tags", e);
		}
	}

	/**
	 * Set object tags.
	 */
	public void setObjectTags(String key, Map<String, String> tags) throws IOException {
		try {
			List<Tag> tagList = new ArrayList<>();
			for (Map.Entry<String, String> entry : tags.entrySet()) {
				tagList.add(Tag.builder().key(entry.getKey()).value(entry.getValue()).build());
			}
			PutObjectTaggingRequest req = PutObjectTaggingRequest.builder()
							.bucket(bucketName)
							.key(key)
							.tagging(Tagging.builder().tagSet(tagList).build())
							.build();
			s3Client.putObjectTagging(req);
		} catch (Exception e) {
			throw new IOException("Failed to set object tags", e);
		}
	}

	/**
	 * Enable versioning on the bucket.
	 */
	public void enableVersioning() throws IOException {
		try {
			PutBucketVersioningRequest req = PutBucketVersioningRequest.builder()
							.bucket(bucketName)
							.versioningConfiguration(
											VersioningConfiguration.builder()
															.status(BucketVersioningStatus.ENABLED)
															.build())
							.build();
			s3Client.putBucketVersioning(req);
		} catch (Exception e) {
			throw new IOException("Failed to enable versioning", e);
		}
	}

	/**
	 * Get versioning status of the bucket.
	 */
	public String getVersioningStatus() throws IOException {
		try {
			GetBucketVersioningRequest req = GetBucketVersioningRequest.builder()
							.bucket(bucketName)
							.build();

			GetBucketVersioningResponse resp = s3Client.getBucketVersioning(req);
			if (resp.status() != null) {
				return resp.status().name();
			}

			return BucketVersioningStatus.SUSPENDED.name(); // default
		} catch (Exception e) {
			throw new IOException("Failed to get versioning status", e);
		}
	}

	/**
	 * Checks if versioning is enabled for the S3 bucket.
	 */
	public boolean isVersioningEnabled() throws IOException {
		String status = getVersioningStatus();
		return "Enabled".equalsIgnoreCase(status);
	}

	/**
	 * Copy an object within S3.
	 */
	public void copyObject(String sourceKey, String destinationKey, Map<String, String> metadata) throws IOException {
		try {
			CopyObjectRequest.Builder reqBuilder = CopyObjectRequest.builder()
							.sourceBucket(bucketName)
							.sourceKey(sourceKey)
							.destinationBucket(bucketName)
							.destinationKey(destinationKey);
			if (metadata != null && !metadata.isEmpty()) {
				reqBuilder.metadata(metadata)
								.metadataDirective(MetadataDirective.REPLACE);
			}
			s3Client.copyObject(reqBuilder.build());
		} catch (Exception e) {
			throw new IOException("Failed to copy object", e);
		}
	}

	/**
	 * Get object metadata without downloading the content.
	 */
	public Map<String, String> getObjectMetadata(String key) throws IOException {
		try {
			HeadObjectRequest req = HeadObjectRequest.builder()
							.bucket(bucketName)
							.key(key)
							.build();
			HeadObjectResponse resp = s3Client.headObject(req);
			Map<String, String> metadata = new HashMap<>();
			if (resp.metadata() != null) {
				metadata.putAll(resp.metadata());
			}
			// Add some standard S3 metadata
			Long contentLength = resp.contentLength();
			if (contentLength != null) {
				metadata.put("Content-Length", contentLength.toString());
			}
			if (resp.lastModified() != null) {
				metadata.put("Last-Modified", resp.lastModified().toString());
			}
			if (resp.eTag() != null) {
				metadata.put("ETag", resp.eTag());
			}
			if (resp.contentType() != null) {
				metadata.put("Content-Type", resp.contentType());
			}
			return metadata;
		} catch (NoSuchKeyException e) {
			throw new IOException("Object not found: " + key, e);
		} catch (Exception e) {
			throw new IOException("Failed to get object metadata", e);
		}
	}

	/**
	 * Delete multiple objects.
	 */
	public List<String> deleteObjects(List<String> keys) throws IOException {
		try {
			List<ObjectIdentifier> objectsToDelete = keys.stream()
							.map(key -> ObjectIdentifier.builder().key(key).build())
							.collect(Collectors.toList());

			DeleteObjectsRequest req = DeleteObjectsRequest.builder()
							.bucket(bucketName)
							.delete(Delete.builder().objects(objectsToDelete).build())
							.build();

			DeleteObjectsResponse resp = s3Client.deleteObjects(req);
			return resp.deleted().stream()
							.map(DeletedObject::key)
							.collect(Collectors.toList());
		} catch (Exception e) {
			throw new IOException("Failed to delete objects", e);
		}
	}
}
