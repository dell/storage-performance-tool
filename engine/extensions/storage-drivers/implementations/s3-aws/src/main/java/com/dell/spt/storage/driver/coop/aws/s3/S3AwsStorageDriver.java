package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.confuse.Config;

import java.nio.file.Files;
import java.nio.file.Path;

import java.io.IOException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AWS SDK implementation of S3 Storage Driver for SPT
 * Comparable to the legacy REST implementation in com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver
 */
public class S3AwsStorageDriver<I extends Item, O extends Operation<I>> extends CoopStorageDriverBase<I, O> {

	private final S3Client s3Client;
	private final String bucketName;
	private final String region;

	public S3AwsStorageDriver(
					String stepId,
					DataInput dataInput,
					Config config,
					boolean verifyFlag,
					int batchSize,
					S3Client s3Client)
					throws IllegalConfigurationException, InterruptedException {

		super(stepId, dataInput, config, verifyFlag, batchSize);
		this.s3Client = s3Client;
		this.bucketName = config.stringVal("bucket");
		this.region = config.stringVal("region");
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		try {
			execute(op);
			handleCompleted(op);
			return true;
		} catch (Exception e) {
			throw new IllegalStateException("AWS S3 operation failed", e);
		}
	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException {

		int i = from;
		for (; i < to; i++) {
			submit(ops.get(i));
		}
		return i - from;
	}

	@Override
	protected int submit(final List<O> ops)
					throws IllegalStateException {

		int count = 0;
		for (O op : ops) {
			submit(op);
			count++;
		}
		return count;
	}

	private void execute(final O op) throws Exception {
		switch (op.type()) {
		case CREATE:
		case UPDATE:
			putObject(op);
			break;

		case READ:
			readObject(op);
			break;

		case DELETE:
			deleteObject(op);
			break;

		default:
			throw new UnsupportedOperationException(op.type().toString());
		}
	}

	private void putObject(final O op) throws Exception {

		if (!(op.item() instanceof PathItem)) {
			throw new UnsupportedOperationException(
							"s3-aws PUT requires PathItem");
		}

		PathItem pathItem = (PathItem) op.item();
		Path path = Path.of(pathItem.name());

		long size = Files.size(path);

		try (InputStream data = Files.newInputStream(path)) {
			s3Client.putObject(
							PutObjectRequest.builder()
											.bucket(bucketName)
											.key(op.dstPath())
											.build(),
							RequestBody.fromInputStream(data, size));
		}
	}

	private void readObject(final O op) throws Exception {
		try (var response = s3Client.getObject(
						GetObjectRequest.builder()
										.bucket(bucketName)
										.key(op.srcPath())
										.build())) {
			// stream intentionally discarded
		}
	}

	private void deleteObject(final O op) {
		s3Client.deleteObject(
						DeleteObjectRequest.builder()
										.bucket(bucketName)
										.key(op.srcPath())
										.build());
	}

	@Override
	protected void doStop() {
		s3Client.close();
	}

	@Override
	public void adjustIoBuffers(final long ioSize, final OpType opType) {
		// No-op for AWS SDK driver
	}

	@Override
	protected String requestNewPath(final String prefix) {
		return prefix;
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		// AWS SDK manages auth tokens internally
		return null;
	}

	/**
	 * List objects in the bucket with optional prefix and options.
	 */
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count,
					final ListOptions options) throws IOException {
		try {
			ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
							.bucket(bucketName)
							.maxKeys(count > 0 ? count : 1000);
			if (prefix != null && !prefix.isEmpty()) {
				reqBuilder.prefix(prefix);
			}
			if (options != null && options.continuationToken() != null) {
				reqBuilder.continuationToken(options.continuationToken());
			}
			ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());

			List<I> result = new ArrayList<>();
			for (S3Object s3obj : resp.contents()) {
				// Using the correct getItem method from ItemFactory
				// Assuming we need to generate an ID and use the size from S3Object
				I item = itemFactory.getItem(s3obj.key(), 0, s3obj.size()); // Using 0 as a placeholder for ID
				result.add(item);
			}
			return result;
		} catch (S3Exception e) {
			throw new IOException("Failed to list objects", e);
		}
	}

	/**
	 * List objects in the bucket with optional prefix.
	 */
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count) throws IOException {
		return list(itemFactory, path, prefix, idRadix, lastPrevItem, count, ListOptions.DEFAULT);
	}

	/**
	 * Constructor with AWS credentials and configuration
	 */
	// public S3AwsStorageDriver(String accessKey, String secretKey, String region, String bucketName, String endpointOverride) {
	//     this.region = region;
	//     this.bucketName = bucketName;

	//     AwsCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

	//     S3ClientBuilder builder = S3Client.builder()
	//             .credentialsProvider(StaticCredentialsProvider.create(credentials))
	//             .region(Region.of(region));

	//     if (endpointOverride != null && !endpointOverride.isEmpty()) {
	//         builder.endpointOverride(java.net.URI.create(endpointOverride));
	//     }

	//     this.s3Client = builder.build();
	// }

	/**
	 * Constructor with existing S3Client
	 */
	// public S3AwsStorageDriver(S3Client s3Client, String bucketName) {
	//     this.s3Client = s3Client;
	//     this.bucketName = bucketName;
	//     this.region = s3Client.serviceClientConfiguration().region().toString();
	// }

	/**
	 * Probe common prefixes (simulate directory listing).
	 */
	public List<String> probeCommonPrefixes(
					final String bucketPath,
					final String prefix,
					final String delimiter,
					final int maxKeys) throws IOException {
		try {
			ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
							.bucket(bucketName)
							.delimiter(delimiter)
							.maxKeys(maxKeys > 0 ? maxKeys : 1000);
			if (prefix != null && !prefix.isEmpty()) {
				reqBuilder.prefix(prefix);
			}
			ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());
			return resp.commonPrefixes().stream()
							.map(CommonPrefix::prefix)
							.collect(Collectors.toList());
		} catch (S3Exception e) {
			throw new IOException("Failed to probe common prefixes", e);
		}
	}

	// public void close() {
	//     if (s3Client != null) {
	//         s3Client.close();
	//     }
	// }

	public String getBucketName() {
		return bucketName;
	}

	public String getRegion() {
		return region;
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
	 * 
	 * @return true if versioning is enabled, false otherwise
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
