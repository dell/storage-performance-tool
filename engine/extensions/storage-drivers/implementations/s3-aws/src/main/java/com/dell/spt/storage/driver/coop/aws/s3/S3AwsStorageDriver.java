package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status; // Added import for Operation.Status
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.confuse.Config;

import java.nio.file.Files;
import java.nio.file.Path;

import java.io.IOException;
import java.nio.channels.Channels;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.core.sync.RequestBody;
import java.util.ArrayList;
import java.util.List;
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
					final String stepId,
					final DataInput dataInput,
					final Config config,
					final boolean verifyFlag,
					final int batchSize,
					S3Client s3Client)
					throws IllegalConfigurationException, InterruptedException {

		super(stepId, dataInput, config, verifyFlag, batchSize);
		this.s3Client = s3Client;

		// Support multiple bucket parameter names for backward compatibility
		String resolvedBucketName;

		try {
			resolvedBucketName = config.stringVal("bucket");
		} catch (Exception e1) {
			try {
				// Try legacy S3 node addresses format
				String nodeAddrs = config.stringVal("storage-net-node-addrs");
				if (nodeAddrs != null && !nodeAddrs.isEmpty()) {
					// Extract bucket from node addresses if it's the first part
					if (nodeAddrs.contains("/")) {
						resolvedBucketName = nodeAddrs.split("/")[0];
					} else {
						resolvedBucketName = nodeAddrs;
					}
				} else {
					// Try to extract from item-output-path (format: /bucketname)
					String outputPath = config.stringVal("item-output-path");
					if (outputPath != null && outputPath.startsWith("/") && outputPath.length() > 1) {
						resolvedBucketName = outputPath.substring(1); // Remove leading slash
					} else {
						// Use default bucket name as fallback
						resolvedBucketName = "spttest";
					}
				}
			} catch (Exception e2) {
				try {
					// Try to extract from item-output-path as fallback
					String outputPath = config.stringVal("item-output-path");
					if (outputPath != null && outputPath.startsWith("/") && outputPath.length() > 1) {
						resolvedBucketName = outputPath.substring(1);
					} else {
						// Use default bucket name as final fallback
						resolvedBucketName = "spttest";
					}
				} catch (Exception e3) {
					// Use default bucket name as final fallback
					resolvedBucketName = "spttest";
				}
			}
		}

		// Final safety check
		if (resolvedBucketName == null || resolvedBucketName.isEmpty()) {
			resolvedBucketName = "spttest";
		}
		this.bucketName = resolvedBucketName;

		// Support optional region parameter with default
		String resolvedRegion;
		try {
			resolvedRegion = config.stringVal("region");
		} catch (Exception e) {
			resolvedRegion = "eu-west-2"; // Default region for S3-compatible service
		}
		this.region = resolvedRegion;
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		try {
			// Acquire concurrency permit
			if (!concurrencyThrottle.tryAcquire()) {
				return false;
			}

			// Start request timing
			op.startRequest();

			execute(op);

			// Finish request timing
			op.finishRequest();

			// Set status to success for metrics counting
			op.status(Status.SUCC);

			// Set bytes transferred for metrics
			if (op.item() instanceof DataItem) {
				DataItem dataItem = (DataItem) op.item();
				if (op instanceof DataOperation) {
					((DataOperation) op).countBytesDone(dataItem.size());
				}
			}

			// Start response timing (before completion)
			op.startResponse();

			// Complete the operation (this will call finishResponse internally)
			complete(op);

			// Release concurrency permit
			concurrencyThrottle.release();

			return true;
		} catch (Exception e) {
			// Release permit on error
			concurrencyThrottle.release();
			throw new IllegalStateException("AWS S3 operation failed", e);
		}
	}

	/**
	 * Complete the operation following the legacy driver pattern
	 */
	private void complete(final O op) {
		try {
			// Finish response timing
			op.finishResponse();
		} catch (final IllegalStateException e) {
			// Ignore invalid state exceptions
		}

		// Handle completion
		handleCompleted(op);
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
			if (submit(op)) {
				count++;
			}
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

		if (op.item() instanceof DataItem) {
			// Handle in-memory data (DataItem) - always use streaming
			DataItem dataItem = (DataItem) op.item();
			s3Client.putObject(
							PutObjectRequest.builder()
											.bucket(bucketName)
											.key(op.dstPath())
											.build(),
							RequestBody.fromInputStream(Channels.newInputStream(dataItem), dataItem.size()));
		} else if (op.item() instanceof PathItem) {
			// Handle file-based data (PathItem) - always use streaming
			PathItem pathItem = (PathItem) op.item();
			Path path = Path.of(pathItem.name());

			long size = Files.size(path);
			s3Client.putObject(
							PutObjectRequest.builder()
											.bucket(bucketName)
											.key(op.dstPath())
											.build(),
							RequestBody.fromInputStream(Files.newInputStream(path), size));
		} else {
			throw new UnsupportedOperationException(
							"s3-aws PUT requires DataItem or PathItem");
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
}
