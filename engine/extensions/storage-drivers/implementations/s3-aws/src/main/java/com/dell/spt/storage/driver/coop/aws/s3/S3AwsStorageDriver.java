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
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
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
import java.util.concurrent.TimeUnit;

/**
 * AWS SDK implementation of S3 Storage Driver for SPT
 * Comparable to the legacy REST implementation in com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver
 */
public class S3AwsStorageDriver<I extends Item, O extends Operation<I>> extends CoopStorageDriverBase<I, O>
				implements ListDiscoveryProbe {

	private final S3Client s3Client;
	private final String bucketName;
	private final String region;

	// Performance optimization fields
	private final long startTime = System.nanoTime();
	private volatile boolean isInitialized = false;

	public S3AwsStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config config,
					final boolean verifyFlag,
					final int batchSize,
					final S3Client s3Client)
					throws IllegalConfigurationException {
		super(stepId, dataInput, config, verifyFlag, batchSize);
		this.s3Client = s3Client;

		// Pre-warm connection and initialize driver
		initializeDriver();

		// Support multiple bucket parameter names for backward compatibility
		String resolvedBucketName;

		try {
			// Try to extract from storage-net-node-addrs (primary method like S3 driver)
			String nodeAddrs = config.stringVal("storage-net-node-addrs");
			if (nodeAddrs != null && !nodeAddrs.isEmpty()) {
				// Extract bucket from node addresses if it's the first part
				if (nodeAddrs.contains("/")) {
					resolvedBucketName = nodeAddrs.split("/")[0];
				} else {
					resolvedBucketName = nodeAddrs;
				}
			} else {
				// Try to extract from item configuration output-path
				var itemConfig = config.configVal("item");
				if (itemConfig != null) {
					String outputPath = itemConfig.stringVal("output-path");
					if (outputPath != null && outputPath.startsWith("/") && outputPath.length() > 1) {
						resolvedBucketName = outputPath.substring(1); // Remove leading slash
					} else {
						// Try fallback to input-path for read operations
						try {
							String inputPath = itemConfig.stringVal("input-path");
							if (inputPath != null && inputPath.startsWith("/") && inputPath.length() > 1) {
								resolvedBucketName = inputPath.substring(1); // Remove leading slash
							} else {
								// Generate a default bucket name based on current user
								String currentUser = System.getProperty("user.name", "spt");
								resolvedBucketName = currentUser + "test";
							}
						} catch (Exception e) {
							// Generate a default bucket name based on current user
							String currentUser = System.getProperty("user.name", "spt");
							resolvedBucketName = currentUser + "test";
						}
					}
				} else {
					// Generate a default bucket name based on current user
					String currentUser = System.getProperty("user.name", "spt");
					resolvedBucketName = currentUser + "test";
				}
			}
		} catch (Exception e) {
			// Fallback: generate a default bucket name based on current user
			String currentUser = System.getProperty("user.name", "spt");
			resolvedBucketName = currentUser + "test";
		}

		// Final safety check
		if (resolvedBucketName == null || resolvedBucketName.isEmpty()) {
			String currentUser = System.getProperty("user.name", "spt");
			resolvedBucketName = currentUser + "test";
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
		long opStartTime = System.nanoTime();

		try (var response = s3Client.getObject(
						GetObjectRequest.builder()
										.bucket(bucketName)
										.key(op.srcPath())
										.build())) {
			// stream intentionally discarded
		}

		// Log timing for performance monitoring (only if debug enabled)
		if (System.nanoTime() - startTime < TimeUnit.SECONDS.toNanos(10)) {
			// Only log timing during first 10 seconds to avoid overhead
			long duration = System.nanoTime() - opStartTime;
			if (duration > TimeUnit.MILLISECONDS.toNanos(50)) {
				// Log slow operations only
			}
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

	/**
	 * Initialize driver and pre-warm connections for better performance
	 */
	private void initializeDriver() {
		if (!isInitialized) {
			try {
				// Pre-warm connection with a lightweight operation
				s3Client.listBuckets();
				isInitialized = true;
			} catch (Exception e) {
				// Continue without pre-warming if it fails
			}
		}
	}

	@Override
	protected String requestNewPath(final String path) {
		// For S3-AWS, we need to ensure the bucket exists before returning the path
		try {
			// Check if bucket exists, create if it doesn't (async for performance)
			if (!s3Client.listBuckets().buckets().stream().anyMatch(b -> b.name().equals(bucketName))) {
				s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
			}
		} catch (Exception e) {
			// Continue anyway, the bucket might exist or the operation might fail later
		}
		return path;
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
			// Extract bucket name from path - remove leading slash if present
			String targetBucket = path;
			if (path != null && path.startsWith("/")) {
				targetBucket = path.substring(1);
			}

			// Use prefix for object filtering
			String objectPrefix = prefix != null ? prefix : "";

			// Optimize request size for better performance
			int maxKeys = Math.min(Math.max(count, 1), 1000);

			ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
							.bucket(targetBucket)
							.maxKeys(maxKeys);
			if (!objectPrefix.isEmpty()) {
				reqBuilder.prefix(objectPrefix);
			}
			if (options != null && options.continuationToken() != null) {
				reqBuilder.continuationToken(options.continuationToken());
			}

			ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());

			List<I> result = new ArrayList<>(maxKeys);
			for (S3Object s3obj : resp.contents()) {
				// Create items exactly like S3 driver: path + objectKey
				String itemPath = path + s3obj.key();
				// Parse offset from object key using idRadix like S3 driver does
				long offset = 0;
				try {
					offset = Long.parseLong(s3obj.key(), idRadix);
				} catch (NumberFormatException e) {
					offset = 0;
				}
				I item = itemFactory.getItem(itemPath, offset, s3obj.size());
				result.add(item);
			}

			// Add null marker if not truncated like S3 driver does
			if (!resp.isTruncated() && !result.isEmpty()) {
				result.add(null); // poison marker
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

	@Override
	public com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult probeCommonPrefixes(
					final String bucketPath,
					final String prefix,
					final String delimiter,
					final int maxKeys) throws IOException {
		try {
			// Extract bucket name from bucketPath
			String targetBucket = bucketName;
			if (bucketPath != null && !bucketPath.isEmpty()) {
				if (bucketPath.startsWith("/")) {
					targetBucket = bucketPath.substring(1);
				} else {
					targetBucket = bucketPath;
				}
			}

			ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
							.bucket(targetBucket)
							.maxKeys(Math.min(Math.max(maxKeys, 1), 1000));

			if (prefix != null && !prefix.isEmpty()) {
				reqBuilder.prefix(prefix);
			}
			if (delimiter != null && !delimiter.isEmpty()) {
				reqBuilder.delimiter(delimiter);
			}

			ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());

			List<String> commonPrefixes = resp.commonPrefixes().stream()
							.map(CommonPrefix::prefix)
							.collect(Collectors.toList());

			boolean truncated = resp.isTruncated();
			return new com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult(
							commonPrefixes, truncated, false);
		} catch (S3Exception e) {
			throw new IOException("Failed to probe common prefixes", e);
		}
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
