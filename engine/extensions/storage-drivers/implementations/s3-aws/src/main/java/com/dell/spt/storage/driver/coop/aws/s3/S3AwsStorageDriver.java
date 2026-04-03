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
import com.dell.spt.storage.driver.coop.nio.NioStorageDriverBase;
import com.github.akurilov.confuse.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class S3AwsStorageDriver<I extends Item, O extends Operation<I>> extends NioStorageDriverBase<I, O>
				implements ListDiscoveryProbe {

	private static final Logger LOG = LoggerFactory.getLogger(S3AwsStorageDriver.class);

	private final S3Client s3Client;
	private final String bucketName;

	public S3AwsStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config config,
					final boolean verifyFlag,
					final int batchSize,
					final S3Client s3Client)
					throws IllegalConfigurationException {
		super(stepId, dataInput, config, verifyFlag, batchSize);
		if (verifyFlag) {
			LOG.warn(
							"S3-AWS driver does not support --item-data-verify; reads will report SUCC without integrity checks");
		}
		this.s3Client = s3Client;

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
	}

	@Override
	protected void invokeNio(final O op) {
		try {
			// Execute AWS SDK operation synchronously - NioStorageDriverBase handles threading
			execute(op);

			// Set bytes transferred for metrics (skip READs — readObject sets actual bytes)
			if (op.type() != OpType.READ && op.item() instanceof DataItem) {
				DataItem dataItem = (DataItem) op.item();
				if (op instanceof DataOperation) {
					((DataOperation) op).countBytesDone(dataItem.size());
				}
			}

			// Use finishOperation helper like FileStorageDriver does
			finishOperation(op);

		} catch (Exception e) {
			// For failed operations, still need to properly complete timing.
			// finishOperation must NOT run here — it would mark the op as SUCC.
			op.status(Status.FAIL_UNKNOWN);
			try {
				op.startResponse();
				op.finishResponse();
			} catch (Exception ignored) {
				// Ignore timing errors on failed operations
			}
		}
	}

	@Override
	protected String requestNewPath(final String path) {
		// Extract bucket name from path and validate/create it
		// Path format: /bucketname or /bucketname/prefix
		final String relPath = path.startsWith("/") ? path.substring(1) : path;
		final int slashPos = relPath.indexOf('/');
		final String targetBucket = slashPos > 0 ? relPath.substring(0, slashPos) : relPath;
		final String bucketPath = "/" + targetBucket;

		try {
			// Validate that the target bucket exists
			s3Client.headBucket(HeadBucketRequest.builder().bucket(targetBucket).build());
		} catch (NoSuchBucketException e) {
			// Bucket doesn't exist — create it (matches standard S3 driver behavior)
			try {
				s3Client.createBucket(CreateBucketRequest.builder().bucket(targetBucket).build());
			} catch (Exception createEx) {
				throw new RuntimeException("Failed to create bucket: " + targetBucket, createEx);
			}
		} catch (Exception e) {
			throw new RuntimeException("Failed to validate bucket: " + targetBucket, e);
		}

		return bucketPath;
	}

	@Override
	protected String requestNewAuthToken(final Credential credential) {
		// Return null, AWS SDK handles authentication
		return null;
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
		// No-op, AWS SDK manages its own buffers
	}

	/**
	 * Resolve bucket name and object key from the operation context.
	 *
	 * The framework splits CSV item paths (e.g., "/large/mkk0lurmliru") into:
	 *   op.dstPath() = "/large"  (bucket/prefix)
	 *   op.item().name() = "mkk0lurmliru"  (key)
	 *
	 * When dstPath is available, bucket comes from dstPath and key from item name.
	 * When dstPath is null/empty (e.g., items with full paths in their name),
	 * falls back to parsing the full path from item.name().
	 *
	 * @return a two-element array: [0] = bucket, [1] = key
	 */
	private String[] resolveBucketAndKey(final O op) {
		final var dstPath = op.dstPath();
		final var itemName = op.item().name();

		if (dstPath != null && !dstPath.isEmpty()) {
			final var rel = dstPath.startsWith("/") ? dstPath.substring(1) : dstPath;
			var key = itemName.startsWith("/") ? itemName.substring(1) : itemName;
			final var slashPos = rel.indexOf('/');
			if (slashPos > 0) {
				// Nested dstPath like "/bucket/prefix" — prepend prefix to key
				return new String[]{rel.substring(0, slashPos), rel.substring(slashPos + 1) + "/" + key
				};
			}
			return new String[]{rel, key
			};
		}

		// No dstPath — parse full path from item name
		return parseBucketAndKey(itemName);
	}

	/**
	 * Extract bucket name and object key from a full item name path.
	 * Item names follow the pattern "/{bucket}/{key}" — the first path segment
	 * is the bucket and the remainder is the object key. Used as a fallback
	 * when op.dstPath() is not available.
	 *
	 * @return a two-element array: [0] = bucket, [1] = key
	 */
	static String[] parseBucketAndKey(final String itemName) {
		final var relPath = itemName.startsWith("/") ? itemName.substring(1) : itemName;
		final var slashPos = relPath.indexOf('/');
		if (slashPos > 0) {
			return new String[]{relPath.substring(0, slashPos), relPath.substring(slashPos + 1)
			};
		}
		// No slash — entire relPath is the bucket, key is absent
		return new String[]{relPath, null
		};
	}

	private void execute(final O op) throws Exception {
		switch (op.type()) {
		case NOOP:
			break;

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
		final var bk = resolveBucketAndKey(op);

		if (op.item() instanceof DataItem) {
			DataItem dataItem = (DataItem) op.item();
			s3Client.putObject(
							PutObjectRequest.builder()
											.bucket(bk[0])
											.key(bk[1])
											.build(),
							RequestBody.fromInputStream(Channels.newInputStream(dataItem), dataItem.size()));
		} else if (op.item() instanceof PathItem) {
			PathItem pathItem = (PathItem) op.item();
			Path path = Path.of(pathItem.name());
			long size = Files.size(path);
			s3Client.putObject(
							PutObjectRequest.builder()
											.bucket(bk[0])
											.key(bk[1])
											.build(),
							RequestBody.fromInputStream(Files.newInputStream(path), size));
		} else {
			throw new UnsupportedOperationException("s3-aws PUT requires DataItem or PathItem");
		}
	}

	private void readObject(final O op) throws Exception {
		final var bk = resolveBucketAndKey(op);

		try (var response = s3Client.getObject(
						GetObjectRequest.builder()
										.bucket(bk[0])
										.key(bk[1])
										.build())) {

			byte[] buffer = new byte[8192];
			long bytesRead = 0;
			int n;

			while ((n = response.read(buffer)) != -1) {
				bytesRead += n;
			}

			if (op instanceof DataOperation) {
				((DataOperation) op).countBytesDone(bytesRead);
			}
		}
	}

	private void deleteObject(final O op) {
		final var bk = resolveBucketAndKey(op);

		s3Client.deleteObject(
						DeleteObjectRequest.builder()
										.bucket(bk[0])
										.key(bk[1])
										.build());
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
			// Extract bucket from path parameter, falling back to configured bucketName
			String targetBucket = bucketName;
			if (path != null && !path.isEmpty()) {
				String rel = path.startsWith("/") ? path.substring(1) : path;
				int slash = rel.indexOf('/');
				targetBucket = slash > 0 ? rel.substring(0, slash) : rel;
			}

			// Optimize request size for better performance
			int maxKeys = Math.min(Math.max(count, 1), 1000);

			ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
							.bucket(targetBucket)
							.maxKeys(maxKeys);
			// Use prefix parameter directly like S3 driver does
			if (prefix != null && !prefix.isEmpty()) {
				reqBuilder.prefix(prefix);
			}

			// Handle pagination: continuationToken → startAfter → lastPrevItem
			if (options != null && options.continuationToken() != null && !options.continuationToken().isEmpty()) {
				reqBuilder.continuationToken(options.continuationToken());
			} else if (options != null && options.startAfter() != null && !options.startAfter().isEmpty()) {
				reqBuilder.startAfter(options.startAfter());
			} else if (lastPrevItem != null) {
				String startAfter = lastPrevItem.name();
				if (startAfter.startsWith("/")) {
					startAfter = startAfter.substring(1);
				}
				reqBuilder.startAfter(startAfter);
			}

			ListObjectsV2Response resp = s3Client.listObjectsV2(reqBuilder.build());

			List<I> result = new ArrayList<>(maxKeys);
			for (S3Object s3obj : resp.contents()) {
				// Use the S3 object key directly as the item name
				// The key already contains the full path (e.g., "awstest1m16trmr271")
				String itemName = "/" + s3obj.key();

				// Parse offset from object key using idRadix like S3 driver does
				long offset = 0;
				try {
					// Try to parse the key as a number for offset
					String keyWithoutPrefix = s3obj.key();
					// Remove any path prefix to get just the numeric part
					int lastSlash = keyWithoutPrefix.lastIndexOf('/');
					if (lastSlash >= 0) {
						keyWithoutPrefix = keyWithoutPrefix.substring(lastSlash + 1);
					}
					offset = Long.parseLong(keyWithoutPrefix, idRadix);
				} catch (NumberFormatException e) {
					offset = 0;
				}
				I item = itemFactory.getItem(itemName, offset, s3obj.size());
				result.add(item);
			}

			// Add null poison marker if not truncated (matches standard S3 driver)
			if (!Boolean.TRUE.equals(resp.isTruncated())) {
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

			boolean truncated = Boolean.TRUE.equals(resp.isTruncated());
			boolean hasContents = resp.contents() != null && !resp.contents().isEmpty();
			return new com.dell.spt.base.storage.driver.ListDiscoveryProbe.DiscoverResult(
							commonPrefixes, hasContents, truncated);
		} catch (S3Exception e) {
			throw new IOException("Failed to probe common prefixes", e);
		}
	}

	public String getBucketName() {
		return bucketName;
	}

}
