package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.storage.driver.coop.nio.NioStorageDriverBase;
import com.github.akurilov.confuse.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dell.spt.base.item.io.DataItemInputStream;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AWS SDK implementation of S3 Storage Driver for SPT
 * Comparable to the legacy REST implementation in com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver
 */
public class S3AwsStorageDriver<I extends Item, O extends Operation<I>> extends NioStorageDriverBase<I, O>
				implements ListDiscoveryProbe {

	private static final Logger LOG = LoggerFactory.getLogger(S3AwsStorageDriver.class);

	// S3 API constants for multipart upload
	private static final String KEY_UPLOAD_ID = "uploadId";
	private static final String KEY_MPU_ABORT = "mpuAbort";
	private static final String KEY_PART_CHECKSUM_PREFIX = "checksum-";

	private final S3AsyncClient s3AsyncClient;
	private final ExecutorService executor;
	private final String bucketName;
	private final boolean checksumEnabled;
	private final ChecksumAlgorithm checksumAlgorithm;
	private final boolean versioningEnabled;
	private final boolean taggingEnabled;
	private final Map<String, String> objectTags;

	public S3AwsStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config config,
					final boolean verifyFlag,
					final int batchSize,
					final S3AsyncClient s3AsyncClient)
					throws IllegalConfigurationException {
		super(stepId, dataInput, config, verifyFlag, batchSize);
		if (verifyFlag) {
			LOG.warn(
							"S3-AWS driver does not support --item-data-verify; reads will report SUCC without integrity checks");
		}
		this.s3AsyncClient = s3AsyncClient;
		this.executor = Executors.newCachedThreadPool();

		this.bucketName = resolveBucketName(config);

		boolean ckEnabled = false;
		ChecksumAlgorithm ckAlgo = null;
		try {
			ckEnabled = config.boolVal("checksum-enabled");
		} catch (Exception e) {
			LOG.debug("Could not read checksum-enabled from config: {}", e.toString());
		}
		if (ckEnabled) {
			try {
				final String algo = config.stringVal("checksum-algorithm");
				ckAlgo = resolveChecksumAlgorithm(algo);
				if (ckAlgo == null) {
					LOG.warn("Unsupported checksum algorithm '{}' for s3-aws driver; checksums will not be applied", algo);
					ckEnabled = false;
				}
			} catch (Exception e) {
				LOG.warn("Could not read checksum-algorithm from config: {}", e.toString());
				ckEnabled = false;
			}
		}
		this.checksumEnabled = ckEnabled;
		this.checksumAlgorithm = ckAlgo;

		// Read versioning configuration
		boolean versioning = false;
		try {
			versioning = config.boolVal("versioning");
		} catch (Exception e) {
			LOG.debug("Could not read versioning from config: {}", e.toString());
		}
		this.versioningEnabled = versioning;
		if (versioningEnabled) {
			LOG.info("S3-AWS driver versioning enabled");
		}

		// Read tagging configuration
		boolean tagging = false;
		Map<String, String> tags = new HashMap<>();
		try {
			final var objectConfig = config.configVal("object");
			if (objectConfig != null) {
				final var taggingConfig = objectConfig.configVal("tagging");
				if (taggingConfig != null) {
					tagging = taggingConfig.boolVal("enabled");
					if (tagging) {
						try {
							final var tagsMap = taggingConfig.mapVal("tags");
							if (tagsMap != null) {
								// Convert Map<String,Object> to Map<String,String>
								for (var entry : tagsMap.entrySet()) {
									if (entry.getValue() != null) {
										tags.put(entry.getKey(), entry.getValue().toString());
									}
								}
							}
						} catch (Exception e) {
							LOG.debug("Could not read tags from config: {}", e.toString());
						}
					}
				}
			}
		} catch (Exception e) {
			LOG.debug("Could not read tagging config: {}", e.toString());
		}
		this.taggingEnabled = tagging;
		this.objectTags = tags;
		if (taggingEnabled) {
			LOG.info("S3-AWS driver tagging enabled with {} tags", tags.size());
		}
	}

	/**
	 * Map a config algorithm name to the AWS SDK ChecksumAlgorithm enum.
	 * Returns null for unsupported algorithms (e.g. MD5 — use Content-MD5 header instead).
	 */
	static ChecksumAlgorithm resolveChecksumAlgorithm(final String algorithm) {
		if (algorithm == null || algorithm.isEmpty()) {
			return null;
		}
		switch (algorithm.toLowerCase()) {
		case "crc32":
			return ChecksumAlgorithm.CRC32;
		case "crc32c":
			return ChecksumAlgorithm.CRC32_C;
		case "sha1":
			return ChecksumAlgorithm.SHA1;
		case "sha256":
			return ChecksumAlgorithm.SHA256;
		default:
			return null;
		}
	}

	/**
	 * Resolve bucket name from config, trying multiple sources in order:
	 * 1. item.output-path (for write operations)
	 * 2. item.input-path (for read operations)
	 * 3. storage-net-node-addrs (parsed as confuse nested path, for compatibility)
	 * 4. System username + "test" (fallback)
	 */
	static String resolveBucketName(final Config config) {
		final Logger log = LoggerFactory.getLogger(S3AwsStorageDriver.class);
		String resolved = null;

		// Try item.output-path first (for write operations)
		try {
			var itemConfig = config.configVal("item");
			if (itemConfig != null) {
				String outputPath = itemConfig.stringVal("output-path");
				if (outputPath != null && outputPath.startsWith("/") && outputPath.length() > 1) {
					resolved = outputPath.substring(1);
				} else {
					// Try item.input-path (for read operations)
					try {
						String inputPath = itemConfig.stringVal("input-path");
						if (inputPath != null && inputPath.startsWith("/") && inputPath.length() > 1) {
							resolved = inputPath.substring(1);
						}
					} catch (Exception e) {
						log.debug("Could not read item.input-path from config: {}", e.toString());
					}
				}
			}
		} catch (Exception e) {
			log.debug("Could not read item config: {}", e.toString());
		}

		// Fallback to storage-net-node-addrs (for compatibility)
		if (resolved == null) {
			try {
				String nodeAddrs = config.stringVal("storage-net-node-addrs");
				if (nodeAddrs != null && !nodeAddrs.isEmpty()) {
					if (nodeAddrs.contains("/")) {
						resolved = nodeAddrs.split("/")[0];
					} else {
						resolved = nodeAddrs;
					}
				}
			} catch (Exception e) {
				log.debug("Could not read storage-net-node-addrs from config: {}", e.toString());
			}
		}

		// Final fallback to username + "test"
		if (resolved == null || resolved.isEmpty()) {
			resolved = System.getProperty("user.name", "spt") + "test";
		}
		log.info("S3-AWS driver resolved bucket name: {}", resolved);
		return resolved;
	}

	@Override
	protected void invokeNio(final O op) {
		try {
			// Execute AWS SDK operation asynchronously and block for result
			// This maintains the existing NioStorageDriverBase threading model
			execute(op).join();

			// Set bytes transferred for metrics (skip READs — readObject sets actual bytes)
			if (op.type() != OpType.READ && op.item() instanceof DataItem) {
				DataItem dataItem = (DataItem) op.item();
				if (op instanceof DataOperation) {
					((DataOperation) op).countBytesDone(dataItem.size());
				}
			}

			// For READ operations, timing is handled in readObject with startResponse/finishResponse
			// For other operations, use finishOperation helper
			if (op.type() != OpType.READ) {
				finishOperation(op);
			} else {
				// READ operations call finishResponse in readObject, just set status
				op.status(Operation.Status.SUCC);
			}

		} catch (Exception e) {
			op.status(classifyFailure(e));
			LOG.error("{} {} failed: {} - {}", op.type(), op.item().name(), e.getClass().getSimpleName(), e.getMessage());
			LOG.debug("{} {} stack trace", op.type(), op.item().name(), e);
			try {
				op.startResponse();
				op.finishResponse();
			} catch (Exception ignored) {}
		}
	}

	/**
	 * Map an exception to the most specific Operation.Status failure code.
	 */
	static Status classifyFailure(final Exception e) {
		if (e instanceof S3Exception) {
			final int statusCode = ((S3Exception) e).statusCode();
			if (statusCode == 401 || statusCode == 403) {
				return Status.RESP_FAIL_AUTH;
			} else if (statusCode == 404) {
				return Status.RESP_FAIL_NOT_FOUND;
			} else if (statusCode >= 400 && statusCode < 500) {
				return Status.RESP_FAIL_CLIENT;
			} else if (statusCode == 504) {
				return Status.FAIL_TIMEOUT;
			} else if (statusCode == 507) {
				return Status.RESP_FAIL_SPACE;
			} else if (statusCode >= 500) {
				return Status.RESP_FAIL_SVC;
			}
		}
		if (e instanceof ApiCallTimeoutException || e instanceof ApiCallAttemptTimeoutException
						|| e instanceof TimeoutException) {
			return Status.FAIL_TIMEOUT;
		}
		if (e instanceof IOException || (e instanceof SdkClientException && e.getCause() instanceof IOException)) {
			return Status.FAIL_IO;
		}
		return Status.FAIL_UNKNOWN;
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
			s3AsyncClient.headBucket(HeadBucketRequest.builder().bucket(targetBucket).build()).join();
		} catch (Exception e) {
			if (e.getCause() instanceof NoSuchBucketException || isNoSuchBucket(e)) {
				// Bucket doesn't exist — create it (matches standard S3 driver behavior)
				try {
					s3AsyncClient.createBucket(CreateBucketRequest.builder().bucket(targetBucket).build()).join();
				} catch (Exception createEx) {
					throw new RuntimeException("Failed to create bucket: " + targetBucket, createEx);
				}
			} else {
				throw new RuntimeException("Failed to validate bucket: " + targetBucket, e);
			}
		}

		return bucketPath;
	}

	private boolean isNoSuchBucket(Exception e) {
		if (e.getCause() instanceof S3Exception) {
			return ((S3Exception) e.getCause()).statusCode() == 404;
		}
		return false;
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
	String[] resolveBucketAndKey(final O op) {
		return resolveBucketAndKey(op, null);
	}

	/**
	 * Resolve bucket name and object key from the operation context, with optional version ID.
	 *
	 * @param op the operation
	 * @param versionId the version ID (can be null)
	 * @return a two-element array: [0] = bucket, [1] = key
	 */
	String[] resolveBucketAndKey(final O op, final String versionId) {
		final var dstPath = op.dstPath();
		final var itemName = op.item().name();

		if (dstPath != null && !dstPath.isEmpty()) {
			final var rel = dstPath.startsWith("/") ? dstPath.substring(1) : dstPath;
			// After a recycle cycle buildItemPath() prepends dstPath to the item
			// name, e.g. "mkk0lurmliru" → "/spttest/mkk0lurmliru".  Strip the
			// prefix so we recover the original key.
			var key = itemName;
			final var dstNorm = dstPath.endsWith("/") ? dstPath : dstPath + "/";
			if (key.startsWith(dstNorm)) {
				key = key.substring(dstNorm.length());
			} else if (key.startsWith("/")) {
				key = key.substring(1);
			}
			final var slashPos = rel.indexOf('/');
			if (slashPos > 0) {
				final var bucket = rel.substring(0, slashPos);
				final var prefix = rel.substring(slashPos + 1);
				// Strip prefix too when buildItemPath already prepended it
				if (key.startsWith(prefix + "/")) {
					key = key.substring(prefix.length() + 1);
				}
				return new String[]{bucket, prefix + "/" + key
				};
			}
			// Strip bucket name when buildItemPath already prepended it
			if (key.startsWith(rel + "/")) {
				key = key.substring(rel.length() + 1);
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
	String[] parseBucketAndKey(final String itemName) {
		final var relPath = itemName.startsWith("/") ? itemName.substring(1) : itemName;
		final var slashPos = relPath.indexOf('/');
		if (slashPos > 0) {
			final var bucket = relPath.substring(0, slashPos);
			final var key = relPath.substring(slashPos + 1);
			return new String[]{bucket, key
			};
		}
		// No slash — entire relPath is treated as the key, use configured bucket
		return new String[]{bucketName, relPath
		};
	}

	/**
	 * Extract version ID from item name if versioning is enabled.
	 * Version IDs are appended with ~ separator: "key~versionId"
	 *
	 * @param itemName the item name
	 * @return array with [0] = key without version, [1] = version ID (or null)
	 */
	String[] extractVersionId(final String itemName) {
		if (!versioningEnabled) {
			return new String[]{itemName, null
			};
		}

		final int tildePos = itemName.lastIndexOf('~');
		if (tildePos > 0) {
			// Check if this looks like a version ID (contains only alphanumeric and special chars)
			final String potentialVersionId = itemName.substring(tildePos + 1);
			if (potentialVersionId.matches("[a-zA-Z0-9._-]+")) {
				return new String[]{itemName.substring(0, tildePos), potentialVersionId
				};
			}
		}

		return new String[]{itemName, null
		};
	}

	CompletableFuture<Void> execute(final O op) {
		// Handle composite operations (multipart upload)
		if (op instanceof CompositeDataOperation) {
			return executeCompositeOperation((CompositeDataOperation) op);
		}

		// Handle partial operations (range reads/part uploads)
		if (op instanceof PartialDataOperation) {
			return executePartialOperation((PartialDataOperation) op);
		}

		// Handle standard operations
		switch (op.type()) {
		case NOOP:
			return CompletableFuture.completedFuture(null);

		case CREATE:
		case UPDATE:
			// Check if this is a copy operation (srcPath present)
			if (op.srcPath() != null && !op.srcPath().isEmpty()) {
				return copyObject(op);
			}
			return putObject(op);

		case READ:
			return readObject(op);

		case STAT:
			return headObject(op);

		case DELETE:
			return deleteObject(op);

		case LIST:
			return listObjects(op);

		default:
			return CompletableFuture.failedFuture(new UnsupportedOperationException(op.type().toString()));
		}
	}

	/**
	 * Handle composite operations (multipart upload).
	 * For CREATE operations, this manages the multipart upload lifecycle:
	 * - Initiate multipart upload if this is the first call
	 * - Abort multipart upload if requested
	 * - Complete multipart upload if all parts are done
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> executeCompositeOperation(final CompositeDataOperation op) {
		if (op.type() == OpType.CREATE) {
			// Check if this is an abort request
			if (op.get(KEY_MPU_ABORT) != null) {
				return abortMultipartUpload(op);
			}

			// Check if all sub-operations are done (complete the upload)
			if (op.allSubOperationsDone()) {
				return completeMultipartUpload(op);
			}

			// Initiate multipart upload (first call)
			return initiateMultipartUpload(op);
		} else if (op.type() == OpType.READ) {
			// Composite READ operations should be handled by the framework
			// by splitting into partial operations, not as a single HTTP request
			return CompletableFuture.failedFuture(
							new UnsupportedOperationException("Composite READ must be handled by partial operations"));
		} else {
			// For DELETE/UPDATE, delegate to standard path
			return putObject((O) op);
		}
	}

	/**
	 * Handle partial operations (range reads and part uploads).
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> executePartialOperation(final PartialDataOperation op) {
		if (op.type() == OpType.CREATE) {
			// This is a part upload for multipart upload
			return uploadPart(op);
		} else if (op.type() == OpType.READ) {
			// This is a range read for parallel download
			return readRange(op);
		} else {
			return CompletableFuture.failedFuture(
							new UnsupportedOperationException("Partial " + op.type() + " operations are not implemented"));
		}
	}

	private CompletableFuture<Void> putObject(final O op) {
		final var bk = resolveBucketAndKey(op);
		var reqBuilder = PutObjectRequest.builder()
						.bucket(bk[0])
						.key(bk[1]);
		if (checksumEnabled && checksumAlgorithm != null) {
			reqBuilder.checksumAlgorithm(checksumAlgorithm);
		}

		// Add tags if tagging is enabled
		if (taggingEnabled && !objectTags.isEmpty()) {
			final var tagSet = objectTags.entrySet().stream()
							.map(entry -> Tag.builder().key(entry.getKey()).value(entry.getValue()).build())
							.collect(Collectors.toList());
			reqBuilder.tagging(Tagging.builder().tagSet(tagSet).build());
		}

		if (op.item() instanceof DataItem) {
			DataItem dataItem = (DataItem) op.item();
			dataItem.position(0);
			try {
				final long size = dataItem.size();
				final DataItemInputStream inputStream = new DataItemInputStream(dataItem);
				return s3AsyncClient.putObject(
								reqBuilder.build(),
								AsyncRequestBody.fromInputStream(inputStream, size, executor))
								.thenApply(response -> null);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		} else if (op.item() instanceof PathItem) {
			PathItem pathItem = (PathItem) op.item();
			Path path = Path.of(pathItem.name());
			return s3AsyncClient.putObject(
							reqBuilder.build(),
							AsyncRequestBody.fromFile(path))
							.thenApply(response -> null);
		} else {
			return CompletableFuture.failedFuture(
							new UnsupportedOperationException("s3-aws PUT requires DataItem or PathItem"));
		}
	}

	private CompletableFuture<Void> readObject(final O op) {
		final var bk = resolveBucketAndKey(op);

		// Extract version ID from item name if versioning is enabled
		final String[] versionInfo = extractVersionId(op.item().name());
		final String key = versionInfo[0];
		final String versionId = versionInfo[1];

		// Use custom response transformer to properly measure timing
		// startResponse() is called when headers are available
		// finishResponse() is called when body is fully consumed
		var reqBuilder = GetObjectRequest.builder()
						.bucket(bk[0])
						.key(key);

		// Add version ID if present
		if (versionId != null) {
			reqBuilder.versionId(versionId);
		}

		return s3AsyncClient.getObject(
						reqBuilder.build(),
						AsyncResponseTransformer.toBlockingInputStream())
						.thenAccept(response -> {
							// Call startResponse() when headers are available (measures latency)
							op.startResponse();

							// Stream the data to count bytes without loading into memory
							try (var inputStream = response) {
								long bytesRead = 0;
								byte[] buffer = new byte[8192];
								int n;
								while ((n = inputStream.read(buffer)) != -1) {
									bytesRead += n;
								}
								if (op instanceof DataOperation) {
									((DataOperation) op).countBytesDone(bytesRead);
								}

								// Call finishResponse() when body is fully consumed (measures duration)
								op.finishResponse();
							} catch (IOException e) {
								throw new RuntimeException("Failed to read S3 object data", e);
							}
						});
	}

	private CompletableFuture<Void> deleteObject(final O op) {
		final var bk = resolveBucketAndKey(op);

		return s3AsyncClient.deleteObject(
						DeleteObjectRequest.builder()
										.bucket(bk[0])
										.key(bk[1])
										.build())
						.thenApply(response -> null);
	}

	private CompletableFuture<Void> headObject(final O op) {
		final var bk = resolveBucketAndKey(op);

		return s3AsyncClient.headObject(
						HeadObjectRequest.builder()
										.bucket(bk[0])
										.key(bk[1])
										.build())
						.thenApply(response -> null);
	}

	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> listObjects(final O op) {
		final var listOp = (ListOperation<? extends PathItem>) op;
		final var options = listOp.options();

		// Resolve bucket from srcPath (e.g. "/spttest" → "spttest")
		String targetBucket = bucketName;
		final var srcPath = op.srcPath();
		if (srcPath != null && !srcPath.isEmpty()) {
			final var rel = srcPath.startsWith("/") ? srcPath.substring(1) : srcPath;
			final var slash = rel.indexOf('/');
			targetBucket = slash > 0 ? rel.substring(0, slash) : rel;
		}

		final var maxKeys = options.maxKeys() > 0 ? Math.min(options.maxKeys(), 1000) : 1000;

		ListObjectsV2Request.Builder reqBuilder = ListObjectsV2Request.builder()
						.bucket(targetBucket)
						.maxKeys(maxKeys);

		// Prefix from the item name (the framework sets item name as the listing prefix)
		final var prefix = op.item().name();
		if (prefix != null && !prefix.isEmpty()) {
			// Strip leading slash — S3 prefixes don't start with /
			reqBuilder.prefix(prefix.startsWith("/") ? prefix.substring(1) : prefix);
		}

		// Delimiter
		final var delimiter = options.delimiter();
		if (delimiter != null && !delimiter.isEmpty()) {
			reqBuilder.delimiter(delimiter);
		}

		// Pagination via continuationToken or startAfter
		final var contToken = options.continuationToken();
		if (contToken != null && !contToken.isEmpty()) {
			reqBuilder.continuationToken(contToken);
		} else {
			final var startAfter = listOp.startAfter();
			if (startAfter != null && !startAfter.isEmpty()) {
				reqBuilder.startAfter(startAfter);
			}
		}

		return s3AsyncClient.listObjectsV2(reqBuilder.build())
						.thenAccept(resp -> {
							int objectCount = 0;
							long bytesTotal = 0;
							String firstKey = null;
							String lastKey = null;

							for (S3Object s3obj : resp.contents()) {
								objectCount++;
								if (options.fetchMetadata()) {
									bytesTotal += s3obj.size();
								}
								final var key = s3obj.key();
								if (firstKey == null) {
									firstKey = key;
								}
								lastKey = key;
							}

							listOp.objectsListed(objectCount);
							listOp.bytesListed(options.fetchMetadata() ? bytesTotal : 0);
							listOp.truncated(Boolean.TRUE.equals(resp.isTruncated()));

							if (firstKey != null) {
								listOp.pageFirstKey(firstKey);
							}
							listOp.continuationToken(resp.nextContinuationToken());

							// Update options with the new continuation token for the next page
							listOp.options(
											options.toBuilder()
															.continuationToken(resp.nextContinuationToken())
															.build());

							if (lastKey != null) {
								listOp.startAfter(lastKey);
							}
							listOp.countBytesDone(listOp.bytesListed());
						});
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

			ListObjectsV2Response resp = s3AsyncClient.listObjectsV2(reqBuilder.build()).join();

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

			ListObjectsV2Response resp = s3AsyncClient.listObjectsV2(reqBuilder.build()).join();

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

	// -----------------------------------------------------------------------
	// Multipart Upload Operations
	// -----------------------------------------------------------------------

	/**
	 * Initiate a multipart upload.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> initiateMultipartUpload(final CompositeDataOperation op) {
		final var bk = resolveBucketAndKey((O) op);
		var reqBuilder = CreateMultipartUploadRequest.builder()
						.bucket(bk[0])
						.key(bk[1]);

		// Add checksum algorithm if enabled
		if (checksumEnabled && checksumAlgorithm != null) {
			reqBuilder.checksumAlgorithm(checksumAlgorithm);
		}

		return s3AsyncClient.createMultipartUpload(reqBuilder.build())
						.thenApply(response -> {
							// Store the upload ID in the operation context
							op.put(KEY_UPLOAD_ID, response.uploadId());
							return null;
						});
	}

	/**
	 * Upload a single part of a multipart upload.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> uploadPart(final PartialDataOperation op) {
		final var parentOp = op.parent();
		if (!(parentOp instanceof CompositeDataOperation)) {
			return CompletableFuture.failedFuture(new IllegalArgumentException("Partial operation parent must be CompositeDataOperation"));
		}

		final CompositeDataOperation parent = (CompositeDataOperation) op.parent();
		final String uploadId = parent.get(KEY_UPLOAD_ID);
		if (uploadId == null) {
			return CompletableFuture.failedFuture(new IllegalStateException("Upload ID not found in parent operation"));
		}

		final var bk = resolveBucketAndKey((O) parent);
		final int partNumber = op.partNumber() + 1; // S3 uses 1-based part numbers

		var reqBuilder = UploadPartRequest.builder()
						.bucket(bk[0])
						.key(bk[1])
						.uploadId(uploadId)
						.partNumber(partNumber);

		if (op.item() instanceof DataItem) {
			DataItem dataItem = (DataItem) op.item();
			dataItem.position(0);
			try {
				final long size = dataItem.size();
				final DataItemInputStream inputStream = new DataItemInputStream(dataItem);
				return s3AsyncClient.uploadPart(
								reqBuilder.build(),
								AsyncRequestBody.fromInputStream(inputStream, size, executor))
								.thenApply(response -> {
									// Store the ETag in the parent operation context
									// Key format: part number as string
									parent.put(String.valueOf(partNumber), response.eTag());
									return null;
								});
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		} else {
			return CompletableFuture.failedFuture(new UnsupportedOperationException("Part upload requires DataItem"));
		}
	}

	/**
	 * Complete a multipart upload.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> completeMultipartUpload(final CompositeDataOperation op) {
		final String uploadId = op.get(KEY_UPLOAD_ID);
		if (uploadId == null) {
			return CompletableFuture.failedFuture(new IllegalStateException("Upload ID not found"));
		}

		final var bk = resolveBucketAndKey((O) op);
		final List<CompletedPart> completedParts = new ArrayList<>();

		// Collect all uploaded parts from the operation context
		for (var subOp : op.subOperations()) {
			if (subOp instanceof PartialDataOperation) {
				final int partNumber = ((PartialDataOperation) subOp).partNumber() + 1;
				final String eTag = op.get(String.valueOf(partNumber));
				if (eTag != null) {
					completedParts.add(CompletedPart.builder()
									.partNumber(partNumber)
									.eTag(eTag)
									.build());
				}
			}
		}

		if (completedParts.isEmpty()) {
			return CompletableFuture.failedFuture(new IllegalStateException("No parts to complete"));
		}

		var reqBuilder = CompleteMultipartUploadRequest.builder()
						.bucket(bk[0])
						.key(bk[1])
						.uploadId(uploadId)
						.multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build());

		return s3AsyncClient.completeMultipartUpload(reqBuilder.build())
						.thenApply(response -> null);
	}

	/**
	 * Abort a multipart upload.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> abortMultipartUpload(final CompositeDataOperation op) {
		final String uploadId = op.get(KEY_UPLOAD_ID);
		if (uploadId == null) {
			return CompletableFuture.failedFuture(new IllegalStateException("Upload ID not found"));
		}

		final var bk = resolveBucketAndKey((O) op);
		return s3AsyncClient.abortMultipartUpload(
						AbortMultipartUploadRequest.builder()
										.bucket(bk[0])
										.key(bk[1])
										.uploadId(uploadId)
										.build())
						.thenApply(response -> null);
	}

	// -----------------------------------------------------------------------
	// Range Read Operations
	// -----------------------------------------------------------------------

	/**
	 * Read a specific byte range of an object.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> readRange(final PartialDataOperation op) {
		final var parentOp = op.parent();
		final var bk = resolveBucketAndKey((O) parentOp);

		// Calculate range based on part number and size
		long rangeStart, rangeEnd;
		try {
			rangeStart = op.partNumber() * op.item().size();
			rangeEnd = rangeStart + op.item().size() - 1;
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}

		var reqBuilder = GetObjectRequest.builder()
						.bucket(bk[0])
						.key(bk[1])
						.range("bytes=" + rangeStart + "-" + rangeEnd);

		return s3AsyncClient.getObject(
						reqBuilder.build(),
						AsyncResponseTransformer.toBlockingInputStream())
						.thenAccept(response -> {
							op.startResponse();
							try (var inputStream = response) {
								long bytesRead = 0;
								byte[] buffer = new byte[8192];
								int n;
								while ((n = inputStream.read(buffer)) != -1) {
									bytesRead += n;
								}
								if (op instanceof DataOperation) {
									((DataOperation) op).countBytesDone(bytesRead);
								}
								op.finishResponse();
							} catch (IOException e) {
								throw new RuntimeException("Failed to read S3 object range", e);
							}
						});
	}

	// -----------------------------------------------------------------------
	// Copy Operations
	// -----------------------------------------------------------------------

	/**
	 * Copy an object from source to destination.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> copyObject(final O op) {
		final var dstBk = resolveBucketAndKey(op);
		final var srcPath = op.srcPath();
		if (srcPath == null || srcPath.isEmpty()) {
			return CompletableFuture.failedFuture(new IllegalArgumentException("Source path is required for copy operation"));
		}

		// Parse source bucket and key from srcPath
		final String[] srcBk = parseBucketAndKey(srcPath);

		var reqBuilder = CopyObjectRequest.builder()
						.sourceBucket(srcBk[0])
						.sourceKey(srcBk[1])
						.destinationBucket(dstBk[0])
						.destinationKey(dstBk[1]);

		return s3AsyncClient.copyObject(reqBuilder.build())
						.thenApply(response -> null);
	}
}
