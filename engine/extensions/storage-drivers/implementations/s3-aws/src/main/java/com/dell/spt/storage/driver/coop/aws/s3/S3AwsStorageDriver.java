package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.integrity.IntegrityResponseObserver;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.io.DataItemInputStream;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.ListedObject;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.storage.driver.coop.nio.NioStorageDriverBase;
import com.github.akurilov.confuse.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.SdkPlugin;
import software.amazon.awssdk.core.SdkServiceClientConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.ChecksumAlgorithm;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
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

	@Override
	protected boolean integrityAdditionalPayloadPassRequired(final O op) {
		return checksumEnabled && checksumAlgorithm != null
						&& (op instanceof CompositeDataOperation || checksumAlgorithm != ChecksumAlgorithm.SHA256);
	}

	static final ExecutionAttribute<ListOperation<? extends PathItem>> LIST_TTFB_OPERATION_ATTRIBUTE = new ExecutionAttribute<>("sptListTtfbOperation");
	static final ExecutionInterceptor LIST_TTFB_INTERCEPTOR = new ListTtfbExecutionInterceptor();
	private static final SdkPlugin LIST_TTFB_SDK_PLUGIN = new ListTtfbSdkPlugin();

	// S3 API constants for multipart upload
	private static final String KEY_UPLOAD_ID = "uploadId";
	private static final String KEY_MPU_ABORT = "mpuAbort";
	private static final String KEY_MPU_FAILURE = "mpuFailure";

	private final S3AsyncClient s3AsyncClient;
	private final ExecutorService executor; // For read operations
	private final ExecutorService uploadExecutor; // Dedicated for upload operations
	private final String bucketName;
	private final boolean checksumEnabled;
	private final ChecksumAlgorithm checksumAlgorithm;
	private final boolean versioningEnabled;
	private final boolean taggingEnabled;
	private final Map<String, String> objectTags;
	private final long smallObjectThresholdBytes;
	private final long partSizeBytes;

	public S3AwsStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config config,
					final boolean verifyFlag,
					final int batchSize,
					final S3AsyncClient s3AsyncClient,
					final long smallObjectThresholdBytes,
					final long partSizeBytes)
					throws IllegalConfigurationException {
		super(stepId, dataInput, config, verifyFlag, batchSize);
		if (verifyFlag) {
			throw new IllegalConfigurationException(
							"S3-AWS driver does not support legacy item.data.verify; "
											+ "use storage.integrity.mode=metadata for whole-object verification");
		}
		this.s3AsyncClient = s3AsyncClient;
		this.smallObjectThresholdBytes = smallObjectThresholdBytes;
		this.partSizeBytes = partSizeBytes;

		// Use virtual threads to support high concurrency without OS-level thread explosion
		// This allows thousands of blocking operations while honoring user's --concurrency limit
		this.executor = Executors.newVirtualThreadPerTaskExecutor();
		this.uploadExecutor = Executors.newVirtualThreadPerTaskExecutor();

		this.bucketName = resolveBucketName(config);

		boolean ckEnabled = false;
		ChecksumAlgorithm ckAlgo = null;
		try {
			final var checksumConfig = config.configVal("checksum");
			if (checksumConfig != null) {
				ckEnabled = checksumConfig.boolVal("enabled");
			}
		} catch (Exception e) {
			LOG.debug("Could not read checksum-enabled from config: {}", e.toString());
		}
		if (ckEnabled) {
			try {
				final var checksumConfig = config.configVal("checksum");
				if (checksumConfig != null) {
					final String algo = checksumConfig.stringVal("algorithm");
					ckAlgo = resolveChecksumAlgorithm(algo);
					if (ckAlgo == null) {
						LOG.warn("Unsupported checksum algorithm '{}' for s3-aws driver; checksums will not be applied", algo);
						ckEnabled = false;
					}
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
			final var objectConfig = config.configVal("object");
			if (objectConfig != null) {
				versioning = objectConfig.boolVal("versioning");
			}
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
		switch (algorithm.toLowerCase(Locale.ROOT)) {
		case "crc32":
			return ChecksumAlgorithm.CRC32;
		case "crc32c":
			return ChecksumAlgorithm.CRC32_C;
		case "sha1":
			return ChecksumAlgorithm.SHA1;
		case "sha256":
			return ChecksumAlgorithm.SHA256;
		case "crc64-nvme":
			return ChecksumAlgorithm.CRC64_NVME;
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
					final int slashPos = nodeAddrs.indexOf('/');
					if (slashPos >= 0) {
						resolved = nodeAddrs.substring(0, slashPos);
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
		final CompositeDataOperation mpuOp = op instanceof CompositeDataOperation
						&& op.type() == OpType.CREATE ? (CompositeDataOperation) op : null;
		final boolean aborting = mpuOp != null && mpuOp.get(KEY_MPU_ABORT) != null;
		final boolean completing = mpuOp != null && !aborting && mpuOp.allSubOperationsDone();
		try {
			// Execute AWS SDK operation asynchronously and block for result.
			execute(op).join();

			// Set bytes transferred for metrics (skip READs — readObject sets actual bytes).
			if (op.type() != OpType.READ && op.item() instanceof DataItem) {
				final DataItem dataItem = (DataItem) op.item();
				if (op instanceof DataOperation) {
					((DataOperation) op).countBytesDone(dataItem.size());
				}
			}

			if (op.type() != OpType.READ) {
				finishOperation(op);
				if (aborting) {
					op.status(Operation.Status.FAIL_UNKNOWN);
					emitMultipartLifecycle(mpuOp, "failed_aborted", true, true, mpuFailure(mpuOp));
				} else if (completing) {
					emitMultipartLifecycle(mpuOp, "completed", false, null, null);
				}
			} else if (op.status() == Operation.Status.ACTIVE) {
				// READ operations call finishResponse in readObject. Preserve terminal corruption.
				op.status(Operation.Status.SUCC);
			}

		} catch (final Exception e) {
			final Status originalStatus = classifyFailure(e);
			if (aborting) {
				emitMultipartLifecycle(
								mpuOp, "failed_orphaned", true, false, "abort failed: " + originalStatus.name());
			} else if (completing && mpuOp.get(KEY_UPLOAD_ID) != null) {
				try {
					abortMultipartUpload(mpuOp).join();
					emitMultipartLifecycle(
									mpuOp, "failed_aborted", true, true, "completion failed: " + originalStatus.name());
				} catch (final Exception abortFailure) {
					emitMultipartLifecycle(
									mpuOp,
									"failed_orphaned",
									true,
									false,
									"completion failed: " + originalStatus.name()
													+ "; abort failed: " + classifyFailure(abortFailure).name());
				}
			}
			op.status(originalStatus);
			LOG.error("{} {} failed: {}", op.type(), op.item().name(), originalStatus);
			LOG.debug("{} {} stack trace", op.type(), op.item().name(), e);
			try {
				op.startResponse();
				op.finishResponse();
			} catch (final Exception responseEx) {
				LOG.debug("{} {} response finalization failed", op.type(), op.item().name(), responseEx);
			}
		}
	}

	@SuppressWarnings("unchecked")
	private void emitMultipartLifecycle(
					final CompositeDataOperation op,
					final String state,
					final boolean abortAttempted,
					final Boolean abortSucceeded,
					final String error) {
		if (!integrityMetadataEnabled()) {
			return;
		}
		final var bucketAndKey = resolveBucketAndKey((O) op);
		IntegrityCsvArtifacts.logMultipartLifecycleOnce(
						op,
						IntegrityCsvArtifacts.nodeIdentity(),
						stepId,
						driverType(),
						bucketAndKey[0],
						bucketAndKey[1],
						op.get(KEY_UPLOAD_ID),
						state,
						abortAttempted,
						abortSucceeded,
						error);
	}

	private static String mpuFailure(final CompositeDataOperation op) {
		final String failure = op.get(KEY_MPU_FAILURE);
		return failure == null ? "multipart part failed" : "multipart part failed: " + failure;
	}

	/**
	 * Map an exception to the most specific Operation.Status failure code.
	 * Unwraps CompletionException to handle async failures properly.
	 */
	static Status classifyFailure(final Exception e) {
		// Unwrap CompletionException from .join() calls
		Throwable cause = e;
		if (e instanceof java.util.concurrent.CompletionException) {
			cause = e.getCause();
			if (cause == null) {
				cause = e;
			}
		}

		// Convert to Exception for type checking
		if (!(cause instanceof Exception)) {
			return Status.FAIL_UNKNOWN;
		}
		final Exception ex = (Exception) cause;

		if (ex instanceof S3Exception) {
			final int statusCode = ((S3Exception) ex).statusCode();
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
		if (ex instanceof ApiCallTimeoutException || ex instanceof ApiCallAttemptTimeoutException
						|| ex instanceof TimeoutException) {
			return Status.FAIL_TIMEOUT;
		}
		if (ex instanceof IOException || (ex instanceof SdkClientException && ex.getCause() instanceof IOException)) {
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
		if (integrityMetadataEnabled() && op.integrityMetadata() != null) {
			reqBuilder.metadata(IntegrityMetadataCodec.logicalMetadata(op.integrityMetadata()));
		}
		if (checksumEnabled && checksumAlgorithm != null) {
			if (checksumAlgorithm == ChecksumAlgorithm.SHA256 && op.integrityMetadata() != null) {
				reqBuilder.checksumSHA256(Base64.getEncoder().encodeToString(
								HexFormat.of().parseHex(op.integrityMetadata().digest())));
			} else {
				reqBuilder.checksumAlgorithm(checksumAlgorithm);
			}
		}

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
				if (size <= 8 * 1024) {
					ByteBuffer buffer = ByteBuffer.allocate((int) size);
					int bytesRead = dataItem.read(buffer);
					if (bytesRead != size) {
						return CompletableFuture.failedFuture(new IOException("Unexpected read size"));
					}
					buffer.flip();
					return dispatchPutObject(
									op, reqBuilder.build(), AsyncRequestBody.fromByteBuffer(buffer));
				}

				LOG.trace(
								"Streaming {} object upload, size={}B, threshold={}B, partSize={}B",
								size <= smallObjectThresholdBytes ? "small" : "large",
								size,
								smallObjectThresholdBytes,
								partSizeBytes);
				final DataItemInputStream inputStream = new DataItemInputStream(dataItem);
				return dispatchPutObject(
								op,
								reqBuilder.build(),
								AsyncRequestBody.fromInputStream(inputStream, size, uploadExecutor));
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		} else if (op.item() instanceof PathItem) {
			Path path = Path.of(op.item().name());
			return dispatchPutObject(op, reqBuilder.build(), AsyncRequestBody.fromFile(path));
		}
		return CompletableFuture.failedFuture(
						new UnsupportedOperationException("s3-aws PUT requires DataItem or PathItem"));
	}

	private CompletableFuture<Void> dispatchPutObject(
					final O op, final PutObjectRequest request, final AsyncRequestBody body) {
		markIntegrityRequestDispatched();
		return s3AsyncClient.putObject(request, body)
						.thenAccept(response -> captureResponseIdentity(
										op, response.versionId(), requestId(response)));
	}

	private static void captureResponseIdentity(
					final Operation<?> op, final String versionId, final String requestId) {
		op.returnedVersionId(versionId);
		op.responseRequestId(requestId);
	}

	private static String requestId(final S3Response response) {
		return response.responseMetadata() == null
						? null
						: response.responseMetadata().requestId();
	}

	private CompletableFuture<Void> readObject(final O op) {
		final var bk = resolveBucketAndKey(op);

		final String key;
		final String versionId;
		if (integrityMetadataEnabled()) {
			key = bk[1];
			versionId = op.requestedVersionId();
		} else {
			// Preserve the legacy key~version carrier outside metadata mode.
			final String[] versionInfo = extractVersionId(bk[1]);
			key = versionInfo[0];
			versionId = versionInfo[1];
		}

		var reqBuilder = GetObjectRequest.builder()
						.bucket(bk[0])
						.key(key);
		if (versionId != null && !versionId.isEmpty()) {
			reqBuilder.versionId(versionId);
		}

		try (var response = s3AsyncClient.getObject(
						reqBuilder.build(),
						AsyncResponseTransformer.toBlockingInputStream()).join()) {
			final GetObjectResponse getResponse = response.response();
			captureResponseIdentity(
							op, getResponse.versionId(), requestId(getResponse));
			final IntegrityResponseObserver integrityObserver = integrityMetadataEnabled()
							? new IntegrityResponseObserver(
											getResponse.metadata().entrySet(), getResponse.contentLength())
							: null;

			op.startResponse();
			long bytesRead = 0;
			byte[] buffer = new byte[8192];
			int n;
			while ((n = response.read(buffer)) != -1) {
				if (bytesRead == 0 && n > 0 && op instanceof DataOperation) {
					((DataOperation) op).startDataResponse();
				}
				if (integrityObserver != null && n > 0) {
					integrityObserver.onBody(ByteBuffer.wrap(buffer, 0, n));
				}
				bytesRead += n;
			}
			if (op instanceof DataOperation) {
				((DataOperation) op).countBytesDone(bytesRead);
			}
			if (integrityObserver != null) {
				final IntegrityVerificationResult result = integrityObserver.finish();
				op.integrityVerificationResult(result);
				recordIntegrityReadResult(result);
				if (!result.verified()) {
					op.status(Operation.Status.RESP_FAIL_CORRUPT);
				}
			}
			op.finishResponse();
		} catch (IOException e) {
			throw new RuntimeException("Failed to read S3 object data", e);
		}

		return CompletableFuture.completedFuture(null);
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
						.maxKeys(maxKeys)
						.overrideConfiguration(b -> b
										.putExecutionAttribute(LIST_TTFB_OPERATION_ATTRIBUTE, listOp)
										.addPlugin(LIST_TTFB_SDK_PLUGIN));

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
							final List<ListedObject> listedObjects = new ArrayList<>(resp.contents().size());
							String firstKey = null;
							String lastKey = null;

							for (S3Object s3obj : resp.contents()) {
								objectCount++;
								if (options.fetchMetadata()) {
									bytesTotal += s3obj.size();
								}
								final var key = s3obj.key();
								listedObjects.add(new ListedObject(key, Math.max(0, s3obj.size())));
								if (firstKey == null) {
									firstKey = key;
								}
								lastKey = key;
							}

							listOp.objectsListed(objectCount);
							listOp.listedObjects(listedObjects);
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
							if (listOp.respDataTimeStart() == 0) {
								LOG.debug("{}: LIST completed before first body byte was observed; TTFB unavailable", listOp);
							}
						});
	}

	private static void markListDataResponseStart(final ListOperation<? extends PathItem> op) {
		if (op.respDataTimeStart() == 0) {
			try {
				op.startDataResponse();
			} catch (final IllegalStateException e) {
				LOG.debug("{}: failed to mark LIST data response start", op, e);
			}
		}
	}

	static Publisher<ByteBuffer> wrapListDataResponsePublisher(
					final Publisher<ByteBuffer> delegate,
					final ListOperation<? extends PathItem> op) {
		final AtomicBoolean firstBodyBytesObserved = new AtomicBoolean(false);
		return subscriber -> delegate.subscribe(new Subscriber<>() {
			@Override
			public void onSubscribe(final Subscription subscription) {
				subscriber.onSubscribe(subscription);
			}

			@Override
			public void onNext(final ByteBuffer byteBuffer) {
				if (byteBuffer != null
								&& byteBuffer.remaining() > 0
								&& firstBodyBytesObserved.compareAndSet(false, true)) {
					markListDataResponseStart(op);
				}
				subscriber.onNext(byteBuffer);
			}

			@Override
			public void onError(final Throwable throwable) {
				subscriber.onError(throwable);
			}

			@Override
			public void onComplete() {
				subscriber.onComplete();
			}
		});
	}

	static final class ListTtfbExecutionInterceptor implements ExecutionInterceptor {
		@Override
		public Optional<Publisher<ByteBuffer>> modifyAsyncHttpResponseContent(
						final Context.ModifyHttpResponse context,
						final ExecutionAttributes executionAttributes) {
			final ListOperation<? extends PathItem> op = executionAttributes.getAttribute(LIST_TTFB_OPERATION_ATTRIBUTE);
			if (op == null) {
				return context.responsePublisher();
			}
			return context.responsePublisher()
							.map(publisher -> wrapListDataResponsePublisher(publisher, op));
		}
	}

	static final class ListTtfbSdkPlugin implements SdkPlugin {
		@Override
		public void configureClient(final SdkServiceClientConfiguration.Builder builder) {
			builder.overrideConfiguration(b -> b.addExecutionInterceptor(LIST_TTFB_INTERCEPTOR));
		}
	}

	/**
	 * List objects in the bucket with optional prefix and options.
	 */
	@Override
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
		} catch (Exception e) {
			// Unwrap CompletionException from .join() call
			Throwable cause = e;
			if (e instanceof java.util.concurrent.CompletionException) {
				cause = e.getCause();
				if (cause == null) {
					cause = e;
				}
			}
			if (cause instanceof S3Exception) {
				throw new IOException("Failed to list objects", cause);
			}
			throw new IOException("Failed to list objects", e);
		}
	}

	/**
	 * List objects in the bucket with optional prefix.
	 */
	@Override
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
		} catch (Exception e) {
			// Unwrap CompletionException from .join() call
			Throwable cause = e;
			if (e instanceof java.util.concurrent.CompletionException) {
				cause = e.getCause();
				if (cause == null) {
					cause = e;
				}
			}
			if (cause instanceof S3Exception) {
				throw new IOException("Failed to probe common prefixes", cause);
			}
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
		if (integrityMetadataEnabled() && op.integrityMetadata() != null) {
			reqBuilder.metadata(IntegrityMetadataCodec.logicalMetadata(op.integrityMetadata()));
		}

		// Add checksum algorithm if enabled
		if (checksumEnabled && checksumAlgorithm != null) {
			reqBuilder.checksumAlgorithm(checksumAlgorithm);
		}

		markIntegrityRequestDispatched();
		return s3AsyncClient.createMultipartUpload(reqBuilder.build())
						.thenAccept(response -> {
							op.put(KEY_UPLOAD_ID, response.uploadId());
							captureResponseIdentity(
											op, null, requestId(response));
						});
	}

	/**
	 * Upload a single part of a multipart upload.
	 */
	@SuppressWarnings("unchecked")
	private CompletableFuture<Void> uploadPart(final PartialDataOperation op) {
		final var parentOp = op.parent();
		if (parentOp == null) {
			return CompletableFuture.failedFuture(new IllegalArgumentException("Partial operation parent must be CompositeDataOperation"));
		}

		final CompositeDataOperation parent = parentOp;
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

		final DataItem dataItem = op.item();
		if (dataItem != null) {
			dataItem.position(0);
			try {
				final long size = dataItem.size();
				final DataItemInputStream inputStream = new DataItemInputStream(dataItem);
				// Use dedicated upload executor to maximize throughput
				return s3AsyncClient.uploadPart(
								reqBuilder.build(),
								AsyncRequestBody.fromInputStream(inputStream, size, uploadExecutor))
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
						.thenAccept(response -> captureResponseIdentity(
										op, response.versionId(), requestId(response)));
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

		// Calculate range based on item offset and size
		// The framework's DataItem tracks the correct slice offset natively
		long rangeStart = op.item().offset();
		long rangeEnd;
		try {
			rangeEnd = rangeStart + op.item().size() - 1;
		} catch (IOException e) {
			return CompletableFuture.failedFuture(e);
		}

		var reqBuilder = GetObjectRequest.builder()
						.bucket(bk[0])
						.key(bk[1])
						.range("bytes=" + rangeStart + "-" + rangeEnd);

		// Get the response asynchronously - this completes when headers arrive
		// Since invokeNio already blocks, we can join() here and read synchronously
		// This avoids the overhead of thenAcceptAsync and the thread pool bottleneck
		try (var response = s3AsyncClient.getObject(
						reqBuilder.build(),
						AsyncResponseTransformer.toBlockingInputStream()).join()) {
			op.startResponse();
			// This blocking read runs on the caller thread (virtual thread)
			long bytesRead = 0;
			byte[] buffer = new byte[8192];
			int n;
			while ((n = response.read(buffer)) != -1) {
				if (bytesRead == 0 && n > 0) {
					op.startDataResponse();
				}
				bytesRead += n;
			}
			op.countBytesDone(bytesRead);
			op.finishResponse();
		} catch (IOException e) {
			throw new RuntimeException("Failed to read S3 object range", e);
		}

		return CompletableFuture.completedFuture(null);
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

	@Override
	protected void doClose()
					throws IOException {
		try {
			// Close the S3AsyncClient to release native CRT resources
			// This closes the underlying event loops and connection pools
			if (s3AsyncClient != null) {
				s3AsyncClient.close();
				LOG.info("S3AsyncClient closed successfully");
			}
		} catch (Exception e) {
			LOG.warn("Failed to close S3AsyncClient: {}", e.getMessage());
		}

		try {
			// Shutdown the executor service to release thread pool resources
			if (executor != null) {
				executor.shutdown();
				// Wait for pending tasks to complete (with timeout)
				if (!executor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
					LOG.warn("Executor service did not terminate gracefully, forcing shutdown");
					executor.shutdownNow();
				}
				LOG.info("Executor service shutdown successfully");
			}
		} catch (InterruptedException e) {
			LOG.warn("Executor service shutdown interrupted");
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			LOG.warn("Failed to shutdown executor service: {}", e.getMessage());
		}

		try {
			// Shutdown the upload executor service to release thread pool resources
			if (uploadExecutor != null) {
				uploadExecutor.shutdown();
				// Wait for pending tasks to complete (with timeout)
				if (!uploadExecutor.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS)) {
					LOG.warn("Upload executor service did not terminate gracefully, forcing shutdown");
					uploadExecutor.shutdownNow();
				}
				LOG.info("Upload executor service shutdown successfully");
			}
		} catch (InterruptedException e) {
			LOG.warn("Upload executor service shutdown interrupted");
			Thread.currentThread().interrupt();
		} catch (Exception e) {
			LOG.warn("Failed to shutdown upload executor service: {}", e.getMessage());
		}

		// Call parent's doClose to clean up base class resources
		super.doClose();
	}
}
