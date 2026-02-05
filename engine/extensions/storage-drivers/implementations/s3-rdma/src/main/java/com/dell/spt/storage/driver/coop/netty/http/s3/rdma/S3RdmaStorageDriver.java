package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.item.op.Operation.SLASH;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver;
import com.github.akurilov.confuse.Config;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * S3 storage driver with RDMA support for high-performance data transfer.
 *
 * <p>Extends {@link S3StorageDriver} to reuse S3 authentication, listing, versioning,
 * tagging, and bucket management. Routes large data operations (PUT/GET) through
 * RDMA when available, while all other operations use the inherited HTTP/Netty path.
 *
 * <h2>V3 Architecture</h2>
 * <p>V3 uses direct libibverbs for RDMA token generation. The protocol flow is:
 * <ol>
 *   <li>Client registers memory buffer with RDMA NIC</li>
 *   <li>Client generates RDMA token (addr:size:rkey:lid:dctn:g:gid)</li>
 *   <li>Client sends HTTP request with {@code x-amz-rdma-token} header</li>
 *   <li>Server performs RDMA READ (PUT) or WRITE (GET) to client memory</li>
 *   <li>Server responds with HTTP status</li>
 *   <li>Client deregisters buffer</li>
 * </ol>
 *
 * <p><b>Note:</b> V3 HTTP integration (adding RDMA token headers to Netty requests)
 * is not yet implemented. Currently falls back to HTTP for all operations.
 */
public class S3RdmaStorageDriver<I extends Item, O extends Operation<I>>
				extends S3StorageDriver<I, O> {

	/** HTTP header name for RDMA token. */
	public static final String RDMA_TOKEN_HEADER = "x-amz-rdma-token";

	private final RdmaConfig rdmaConfig;
	private final RdmaTransport rdmaTransport;
	private final String endpointUrl;

	public S3RdmaStorageDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);

		// Parse RDMA configuration
		rdmaConfig = new RdmaConfig(storageConfig.configVal("rdma"));
		Loggers.MSG.info("{}: RDMA config: {}", stepId, rdmaConfig);

		// Build endpoint URL from storage node config
		endpointUrl = buildEndpointUrl();

		// Initialize RDMA transport
		rdmaTransport = new RdmaTransport(rdmaConfig);
		if (rdmaConfig.isEnabled()) {
			final boolean ok = rdmaTransport.init(
							endpointUrl, credential.getUid(), credential.getSecret());
			if (ok) {
				Loggers.MSG.info("{}: RDMA V3 transport initialized for endpoint {}", stepId, endpointUrl);
			} else if (rdmaConfig.isFallbackEnabled()) {
				Loggers.MSG.warn(
								"{}: RDMA unavailable, falling back to HTTP for all operations", stepId);
			} else {
				throw new IllegalConfigurationException(
								"RDMA initialization failed and fallback is disabled");
			}
		} else {
			Loggers.MSG.info("{}: RDMA disabled by configuration, using HTTP", stepId);
		}
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		if (shouldUseRdma(op)) {
			Loggers.MSG.trace("{}: submit(op) routing to RDMA, op={}", stepId, op.type());
			return submitRdma(op);
		}
		return super.submit(op);
	}

	/**
	 * Override batch submit to route RDMA-eligible operations through our RDMA path.
	 *
	 * The parent class's batch submit doesn't call single-item submit(), so we must
	 * intercept here to ensure RDMA operations are properly routed.
	 */
	@Override
	protected int submit(final List<O> ops, final int from, final int to) throws IllegalStateException {
		if (!rdmaTransport.isAvailable()) {
			// Fast path: if RDMA is not available, use parent's optimized batch submit
			return super.submit(ops, from, to);
		}

		// Process operations individually to allow RDMA routing decisions per-operation
		int submitted = 0;
		for (int i = from; i < to; i++) {
			final O op = ops.get(i);
			if (submit(op)) {
				submitted++;
			} else {
				// Stop on first failure (throttle exhausted)
				break;
			}
		}
		Loggers.MSG.trace("{}: submit(batch) processed {} of {} ops", stepId, submitted, to - from);
		return submitted;
	}

	/**
	 * Determine whether this operation should use the RDMA data path.
	 *
	 * RDMA is used when all of the following are true:
	 * - RDMA transport is initialized and available
	 * - The operation is a data operation (CREATE or READ)
	 * - The data item size meets or exceeds the configured threshold
	 */
	private boolean shouldUseRdma(final O op) {
		if (!rdmaTransport.isAvailable()) {
			// TRACE level - checked on every operation in high-throughput workloads
			Loggers.MSG.trace("{}: RDMA skip: transport not available", stepId);
			return false;
		}
		if (!(op instanceof DataOperation)) {
			Loggers.MSG.trace("{}: RDMA skip: not a DataOperation", stepId);
			return false;
		}
		final var opType = op.type();
		if (opType != OpType.CREATE && opType != OpType.READ) {
			Loggers.MSG.trace("{}: RDMA skip: opType={} not CREATE/READ", stepId, opType);
			return false;
		}
		final var dataOp = (DataOperation) op;
		try {
			final long size = dataOp.item().size();
			final boolean useRdma = size >= rdmaConfig.getThresholdBytes();
			if (!useRdma) {
				Loggers.MSG.trace("{}: RDMA skip: size={} < threshold={}", stepId, size, rdmaConfig.getThresholdBytes());
			}
			return useRdma;
		} catch (final IOException e) {
			Loggers.MSG.warn("{}: RDMA skip: IOException getting size", stepId);
			return false;
		}
	}

	/**
	 * Submit a data operation via the RDMA path.
	 *
	 * <p>V3 Architecture: This method should:
	 * <ol>
	 *   <li>Allocate and register a buffer</li>
	 *   <li>For PUT: copy data to buffer</li>
	 *   <li>Generate RDMA token</li>
	 *   <li>Submit HTTP request with x-amz-rdma-token header</li>
	 *   <li>Wait for response</li>
	 *   <li>Deregister buffer</li>
	 * </ol>
	 *
	 * <p><b>TODO:</b> HTTP header integration with Netty pipeline is not yet implemented.
	 * Currently falls back to standard HTTP.
	 */
	private boolean submitRdma(final O op) {
		if (!isStarted()) {
			throw new IllegalStateException();
		}

		final var dataOp = (DataOperation) op;
		final var item = dataOp.item();
		final var opType = op.type();
		final int size;
		try {
			size = (int) item.size();
		} catch (final IOException e) {
			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return true; // Operation completed (with failure)
		}

		// V3: Allocate buffer and register for RDMA
		ByteBuffer buf = null;
		long mrHandle = 0;
		try {
			buf = rdmaTransport.allocateBuffer(size);
			mrHandle = rdmaTransport.registerBuffer(buf, size);

			if (mrHandle == 0) {
				// Registration failed - fall back to HTTP
				Loggers.MSG.trace("{}: RDMA buffer registration failed, falling back to HTTP", stepId);
				rdmaTransport.freeBuffer(buf);
				return super.submit(op);
			}

			// For PUT: copy data into registered buffer
			if (opType == OpType.CREATE) {
				transferDataItemToBuffer((DataItem) item, buf, size);
			}

			// Generate RDMA token
			final String token = rdmaTransport.generateToken(mrHandle, size);
			if (token == null) {
				// Token generation failed - fall back to HTTP
				Loggers.MSG.trace("{}: RDMA token generation failed, falling back to HTTP", stepId);
				rdmaTransport.deregisterBuffer(buf, mrHandle);
				rdmaTransport.freeBuffer(buf);
				return super.submit(op);
			}

			// TODO: V3 HTTP Integration
			// The token needs to be added as an HTTP header (x-amz-rdma-token) to the request.
			// This requires integration with the Netty pipeline in S3StorageDriver.
			// For now, fall back to standard HTTP.
			Loggers.MSG.debug("{}: RDMA token generated: {} (HTTP integration pending)", stepId, token);

			// Fall back to HTTP until header integration is complete
			rdmaTransport.deregisterBuffer(buf, mrHandle);
			rdmaTransport.freeBuffer(buf);
			return super.submit(op);

		} catch (final Exception e) {
			LogUtil.exception(Level.WARN, e, "{}: RDMA submit failed for {}",
							stepId, item.name());

			// Clean up
			if (mrHandle != 0) {
				rdmaTransport.deregisterBuffer(buf, mrHandle);
			}
			if (buf != null) {
				rdmaTransport.freeBuffer(buf);
			}

			if (rdmaConfig.isFallbackEnabled()) {
				Loggers.MSG.trace("{}: falling back to HTTP after RDMA exception", stepId);
				return super.submit(op);
			}

			// Complete operation as failed
			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return true; // Operation completed (with failure)
		}
	}

	/**
	 * Copy data from a DataItem into a direct ByteBuffer for RDMA transfer.
	 */
	private void transferDataItemToBuffer(final DataItem item, final ByteBuffer dst, final int size)
					throws IOException {
		dst.clear();
		dst.limit(size);
		int totalRead = 0;
		while (totalRead < size) {
			final int bytesRead = item.read(dst);
			if (bytesRead < 0) {
				break;
			}
			totalRead += bytesRead;
		}
		dst.flip();
	}

	/**
	 * Extract the bucket name from the driver's namespace configuration.
	 * The namespace is typically the bucket name, possibly with a leading slash.
	 */
	private String extractBucket() {
		if (namespace == null || namespace.isEmpty()) {
			return "";
		}
		final String ns = namespace.startsWith(SLASH) ? namespace.substring(1) : namespace;
		final int slashPos = ns.indexOf(SLASH);
		return slashPos > 0 ? ns.substring(0, slashPos) : ns;
	}

	/**
	 * Extract the object key from an item's name.
	 */
	private String extractKey(final Item item) {
		final String name = item.name();
		return name.startsWith(SLASH) ? name.substring(1) : name;
	}

	/**
	 * Build the S3 endpoint URL from the storage node configuration.
	 */
	private String buildEndpointUrl() {
		final String addr = storageNodeAddrs[0];
		final String host;
		final int port;
		final int colonPos = addr.lastIndexOf(':');
		if (colonPos > 0) {
			host = addr.substring(0, colonPos);
			port = Integer.parseInt(addr.substring(colonPos + 1));
		} else {
			host = addr;
			port = storageNodePort;
		}
		// Determine scheme: use HTTPS if port is 443 or 9021, otherwise HTTP
		final String scheme = (port == 443 || port == 9021) ? "https" : "http";
		return scheme + "://" + host + ":" + port;
	}

	@Override
	protected void doClose() throws IOException {
		rdmaTransport.close();
		super.doClose();
	}
}
