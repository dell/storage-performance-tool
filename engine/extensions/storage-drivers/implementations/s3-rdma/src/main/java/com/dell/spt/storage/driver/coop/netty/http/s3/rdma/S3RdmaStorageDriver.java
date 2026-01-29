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

/**
 * S3 storage driver with RDMA support for high-performance data transfer.
 *
 * Extends {@link S3StorageDriver} to reuse S3 authentication, listing, versioning,
 * tagging, and bucket management. Overrides {@code submit()} to route large data
 * operations (PUT/GET) through RDMA via libobjclient (JNI), while all other
 * operations use the inherited HTTP/Netty path.
 */
public class S3RdmaStorageDriver<I extends Item, O extends Operation<I>>
				extends S3StorageDriver<I, O> {

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
				Loggers.MSG.info("{}: RDMA transport initialized for endpoint {}", stepId, endpointUrl);
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
			return submitRdma(op);
		}
		return super.submit(op);
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
			return false;
		}
		if (!(op instanceof DataOperation)) {
			return false;
		}
		final var opType = op.type();
		if (opType != OpType.CREATE && opType != OpType.READ) {
			return false;
		}
		final var dataOp = (DataOperation) op;
		try {
			return dataOp.item().size() >= rdmaConfig.getThresholdBytes();
		} catch (final IOException e) {
			return false;
		}
	}

	/**
	 * Submit a data operation via the RDMA path.
	 *
	 * This bypasses the Netty pipeline entirely — libobjclient has its own
	 * HTTP/RDMA stack. The call is synchronous (blocking).
	 */
	private boolean submitRdma(final O op) {
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		if (!concurrencyThrottle.tryAcquire()) {
			return false;
		}

		final var dataOp = (DataOperation) op;
		final var item = dataOp.item();
		final var opType = op.type();
		final int size;
		try {
			size = (int) item.size();
		} catch (final IOException e) {
			concurrencyThrottle.release();
			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return false;
		}
		ByteBuffer buf = null;

		try {
			buf = rdmaTransport.allocateBuffer(size);
			final String bucket = extractBucket();
			final String key = extractKey(item);
			int status;

			if (opType == OpType.CREATE) {
				transferDataItemToBuffer((DataItem) item, buf, size);
				op.startRequest();
				status = rdmaTransport.putObject(bucket, key, buf, size);
			} else {
				// READ
				op.startRequest();
				status = rdmaTransport.getObject(bucket, key, buf, size);
			}

			op.startResponse();

			if (status >= 200 && status < 300) {
				op.status(Operation.Status.SUCC);
			} else if (status < 0 && rdmaConfig.isFallbackEnabled()) {
				// RDMA failed — release throttle and fall back to HTTP
				concurrencyThrottle.release();
				Loggers.MSG.debug("{}: RDMA failed for {}, falling back to HTTP", stepId, item.name());
				return super.submit(op);
			} else {
				Loggers.MSG.warn("{}: RDMA operation failed with status {}", stepId, status);
				op.status(Operation.Status.FAIL_IO);
			}

			op.finishResponse();
			concurrencyThrottle.release();
			handleCompleted(op);
			return true;

		} catch (final Exception e) {
			LogUtil.exception(Level.WARN, e, "{}: RDMA submit failed for {}", stepId,
							op instanceof DataOperation ? ((DataOperation) op).item().name() : op.toString());
			concurrencyThrottle.release();

			if (rdmaConfig.isFallbackEnabled()) {
				Loggers.MSG.debug("{}: falling back to HTTP after RDMA exception", stepId);
				return super.submit(op);
			}

			op.status(Operation.Status.FAIL_IO);
			try {
				op.startResponse();
			} catch (final IllegalStateException ignored) {}
			try {
				op.finishResponse();
			} catch (final IllegalStateException ignored) {}
			handleCompleted(op);
			return false;

		} finally {
			if (buf != null) {
				rdmaTransport.freeBuffer(buf);
			}
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
