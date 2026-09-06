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

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * S3 storage driver with RDMA support for high-performance data transfer.
 *
 * <p>Extends {@link S3StorageDriver} to reuse S3 authentication, listing, versioning,
 * tagging, and bucket management. Routes large data operations (PUT/GET) through
 * RDMA when available, while all other operations use the inherited HTTP/Netty path.
 *
 * <h2>Architecture</h2>
 * <p>Uses direct libibverbs for RDMA token generation. The protocol flow is:
 * <ol>
 *   <li>Client registers memory buffer with RDMA NIC</li>
 *   <li>Client generates RDMA token (addr:size:rkey:lid:dctn:g:gid)</li>
 *   <li>Client sends HTTP request with {@code x-amz-rdma-token} header</li>
 *   <li>Server performs RDMA READ (PUT) or WRITE (GET) to client memory</li>
 *   <li>Server responds with HTTP status</li>
 *   <li>Client deregisters buffer</li>
 * </ol>
 *
 * <p>See RDMA_ARCHITECTURE_V3.md for design details.
 */
public class S3RdmaStorageDriver<I extends Item, O extends Operation<I>>
				extends S3StorageDriver<I, O> {

	/** HTTP header name for RDMA token. */
	public static final String RDMA_TOKEN_HEADER = "x-amz-rdma-token";

	private static final AttributeKey<RdmaContext> RDMA_CONTEXT_ATTR_KEY = AttributeKey.newInstance("spt-rdma-attempt");

	/**
	 * Per-operation RDMA state tracked across the request/response lifecycle.
	 * Created in submitRdma(), read in httpRequest(), cleaned up in complete().
	 */
	private static final class RdmaContext {
		final String token;
		final ByteBuffer buffer;
		final long mrHandle;
		final OpType opType;
		final int size;
		volatile Channel channel;
		volatile long startTimeNanos;

		RdmaContext(final String token, final ByteBuffer buffer, final long mrHandle,
						final OpType opType, final int size) {
			this.token = token;
			this.buffer = buffer;
			this.mrHandle = mrHandle;
			this.opType = opType;
			this.size = size;
		}
	}

	/** In-flight RDMA operations keyed by Operation identity. */
	private final ConcurrentMap<Operation<?>, RdmaContext> rdmaOps = new ConcurrentHashMap<>();

	/** Log-once guard for the below-threshold warning. */
	private final AtomicBoolean belowThresholdWarned = new AtomicBoolean(false);

	/**
	 * ThreadLocal to pass the RDMA token from httpRequest() into applyMetaDataHeaders().
	 * Used because applyMetaDataHeaders() does not receive the Operation as a parameter.
	 * The ThreadLocal is only live during the synchronous httpRequest() call.
	 */
	private static final ThreadLocal<String> CURRENT_RDMA_TOKEN = new ThreadLocal<>();

	private final RdmaConfig rdmaConfig;
	private final RdmaTransport rdmaTransport;
	private final String endpointAddrs;
	private final ScheduledExecutorService rdmaReaper;

	public S3RdmaStorageDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		this(
						stepId,
						itemDataInput,
						storageConfig,
						verifyFlag,
						batchSize,
						RdmaTransport::new);
	}

	S3RdmaStorageDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final Function<RdmaConfig, RdmaTransport> transportFactory)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);

		// Parse RDMA configuration
		rdmaConfig = new RdmaConfig(storageConfig.configVal("rdma"));
		Loggers.MSG.info("{}: RDMA config: {}", stepId, rdmaConfig);

		// Build endpoint address summary from storage node config
		endpointAddrs = buildEndpointAddrs();

		// Initialize RDMA transport
		rdmaTransport = Objects.requireNonNull(
						transportFactory, "RDMA transport factory").apply(rdmaConfig);
		Objects.requireNonNull(rdmaTransport, "RDMA transport");
		boolean transportInitialized = false;
		try {
			if (rdmaConfig.isEnabled()) {
				final boolean ok = rdmaTransport.init(
								endpointAddrs, credential.getUid(), credential.getSecret());
				if (ok) {
					Loggers.MSG.info("{}: RDMA transport initialized, {} node(s): {}",
									stepId, storageNodeAddrs.length, endpointAddrs);
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

			// Start the RDMA operation timeout reaper
			if (rdmaTransport.isAvailable() && rdmaConfig.getTimeoutMs() > 0) {
				rdmaReaper = Executors.newSingleThreadScheduledExecutor(r -> {
					final Thread t = new Thread(r, stepId + "-rdma-reaper");
					t.setDaemon(true);
					return t;
				});
				final long intervalMs = Math.max(rdmaConfig.getTimeoutMs() / 2, 1000);
				rdmaReaper.scheduleAtFixedRate(this::reapTimedOutOps, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
				Loggers.MSG.info("{}: RDMA operation timeout reaper started (timeoutMs={}, intervalMs={})",
								stepId, rdmaConfig.getTimeoutMs(), intervalMs);
			} else {
				rdmaReaper = null;
			}
			transportInitialized = true;
		} finally {
			if (!transportInitialized) {
				rdmaTransport.close();
			}
		}
	}

	/**
	 * RDMA buffer registration and token creation happen in {@link #submit(Operation)}, and
	 * {@link #httpRequest} only adds the RDMA header when that context exists. Completion-driven
	 * direct dispatch sends through {@code sendRequest} without calling {@code submit}, so an
	 * operation dispatched that way would silently fall back to HTTP. Keep the dispatcher path.
	 */
	@Override
	protected boolean supportsDirectDispatch() {
		return false;
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
				if (belowThresholdWarned.compareAndSet(false, true)) {
					Loggers.MSG.warn(
									"{}: object size ({}) below RDMA threshold ({}); using HTTP path."
													+ " To force RDMA for all sizes, set storage.rdma.thresholdBytes=0",
									stepId, size, rdmaConfig.getThresholdBytes());
				}
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
	 * <p>Registers a buffer, generates an RDMA token, stores the per-operation
	 * RDMA context, then delegates to the standard HTTP pipeline. The overridden
	 * {@link #httpRequest} and {@link #applyMetaDataHeaders} methods inject the
	 * token header, and the overridden {@link #complete} method cleans up RDMA
	 * resources after the server responds.
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
			final long rawSize = item.size();
			if (rawSize > Integer.MAX_VALUE) {
				Loggers.MSG.trace("{}: RDMA skip: size={} exceeds int range, falling back to HTTP", stepId, rawSize);
				return super.submit(op);
			}
			size = (int) rawSize;
		} catch (final IOException e) {
			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return true;
		}

		ByteBuffer buf = null;
		long mrHandle = 0;
		try {
			buf = ByteBuffer.allocateDirect(size);

			mrHandle = rdmaTransport.registerBuffer(buf, size);
			if (mrHandle == 0) {
				Loggers.MSG.trace("{}: RDMA buffer registration failed, falling back to HTTP", stepId);
				return super.submit(op);
			}

			// For PUT: copy data into the registered buffer
			if (opType == OpType.CREATE) {
				transferDataItemToBuffer((DataItem) item, buf, size);
				// Mark bytes as done — sendRequestData() will be skipped for RDMA PUT
				// because httpRequest() returns a FullHttpRequest with empty body
				dataOp.countBytesDone(size);
			}

			final String token = rdmaTransport.generateToken(mrHandle, size);
			if (token == null) {
				Loggers.MSG.trace("{}: RDMA token generation failed, falling back to HTTP", stepId);
				rdmaTransport.deregisterBuffer(buf, mrHandle);
				return super.submit(op);
			}

			// Store RDMA context — httpRequest() adds the header, complete() cleans up
			rdmaOps.put(op, new RdmaContext(token, buf, mrHandle, opType, size));

			Loggers.MSG.debug("{}: RDMA submit: type={} size={} token={}", stepId, opType, size, token);

			final boolean submitted = super.submit(op);
			if (!submitted) {
				// Throttle exhausted — clean up since complete() won't be called
				cleanupRdmaContext(op);
			}
			return submitted;

		} catch (final Exception e) {
			LogUtil.exception(Level.WARN, e, "{}: RDMA submit failed for {}",
							stepId, item.name());

			// Clean up — check rdmaOps first (context may already be stored)
			if (!cleanupRdmaContext(op)) {
				if (mrHandle != 0) {
					rdmaTransport.deregisterBuffer(buf, mrHandle);
				}
			}

			if (rdmaConfig.isFallbackEnabled()) {
				Loggers.MSG.trace("{}: falling back to HTTP after RDMA exception", stepId);
				return super.submit(op);
			}

			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return true;
		}
	}

	/** Remove and clean up RDMA context for an operation. Returns true if found. */
	private boolean cleanupRdmaContext(final O op) {
		final RdmaContext ctx;
		synchronized (op) {
			ctx = rdmaOps.remove(op);
		}
		if (ctx != null) {
			rdmaTransport.deregisterBuffer(ctx.buffer, ctx.mrHandle);
			return true;
		}
		return false;
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
		if (totalRead < size) {
			throw new IOException("RDMA short read: expected " + size + " bytes but got " + totalRead);
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
	 * Build a comma-separated summary of all configured S3 endpoint addresses.
	 * Used for logging and for the non-null validation in {@link RdmaTransport#init}.
	 */
	private String buildEndpointAddrs() {
		final var sb = new StringBuilder();
		for (int i = 0; i < storageNodeAddrs.length; i++) {
			if (i > 0)
				sb.append(',');
			final String addr = storageNodeAddrs[i];
			final int colonPos = addr.lastIndexOf(':');
			final String host;
			final int port;
			if (colonPos > 0) {
				host = addr.substring(0, colonPos);
				port = Integer.parseInt(addr.substring(colonPos + 1));
			} else {
				host = addr;
				port = storageNodePort;
			}
			final String scheme = (port == 443 || port == 9021) ? "https" : "http";
			sb.append(scheme).append("://").append(host).append(':').append(port);
		}
		return sb.toString();
	}

	@Override
	protected void bindRequestChannel(final Channel channel, final O op) {
		synchronized (op) {
			final RdmaContext ctx = rdmaOps.get(op);
			channel.attr(RDMA_CONTEXT_ATTR_KEY).set(ctx);
			if (ctx != null) {
				ctx.channel = channel;
			}
		}
	}

	@Override
	protected void onRequestDispatched(final Channel channel, final O op) {
		final RdmaContext ctx = channel.attr(RDMA_CONTEXT_ATTR_KEY).get();
		if (ctx != null) {
			synchronized (op) {
				if (rdmaOps.get(op) == ctx && ctx.startTimeNanos == 0) {
					ctx.startTimeNanos = System.nanoTime();
				}
			}
		}
	}

	/**
	 * Override to inject the RDMA token header and suppress the HTTP body for PUT.
	 *
	 * <p>For RDMA PUT (CREATE): returns a {@link DefaultFullHttpRequest} with an empty body.
	 * This causes {@code sendRequest()} to skip {@code sendRequestData()}, since the server
	 * reads the data directly from client memory via RDMA READ. Content-Length is preserved
	 * from the base class so the server knows how much to read.
	 *
	 * <p>For RDMA GET (READ): returns the normal request unchanged. The server will RDMA WRITE
	 * data to the client buffer; the response body handling is in {@link #complete}.
	 */
	@Override
	protected HttpRequest httpRequest(final O op, final String nodeAddr) throws URISyntaxException {
		final RdmaContext ctx = rdmaOps.get(op);
		if (ctx != null) {
			CURRENT_RDMA_TOKEN.set(ctx.token);
		}
		try {
			final HttpRequest request = super.httpRequest(op, nodeAddr);
			if (ctx != null && ctx.opType == OpType.CREATE) {
				// Return FullHttpRequest with empty body — suppresses sendRequestData()
				return new DefaultFullHttpRequest(
								request.protocolVersion(), request.method(), request.uri(),
								Unpooled.EMPTY_BUFFER, request.headers(), EmptyHttpHeaders.INSTANCE);
			}
			return request;
		} finally {
			CURRENT_RDMA_TOKEN.remove();
		}
	}

	/**
	 * Inject the RDMA token as an HTTP header before auth signing.
	 *
	 * <p>This is called from {@code HttpStorageDriverBase.httpRequest()},
	 * before {@code applyAuthHeaders()}, ensuring the token is included
	 * in the SigV4 canonical request.
	 *
	 * <p>For RDMA PUT, Content-Length must be set to 0. The server's HTTP framework
	 * (Jetty) blocks until Content-Length bytes of body data arrive over TCP; since
	 * RDMA PUT sends no HTTP body (the server reads data via RDMA READ from the
	 * client's registered memory), a non-zero Content-Length causes the request to
	 * hang until timeout. The actual data size is conveyed in the RDMA token.
	 */
	@Override
	protected void applyMetaDataHeaders(final HttpHeaders httpHeaders) {
		super.applyMetaDataHeaders(httpHeaders);
		final String token = CURRENT_RDMA_TOKEN.get();
		if (token != null) {
			httpHeaders.set(RDMA_TOKEN_HEADER, token);
			// RDMA PUT: Content-Length must be 0 so Jetty dispatches immediately
			// instead of blocking for body bytes. Only override when the original
			// Content-Length > 0 (i.e., PUT); GET already has Content-Length 0.
			if (httpHeaders.getInt(HttpHeaderNames.CONTENT_LENGTH, 0) > 0) {
				httpHeaders.set(HttpHeaderNames.CONTENT_LENGTH, 0);
			}
		}
	}

	@Override
	protected boolean observesReadBodyOutOfBand(final O op) {
		return rdmaOps.containsKey(op) && OpType.READ.equals(op.type());
	}

	/**
	 * Clean up RDMA resources after the server responds.
	 *
	 * <p>For GET operations, the server has already RDMA-written data to our buffer
	 * by the time the HTTP response arrives, so we account for the transferred bytes.
	 */
	@Override
	public void complete(final Channel channel, final O op) {
		final RdmaContext responseContext = channel == null
						? null
						: channel.attr(RDMA_CONTEXT_ATTR_KEY).getAndSet(null);
		if (responseContext == null) {
			super.complete(channel, op);
			return;
		}
		final boolean claimed;
		synchronized (op) {
			claimed = rdmaOps.remove(op, responseContext);
		}
		if (!claimed) {
			discardOutOfBandIntegrityRead(channel);
			return;
		}
		completeRdmaContext(channel, op, responseContext);
		super.complete(channel, op);
	}

	private void completeRdmaContext(final Channel channel, final O op, final RdmaContext ctx) {
		try {
			if (op.status() == Operation.Status.SUCC && ctx.opType == OpType.READ) {
				final ByteBuffer body = ctx.buffer.asReadOnlyBuffer();
				body.position(0);
				body.limit(ctx.size);
				finishOutOfBandIntegrityRead(channel, op, body);
				((DataOperation) op).countBytesDone(ctx.size);
			}
		} finally {
			rdmaTransport.deregisterBuffer(ctx.buffer, ctx.mrHandle);
		}
	}

	/**
	 * Reap RDMA operations that have exceeded the configured timeout.
	 * Called periodically by the scheduled reaper thread.
	 */
	@SuppressWarnings("unchecked")
	private void reapTimedOutOps() {
		try {
			final long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(rdmaConfig.getTimeoutMs());
			final long now = System.nanoTime();
			int reaped = 0;
			for (final var entry : rdmaOps.entrySet()) {
				final var ctx = entry.getValue();
				if (ctx.startTimeNanos != 0 && now - ctx.startTimeNanos > timeoutNanos) {
					final Operation<?> op = entry.getKey();
					final boolean claimed;
					synchronized (op) {
						claimed = ctx.startTimeNanos != 0 && rdmaOps.remove(op, ctx);
					}
					if (claimed) {
						final long elapsedMs = TimeUnit.NANOSECONDS.toMillis(now - ctx.startTimeNanos);
						Loggers.MSG.warn("{}: RDMA operation timed out: type={} size={} elapsed={}ms status={} item={}",
										stepId, ctx.opType, ctx.size, elapsedMs, op.status(),
										(op instanceof DataOperation ? ((DataOperation) op).item().name() : "?"));
						rdmaTransport.deregisterBuffer(ctx.buffer, ctx.mrHandle);
						op.status(Operation.Status.FAIL_IO);
						discardOutOfBandIntegrityRead(ctx.channel);
						super.complete(ctx.channel, (O) op);
						reaped++;
					}
				}
			}
			if (reaped > 0) {
				Loggers.MSG.warn("{}: RDMA reaper cleaned up {} timed-out operations ({} still in-flight)",
								stepId, reaped, rdmaOps.size());
			}
		} catch (final Exception e) {
			Loggers.MSG.warn("{}: RDMA reaper exception: {}", stepId, e.getMessage());
		}
	}

	@Override
	protected void doClose() throws IOException {
		// Stop the reaper and wait for it to finish — prevents race where the reaper
		// calls deregisterBuffer() on a transport that close() has already destroyed
		if (rdmaReaper != null) {
			rdmaReaper.shutdownNow();
			try {
				if (!rdmaReaper.awaitTermination(5, TimeUnit.SECONDS)) {
					Loggers.MSG.warn("{}: RDMA reaper did not terminate within 5s", stepId);
				}
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		// Drain any leaked RDMA contexts (e.g. from interrupted operations).
		// Use remove(op, ctx) to atomically claim each entry — prevents double-free
		// if a concurrent complete() is still draining the last in-flight operations.
		for (final var it = rdmaOps.entrySet().iterator(); it.hasNext();) {
			final var entry = it.next();
			final var ctx = entry.getValue();
			if (rdmaOps.remove(entry.getKey(), ctx)) {
				rdmaTransport.deregisterBuffer(ctx.buffer, ctx.mrHandle);
			}
		}
		rdmaTransport.close();
		super.doClose();
	}
}
