package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver;
import com.github.akurilov.confuse.Config;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.AttributeKey;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpStatusClass;

import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.IntFunction;

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

	/** Pipeline name of the handler that records the RDMA reply ahead of S3 response handling. */
	static final String RDMA_REPLY_HANDLER_NAME = "spt-rdma-reply";

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
		// Response fields are written and read on the channel's event loop.
		int reply = RdmaReplyContract.REPLY_ABSENT;
		long bytesTransferred = RdmaReplyContract.VALUE_ABSENT;
		long responseContentLength = RdmaReplyContract.VALUE_ABSENT;
		long responseBodyBytes;
		/**
		 * The server's final (non-1xx) response was received, so it no longer accesses the buffer.
		 * An informational response does not end the request.
		 */
		boolean finalResponseObserved;
		/** Pool buffer backing this request, or {@code null} for a per-operation buffer. */
		RdmaBufferPool.PooledBuffer pooled;
		/** Stale content was invalidated; the buffer is reusable only after a verified fill. */
		boolean contentInvalidated;

		RdmaContext(final String token, final ByteBuffer buffer, final long mrHandle,
						final OpType opType, final int size) {
			this.token = token;
			this.buffer = buffer;
			this.mrHandle = mrHandle;
			this.opType = opType;
			this.size = size;
		}

		void observeResponse(final HttpResponse response) {
			if (response.status().codeClass() == HttpStatusClass.INFORMATIONAL) {
				return;
			}
			finalResponseObserved = true;
			final HttpHeaders headers = response.headers();
			reply = RdmaReplyContract.parseReply(headers.get(RdmaReplyContract.REPLY_HEADER));
			bytesTransferred = RdmaReplyContract.parseCount(
							headers.get(RdmaReplyContract.BYTES_TRANSFERRED_HEADER));
			responseContentLength = RdmaReplyContract.parseCount(headers.get(HttpHeaderNames.CONTENT_LENGTH));
		}

		/** HTTP body size, including chunked bodies that carry no Content-Length. */
		long bodyBytes() {
			return Math.max(responseContentLength, responseBodyBytes);
		}

		boolean replyAcceptedRdma() {
			return reply == RdmaReplyContract.REPLY_OK
							|| reply == RdmaReplyContract.REPLY_NO_CONTENT
							|| reply == RdmaReplyContract.REPLY_PARTIAL_CONTENT;
		}
	}

	/**
	 * Records the RDMA reply headers and the HTTP body size into the channel's {@link RdmaContext}
	 * before the S3 response handler runs, so body routing and completion can follow what the
	 * server actually did.
	 */
	@ChannelHandler.Sharable
	private static final class RdmaReplyObserver extends ChannelInboundHandlerAdapter {
		static final RdmaReplyObserver INSTANCE = new RdmaReplyObserver();

		@Override
		public void channelRead(final ChannelHandlerContext handlerContext, final Object msg) {
			if (msg instanceof HttpResponse || msg instanceof HttpContent) {
				final RdmaContext ctx = handlerContext.channel().attr(RDMA_CONTEXT_ATTR_KEY).get();
				if (ctx != null) {
					if (msg instanceof HttpResponse response) {
						ctx.observeResponse(response);
					}
					if (msg instanceof HttpContent content) {
						ctx.responseBodyBytes += content.content().readableBytes();
					}
				}
			}
			handlerContext.fireChannelRead(msg);
		}
	}

	/** In-flight RDMA operations keyed by Operation identity. */
	private final ConcurrentMap<Operation<?>, RdmaContext> rdmaOps = new ConcurrentHashMap<>();

	/** Log-once guard for the below-threshold warning. */
	private final AtomicBoolean belowThresholdWarned = new AtomicBoolean(false);
	private final AtomicBoolean oversizeWarned = new AtomicBoolean(false);
	private final AtomicBoolean prepareFailureWarned = new AtomicBoolean(false);
	private final AtomicBoolean declinedWarned = new AtomicBoolean(false);
	private final AtomicBoolean protocolErrorWarned = new AtomicBoolean(false);
	private final AtomicBoolean bytesAssumedWarned = new AtomicBoolean(false);

	private final RdmaPathStats pathStats = new RdmaPathStats();

	/** Spacing of the bytes altered in a reused GET buffer; one per page. */
	static final int STALE_CONTENT_STRIDE_BYTES = 4096;

	/** Bound used when the driver's concurrency is unlimited. */
	private static final int UNLIMITED_CONCURRENCY_POOL_LIMIT = 256;

	/** Reused registered buffers, or {@code null} when storage.rdma.bufferPool is disabled. */
	private final RdmaBufferPool bufferPool;

	/** Allocates per-operation buffers; replaced only by tests to simulate exhausted direct memory. */
	IntFunction<ByteBuffer> directAllocator = ByteBuffer::allocateDirect;

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

		// Build endpoint address summary from storage node config
		endpointAddrs = buildEndpointAddrs();

		// Parse RDMA configuration; without an explicit local address, use the one routed to the
		// storage endpoint so each worker host selects its own RoCE GID.
		// An explicit device keeps native device selection authoritative: the native layer binds
		// by address first and would otherwise use whichever NIC carries the routed address.
		final RdmaConfig configured = new RdmaConfig(storageConfig.configVal("rdma"));
		rdmaConfig = configured.isEnabled() && configured.getLocalIp().isEmpty() && !configured.hasExplicitDevice()
						? withRoutedLocalIp(configured)
						: configured;
		Loggers.MSG.info("{}: RDMA config: {}", stepId, rdmaConfig);

		// Initialize RDMA transport
		rdmaTransport = Objects.requireNonNull(
						transportFactory, "RDMA transport factory").apply(rdmaConfig);
		Objects.requireNonNull(rdmaTransport, "RDMA transport");
		// In-flight RDMA operations are bounded by the concurrency throttle; one extra buffer per
		// size class covers an operation prepared just before the throttle rejects it.
		bufferPool = rdmaConfig.isBufferPoolEnabled()
						? new RdmaBufferPool(rdmaTransport,
										concurrencyLimit > 0 ? concurrencyLimit + 1 : UNLIMITED_CONCURRENCY_POOL_LIMIT,
										RdmaBufferPool.defaultMaxPooledBytes())
						: null;
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
				// The task handles its own exceptions and ends with shutdownNow() in doClose().
				final ScheduledFuture<?> unused = rdmaReaper.scheduleAtFixedRate(
								this::reapTimedOutOps, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
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
		if (op instanceof CompositeOperation) {
			// Multipart initiate/complete requests carry no payload; their parts carry the data.
			return false;
		}
		final var dataOp = (DataOperation) op;
		if (opType == OpType.READ && (op instanceof PartialOperation || isRangedRead(dataOp))) {
			// Ranged reads are not proposed for RDMA: the token and buffer describe the whole object.
			pathStats.httpIneligible.increment();
			return false;
		}
		try {
			final long size = dataOp.item().size();
			final boolean useRdma = size >= rdmaConfig.getThresholdBytes();
			if (!useRdma) {
				pathStats.httpBelowThreshold.increment();
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

	private static boolean isRangedRead(final DataOperation dataOp) {
		final var fixedRanges = dataOp.fixedRanges();
		return (fixedRanges != null && !fixedRanges.isEmpty()) || dataOp.randomRangesCount() > 0;
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
				pathStats.httpOversize.increment();
				if (oversizeWarned.compareAndSet(false, true)) {
					Loggers.MSG.warn("{}: object size ({}) exceeds the largest RDMA buffer ({}); using HTTP path",
									stepId, rawSize, Integer.MAX_VALUE);
				}
				return super.submit(op);
			}
			size = (int) rawSize;
		} catch (final IOException e) {
			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return true;
		}

		// Until the context is stored, this method owns the buffer; afterwards the context does.
		ByteBuffer buf = null;
		long mrHandle = 0;
		RdmaBufferPool.PooledBuffer pooled = null;
		boolean invalidated = false;
		try {
			pooled = bufferPool == null ? null : bufferPool.acquire(size);
			if (pooled != null) {
				buf = pooled.buffer();
				mrHandle = pooled.mrHandle();
				// Integrity verification is the only check of RDMA-delivered GET content.
				if (opType == OpType.READ && integrityMetadataEnabled()) {
					invalidateStaleContent(buf, size);
					invalidated = true;
				}
			} else {
				buf = RdmaBufferPool.allocateUnpooled(size, bufferPool, directAllocator);
				if (buf == null) {
					return prepareFailed(op, "direct memory exhausted");
				}
				mrHandle = rdmaTransport.registerBuffer(buf, size);
				if (mrHandle == 0) {
					return prepareFailed(op, "buffer registration failed");
				}
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
				// Nothing was sent, so the buffer is reusable unless its content was invalidated.
				releaseBuffer(pooled, buf, mrHandle, !invalidated);
				pooled = null;
				mrHandle = 0;
				return prepareFailed(op, "token generation failed");
			}

			// Store RDMA context — httpRequest() adds the header, complete() cleans up
			final RdmaContext ctx = new RdmaContext(token, buf, mrHandle, opType, size);
			ctx.pooled = pooled;
			ctx.contentInvalidated = invalidated;
			rdmaOps.put(op, ctx);
			pooled = null;
			mrHandle = 0;

			Loggers.MSG.debug("{}: RDMA submit: type={} size={} token={}", stepId, opType, size, token);

			final boolean submitted = super.submit(op);
			if (!submitted) {
				// Throttle exhausted: the request was not sent and complete() won't be called.
				cleanupRdmaContext(op, true);
			}
			return submitted;

		} catch (final Exception e) {
			LogUtil.exception(Level.DEBUG, e, "{}: RDMA submit failed for {}",
							stepId, item.name());

			// The request may have been dispatched, so the buffer is not reused. A stored context
			// owns the buffer; if it was already completed, there is nothing left to clean up.
			if (!cleanupRdmaContext(op, false)) {
				releaseBuffer(pooled, buf, mrHandle, false);
			}

			return prepareFailed(op, e.getClass().getSimpleName());
		}
	}

	/**
	 * Handles an RDMA preparation failure: with fallback enabled the operation is sent over HTTP,
	 * otherwise it fails rather than being reported as an RDMA transfer.
	 */
	private boolean prepareFailed(final O op, final String reason) {
		final boolean fallback = rdmaConfig.isFallbackEnabled();
		if (prepareFailureWarned.compareAndSet(false, true)) {
			Loggers.MSG.warn("{}: RDMA preparation failed ({}); {}", stepId, reason,
							fallback ? "falling back to HTTP (storage.rdma.fallback=true)"
											: "failing the operation (storage.rdma.fallback=false)");
		}
		if (fallback) {
			pathStats.httpFallback.increment();
			return super.submit(op);
		}
		pathStats.prepareFailed.increment();
		op.status(Operation.Status.FAIL_IO);
		// This completion bypasses finishResponse(), so the part is settled here.
		markPartSettled(op);
		handleCompleted(op);
		return true;
	}

	/**
	 * Marks a multipart part's sub-task complete on its parent.
	 *
	 * <p>A part is normally settled by {@code PartialDataOperationImpl.finishResponse()}, and a part
	 * retry undoes that mark. Completion paths that do not reach that settlement must call this
	 * exactly once so retries and finalization keep the parent's pending count balanced.
	 */
	static void markPartSettled(final Operation<?> op) {
		if (op instanceof PartialOperation<?> part) {
			part.parent().markSubTaskCompleted();
		}
	}

	/** Remove and clean up RDMA context for an operation. Returns true if found. */
	private boolean cleanupRdmaContext(final O op, final boolean reusable) {
		final RdmaContext ctx;
		synchronized (op) {
			ctx = rdmaOps.remove(op);
		}
		if (ctx != null) {
			releaseBuffer(ctx.pooled, ctx.buffer, ctx.mrHandle, reusable && !ctx.contentInvalidated);
			return true;
		}
		return false;
	}

	/**
	 * Ends a request's use of its buffer. A pool buffer is reused only when {@code reusable}:
	 * the request was never sent, or the server's response was received. Otherwise the server
	 * might still access it, so it is deregistered and dropped.
	 */
	private void releaseBuffer(
					final RdmaBufferPool.PooledBuffer pooled, final ByteBuffer buffer, final long mrHandle,
					final boolean reusable) {
		if (pooled != null) {
			if (reusable) {
				bufferPool.release(pooled);
			} else {
				bufferPool.discard(pooled);
			}
		} else if (mrHandle != 0) {
			rdmaTransport.deregisterBuffer(buffer, mrHandle);
		}
	}

	/**
	 * Alters a reused buffer at every page so a read the server reports but does not write cannot
	 * pass verification on content left by an earlier read of the same object.
	 */
	static void invalidateStaleContent(final ByteBuffer buffer, final int size) {
		for (long i = 0; i < size; i += STALE_CONTENT_STRIDE_BYTES) {
			buffer.put((int) i, (byte) ~buffer.get((int) i));
		}
		final int last = size - 1;
		if (last > 0 && last % STALE_CONTENT_STRIDE_BYTES != 0) {
			buffer.put(last, (byte) ~buffer.get(last));
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
		if (totalRead < size) {
			throw new IOException("RDMA short read: expected " + size + " bytes but got " + totalRead);
		}
		dst.flip();
	}

	private RdmaConfig withRoutedLocalIp(final RdmaConfig configured) {
		if (storageNodeAddrs.length == 0) {
			return configured;
		}
		final String addr = storageNodeAddrs[0];
		final int colonPos = addr.lastIndexOf(':');
		final String host = colonPos > 0 ? addr.substring(0, colonPos) : addr;
		final int port = colonPos > 0 ? Integer.parseInt(addr.substring(colonPos + 1)) : storageNodePort;
		final String localIp = routedLocalAddress(host, port);
		if (localIp.isEmpty()) {
			Loggers.MSG.info("{}: RDMA local address not resolved by route to {}; native GID selection applies",
							stepId, host);
			return configured;
		}
		Loggers.MSG.info("{}: RDMA local address {} selected by route to {}", stepId, localIp, host);
		return configured.withLocalIp(localIp);
	}

	/**
	 * Returns the local IPv4 address the OS routes toward {@code host}, or an empty string when
	 * it cannot be determined or is a loopback/wildcard address. Connecting a UDP socket only
	 * performs the route lookup; no packet is sent.
	 */
	static String routedLocalAddress(final String host, final int port) {
		try (final DatagramSocket socket = new DatagramSocket()) {
			socket.connect(InetAddress.getByName(host), port);
			final InetAddress local = socket.getLocalAddress();
			if (local instanceof Inet4Address && !local.isLoopbackAddress() && !local.isAnyLocalAddress()) {
				return local.getHostAddress();
			}
		} catch (final IOException | RuntimeException e) {
			Loggers.MSG.debug("RDMA local address route lookup to {} failed: {}", host, e.getMessage());
		}
		return "";
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
	protected void appendHandlers(final Channel channel) {
		super.appendHandlers(channel);
		final ChannelPipeline pipeline = channel.pipeline();
		pipeline.addBefore(pipeline.lastContext().name(), RDMA_REPLY_HANDLER_NAME, RdmaReplyObserver.INSTANCE);
	}

	/**
	 * A GET body is out of band only when the server reported an RDMA transfer; a declined GET
	 * carries the object in the HTTP body and is verified in band like any HTTP read.
	 */
	@Override
	protected boolean observesReadBodyOutOfBand(final O op) {
		if (!OpType.READ.equals(op.type())) {
			return false;
		}
		final RdmaContext ctx = rdmaOps.get(op);
		return ctx != null && ctx.replyAcceptedRdma();
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
		boolean verifiedFill = false;
		try {
			if (op.status() != Operation.Status.SUCC) {
				pathStats.rdmaHttpError.increment();
				return;
			}
			final RdmaReplyContract.Result result = RdmaReplyContract.classify(
							ctx.opType, ctx.reply, ctx.bytesTransferred, ctx.bodyBytes(), ctx.size,
							rdmaConfig.isAllowMissingBytesHeader());
			final var dataOp = (DataOperation) op;
			switch (result.outcome()) {
			case TRANSFERRED -> {
				pathStats.rdmaTransferred.increment();
				if (result.bytesAssumed()) {
					pathStats.rdmaBytesAssumed.increment();
					if (bytesAssumedWarned.compareAndSet(false, true)) {
						Loggers.MSG.warn("{}: RDMA GET success without x-amz-rdma-bytes-transferred; counting the "
										+ "requested size (storage.rdma.allowMissingBytesHeader=true)", stepId);
					}
				}
				if (ctx.opType == OpType.READ) {
					final ByteBuffer body = ctx.buffer.asReadOnlyBuffer();
					body.clear();
					body.limit((int) result.bytes());
					final IntegrityVerificationResult prior = op.integrityVerificationResult();
					finishOutOfBandIntegrityRead(channel, op, body);
					// Only a result produced by this read counts.
					final IntegrityVerificationResult verification = op.integrityVerificationResult();
					verifiedFill = verification != null && verification != prior && verification.verified()
									&& op.status() == Operation.Status.SUCC;
					dataOp.countBytesDone(result.bytes());
				}
			}
			case DECLINED -> {
				pathStats.rdmaDeclined.increment();
				// A declined PUT stored nothing. A declined GET delivered its body over HTTP,
				// which counts only when HTTP fallback is allowed.
				final boolean fail = ctx.opType == OpType.CREATE || !rdmaConfig.isFallbackEnabled();
				if (declinedWarned.compareAndSet(false, true)) {
					Loggers.MSG.warn("{}: server declined RDMA ({}) for {}; {}", stepId, result.reason(),
									ctx.opType, fail ? "operations fail"
													: "GET bodies arrive over HTTP (storage.rdma.fallback=true)");
				}
				if (fail) {
					op.status(Operation.Status.RESP_FAIL_SVC);
					dataOp.countBytesDone(0);
				}
			}
			case PROTOCOL_ERROR -> {
				pathStats.rdmaProtocolError.increment();
				if (protocolErrorWarned.compareAndSet(false, true)) {
					Loggers.MSG.warn("{}: RDMA reply contract violated for {}: {}",
									stepId, ctx.opType, result.reason());
				}
				discardOutOfBandIntegrityRead(channel);
				op.status(Operation.Status.RESP_FAIL_CORRUPT);
				dataOp.countBytesDone(0);
			}
			}
		} finally {
			// An invalidated buffer holds altered stale content unless the server wrote a payload
			// that verified; reusing it otherwise would let a second invalidation restore the content.
			releaseBuffer(ctx.pooled, ctx.buffer, ctx.mrHandle,
							ctx.finalResponseObserved && (!ctx.contentInvalidated || verifiedFill));
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
						// The server may still access a timed-out request's buffer.
						releaseBuffer(ctx.pooled, ctx.buffer, ctx.mrHandle, false);
						pathStats.rdmaTimedOut.increment();
						op.status(Operation.Status.FAIL_IO);
						// finishResponse() throws before settling a part whose response never
						// started; it settles it otherwise (timing may carry over from a retry).
						if (op.respTimeStart() == 0) {
							markPartSettled(op);
						}
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
				releaseBuffer(ctx.pooled, ctx.buffer, ctx.mrHandle, false);
			}
		}
		if (bufferPool != null) {
			bufferPool.close();
		}
		Loggers.MSG.info("{}: RDMA data path summary (transport {}): {}{}", stepId,
						rdmaTransport.isAvailable() ? "available" : "unavailable", pathStats.summary(),
						bufferPool == null ? ", bufferPool=disabled" : ", " + bufferPool.summary());
		rdmaTransport.close();
		super.doClose();
	}

	RdmaPathStats pathStats() {
		return pathStats;
	}

	RdmaBufferPool bufferPool() {
		return bufferPool;
	}
}
