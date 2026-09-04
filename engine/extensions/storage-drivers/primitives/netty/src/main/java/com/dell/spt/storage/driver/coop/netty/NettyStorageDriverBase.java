package com.dell.spt.storage.driver.coop.netty;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.item.DataItem.rangeCount;
import static com.dell.spt.base.item.op.Operation.Status.SUCC;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.util.BinarySizeFormat;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.dell.spt.storage.driver.coop.netty.data.DataItemFileRegion;
import com.dell.spt.storage.driver.coop.netty.data.SeekableByteChannelChunkedNioStream;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import com.github.akurilov.netty.connection.pool.MultiNodeConnPoolImpl;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.handler.timeout.IdleStateHandler;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.security.Provider;
import java.security.Security;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

/** Created by kurila on 30.09.16. */
public abstract class NettyStorageDriverBase<I extends Item, O extends Operation<I>>
				extends CoopStorageDriverBase<I, O> implements NettyStorageDriver<I, O>, ChannelPoolHandler {

	private static final String CLS_NAME = NettyStorageDriverBase.class.getSimpleName();
	private static final String BC_JSSE_PROVIDER_NAME = "BCJSSE";
	private static final String BC_JSSE_PROVIDER_CLASS = "org.bouncycastle.jsse.provider.BouncyCastleJsseProvider";
	private static final String BC_JCE_PROVIDER_CLASS = "org.bouncycastle.jce.provider.BouncyCastleProvider";

	static final long IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS = 5;
	static {
		final java.util.logging.Logger julConnPoolLogger = java.util.logging.Logger.getLogger(MultiNodeConnPoolImpl.class.getName());
		julConnPoolLogger.setLevel(java.util.logging.Level.WARNING);
		for (final java.util.logging.Handler handler : julConnPoolLogger.getHandlers()) {
			handler.setLevel(java.util.logging.Level.WARNING);
		}
	}

	private final EventLoopGroup ioExecutor;
	protected final String storageNodeAddrs[];
	protected final Bootstrap bootstrap;
	protected final int storageNodePort;
	protected final int connAttemptsLimit;
	protected final int netTimeoutMilliSec;
	private final Class<SocketChannel> socketChannelCls;
	private final NonBlockingConnPool connPool;
	private final boolean sslFlag;
	private final SslContext sslCtx;
	private final String[] sslNamedGroups;
	private final String sslPqcMode;
	private final AtomicBoolean namedGroupsWarned = new AtomicBoolean(false);
	private final AtomicBoolean tlsHandshakeLogged = new AtomicBoolean(false);
	private final AtomicBoolean channelFailureWarned = new AtomicBoolean(false);
	private final AtomicBoolean connectionLeaseFailureWarned = new AtomicBoolean(false);
	private final AtomicBoolean submissionFailureWarned = new AtomicBoolean(false);
	protected final ChannelFutureListener reqSentCallback = this::sendFullRequestComplete;

	@SuppressWarnings("unchecked")
	protected NettyStorageDriverBase(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {

		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);

		final var netConfig = storageConfig.configVal("net");
		final var sslConfig = netConfig.configVal("ssl");
		sslFlag = sslConfig.boolVal("enabled");
		if (sslFlag) {
			final var protocols = sslConfig.<String> listVal("protocols");
			Loggers.MSG.info("{}: SSL/TLS protocols: {}", stepId, String.join(", ", protocols));
			final var userCiphers = sslConfig.<String> listVal("ciphers");
			final var providerName = sslConfig.stringVal("provider");
			final var provider = SslProvider.valueOf(providerName);
			sslPqcMode = normalizedPqcMode(sslStringVal(sslConfig, "pqcMode", "off"));
			final var jsseProviderName = sslStringVal(sslConfig, "jsseProvider", null);
			sslNamedGroups = sslListVal(sslConfig, "namedGroups").toArray(new String[]{});
			Loggers.MSG.info("{}: SSL/TLS provider: {}", stepId, providerName);
			if (sslNamedGroups.length > 0) {
				Loggers.MSG.info("{}: SSL/TLS named groups: {}", stepId, String.join(", ", sslNamedGroups));
			}
			try {
				final var sslBuilder = SslContextBuilder
								.forClient()
								.trustManager(InsecureTrustManagerFactory.INSTANCE)
								.sslProvider(provider)
								.protocols(protocols.toArray(new String[]{}))
								.ciphers(userCiphers);
				if (SslProvider.JDK.equals(provider) && !"off".equals(sslPqcMode)) {
					final var jsseProvider = resolveJsseProvider(jsseProviderName);
					if (jsseProvider == null) {
						if ("require".equals(sslPqcMode)) {
							throw new IllegalConfigurationException(
											"PQC TLS provider \"" + jsseProviderName + "\" is unavailable in require mode");
						}
						Loggers.MSG.warn(
										"{}: PQC TLS provider \"{}\" is unavailable; falling back to default JSSE provider",
										stepId,
										jsseProviderName);
					} else {
						sslBuilder.sslContextProvider(jsseProvider);
						Loggers.MSG.info("{}: SSL/TLS JSSE provider: {}", stepId, jsseProvider.getName());
					}
				}
				sslCtx = sslBuilder.build();
				Loggers.MSG.info("{}: SSL/TLS cipher suites: {}", stepId, sslCtx.cipherSuites());
				Loggers.MSG.info("{}: SSL/TLS PQC mode: {}", stepId, sslPqcMode);
			} catch (final SSLException e) {
				throw new IllegalConfigurationException("Failed to build the SSL context", e);
			}
		} else {
			sslCtx = null;
			sslNamedGroups = new String[]{};
			sslPqcMode = "off";
		}
		final var sto = netConfig.intVal("timeoutMilliSec");
		if (sto > 0) {
			this.netTimeoutMilliSec = sto;
		} else {
			this.netTimeoutMilliSec = Integer.MAX_VALUE;
		}
		final var nodeConfig = netConfig.configVal("node");
		storageNodePort = nodeConfig.intVal("port");
		connAttemptsLimit = nodeConfig.intVal("connAttemptsLimit");
		final String t[] = nodeConfig.<String> listVal("addrs").toArray(new String[]{});
		storageNodeAddrs = new String[t.length];
		String n;
		for (var i = 0; i < t.length; i++) {
			n = t[i];
			storageNodeAddrs[i] = n + (n.contains(":") ? "" : ":" + storageNodePort);
		}

		final int workerCount;
		final var confWorkerCount = storageConfig.intVal("driver-threads");
		if (confWorkerCount < 1) {
			workerCount = Math.max(4, ThreadUtil.getHardwareThreadCount() / 8);
		} else {
			workerCount = confWorkerCount;
		}

		final Transport transportKey;
		final var transportConfig = netConfig.stringVal("transport");
		if (transportConfig == null || transportConfig.isEmpty()) {
			if (Epoll.isAvailable()) {
				transportKey = Transport.EPOLL;
			} else if (KQueue.isAvailable()) {
				transportKey = Transport.KQUEUE;
			} else {
				transportKey = Transport.NIO;
			}
		} else {
			transportKey = Transport.valueOf(transportConfig.toUpperCase(Locale.ROOT));
		}
		Loggers.MSG.info("{}: netty transport: {}", toString(), transportKey);

		try {

			final var ioExecutorClsName = IO_EXECUTOR_IMPLS.get(transportKey);
			final var transportCls = (Class<EventLoopGroup>) Class.forName(ioExecutorClsName);
			ioExecutor = transportCls
							.getConstructor(Integer.TYPE, ThreadFactory.class)
							.newInstance(workerCount, new LogContextThreadFactory("ioWorker", true));
			Loggers.MSG.info("{}: use {} I/O workers", toString(), workerCount);

			final var ioRatio = netConfig.intVal("ioRatio");
			try {
				final var setIoRatioMethod = transportCls.getMethod("setIoRatio", Integer.TYPE);
				setIoRatioMethod.invoke(ioExecutor, ioRatio);
			} catch (final ReflectiveOperationException e) {
				LogUtil.exception(Level.ERROR, e, "Failed to set the I/O ratio");
			}

		} catch (final ReflectiveOperationException e) {
			throw new AssertionError(e);
		}

		final var socketChannelClsName = SOCKET_CHANNEL_IMPLS.get(transportKey);
		try {
			socketChannelCls = (Class<SocketChannel>) Class.forName(socketChannelClsName);
		} catch (final ReflectiveOperationException e) {
			throw new AssertionError(e);
		}

		bootstrap = new Bootstrap().group(ioExecutor).channel(socketChannelCls);
		bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		// bootstrap.option(ChannelOption.ALLOW_HALF_CLOSURE)
		// bootstrap.option(ChannelOption.RCVBUF_ALLOCATOR, )
		// bootstrap.option(ChannelOption.MESSAGE_SIZE_ESTIMATOR)
		// bootstrap.option(ChannelOption.AUTO_READ)
		bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, netConfig.intVal("timeoutMilliSec"));
		bootstrap.option(ChannelOption.WRITE_SPIN_COUNT, netConfig.intVal("writeSpinCount"));
		int size = netConfig.intVal("rcvBuf");
		if (size > 0) {
			bootstrap.option(ChannelOption.SO_RCVBUF, size);
		}
		size = netConfig.intVal("sndBuf");
		if (size > 0) {
			bootstrap.option(ChannelOption.SO_SNDBUF, size);
		}
		// bootstrap.option(ChannelOption.SO_BACKLOG, netConfig.getBindBacklogSize());
		bootstrap.option(ChannelOption.SO_KEEPALIVE, netConfig.boolVal("keepAlive"));
		bootstrap.option(ChannelOption.SO_LINGER, netConfig.intVal("linger"));
		bootstrap.option(ChannelOption.SO_REUSEADDR, netConfig.boolVal("reuseAddr"));
		bootstrap.option(ChannelOption.TCP_NODELAY, netConfig.boolVal("tcpNoDelay"));
		try (final var logCtx = CloseableThreadContext.put(KEY_STEP_ID, this.stepId).put(KEY_CLASS_NAME, CLS_NAME)) {
			connPool = createConnectionPool();
		}
	}

	protected NonBlockingConnPool createConnectionPool() {
		return new MultiNodeConnPoolImpl(
						storageNodeAddrs,
						bootstrap,
						this,
						storageNodePort,
						connAttemptsLimit,
						netTimeoutMilliSec,
						TimeUnit.MILLISECONDS);
	}

	final boolean shouldLogChannelFailureWarning() {
		return channelFailureWarned.compareAndSet(false, true);
	}

	@Override
	public final void adjustIoBuffers(final long avgTransferSize, final OpType opType) {
		final int size;
		try (final var logCtx = CloseableThreadContext.put(KEY_STEP_ID, stepId).put(KEY_CLASS_NAME, CLS_NAME)) {
			if (avgTransferSize < BUFF_SIZE_MIN) {
				size = BUFF_SIZE_MIN;
			} else if (BUFF_SIZE_MAX < avgTransferSize) {
				size = BUFF_SIZE_MAX;
			} else {
				size = (int) avgTransferSize;
			}
			if (OpType.CREATE.equals(opType)) {
				Loggers.MSG.info("Adjust output buffer size: {}", BinarySizeFormat.formatFixedSize(size));
				bootstrap.option(ChannelOption.SO_RCVBUF, BUFF_SIZE_MIN);
				bootstrap.option(ChannelOption.SO_SNDBUF, size);
			} else if (OpType.READ.equals(opType)) {
				Loggers.MSG.info("Adjust input buffer size: {}", BinarySizeFormat.formatFixedSize(size));
				bootstrap.option(ChannelOption.SO_RCVBUF, size);
				bootstrap.option(ChannelOption.SO_SNDBUF, BUFF_SIZE_MIN);
			} else if (OpType.NOOP.equals(opType)) {
				Loggers.MSG.info("Adjust I/O buffer sizes: {}", BinarySizeFormat.formatFixedSize(size));
				bootstrap.option(ChannelOption.SO_RCVBUF, size);
				bootstrap.option(ChannelOption.SO_SNDBUF, size);
			} else {
				bootstrap.option(ChannelOption.SO_RCVBUF, BUFF_SIZE_MIN);
				bootstrap.option(ChannelOption.SO_SNDBUF, BUFF_SIZE_MIN);
			}
		}
	}

	protected Channel getUnpooledConnection(final String storageNodeAddr, final int storageNodePort)
					throws ConnectException, InterruptedException {

		final InetSocketAddress socketAddr;
		if (storageNodeAddr.contains(":")) {
			final String addrParts[] = storageNodeAddr.split(":");
			socketAddr = new InetSocketAddress(addrParts[0], Integer.parseInt(addrParts[1]));
		} else {
			socketAddr = new InetSocketAddress(storageNodeAddr, storageNodePort);
		}

		final Bootstrap bootstrap = new Bootstrap()
						.group(ioExecutor)
						.channel(socketChannelCls)
						.handler(
										new ChannelInitializer<SocketChannel>() {
											@Override
											protected final void initChannel(final SocketChannel conn) throws Exception {
												try (final var logCtx = CloseableThreadContext.put(KEY_STEP_ID, stepId)
																.put(KEY_CLASS_NAME, CLS_NAME)) {
													appendHandlers(conn);
													Loggers.MSG.debug(
																	"{}: new unpooled connection {}, pipeline: {}",
																	stepId,
																	conn.hashCode(),
																	conn.pipeline());
												}
											}
										});

		final Channel conn;
		final var connFuture = bootstrap.connect(socketAddr);
		if (netTimeoutMilliSec > 0) {
			if (connFuture.await(netTimeoutMilliSec, TimeUnit.MILLISECONDS)) {
				conn = connFuture.channel();
			} else {
				throw new ConnectTimeoutException();
			}
		} else {
			conn = connFuture.sync().channel();
		}
		return conn;
	}

	@Override
	protected void doStart() throws IllegalStateException {
		super.doStart();
		if (concurrencyLimit > 0) {
			try {
				connPool.preConnect(concurrencyLimit);
			} catch (final ConnectException e) {
				LogUtil.exception(Level.WARN, e, "Failed to pre-create the connections");
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
		}
	}

	private Channel leaseActiveConnection() throws ConnectException {
		while (true) {
			final Channel conn = connPool.lease();
			if (conn.isActive()) {
				conn.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
				return conn;
			}
			conn.close();
			connPool.release(conn);
		}
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {

		ThreadContext.put(KEY_STEP_ID, stepId);
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);

		Channel conn = null;
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		if (concurrencyThrottle.tryAcquire()) {
			var dispatched = false;
			try {
				if (OpType.NOOP.equals(op.type())) {
					if (!beginDispatch(op)) {
						concurrencyThrottle.release();
						return false;
					}
					dispatched = true;
					op.startRequest();
					sendRequest(null, op);
					op.finishRequest();
					concurrencyThrottle.release();
					op.status(SUCC);
					op.startResponse();
					completeAfterSubmissionSafely(null, op);
				} else {
					if (!beginDispatch(op)) {
						concurrencyThrottle.release();
						return false;
					}
					dispatched = true;
					conn = leaseActiveConnection();
					conn.attr(ATTR_KEY_OPERATION).set(op);
					op.nodeAddr(conn.attr(ATTR_KEY_NODE).get());
					op.startRequest();
					sendRequest(conn, op);
				}
			} catch (final ConnectException e) {
				logConnectionLeaseFailure(e);
				if (!dispatched) {
					concurrencyThrottle.release();
					op.status(Operation.Status.FAIL_IO);
					completeAfterSubmissionSafely(null, op);
					return false;
				}
				op.status(Operation.Status.FAIL_IO);
				if (conn == null) {
					concurrencyThrottle.release();
				}
				completeAfterSubmissionSafely(conn, op);
				return false;
			} catch (final Throwable thrown) {
				throwUncheckedIfInterrupted(thrown);
				logSubmissionFailure(thrown);
				if (!dispatched) {
					if (conn == null) {
						concurrencyThrottle.release();
					} else {
						releaseUndispatchedConnection(conn, true);
					}
					return false;
				}
				op.status(Operation.Status.FAIL_UNKNOWN);
				if (conn == null) {
					concurrencyThrottle.release();
				}
				completeAfterSubmissionSafely(conn, op);
				return false;
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	protected int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException {

		if (ops.size() == 0) {
			return 0;
		}
		final var needed = to - from;
		if (needed == 0) {
			return 0;
		}
		var permits = concurrencyThrottle.drainPermits();
		if (permits == 0) {
			return 0;
		}
		if (permits > needed) {
			concurrencyThrottle.release(permits - needed);
			permits = needed;
		}

		ThreadContext.put(KEY_STEP_ID, stepId);
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);

		Channel conn = null;
		O nextOp = null;
		var n = 0;
		while (n < permits && isStarted() && isAdmissionOpen()) {
			conn = null; // reset so a stale channel from a prior iteration is never used
			nextOp = ops.get(from + n);
			var dispatched = false;
			try {
				if (OpType.NOOP.equals(nextOp.type())) {
					if (!beginDispatch(nextOp)) {
						break;
					}
					dispatched = true;
					nextOp.startRequest();
					sendRequest(null, nextOp);
					nextOp.finishRequest();
					concurrencyThrottle.release();
					nextOp.status(SUCC);
					nextOp.startResponse();
					completeAfterSubmissionSafely(null, nextOp);
				} else {
					if (!beginDispatch(nextOp)) {
						break;
					}
					dispatched = true;
					conn = leaseActiveConnection();
					conn.attr(ATTR_KEY_OPERATION).set(nextOp);
					nextOp.nodeAddr(conn.attr(ATTR_KEY_NODE).get());
					nextOp.startRequest();
					sendRequest(conn, nextOp);
				}
				n++;
			} catch (final ConnectException e) {
				logConnectionLeaseFailure(e);
				if (!dispatched) {
					concurrencyThrottle.release();
					nextOp.status(Operation.Status.FAIL_IO);
					completeAfterSubmissionSafely(null, nextOp);
					n++;
					break;
				}
				nextOp.status(Operation.Status.FAIL_IO);
				if (conn == null) {
					concurrencyThrottle.release();
				}
				completeAfterSubmissionSafely(conn, nextOp);
				n++;
				break;
			} catch (final Throwable thrown) {
				throwUncheckedIfInterrupted(thrown);
				logSubmissionFailure(thrown);
				if (!dispatched) {
					if (conn != null) {
						releaseUndispatchedConnection(conn, false);
					}
					break;
				}
				nextOp.status(Operation.Status.FAIL_UNKNOWN);
				if (conn == null) {
					concurrencyThrottle.release();
				}
				completeAfterSubmissionSafely(conn, nextOp);
				n++;
			}
		}
		// Shutdown or connection acquisition may stop the batch before every drained permit
		// becomes an in-flight request. Completed and asynchronous requests released or own
		// their permits; return only permits which never crossed dispatch.
		if (n < permits) {
			concurrencyThrottle.release(permits - n);
		}
		return n;
	}

	private void completeAfterSubmissionSafely(final Channel conn, final O op) {
		try {
			complete(conn, op);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			// Result publication is extension-facing and may fail per operation. Completion has
			// already released transport ownership, so keep the submit loop bounded and let the
			// lifecycle deadline retain the unresolved outcome without retrying the release.
			LogUtil.exception(Level.DEBUG, cause, "Load operation result publication failure");
		}
	}

	private void logConnectionLeaseFailure(final ConnectException failure) {
		// Constructor-bypassing compatibility tests leave final fields null. Production instances
		// emit one actionable warning per driver and demote repetitive failures to DEBUG.
		if (connectionLeaseFailureWarned == null
						|| connectionLeaseFailureWarned.compareAndSet(false, true)) {
			LogUtil.exception(
							Level.WARN, failure,
							"Failed to lease the connection for a load operation; further failures are logged at DEBUG");
		} else {
			LogUtil.exception(Level.DEBUG, failure, "Failed to lease the connection for a load operation");
		}
	}

	private void logSubmissionFailure(final Throwable failure) {
		// The submission loop may encounter the same underlying failure for every operation in a
		// batch. Preserve one actionable warning per driver and retain later diagnostics at DEBUG.
		if (submissionFailureWarned == null
						|| submissionFailureWarned.compareAndSet(false, true)) {
			LogUtil.exception(
							Level.WARN, failure,
							"Failed to submit the load operations; further failures are logged at DEBUG");
		} else {
			LogUtil.exception(Level.DEBUG, failure, "Failed to submit the load operations");
		}
	}

	private void releaseUndispatchedConnection(final Channel conn, final boolean releasePermit) {
		if (conn.hasAttr(ATTR_KEY_OPERATION)) {
			conn.attr(ATTR_KEY_OPERATION).set(null);
		}
		conn.attr(ATTR_KEY_RELEASED).set(Boolean.TRUE);
		connPool.release(conn);
		if (releasePermit) {
			concurrencyThrottle.release();
		}
	}

	@Override
	protected final int submit(final List<O> ops)
					throws IllegalStateException {
		return submit(ops, 0, ops.size());
	}

	/**
	* Note that the particular implementation should also invoke the {@link #sendRequestData(Channel,
	* Operation)} method to send the actual payload (if any).
	*
	* @param channel the channel to send request to
	* @param op the load operation describing the item and the operation type to perform
	*/
	protected abstract void sendRequest(final Channel channel, final O op);

	protected final void sendRequestData(final Channel channel, final O op) throws IOException {

		final var opType = op.type();

		if (OpType.CREATE.equals(opType)) {
			final var item = op.item();
			if (item instanceof DataItem) {
				final var dataOp = (DataOperation) op;
				if (!(dataOp instanceof CompositeDataOperation)) {
					final var dataItem = (DataItem) item;
					final var srcPath = dataOp.srcPath();
					if (0 < dataItem.size() && (null == srcPath || srcPath.isEmpty())) {
						if (sslFlag) {
							channel.write(new SeekableByteChannelChunkedNioStream(dataItem));
						} else {
							channel.write(new DataItemFileRegion(dataItem));
						}
					}
					dataOp.countBytesDone(dataItem.size());
				}
			}
		} else if (OpType.UPDATE.equals(opType)) {
			final var item = op.item();
			if (item instanceof DataItem) {

				final var dataItem = (DataItem) item;
				final var dataOp = (DataOperation) op;

				final var fixedRanges = (List<Range>) dataOp.fixedRanges();
				if (fixedRanges == null || fixedRanges.isEmpty()) {
					// random ranges update case
					final var updRangesMaskPair = dataOp.markedRangesMaskPair();
					final var rangeCount = rangeCount(dataItem.size());
					DataItem updatedRange;
					if (sslFlag) {
						// current layer updates first
						for (var i = 0; i < rangeCount; i++) {
							if (updRangesMaskPair[0].get(i)) {
								dataOp.currRangeIdx(i);
								updatedRange = dataOp.currRangeUpdate();
								channel.write(new SeekableByteChannelChunkedNioStream(updatedRange));
							}
						}
						// then next layer updates if any
						for (var i = 0; i < rangeCount; i++) {
							if (updRangesMaskPair[1].get(i)) {
								dataOp.currRangeIdx(i);
								updatedRange = dataOp.currRangeUpdate();
								channel.write(new SeekableByteChannelChunkedNioStream(updatedRange));
							}
						}
					} else {
						// current layer updates first
						for (var i = 0; i < rangeCount; i++) {
							if (updRangesMaskPair[0].get(i)) {
								dataOp.currRangeIdx(i);
								updatedRange = dataOp.currRangeUpdate();
								channel.write(new DataItemFileRegion(updatedRange));
							}
						}
						// then next layer updates if any
						for (var i = 0; i < rangeCount; i++) {
							if (updRangesMaskPair[1].get(i)) {
								dataOp.currRangeIdx(i);
								updatedRange = dataOp.currRangeUpdate();
								channel.write(new DataItemFileRegion(updatedRange));
							}
						}
					}
					dataItem.commitUpdatedRanges(dataOp.markedRangesMaskPair());
				} else { // fixed byte ranges case
					final var baseItemSize = dataItem.size();
					long beg;
					long end;
					long size;
					if (sslFlag) {
						for (final var fixedRange : fixedRanges) {
							beg = fixedRange.getBeg();
							end = fixedRange.getEnd();
							size = fixedRange.getSize();
							if (size == -1) {
								if (beg == -1) {
									beg = baseItemSize - end;
									size = end;
								} else if (end == -1) {
									size = baseItemSize - beg;
								} else {
									size = end - beg + 1;
								}
							} else {
								// append
								beg = baseItemSize;
								// note down the new size
								dataItem.size(dataItem.size() + dataOp.markedRangesSize());
							}
							channel.write(new SeekableByteChannelChunkedNioStream(dataItem.slice(beg, size)));
						}
					} else {
						for (final var fixedRange : fixedRanges) {
							beg = fixedRange.getBeg();
							end = fixedRange.getEnd();
							size = fixedRange.getSize();
							if (size == -1) {
								if (beg == -1) {
									beg = baseItemSize - end;
									size = end;
								} else if (end == -1) {
									size = baseItemSize - beg;
								} else {
									size = end - beg + 1;
								}
							} else {
								// append
								beg = baseItemSize;
								// note down the new size
								dataItem.size(dataItem.size() + dataOp.markedRangesSize());
							}
							channel.write(new DataItemFileRegion(dataItem.slice(beg, size)));
						}
					}
				}
				dataOp.countBytesDone(dataOp.markedRangesSize());
			}
		}
	}

	void sendFullRequestComplete(final ChannelFuture future) {
		final Object rawOp = future.channel().attr(ATTR_KEY_OPERATION).get();
		if (rawOp != null && !(rawOp instanceof Operation)) {
			LogUtil.trace(Loggers.ERR, Level.ERROR, new ClassCastException(
							"sendFullRequestComplete: ATTR_KEY_OPERATION contains "
											+ rawOp.getClass().getName() + " instead of Operation"),
							"channel={}", future.channel());
			return;
		}
		final var op = (Operation) rawOp;
		try {
			op.finishRequest();
		} catch (final IllegalStateException e) {
			LogUtil.exception(Level.DEBUG, e, "{}", op.toString());
		}
	}

	@Override
	public void complete(final Channel channel, final O op) {

		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
		ThreadContext.put(KEY_STEP_ID, stepId);

		try {
			if (op instanceof DeleteRequestOperation deleteOperation) {
				deleteOperation.markResponseLastByteReceived();
			} else {
				op.finishResponse();
			}
		} catch (final IllegalStateException e) {
			LogUtil.exception(Level.DEBUG, e, "{}: invalid load operation state", op.toString());
		}

		// An idle channel remains in the pipeline after it is returned to the pool.
		// Do not let a later IdleStateEvent complete this already-finished operation again.
		if (channel != null && channel.hasAttr(ATTR_KEY_OPERATION)) {
			channel.attr(ATTR_KEY_OPERATION).compareAndSet(op, null);
		}

		if (op.status() != Operation.Status.SUCC && channel != null) {
			channel.close();
		}

		final boolean transportHeld = channel != null
						&& !channel.attr(ATTR_KEY_RELEASED).getAndSet(Boolean.TRUE);
		if (transportHeld && directDispatchEnabled()
						&& op.status() == Operation.Status.SUCC
						&& channel.isActive()
						&& isDirectDispatchEligible(op)) {
			// Keep the permit and the channel across completion so the next queued operation can
			// start on this event-loop thread without a dispatcher hand-off or a pool round trip.
			var transferred = false;
			try {
				handleCompleted(op);
				transferred = tryDirectDispatch(channel);
			} finally {
				if (!transferred) {
					releaseTransport(channel);
				}
			}
			return;
		}
		// Release the permit and channel before reporting completion. Recycled
		// operations return through the shared LoadGenerator queue for redispatch.
		if (transportHeld) {
			releaseTransport(channel);
		}
		handleCompleted(op);
	}

	private void releaseTransport(final Channel channel) {
		concurrencyThrottle.release();
		connPool.release(channel);
		signalDispatchCapacityAvailable();
	}

	/**
	 * Sends the next plain queued operation on {@code conn}, which the caller still owns together
	 * with one concurrency permit. Returns {@code false} when no operation was taken and the caller
	 * must release both; {@code true} when ownership passed to the next operation, including a send
	 * failure which completes that operation and releases through the normal path.
	 */
	private boolean tryDirectDispatch(final Channel conn) {
		final O nextOp = pollForDirectDispatch();
		if (nextOp == null) {
			return false;
		}
		conn.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
		try {
			conn.attr(ATTR_KEY_OPERATION).set(nextOp);
			nextOp.nodeAddr(conn.attr(ATTR_KEY_NODE).get());
			nextOp.startRequest();
			sendRequest(conn, nextOp);
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			logSubmissionFailure(thrown);
			nextOp.status(Operation.Status.FAIL_UNKNOWN);
			completeAfterSubmissionSafely(conn, nextOp);
		}
		return true;
	}

	@Override
	public final void channelReleased(final Channel channel) throws Exception {}

	@Override
	public final void channelAcquired(final Channel channel) throws Exception {}

	@Override
	public final void channelCreated(final Channel channel) throws Exception {
		try (final var ctx = CloseableThreadContext.put(KEY_STEP_ID, stepId).put(KEY_CLASS_NAME, CLS_NAME)) {
			appendHandlers(channel);
			if (Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace(
								"{}: new channel pipeline configured: {}", stepId, channel.pipeline().toString());
			}
		}
	}

	protected void appendHandlers(final Channel channel) {
		final var pipeline = channel.pipeline();
		if (sslFlag) {
			Loggers.MSG.debug("{}: SSL/TLS is enabled for the channel", stepId);
			final var sslHandler = (SslHandler) sslCtx.newHandler(channel.alloc());
			applyNamedGroups(sslHandler.engine());
			sslHandler.handshakeFuture().addListener(future -> {
				if (future.isSuccess()) {
					logNegotiatedTls(sslHandler);
				}
			});
			pipeline.addLast(sslHandler);
		}
		if (netTimeoutMilliSec > 0) {
			pipeline.addLast(
							new IdleStateHandler(
											netTimeoutMilliSec, netTimeoutMilliSec, netTimeoutMilliSec, TimeUnit.MILLISECONDS));
		}
	}

	void logNegotiatedTls(final SslHandler sslHandler) {
		if (!tlsHandshakeLogged.compareAndSet(false, true)) {
			return;
		}
		final var session = sslHandler.engine().getSession();
		Loggers.MSG.info(
						"{}: negotiated TLS protocol={}, cipher={}",
						stepId,
						session.getProtocol(),
						session.getCipherSuite());
	}

	void applyNamedGroups(final SSLEngine sslEngine) {
		if (sslNamedGroups.length == 0) {
			return;
		}
		try {
			final SSLParameters sslParameters = sslEngine.getSSLParameters();
			sslParameters.setNamedGroups(sslNamedGroups);
			sslEngine.setSSLParameters(sslParameters);
		} catch (final RuntimeException e) {
			if ("require".equals(sslPqcMode)) {
				throw e;
			}
			if (namedGroupsWarned.compareAndSet(false, true)) {
				LogUtil.exception(Level.WARN, e, "{}: failed to apply SSL named groups", stepId);
			}
		}
	}

	private static String normalizedPqcMode(final String mode) {
		if (mode == null || mode.isBlank()) {
			return "off";
		}
		switch (mode.toLowerCase(Locale.ROOT)) {
		case "off":
		case "prefer":
		case "require":
			return mode.toLowerCase(Locale.ROOT);
		default:
			return "off";
		}
	}

	private static Provider resolveJsseProvider(final String providerName) {
		if (providerName == null || providerName.isBlank()) {
			return null;
		}
		final var normalizedName = providerName.trim();
		final var provider = Security.getProvider(normalizedName);
		if (provider != null) {
			return provider;
		}
		if (BC_JSSE_PROVIDER_NAME.equalsIgnoreCase(normalizedName)) {
			return newBouncyCastleJsseProvider();
		}
		final var directProvider = instantiateProviderClass(normalizedName);
		if (directProvider != null) {
			if (BC_JSSE_PROVIDER_CLASS.equals(directProvider.getClass().getName())) {
				final var wiredProvider = newBouncyCastleJsseProvider();
				return wiredProvider == null ? directProvider : wiredProvider;
			}
			return directProvider;
		}
		return null;
	}

	private static Provider instantiateProviderClass(final String providerClassName) {
		try {
			final var providerClass = Class.forName(providerClassName);
			if (Provider.class.isAssignableFrom(providerClass)) {
				return (Provider) providerClass.getDeclaredConstructor().newInstance();
			}
		} catch (final ReflectiveOperationException ignored) {}
		return null;
	}

	private static Provider newBouncyCastleJsseProvider() {
		try {
			final var jceProviderClass = Class.forName(BC_JCE_PROVIDER_CLASS);
			final var jceProvider = (Provider) jceProviderClass.getDeclaredConstructor().newInstance();
			final var jsseProviderClass = Class.forName(BC_JSSE_PROVIDER_CLASS);
			final var ctor = jsseProviderClass.getConstructor(Provider.class);
			return (Provider) ctor.newInstance(jceProvider);
		} catch (final ReflectiveOperationException ignored) {
			return null;
		}
	}

	private static String sslStringVal(final Config sslConfig, final String key, final String fallback) {
		try {
			final var value = sslConfig.stringVal(key);
			return value == null ? fallback : value;
		} catch (final InvalidValuePathException | NoSuchElementException ignored) {
			return fallback;
		}
	}

	private static List<String> sslListVal(final Config sslConfig, final String key) {
		try {
			final var value = sslConfig.<String> listVal(key);
			return value == null ? List.of() : value;
		} catch (final InvalidValuePathException | NoSuchElementException ignored) {
			return List.of();
		}
	}

	@Override
	protected final void doStop() throws IllegalStateException {
		try (final var ctx = CloseableThreadContext.put(KEY_STEP_ID, stepId).put(KEY_CLASS_NAME, CLS_NAME)) {
			Loggers.MSG.debug("{}: shutdown the I/O executor", toString());
			if (shutdownIoExecutor(ioExecutor)) {
				Loggers.MSG.debug("{}: I/O workers stopped in time", toString());
			} else {
				Loggers.ERR.warn(
								"{}: I/O workers did not stop within {} s",
								toString(),
								IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS);
			}
		}

	}

	static boolean shutdownIoExecutor(final EventLoopGroup ioExecutor) {
		return ioExecutor
						.shutdownGracefully(0, 0, TimeUnit.NANOSECONDS)
						.awaitUninterruptibly(IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	@Override
	protected void doClose() throws IllegalStateException, IOException {
		try {
			connPool.close();
		} catch (final IOException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to close the connection pool", toString());
		}
		super.doClose();
	}
}
