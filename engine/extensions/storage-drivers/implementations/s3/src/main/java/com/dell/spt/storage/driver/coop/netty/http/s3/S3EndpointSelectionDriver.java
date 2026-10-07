package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_OPERATION;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.endpoint.DestinationSource;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings;
import com.dell.spt.storage.driver.coop.netty.endpoint.RoundRobinDestinations;
import com.dell.spt.storage.driver.coop.netty.endpoint.SelectingConnectionPool;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SNIHostName;
import org.apache.logging.log4j.Level;

/**
 * Netty S3 driver for opt-in endpoint selection. Each request attempt takes its destination from
 * the configured mode and connects asynchronously, so the dispatcher never waits on setup.
 * Dispatch is claimed before setup, exactly as the default pool lease does; a setup failure
 * completes the operation as {@code FAIL_IO}. When a logical hostname is configured, HTTP Host,
 * request signing and TLS SNI use it, while the operation records the selected {@code ip:port}.
 */
final class S3EndpointSelectionDriver<I extends Item, O extends Operation<I>> extends S3StorageDriver<I, O> {

	private final EndpointSelectionSettings settings;
	private final String logicalHostname;
	private final AtomicBoolean setupFailureWarned = new AtomicBoolean();
	// Assigned by createConnectionPool() while the superclass constructor runs; no initializer, so
	// the assignment survives this class's own field initialization.
	private SelectingConnectionPool selectingPool;

	S3EndpointSelectionDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final EndpointSelectionSettings settings)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);
		this.settings = settings;
		this.logicalHostname = settings.hostname().orElse(null);
		final DestinationSource destinations = switch (settings.mode()) {
		case ROUND_ROBIN -> new RoundRobinDestinations(settings.destinations());
		default -> throw new IllegalConfigurationException(
						"Endpoint selection \"" + settings.mode().configValue() + "\" is not available yet");
		};
		// Each destination may keep one connection beyond the in-flight limit, so strict rotation can
		// still reuse connections at low concurrency.
		final var idleLimit = concurrencyLimit > 0
						? (int) Math.min(Integer.MAX_VALUE, (long) concurrencyLimit + settings.destinations().size())
						: Integer.MAX_VALUE;
		selectingPool.bind(
						destinations,
						true,
						idleLimit,
						settings.connectTimeoutMillis(),
						settings.connectTimeoutMillis());
		if (logicalHostname != null) {
			helperAuthority(logicalHostname + ":" + settings.destinations().get(0).getPort());
		}
		Loggers.MSG.info("{}: endpoint selection {} over {} destination(s), logical hostname: {}",
						stepId, settings.mode().configValue(), settings.destinations().size(),
						logicalHostname == null ? "none (Host is the selected address)" : logicalHostname);
	}

	@Override
	protected NonBlockingConnPool createConnectionPool() {
		selectingPool = new SelectingConnectionPool(bootstrap, this);
		return selectingPool;
	}

	/** Every request attempt selects its destination; a completed channel never carries the next request. */
	@Override
	protected boolean supportsDirectDispatch() {
		return false;
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		if (usesNoChannel(op)) {
			return super.submit(op);
		}
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		if (!concurrencyThrottle.tryAcquire()) {
			return false;
		}
		if (!beginDispatch(op)) {
			concurrencyThrottle.release();
			return false;
		}
		final Future<Channel> acquisition;
		try {
			acquisition = selectingPool.acquire();
		} catch (final RuntimeException e) {
			failSetup(op, e);
			return true;
		}
		acquisition.addListener((Future<Channel> acquired) -> onAcquired(op, acquired));
		return true;
	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to) throws IllegalStateException {
		var i = from;
		while (i < to && submit(ops.get(i))) {
			i++;
		}
		return i - from;
	}

	/** NOOP and composite READ passes complete without a channel through the inherited route. */
	private static boolean usesNoChannel(final Operation<?> op) {
		return OpType.NOOP.equals(op.type())
						|| (op instanceof CompositeDataOperation && OpType.READ.equals(op.type()));
	}

	private void onAcquired(final O op, final Future<Channel> acquired) {
		if (!acquired.isSuccess()) {
			failSetup(op, acquired.cause());
			return;
		}
		final var channel = acquired.getNow();
		try {
			channel.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
			channel.attr(ATTR_KEY_OPERATION).set(op);
			op.nodeAddr(channel.attr(ATTR_KEY_NODE).get());
			op.startRequest();
			sendRequest(channel, op);
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			logSetupFailure(thrown);
			op.status(Operation.Status.FAIL_UNKNOWN);
			completeQuietly(channel, op);
		}
	}

	/** Mirrors a failed default-pool lease: the dispatched operation fails with FAIL_IO. */
	private void failSetup(final O op, final Throwable cause) {
		logSetupFailure(cause);
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
		if (isStopped()) {
			// Like an in-flight response after stop, nothing is published.
			return;
		}
		op.status(Operation.Status.FAIL_IO);
		completeQuietly(null, op);
	}

	private void completeQuietly(final Channel channel, final O op) {
		try {
			complete(channel, op);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			LogUtil.exception(Level.DEBUG, cause, "Load operation result publication failure");
		}
	}

	private void logSetupFailure(final Throwable failure) {
		if (setupFailureWarned.compareAndSet(false, true)) {
			LogUtil.exception(Level.WARN, failure,
							"Endpoint selection failed to set up a request; further failures are logged at DEBUG");
		} else {
			LogUtil.exception(Level.DEBUG, failure, "Endpoint selection failed to set up a request");
		}
	}

	@Override
	protected HttpRequest httpRequest(final O op, final String nodeAddr) throws URISyntaxException {
		return super.httpRequest(op, authority(nodeAddr));
	}

	/** Logical hostname with the selected port, or the selected {@code ip:port} without one. */
	private String authority(final String nodeAddr) {
		if (logicalHostname == null || nodeAddr == null) {
			return nodeAddr;
		}
		return logicalHostname + nodeAddr.substring(nodeAddr.lastIndexOf(':'));
	}

	/** Helper requests (bucket setup, listing, verification, abort) also follow the selection. */
	@Override
	protected Channel getUnpooledConnection(final String storageNodeAddr, final int storageNodePort)
					throws ConnectException {
		return selectingPool.connectUnpooled();
	}

	@Override
	protected void appendHandlers(final Channel channel) {
		super.appendHandlers(channel);
		final var ssl = logicalHostname == null ? null : channel.pipeline().get(SslHandler.class);
		if (ssl != null) {
			final var engine = ssl.engine();
			final var parameters = engine.getSSLParameters();
			parameters.setServerNames(List.of(new SNIHostName(logicalHostname)));
			engine.setSSLParameters(parameters);
		}
	}

	EndpointSelectionSettings settings() {
		return settings;
	}

	SelectingConnectionPool connectionPool() {
		return selectingPool;
	}
}
