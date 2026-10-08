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
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionConstants;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionCounters;
import com.dell.spt.storage.driver.coop.netty.endpoint.EndpointSelectionSettings;
import com.dell.spt.storage.driver.coop.netty.endpoint.SelectingConnectionPool;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import org.apache.logging.log4j.Level;

/**
 * Netty S3 driver for opt-in endpoint selection. Each request attempt takes its destination from
 * the configured mode and connects asynchronously, so the dispatcher never waits on setup. Round
 * robin reuses idle connections per destination; per-request DNS resolves afresh, sends
 * {@code Connection: close} and closes every connection after its response.
 * Dispatch is claimed before setup, exactly as the default pool lease does; a setup failure
 * completes the operation as {@code FAIL_IO}. When a logical hostname is configured, HTTP Host,
 * request signing and TLS SNI use it, while the operation records the selected {@code ip:port}.
 */
final class S3EndpointSelectionDriver<I extends Item, O extends Operation<I>> extends S3StorageDriver<I, O> {

	private final EndpointSelectionSupport selection;
	// Assigned by createConnectionPool() while the superclass constructor runs; no initializer, so
	// the assignment survives this class's own field initialization.
	private SelectingConnectionPool selectingPool;

	/**
	 * Creates the driver for a non-default mode. DNS server discovery happens first, so a host
	 * without a usable resolver configuration fails before any network resource exists.
	 */
	static <I extends Item, O extends Operation<I>> S3EndpointSelectionDriver<I, O> create(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final EndpointSelectionSettings settings,
					final Path resolvConf)
					throws IllegalConfigurationException, InterruptedException {
		return new S3EndpointSelectionDriver<>(stepId, itemDataInput, storageConfig, verifyFlag, batchSize,
						EndpointSelectionSupport.create(stepId, settings, resolvConf));
	}

	static <I extends Item, O extends Operation<I>> S3EndpointSelectionDriver<I, O> create(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final EndpointSelectionSettings settings)
					throws IllegalConfigurationException, InterruptedException {
		return create(stepId, itemDataInput, storageConfig, verifyFlag, batchSize, settings,
						EndpointSelectionConstants.RESOLV_CONF);
	}

	private S3EndpointSelectionDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final EndpointSelectionSupport selection)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);
		this.selection = selection;
		final var helperAuthority = selection.bind(selectingPool, concurrencyLimit, sharedHeaders);
		if (helperAuthority != null) {
			helperAuthority(helperAuthority);
		}
	}

	@Override
	protected void doStart() throws IllegalStateException {
		super.doStart();
		selection.warnIfHostConfiguredDns();
	}

	@Override
	protected void doClose() throws IllegalStateException, IOException {
		try {
			super.doClose();
		} finally {
			selection.logSummary();
		}
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
		try {
			// The pool reports exactly once, even when the event loops terminate first (close settles it).
			selectingPool.acquire((channel, failure) -> onAcquired(op, channel, failure));
		} catch (final RuntimeException e) {
			failSetup(op, e);
		}
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

	private void onAcquired(final O op, final Channel channel, final Throwable failure) {
		if (failure != null) {
			failSetup(op, failure);
			return;
		}
		try {
			channel.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
			channel.attr(ATTR_KEY_OPERATION).set(op);
			op.nodeAddr(channel.attr(ATTR_KEY_NODE).get());
			op.startRequest();
			sendRequest(channel, op);
		} catch (final Throwable thrown) {
			throwUncheckedIfInterrupted(thrown);
			selection.logSetupFailure(thrown);
			op.status(Operation.Status.FAIL_UNKNOWN);
			completeQuietly(channel, op);
		}
	}

	/** Mirrors a failed default-pool lease: the dispatched operation fails with FAIL_IO. */
	private void failSetup(final O op, final Throwable cause) {
		selection.logSetupFailure(cause);
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

	@Override
	protected HttpRequest httpRequest(final O op, final String nodeAddr) throws URISyntaxException {
		return super.httpRequest(op, selection.authority(nodeAddr));
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
		selection.applySni(channel);
	}

	EndpointSelectionSettings settings() {
		return selection.settings();
	}

	List<InetSocketAddress> dnsServers() {
		return selection.dnsServers();
	}

	EndpointSelectionCounters counters() {
		return selection.counters();
	}

	boolean hostConfiguredDns() {
		return selection.hostConfiguredDns();
	}

	SelectingConnectionPool connectionPool() {
		return selectingPool;
	}
}
