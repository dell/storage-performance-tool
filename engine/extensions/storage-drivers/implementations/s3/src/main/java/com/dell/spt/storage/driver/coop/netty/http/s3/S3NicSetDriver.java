package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_OPERATION;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import java.util.List;
import org.apache.logging.log4j.Level;

/**
 * Netty S3 driver with a NIC set ({@code storage.net.interfaces}). At start it resolves its NICs
 * against the host, pairs them with its endpoints, creates one event-loop group per NIC and probes
 * every Path before the first operation. Each request attempt then takes a connection from a NIC's
 * Path Pool, bound to that NIC and connected asynchronously to the endpoint's frozen address, so the
 * dispatcher never waits on setup. The operation records the configured endpoint string, which
 * also stays the Host and signing authority. A setup failure, a Path in connect back-off included,
 * completes the operation as {@code FAIL_IO}, as a failed default-pool lease does. Control requests
 * keep the kernel route on the base event-loop group (item 11).
 */
final class S3NicSetDriver<I extends Item, O extends Operation<I>> extends S3StorageDriver<I, O> {

	private final NicSetSupport nicSet;
	// Assigned by createConnectionPool() while the superclass constructor runs; no initializer, so
	// the assignment survives this class's own field initialization.
	private NicSetConnectionPool nicPool;

	static <I extends Item, O extends Operation<I>> S3NicSetDriver<I, O> create(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final NicSetSettings settings)
					throws IllegalConfigurationException, InterruptedException {
		return create(stepId, itemDataInput, storageConfig, verifyFlag, batchSize, settings, NicSetSupport.Environment.system());
	}

	static <I extends Item, O extends Operation<I>> S3NicSetDriver<I, O> create(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final NicSetSettings settings,
					final NicSetSupport.Environment environment)
					throws IllegalConfigurationException, InterruptedException {
		return new S3NicSetDriver<>(stepId, itemDataInput, storageConfig, verifyFlag, batchSize, settings, environment);
	}

	private S3NicSetDriver(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize,
					final NicSetSettings settings,
					final NicSetSupport.Environment environment)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, itemDataInput, storageConfig, verifyFlag, batchSize);
		this.nicSet = new NicSetSupport(toString(), settings, environment);
	}

	/** Empty until start opens it; the bootstrap snapshot gives every Path the release's socket options. */
	@Override
	protected NonBlockingConnPool createConnectionPool() {
		nicPool = new NicSetConnectionPool(bootstrap.clone(), this);
		return nicPool;
	}

	@Override
	protected void doStart() throws IllegalStateException {
		try {
			nicSet.open(nicPool, List.of(storageNodeAddrs), concurrencyLimit, this::createIoGroup, this::shutdownExtraIoGroups);
			super.doStart();
			nicSet.started(nicPool, concurrencyLimit);
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		}
	}

	@Override
	protected void beforeIoShutdown() {
		nicPool.closeAdmission();
	}

	/** Every request attempt takes its connection from a Path Pool; ticket 04 adds direct dispatch. */
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
			nicPool.acquire((channel, failure) -> onAcquired(op, channel, failure));
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
			nicSet.logSetupFailure(thrown, false);
			op.status(Operation.Status.FAIL_UNKNOWN);
			completeQuietly(channel, op);
		}
	}

	/** Mirrors a failed default-pool lease: the dispatched operation fails with FAIL_IO. */
	private void failSetup(final O op, final Throwable cause) {
		nicSet.logSetupFailure(cause, isStopped());
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

	NicSetConnectionPool connectionPool() {
		return nicPool;
	}
}
