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
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker.DispatchToken;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.nic.NicPathCounting;
import com.dell.spt.base.nic.NicSetResources;
import com.dell.spt.base.nic.NicPathRows;
import com.dell.spt.storage.driver.coop.netty.nic.NicAdmission;
import com.dell.spt.storage.driver.coop.netty.nic.NicPathCounters;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import java.util.List;
import org.apache.logging.log4j.Level;

/**
 * Netty S3 driver with a NIC set ({@code storage.net.interfaces}). At start it resolves its NICs
 * against the host, pairs them with its endpoints, creates one event-loop group per NIC and probes
 * every Path before the first operation. Each request attempt then takes its concurrency permit
 * and a NIC with room ({@link NicAdmission}), and a connection from that NIC's Path Pool, bound to
 * the NIC and connected asynchronously to the endpoint's frozen address, so the dispatcher never
 * waits on setup. The operation records the configured endpoint string, which also stays the Host
 * and signing authority. Control requests keep the kernel route on the base event-loop group (item
 * 11).
 *
 * <p>Custody passes at the request write, not at admission (item 9c): the operation stays queued
 * while its attempt waits for a connection, and the write marks it dispatched under the admission
 * lock only if the queued circulation the dispatch task captured is still current. A stop or a
 * stale circulation therefore writes nothing and leaves the operation to queue recovery. A setup
 * failure, a Path in connect back-off included, takes custody the same way and completes the
 * operation as {@code FAIL_IO}, as a failed default-pool lease does. A completed attempt hands its
 * connection and permit straight to the next queued operation only when every NIC has its own share
 * of the concurrency limit (D37).
 *
 * <p>Each attempt counts on one row of the per-path CSV (item 18, D59): admitted when its request
 * is bound to a Path's connection, or with its setup failure on the Path, the NIC or no NIC it
 * reached, and then ended. An attempt's outcome is its response's status before the multipart
 * rules reinterpret it for the operation; it counts only when the tracker committed the operation
 * as terminal, so an attempt still in flight at the drain cut stays unresolved.
 */
final class S3NicSetDriver<I extends Item, O extends Operation<I>> extends S3StorageDriver<I, O>
				implements NicPathCounting {

	// The row of the attempt a connection carries, from its request binding to its completion.
	private static final AttributeKey<NicPathCounters.Row> ATTR_KEY_ROW = AttributeKey.valueOf("nicPathRow");

	private final NicSetSupport nicSet;
	// Assigned by createConnectionPool() while the superclass constructor runs; no initializer, so
	// the assignment survives this class's own field initialization.
	private NicSetConnectionPool nicPool;
	// Assigned at start before the dispatch task and the NICs' event loops run.
	private NicPathCounters counters;

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
			counters = nicSet.counters();
			if (!NicSetSettings.hasShares(concurrencyLimit, nicPool.nicCount())) {
				disableDirectDispatch();
			}
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

	@Override
	public NicPathRows nicPathRows() {
		// The stop's flag first: it was written after start assigned the counters.
		final var ioStopped = ioStoppedInTime();
		final var table = counters;
		return table == null || ioStopped == null ? null : table.snapshot(ioStopped && dispatchStoppedInTime());
	}

	@Override
	public NicSetResources nicSetResources() {
		return nicSet.resources();
	}

	/** Runs as the request is sent: a submitted or a directly dispatched attempt is admitted on its Path. */
	@Override
	protected void bindRequestChannel(final Channel channel, final O op) {
		final var row = counters.path(channel);
		if (row != null) {
			row.attempted();
			channel.attr(ATTR_KEY_ROW).set(row);
		}
		super.bindRequestChannel(channel, op);
	}

	/** Ends the attempt the channel carries on its row; a repeated completion finds no row. */
	@Override
	public void complete(final Channel channel, final O op) {
		final var row = channel == null ? null : channel.attr(ATTR_KEY_ROW).getAndSet(null);
		if (row == null) {
			super.complete(channel, op);
			return;
		}
		// Read before completion, which may hand the operation on.
		final var lifecycle = op.lifecycle();
		final var succeeded = op.status() == Operation.Status.SUCC;
		final long bytes = succeeded ? payloadBytes(op) : 0;
		super.complete(channel, op);
		if (terminal(lifecycle)) {
			if (succeeded) {
				row.succeeded(bytes);
			} else {
				row.failed();
			}
		}
	}

	/** The request body of a write or the response body of a read; a multipart parent's requests carry none. */
	private static long payloadBytes(final Operation<?> op) {
		return op instanceof DataOperation<?> data && !(op instanceof CompositeOperation) ? data.countBytesDone() : 0;
	}

	/** Whether the tracker committed the circulation; an untracked operation has no drain cut. */
	private static boolean terminal(final OperationLifecycle lifecycle) {
		return !lifecycle.isTracked() || lifecycle.state() == OperationLifecycleState.TERMINAL;
	}

	/** The write, not the accepted submit, starts transport, except on the inherited route. */
	@Override
	protected boolean successfulSubmitStartsTransport(final O op) {
		return usesNoChannel(op);
	}

	/** A free permit and a NIC with room, so the dispatch task parks while every NIC is full. */
	@Override
	protected boolean hasAvailableDispatchCapacity() {
		return super.hasAvailableDispatchCapacity() && nicPool.admission().hasRoom();
	}

	@Override
	protected int dispatchAttemptLimit(final int bufferedOperationCount) {
		return Math.min(super.dispatchAttemptLimit(bufferedOperationCount), nicPool.admission().room());
	}

	/** Pool first, so the NIC's room comes with an idle connection, then the NIC and the permit. */
	@Override
	protected void releaseTransport(final Channel channel) {
		final int nic = NicSetConnectionPool.nicIndex(channel);
		nicPool.release(channel);
		if (nic >= 0) {
			nicPool.admission().attemptEnded(nic);
		}
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		if (usesNoChannel(op)) {
			return super.submit(op);
		}
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		// No admission lock here: the operation stays queued until the write, which checks the
		// token under that lock, so a capture racing a stop is left to queue recovery.
		if (!isAdmissionOpen()) {
			return false;
		}
		final var token = operationLifecycle().queuedDispatchToken(op);
		if (token == null && !continuesDispatchedComposite(op)) {
			return false;
		}
		if (!concurrencyThrottle.tryAcquire()) {
			return false;
		}
		final var admission = nicPool.admission();
		final int nic = admission.take();
		if (nic < 0) {
			concurrencyThrottle.release();
			return false;
		}
		try {
			// The pool reports exactly once, even when the event loops terminate first (close settles it).
			nicPool.acquire(nic, new NicSetConnectionPool.Acquisition() {
				@Override
				public void complete(final Channel channel, final Throwable failure) {
					if (failure == null) {
						onAcquired(op, token, nic, channel);
					} else {
						failSetup(op, token, nic, -1, failure);
					}
				}

				@Override
				public void failed(final int path, final Throwable failure) {
					failSetup(op, token, nic, path, failure);
				}
			});
		} catch (final RuntimeException e) {
			failSetup(op, token, nic, -1, e);
		}
		return true;
	}

	/** A composite's finalization, which continues its dispatched attempt and has no queued circulation. */
	private boolean continuesDispatchedComposite(final O op) {
		return op instanceof CompositeOperation
						&& operationLifecycle().stateOf(op) == OperationLifecycleState.DISPATCHED;
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

	/** Runs on the channel's event loop. */
	private void onAcquired(final O op, final DispatchToken<O> token, final int nic, final Channel channel) {
		final var admission = nicPool.admission();
		if (admission.waitEnded(nic)) {
			signalDispatchCapacityAvailable();
		}
		if (!withDispatchAdmission(() -> claimCapturedDispatch(op, token))) {
			// Stopped, or the circulation is stale: nothing is written and queue recovery owns the operation.
			final var row = counters.path(channel);
			row.attempted();
			row.interrupted();
			nicPool.release(channel);
			admission.attemptEnded(nic);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
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
			if (channel.attr(ATTR_KEY_ROW).get() == null) {
				// Failed before its request was bound: admit the attempt here, so completion ends it.
				final var row = counters.path(channel);
				row.attempted();
				channel.attr(ATTR_KEY_ROW).set(row);
			}
			op.status(Operation.Status.FAIL_UNKNOWN);
			completeQuietly(channel, op);
		}
	}

	/**
	 * Mirrors a failed default-pool lease: the operation fails with FAIL_IO, if it can still be
	 * marked dispatched. After a stop, or for a stale circulation, queue recovery owns it instead.
	 * The attempt counts on the Path chosen for it, or on its NIC when none was ({@code path} -1):
	 * failed when the operation ended so, interrupted when nothing ended it.
	 */
	private void failSetup(final O op, final DispatchToken<O> token, final int nic, final int path, final Throwable cause) {
		final var row = counters.path(nic, path);
		row.attempted();
		final boolean claimed = withDispatchAdmission(() -> claimCapturedDispatch(op, token));
		nicSet.logSetupFailure(cause, !claimed);
		final var admission = nicPool.admission();
		admission.waitEnded(nic);
		admission.attemptEnded(nic);
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
		if (!claimed) {
			row.interrupted();
			return;
		}
		final var lifecycle = op.lifecycle();
		op.status(Operation.Status.FAIL_IO);
		completeQuietly(null, op);
		if (terminal(lifecycle)) {
			row.failed();
		} else {
			row.interrupted();
		}
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
