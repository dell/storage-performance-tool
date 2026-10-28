package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import java.util.List;

/**
 * Partial-object Reads with a NIC set. Every range attempt, retries included, takes a connection
 * from a NIC's Path Pool, which connects asynchronously, so the dispatcher never waits on setup.
 * Once connected, the inherited flight, request handoff and settlement apply unchanged, with the
 * configured endpoint string as Host and signing authority. A setup failure, a Path in connect
 * back-off included, settles the attempt as {@code FAIL_IO}, as a failed default-pool lease does,
 * and the existing retry policy applies.
 */
final class S3RangeNicSetDriver extends S3RangeStorageDriver {

	private final NicSetSupport nicSet;
	// Assigned by createRangePool() while the superclass constructor runs; no initializer, so the
	// assignment survives this class's own field initialization.
	private NicSetConnectionPool nicPool;

	static S3RangeNicSetDriver create(final String stepId, final DataInput input, final Config storage, final int batchSize,
					final RangeReadPolicy policy, final NicSetSettings settings) throws InterruptedException {
		return create(stepId, input, storage, batchSize, policy, settings, NicSetSupport.Environment.system());
	}

	static S3RangeNicSetDriver create(final String stepId, final DataInput input, final Config storage, final int batchSize,
					final RangeReadPolicy policy, final NicSetSettings settings, final NicSetSupport.Environment environment)
					throws InterruptedException {
		return new S3RangeNicSetDriver(stepId, input, storage, batchSize, policy, settings, environment);
	}

	private S3RangeNicSetDriver(final String stepId, final DataInput input, final Config storage, final int batchSize,
					final RangeReadPolicy policy, final NicSetSettings settings, final NicSetSupport.Environment environment)
					throws InterruptedException {
		super(stepId, input, storage, batchSize, policy);
		this.nicSet = new NicSetSupport(toString(), settings, environment);
	}

	/** Empty until start opens it; the bootstrap snapshot gives every Path the release's socket options. */
	@Override
	NonBlockingConnPool createRangePool() {
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

	@Override
	protected boolean submit(final RangeReadOperation<DataItem> op) {
		if (!isStarted() || !beginDispatch(op) || !concurrencyThrottle.tryAcquire())
			return false;
		final var attempt = ranges.capture(op);
		if (attempt == null) {
			concurrencyThrottle.release();
			return true;
		}
		try {
			// The pool reports exactly once: a channel on its own event loop, or the failure.
			nicPool.acquire((channel, failure) -> {
				if (failure == null)
					start(op, attempt, channel);
				else
					failSetup(op, attempt, failure);
			});
		} catch (final RuntimeException e) {
			failSetup(op, attempt, e);
		}
		return true;
	}

	/** Runs on the channel's event loop, as the default route's handoff task does. */
	private void start(final RangeReadOperation<DataItem> op, final RangeReadAttempt attempt, final Channel channel) {
		final var flight = new Flight(op, op.circulation(), attempt, channel);
		if (flights.putIfAbsent(attempt, flight) != null) {
			final var unusedClose = channel.close();
			nicPool.release(channel);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			return;
		}
		// The Host is the Path's configured endpoint string.
		send(flight, channel.attr(ATTR_KEY_NODE).get());
	}

	/** Mirrors a failed default-pool lease: the attempt fails with FAIL_IO and its permit is released. */
	private void failSetup(final RangeReadOperation<DataItem> op, final RangeReadAttempt attempt, final Throwable failure) {
		// False when shutdown recovery already settled the attempt.
		final boolean counted = attempt.transportFailure(Operation.Status.FAIL_IO);
		nicSet.logSetupFailure(failure, !counted);
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
		completeAttempt(op, op.circulation(), attempt);
	}

	NicSetConnectionPool connectionPool() {
		return nicPool;
	}
}
