package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadAttempt;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.storage.driver.coop.netty.nic.NicAdmission;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetConnectionPool;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSettings;
import com.dell.spt.storage.driver.coop.netty.nic.NicSetSupport;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import java.util.List;

/**
 * Partial-object Reads with a NIC set. Every range attempt, retries included, takes its
 * concurrency permit and a NIC with room ({@link NicAdmission}), and a connection from that NIC's
 * Path Pool, which connects asynchronously, so the dispatcher never waits on setup. Once connected,
 * the inherited flight, request handoff and settlement apply unchanged, with the configured
 * endpoint string as Host and signing authority. A setup failure, a Path in connect back-off
 * included, settles the attempt as {@code FAIL_IO} while admission is open, as a failed
 * default-pool lease does, and the existing retry policy applies; after a stop, the attempt is
 * fenced and recovered as one that never reached its request.
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

	/** A free permit and a NIC with room, so the dispatch task parks while every NIC is full. */
	@Override
	protected boolean hasAvailableDispatchCapacity() {
		return super.hasAvailableDispatchCapacity() && nicPool.admission().hasRoom();
	}

	@Override
	protected int dispatchAttemptLimit(final int bufferedOperationCount) {
		return Math.min(super.dispatchAttemptLimit(bufferedOperationCount), nicPool.admission().room());
	}

	@Override
	void transportReleased(final Channel channel) {
		final int nic = NicSetConnectionPool.nicIndex(channel);
		if (nic >= 0) {
			nicPool.admission().attemptEnded(nic);
		}
	}

	@Override
	protected boolean submit(final RangeReadOperation<DataItem> op) {
		if (!isStarted() || !beginDispatch(op) || !concurrencyThrottle.tryAcquire())
			return false;
		final var admission = nicPool.admission();
		final int nic = admission.take();
		if (nic < 0) {
			concurrencyThrottle.release();
			return false;
		}
		final var attempt = ranges.capture(op);
		if (attempt == null) {
			admission.cancelled(nic);
			concurrencyThrottle.release();
			return true;
		}
		try {
			// The pool reports exactly once: a channel on its own event loop, or the failure.
			nicPool.acquire(nic, (channel, failure) -> {
				if (failure == null)
					start(op, attempt, nic, channel);
				else
					failSetup(op, attempt, nic, failure);
			});
		} catch (final RuntimeException e) {
			failSetup(op, attempt, nic, e);
		}
		return true;
	}

	/** Runs on the channel's event loop, as the default route's handoff task does. */
	private void start(final RangeReadOperation<DataItem> op, final RangeReadAttempt attempt, final int nic,
					final Channel channel) {
		final var admission = nicPool.admission();
		if (admission.waitEnded(nic))
			signalDispatchCapacityAvailable();
		final var flight = new Flight(op, op.circulation(), attempt, channel);
		if (flights.putIfAbsent(attempt, flight) != null) {
			final var unusedClose = channel.close();
			nicPool.release(channel);
			admission.attemptEnded(nic);
			concurrencyThrottle.release();
			signalDispatchCapacityAvailable();
			return;
		}
		// The Host is the Path's configured endpoint string.
		send(flight, channel.attr(ATTR_KEY_NODE).get());
	}

	/**
	 * Mirrors a failed default-pool lease while admission is open: the attempt fails with FAIL_IO
	 * and its permits are released. After a stop it is fenced as one that never reached its
	 * request, as a fenced write is.
	 */
	private void failSetup(final RangeReadOperation<DataItem> op, final RangeReadAttempt attempt, final int nic,
					final Throwable failure) {
		// False after a stop, or when shutdown recovery already settled the attempt.
		final boolean counted = withDispatchAdmission(() -> attempt.transportFailure(Operation.Status.FAIL_IO));
		nicSet.logSetupFailure(failure, !counted);
		final var admission = nicPool.admission();
		admission.waitEnded(nic);
		admission.attemptEnded(nic);
		concurrencyThrottle.release();
		signalDispatchCapacityAvailable();
		if (counted) {
			completeAttempt(op, op.circulation(), attempt);
		} else {
			attempt.cancelBeforeHandoff();
			recover(op);
		}
	}

	NicSetConnectionPool connectionPool() {
		return nicPool;
	}
}
