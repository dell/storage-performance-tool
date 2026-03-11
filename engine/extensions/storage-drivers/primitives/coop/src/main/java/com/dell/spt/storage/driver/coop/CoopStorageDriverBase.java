package com.dell.spt.storage.driver.coop;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.CloseableThreadContext;

/**
 * Cooperative storage driver base. This layer is a pure lifecycle/metrics wrapper:
 * <ul>
 *   <li>{@link #put} calls {@link #prepare} then {@link #submit} — no intermediate queue.</li>
 *   <li>Backpressure is enforced entirely inside each driver's {@link #submit}: the contract is
 *       "accept this op and begin processing it, blocking the caller if the concurrency limit
 *       is reached."</li>
 *   <li>Child ops (composite/partial) are submitted directly via {@link #submit} from
 *       {@link #handleCompleted}, not queued.</li>
 * </ul>
 */
public abstract class CoopStorageDriverBase<I extends Item, O extends Operation<I>>
				extends StorageDriverBase<I, O> implements StorageDriver<I, O> {

	protected final Semaphore concurrencyThrottle;
	private final LongAdder scheduledOpCount = new LongAdder();
	private final LongAdder completedOpCount = new LongAdder();

	protected CoopStorageDriverBase(
					final String testStepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag);
		this.concurrencyThrottle = new Semaphore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE, true);
	}

	@Override
	protected void doStart() throws IllegalStateException {
	}

	@Override
	public final boolean put(final O op) {
		if (!isStarted()) {
			throwUnchecked(new EOFException());
		}
		if (prepare(op) && submit(op)) {
			scheduledOpCount.increment();
			return true;
		}
		return false;
	}

	@Override
	public final int put(final List<O> ops, final int from, final int to) {
		if (!isStarted()) {
			throwUnchecked(new EOFException());
		}
		var i = from;
		O nextOp;
		while (i < to && isStarted()) {
			nextOp = ops.get(i);
			if (prepare(nextOp) && submit(nextOp)) {
				scheduledOpCount.increment();
				i++;
			} else {
				break;
			}
		}
		return i - from;
	}

	@Override
	public final int put(final List<O> ops) {
		if (!isStarted()) {
			throwUnchecked(new EOFException());
		}
		var n = 0;
		for (final var nextOp : ops) {
			if (isStarted()) {
				if (prepare(nextOp) && submit(nextOp)) {
					scheduledOpCount.increment();
					n++;
				} else {
					break;
				}
			} else {
				break;
			}
		}
		return n;
	}

	@Override
	public final int activeOpCount() {
		if (concurrencyLimit > 0) {
			return concurrencyLimit - concurrencyThrottle.availablePermits();
		} else {
			return Integer.MAX_VALUE - concurrencyThrottle.availablePermits();
		}
	}

	@Override
	public final long scheduledOpCount() {
		return scheduledOpCount.sum();
	}

	@Override
	public final long completedOpCount() {
		return completedOpCount.sum();
	}

	@Override
	public final boolean isIdle() {
		if (concurrencyLimit > 0) {
			return !concurrencyThrottle.hasQueuedThreads()
							&& concurrencyThrottle.availablePermits() >= concurrencyLimit;
		} else {
			return concurrencyThrottle.availablePermits() == Integer.MAX_VALUE;
		}
	}

	protected abstract boolean submit(final O op) throws IllegalStateException;

	protected abstract int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException;

	protected abstract int submit(final List<O> ops)
					throws IllegalStateException;

	/**
	 * Handle a completed operation. Delivers the result via the output and submits any
	 * child operations (composite sub-ops or partial parent finalization) directly through
	 * {@link #submit}. The caller MUST release the concurrency permit BEFORE calling this
	 * method so that child op submission can re-acquire a permit without deadlocking.
	 */
	@SuppressWarnings("unchecked")
	protected final boolean handleCompleted(final O op) {
		if (super.handleCompleted(op)) {
			completedOpCount.increment();
			if (op instanceof CompositeOperation) {
				final var parentOp = (CompositeOperation) op;
				if (!parentOp.allSubOperationsDone()) {
					final List<O> subOps = parentOp.subOperations();
					for (final O nextSubOp : subOps) {
						if (!submit(nextSubOp)) {
							Loggers.ERR.warn(
											"{}: Failed to submit child operation", toString());
							return false;
						}
					}
				}
			} else if (op instanceof PartialOperation) {
				final var subOp = (PartialOperation) op;
				final var parentOp = subOp.parent();
				if (parentOp.allSubOperationsDone()) {
					// execute once again to finalize the things if necessary:
					// complete the multipart upload, for example
					if (!submit((O) parentOp)) {
						Loggers.ERR.warn(
										"{}: Failed to submit child operation", toString());
						return false;
					}
				}
			}
			return true;
		} else {
			return false;
		}
	}

	@Override
	protected void doShutdown() {
		Loggers.MSG.debug("{}: shut down", toString());
	}

	@Override
	public boolean await(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
		return false;
	}

	@Override
	protected void doClose() throws IOException, IllegalStateException {
		try (final var logCtx = CloseableThreadContext.put(KEY_STEP_ID, stepId)
						.put(KEY_CLASS_NAME, StorageDriverBase.class.getSimpleName())) {
			super.doClose();
		}
	}
}
