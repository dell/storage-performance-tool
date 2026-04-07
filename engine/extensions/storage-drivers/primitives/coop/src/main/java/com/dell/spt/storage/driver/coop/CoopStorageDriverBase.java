package com.dell.spt.storage.driver.coop;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.CloseableThreadContext;

public abstract class CoopStorageDriverBase<I extends Item, O extends Operation<I>>
				extends StorageDriverBase<I, O> implements StorageDriver<I, O> {

	static final int MAX_PART_RETRIES = 3;

	protected final Semaphore concurrencyThrottle;
	protected final BlockingQueue<O> childOpQueue;
	private final BlockingQueue<O> inOpQueue;
	private final LongAdder scheduledOpCount = new LongAdder();
	private final LongAdder completedOpCount = new LongAdder();
	private final ReentrantLock dispatchLock = new ReentrantLock();
	private final Condition dispatchReady = dispatchLock.newCondition();
	private final OperationDispatchTask opDispatchTask;
	protected volatile int fastRecycleConcurrencyThreshold = 0;

	protected CoopStorageDriverBase(
					final String testStepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag);
		final var inQueueLimit = storageConfig.intVal("driver-limit-queue-input");
		this.childOpQueue = new ArrayBlockingQueue<>(inQueueLimit);
		this.inOpQueue = new ArrayBlockingQueue<>(inQueueLimit);
		this.concurrencyThrottle = new Semaphore(concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE, true);
		this.opDispatchTask = new OperationDispatchTask<>(
						ServiceTaskExecutor.VT_EXECUTOR, this, inOpQueue, childOpQueue, stepId, batchSize,
						dispatchLock, dispatchReady);
	}

	@Override
	protected void doStart() throws IllegalStateException {
		opDispatchTask.start();
	}

	@Override
	public final boolean put(final O op) {
		if (!isStarted()) {
			throwUnchecked(new EOFException());
		}
		if (prepare(op) && inOpQueue.offer(op)) {
			scheduledOpCount.increment();
			signalDispatch();
			return true;
		} else {
			return false;
		}
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
			if (prepare(nextOp) && inOpQueue.offer(ops.get(i))) {
				i++;
			} else {
				break;
			}
		}
		final var n = i - from;
		scheduledOpCount.add(n);
		if (n > 0) {
			signalDispatch();
		}
		return n;
	}

	@Override
	public final int put(final List<O> ops) {
		if (!isStarted()) {
			throwUnchecked(new EOFException());
		}
		var n = 0;
		for (final var nextOp : ops) {
			if (isStarted()) {
				if (prepare(nextOp) && inOpQueue.offer(nextOp)) {
					n++;
				} else {
					break;
				}
			} else {
				break;
			}
		}
		scheduledOpCount.add(n);
		if (n > 0) {
			signalDispatch();
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

	@SuppressWarnings("unchecked")
	protected final boolean handleCompleted(final O op) {
		if (super.handleCompleted(op)) {
			completedOpCount.increment();
			if (op instanceof CompositeOperation) {
				final var parentOp = (CompositeOperation) op;
				if (!parentOp.allSubOperationsDone()) {
					final List<O> subOps = parentOp.subOperations();
					for (final O nextSubOp : subOps) {
						if (!childOpQueue.offer(nextSubOp)) {
							Loggers.ERR.warn(
											"{}: Child operations queue overflow, dropping the operation", toString());
							return false;
						}
					}
				}
			} else if (op instanceof PartialOperation) {
				final var subOp = (PartialOperation) op;
				final var parentOp = subOp.parent();
				if (op.status() != Operation.Status.SUCC) {
					if (subOp.retryCount() < MAX_PART_RETRIES) {
						// retry the individual part instead of aborting the whole MPU
						final var failStatus = op.status();
						subOp.incrementRetryCount();
						parentOp.undoMarkSubTaskCompleted();
						op.status(Operation.Status.PENDING);
						if (op.item() instanceof DataItem dataItem) {
							dataItem.reset();
						}
						Loggers.ERR.warn(
										"{}: part #{} failed ({}), retry {}/{}",
										toString(), subOp.partNumber(), failStatus,
										subOp.retryCount(), MAX_PART_RETRIES);
						if (!childOpQueue.offer(op)) {
							Loggers.ERR.warn(
											"{}: Child operations queue overflow, dropping retry", toString());
						}
					} else {
						// retries exhausted — abort the MPU
						parentOp.put("mpuAbort", "true");
					}
				}
				if (parentOp.allSubOperationsDone()) {
					// execute once again to finalize the things if necessary:
					// complete the multipart upload, for example
					if (!childOpQueue.offer((O) parentOp)) {
						Loggers.ERR.warn(
										"{}: Child operations queue overflow, dropping the operation", toString());
						return false;
					}
				}
			}
			signalDispatch();
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Wake the dispatch task. Called when new ops are available (put) or when
	 * a completion frees capacity (handleCompleted). Uses lock() instead of
	 * tryLock() to guarantee delivery — the dispatch task only holds the lock
	 * for nanoseconds (double-check before await), so contention is negligible.
	 */
	private void signalDispatch() {
		dispatchLock.lock();
		try {
			dispatchReady.signal();
		} finally {
			dispatchLock.unlock();
		}
	}

	@Override
	public void enableFastRecycle(final int concurrencyThreshold) {
		this.fastRecycleConcurrencyThreshold = concurrencyThreshold;
		Loggers.MSG.info("{}: fast-recycle enabled, concurrency threshold = {}", toString(), concurrencyThreshold);
	}

	/**
	 * Returns {@code true} when fast-recycle has been enabled on this driver.
	 * Used by the dispatch task to extend its idle wait when the driver is
	 * cycling ops inline and no new work is expected via the queues.
	 */
	protected boolean isFastRecycleEnabled() {
		return fastRecycleConcurrencyThreshold > 0;
	}

	/**
	 * Check whether the given completed operation is eligible for the fast-recycle
	 * short-circuit.  Returns {@code true} only when:
	 * <ul>
	 *   <li>fast-recycle has been enabled (threshold &gt; 0)</li>
	 *   <li>the current active-op count is &le; the threshold</li>
	 *   <li>the op finished successfully</li>
	 *   <li>the op is a simple (non-composite, non-partial) operation</li>
	 *   <li>the driver is still running</li>
	 * </ul>
	 */
	protected boolean isFastRecycleEligible(final O op) {
		final int threshold = fastRecycleConcurrencyThreshold;
		return threshold > 0
						&& activeOpCount() <= threshold
						&& op.status() == Operation.Status.SUCC
						&& !(op instanceof CompositeOperation)
						&& !(op instanceof PartialOperation)
						&& isStarted();
	}

	@Override
	protected void doShutdown() {
		opDispatchTask.stop();
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
			opDispatchTask.close();
			childOpQueue.clear();
			inOpQueue.clear();
		}
	}
}
