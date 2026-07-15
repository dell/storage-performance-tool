package com.dell.spt.storage.driver.coop;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.CloseableThreadContext;

public abstract class CoopStorageDriverBase<I extends Item, O extends Operation<I>>
				extends StorageDriverBase<I, O> implements StorageDriver<I, O> {

	static final int MAX_PART_RETRIES = 3;
	static final String CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY = "spt.mpu.child.enqueue.timeout.millis";
	static final long DEFAULT_CHILD_OP_ENQUEUE_TIMEOUT_MILLIS = 30_000L;
	private static final String KEY_FINALIZATION_ENQUEUED = "sptFinalizationEnqueued";

	protected final Semaphore concurrencyThrottle;
	protected volatile Semaphore mpuObjectThrottle;
	protected volatile int mpuMaxParts;
	protected final BlockingQueue<O> childOpQueue;
	private final BlockingQueue<O> inOpQueue;
	private final LongAdder scheduledOpCount = new LongAdder();
	private final LongAdder completedOpCount = new LongAdder();
	private final ReentrantLock dispatchLock = new ReentrantLock();
	private final Condition dispatchReady = dispatchLock.newCondition();
	private final OperationDispatchTask opDispatchTask;
	private final Object mpuSchedulingLock = new Object();
	private final int configuredMpuObjectLimit;
	private final int configuredMpuPartLimit;
	private volatile boolean mpuSchedulingInitialized = false;
	protected volatile int fastRecycleConcurrencyThreshold = 0;
	private volatile boolean fastRecycleQuiesceActive = false;

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

		int mpuObjects = 0;
		int maxParts = 0;
		try {
			mpuObjects = storageConfig.intVal("driver-limit-multipart-objects");
		} catch (final NoSuchElementException | InvalidValuePathException ignored) {} catch (final Exception e) {
			Loggers.ERR.warn("{}: Failed to parse multipart limits: {}. Proceeding with unlimited.", testStepId, e.getMessage());
		}
		try {
			maxParts = storageConfig.intVal("driver-limit-multipart-parts");
		} catch (final NoSuchElementException | InvalidValuePathException ignored) {} catch (final Exception e) {
			Loggers.ERR.warn("{}: Failed to parse multipart limits: {}. Proceeding with unlimited.", testStepId, e.getMessage());
		}

		this.configuredMpuObjectLimit = Math.max(0, mpuObjects);
		this.configuredMpuPartLimit = Math.max(0, maxParts);
		this.mpuMaxParts = configuredMpuPartLimit;
		this.mpuObjectThrottle = configuredMpuObjectLimit > 0 ? new Semaphore(configuredMpuObjectLimit, true) : null;

		this.opDispatchTask = new OperationDispatchTask<>(
						ServiceTaskExecutor.TASK_EXECUTOR, this, inOpQueue, childOpQueue, stepId, batchSize,
						dispatchLock, dispatchReady, inQueueLimit);
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

	/** Check if the concurrency throttle has available permits. Package-private —
	 *  used by OperationDispatchTask to double-check before entering backpressure await. */
	boolean hasAvailableDispatchCapacity() {
		return concurrencyThrottle.availablePermits() > 0;
	}

	protected abstract boolean submit(final O op) throws IllegalStateException;

	protected abstract int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException;

	protected abstract int submit(final List<O> ops)
					throws IllegalStateException;

	boolean isMpuInit(final O op) {
		if (op instanceof CompositeOperation && OpType.CREATE.equals(op.type())) {
			final var compositeOp = (CompositeOperation) op;
			return !compositeOp.allSubOperationsDone() && compositeOp.get("uploadId") == null && !OpType.NOOP.equals(op.type());
		}
		return false;
	}

	boolean tryAcquireMpuObjectPermit(final O op) {
		if (!isMpuInit(op)) {
			return true;
		}
		ensureMpuScheduling(op);
		return mpuObjectThrottle == null || mpuObjectThrottle.tryAcquire();
	}

	void releaseMpuObjectPermit() {
		if (mpuObjectThrottle != null) {
			mpuObjectThrottle.release();
			signalDispatch();
		}
	}

	private void safeReleaseMpuObjectPermit(final O op) {
		if (OpType.CREATE.equals(op.type())) {
			final CompositeOperation<?> parentOp;
			if (op instanceof PartialOperation) {
				parentOp = ((PartialOperation<?>) op).parent();
			} else if (op instanceof CompositeOperation) {
				parentOp = (CompositeOperation<?>) op;
			} else {
				return;
			}
			synchronized (parentOp) {
				if (parentOp.get("permitReleased") == null) {
					parentOp.put("permitReleased", "true");
					if (mpuObjectThrottle != null) {
						mpuObjectThrottle.release();
						signalDispatch();
					}
				}
			}
		}
	}

	private void ensureMpuScheduling(final O op) {
		if (mpuSchedulingInitialized || !isMpuInit(op) || !(op instanceof CompositeOperation)) {
			return;
		}
		synchronized (mpuSchedulingLock) {
			if (mpuSchedulingInitialized) {
				return;
			}
			final int partsPerObject = Math.max(1, ((CompositeOperation<?>) op).subOperations().size());
			final int concurrency = effectiveMpuSchedulingConcurrency();
			final int activeMpuObjects;
			final int partWindowPerObject;

			if (configuredMpuObjectLimit > 0 && configuredMpuPartLimit > 0) {
				activeMpuObjects = configuredMpuObjectLimit;
				partWindowPerObject = configuredMpuPartLimit;
			} else if (configuredMpuObjectLimit > 0) {
				activeMpuObjects = configuredMpuObjectLimit;
				partWindowPerObject = Math.min(partsPerObject, ceilDiv(concurrency, activeMpuObjects));
			} else if (configuredMpuPartLimit > 0) {
				partWindowPerObject = Math.min(partsPerObject, configuredMpuPartLimit);
				activeMpuObjects = ceilDiv(concurrency, partWindowPerObject);
			} else {
				partWindowPerObject = Math.min(partsPerObject, concurrency);
				activeMpuObjects = ceilDiv(concurrency, partWindowPerObject);
			}

			mpuMaxParts = Math.max(1, partWindowPerObject);
			if (mpuObjectThrottle == null) {
				mpuObjectThrottle = new Semaphore(Math.max(1, activeMpuObjects), true);
			}

			logEffectiveMpuScheduling(partsPerObject, concurrency, Math.max(1, activeMpuObjects), mpuMaxParts);
			mpuSchedulingInitialized = true;
		}
	}

	private int effectiveMpuSchedulingConcurrency() {
		if (concurrencyLimit > 0) {
			return concurrencyLimit;
		}
		return Math.max(1, ioWorkerCount);
	}

	private static int ceilDiv(final int dividend, final int divisor) {
		return Math.max(1, (dividend + Math.max(1, divisor) - 1) / Math.max(1, divisor));
	}

	private void logEffectiveMpuScheduling(
					final int partsPerObject, final int concurrency, final int activeMpuObjects,
					final int partWindowPerObject) {
		Loggers.MSG.info(
						"{}: effective MPU scheduling: mpu_enabled=true, service_threads={}, parts_per_object={}, "
										+ "active_mpu_objects={}, part_window_per_object={}, explicit_object_limit={}, "
										+ "explicit_part_limit={}",
						toString(), concurrency, partsPerObject, activeMpuObjects, partWindowPerObject,
						configuredMpuObjectLimit > 0, configuredMpuPartLimit > 0);

		if (configuredMpuObjectLimit > 0 || configuredMpuPartLimit > 0) {
			final long theoreticalInFlightParts = (long) activeMpuObjects * partWindowPerObject;
			if (theoreticalInFlightParts < concurrency) {
				Loggers.MSG.warn(
								"{}: explicit MPU limits allow at most {} in-flight parts for {} service threads; "
												+ "some threads may be idle",
								toString(), theoreticalInFlightParts, concurrency);
			} else if (theoreticalInFlightParts > concurrency) {
				Loggers.MSG.warn(
								"{}: explicit MPU limits allow up to {} ready parts for {} service threads; "
												+ "extra work may increase queue pressure",
								toString(), theoreticalInFlightParts, concurrency);
			}
		}
	}

	private boolean enqueueChildOp(final O childOp, final O permitOwner, final String failureContext) {
		if (childOpQueue.offer(childOp)) {
			signalDispatch();
			return true;
		}
		try {
			ServiceTaskExecutor.VT_EXECUTOR.submit(() -> enqueueChildOpWithTimeout(childOp, permitOwner, failureContext));
			return true;
		} catch (final RejectedExecutionException e) {
			Loggers.ERR.error(
							"{}: Failed to schedule MPU {} enqueue; failing affected MPU",
							toString(), failureContext, e);
			childOp.status(Operation.Status.FAIL_TIMEOUT);
			safeReleaseMpuObjectPermit(permitOwner);
			return false;
		}
	}

	private void enqueueChildOpWithTimeout(final O childOp, final O permitOwner, final String failureContext) {
		try {
			if (childOpQueue.offer(childOp, childOpEnqueueTimeoutMillis(), TimeUnit.MILLISECONDS)) {
				signalDispatch();
				return;
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		failChildOpEnqueue(childOp, permitOwner, failureContext);
	}

	private void failChildOpEnqueue(final O childOp, final O permitOwner, final String failureContext) {
		Loggers.ERR.error(
						"{}: Timed out enqueueing MPU {} after {} ms; failing affected MPU",
						toString(), failureContext, childOpEnqueueTimeoutMillis());
		childOp.status(Operation.Status.FAIL_TIMEOUT);
		safeReleaseMpuObjectPermit(permitOwner);
	}

	private static long childOpEnqueueTimeoutMillis() {
		return Long.getLong(CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY, DEFAULT_CHILD_OP_ENQUEUE_TIMEOUT_MILLIS);
	}

	@SuppressWarnings("unchecked")
	protected final boolean handleCompleted(final O op) {
		final boolean accepted = super.handleCompleted(op);
		if (!accepted) {
			if (op instanceof CompositeOperation || op instanceof PartialOperation) {
				safeReleaseMpuObjectPermit(op);
			}
			return false;
		}

		completedOpCount.increment();
		if (op instanceof CompositeOperation) {
			final var parentOp = (CompositeOperation) op;
			if (!parentOp.allSubOperationsDone()) {
				if (op.status() == Operation.Status.SUCC) {
					final List<O> subOps = (List<O>) parentOp.nextSubOperations(mpuMaxParts > 0 ? mpuMaxParts : Integer.MAX_VALUE);
					for (final O nextSubOp : subOps) {
						if (!enqueueChildOp(nextSubOp, op, "part operation")) {
							return false;
						}
					}
				} else {
					safeReleaseMpuObjectPermit(op);
				}
			} else {
				safeReleaseMpuObjectPermit(op);
			}
		} else if (op instanceof PartialOperation) {
			final var subOp = (PartialOperation) op;
			final var parentOp = subOp.parent();
			List<O> nextChildOps = List.of();
			String childFailureContext = "part operation";
			boolean enqueueFinalization;
			synchronized (parentOp) {
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
						nextChildOps = List.of(op);
						childFailureContext = "part retry";
					} else {
						// retries exhausted — abort the MPU
						parentOp.put("mpuAbort", "true");
					}
				} else if (parentOp.get("mpuAbort") == null) {
					// Part succeeded, fetch next part if any
					nextChildOps = (List<O>) parentOp.nextSubOperations(1);
				}
				enqueueFinalization = shouldEnqueueFinalization(parentOp);
			}
			for (final O nextChildOp : nextChildOps) {
				if (!enqueueChildOp(nextChildOp, op, childFailureContext)) {
					return false;
				}
			}
			if (enqueueFinalization) {
				// Execute once again to finalize the composite operation, for example
				// completing the multipart upload. The finalization gate prevents
				// concurrent part completions from enqueueing the same parent twice.
				if (!enqueueChildOp((O) parentOp, op, "completion operation")) {
					return false;
				}
			}
		}
		signalDispatch();
		return true;
	}

	private boolean shouldEnqueueFinalization(final CompositeOperation parentOp) {
		if (parentOp.get("mpuAbort") != null) {
			return markFinalizationEnqueued(parentOp);
		}
		if (!parentOp.allSubOperationsDone()) {
			return false;
		}
		for (final var rawSubOp : parentOp.subOperations()) {
			final var subOp = (PartialOperation) rawSubOp;
			if (subOp.status() != Operation.Status.SUCC) {
				return false;
			}
		}
		return markFinalizationEnqueued(parentOp);
	}

	private boolean markFinalizationEnqueued(final CompositeOperation parentOp) {
		synchronized (parentOp) {
			if (parentOp.get(KEY_FINALIZATION_ENQUEUED) != null) {
				return false;
			}
			parentOp.put(KEY_FINALIZATION_ENQUEUED, Boolean.TRUE.toString());
			return true;
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

	@Override
	public void enableFastRecycleQuiesce() {
		this.fastRecycleQuiesceActive = true;
		Loggers.MSG.info("{}: fast-recycle quiesce active (dispatch task will extend idle wait)", toString());
	}

	/**
	 * Returns {@code true} when quiesce mode is active — i.e. the configured
	 * concurrency is low enough that fast-recycle handles most operations and
	 * the dispatch/generator VTs may park on long waits.
	 */
	protected boolean isFastRecycleQuiesceActive() {
		return fastRecycleQuiesceActive;
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
