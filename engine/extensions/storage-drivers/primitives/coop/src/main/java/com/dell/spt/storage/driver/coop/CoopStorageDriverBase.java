package com.dell.spt.storage.driver.coop;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Constants.LIFECYCLE_POLL_INTERVAL_MILLIS;
import static com.dell.spt.base.Constants.TASK_STOP_WAIT_SECONDS;
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
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.CloseableThreadContext;

public abstract class CoopStorageDriverBase<I extends Item, O extends Operation<I>>
				extends StorageDriverBase<I, O> implements StorageDriver<I, O> {

	static final int MAX_PART_RETRIES = 3;
	static final String CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY = "spt.mpu.child.enqueue.timeout.millis";
	static final long DEFAULT_CHILD_OP_ENQUEUE_TIMEOUT_MILLIS = 30_000L;
	/** Experimental: completion-driven direct dispatch for built-in transport drivers. */
	static final String DIRECT_DISPATCH_PROPERTY = "spt.dispatch.direct";
	private static final String KEY_FINALIZATION_ENQUEUED = "sptFinalizationEnqueued";

	protected final Semaphore concurrencyThrottle;
	protected volatile Semaphore mpuObjectThrottle;
	protected volatile int mpuMaxParts;
	protected final BlockingQueue<O> childOpQueue;
	private final BlockingQueue<O> inOpQueue;
	private final LongAdder scheduledOpCount = new LongAdder();
	private final LongAdder completedOpCount = new LongAdder();
	private final ReentrantLock dispatchLock = new ReentrantLock();
	// Admission and dispatch intentionally share one lock to preserve their shutdown boundary.
	private final ReentrantLock admissionLock = dispatchLock;
	private final OperationDispatchTask<I, O> opDispatchTask;
	private final Object mpuSchedulingLock = new Object();
	private final int configuredMpuObjectLimit;
	private final int configuredMpuPartLimit;
	private final boolean directDispatchEnabled = Boolean.getBoolean(DIRECT_DISPATCH_PROPERTY);
	private volatile boolean mpuSchedulingInitialized = false;
	/**
	 * @deprecated retained for binary compatibility only; SPT never reads this field and writes
	 *             have no effect
	 */
	@Deprecated
	protected volatile int fastRecycleConcurrencyThreshold = 0;
	private volatile boolean fastRecycleWarningLogged;
	private volatile boolean admissionOpen = true;

	protected CoopStorageDriverBase(
					final String testStepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag);
		enableOperationLifecycle();
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
						dispatchLock, dispatchLock.newCondition(), inQueueLimit);
	}

	@Override
	protected void doStart() throws IllegalStateException {
		admissionOpen = true;
		if (opDispatchTask.isStopped()) {
			opDispatchTask.restart();
		} else {
			opDispatchTask.start();
		}
	}

	@Override
	public final boolean put(final O op) {
		if (!isStarted() || !admissionOpen) {
			throwUnchecked(new EOFException());
		}
		if (inOpQueue.remainingCapacity() == 0 || !prepare(op)) {
			return false;
		}
		admissionLock.lock();
		try {
			if (!admissionOpen) {
				throwUnchecked(new EOFException());
			}
			if (offerIncomingOperation(op)) {
				scheduledOpCount.increment();
				signalDispatch();
				return true;
			}
			return false;
		} finally {
			admissionLock.unlock();
		}
	}

	@Override
	public final int put(final List<O> ops, final int from, final int to) {
		if (!isStarted() || !admissionOpen) {
			throwUnchecked(new EOFException());
		}
		var i = from;
		while (i < to && isStarted()) {
			if (inOpQueue.remainingCapacity() == 0) {
				break;
			}
			final O nextOp = ops.get(i);
			if (!prepare(nextOp)) {
				break;
			}
			admissionLock.lock();
			try {
				if (admissionOpen && offerIncomingOperation(nextOp)) {
					i++;
				} else {
					break;
				}
			} finally {
				admissionLock.unlock();
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
		if (!isStarted() || !admissionOpen) {
			throwUnchecked(new EOFException());
		}
		var n = 0;
		for (final var nextOp : ops) {
			if (!isStarted() || inOpQueue.remainingCapacity() == 0 || !prepare(nextOp)) {
				break;
			}
			admissionLock.lock();
			try {
				if (admissionOpen && offerIncomingOperation(nextOp)) {
					n++;
				} else {
					break;
				}
			} finally {
				admissionLock.unlock();
			}
		}
		scheduledOpCount.add(n);
		if (n > 0) {
			signalDispatch();
		}
		return n;
	}

	private boolean offerIncomingOperation(final O op) {
		dispatchLock.lock();
		try {
			if (!inOpQueue.offer(op)) {
				return false;
			}
			if (operationLifecycle().driverQueued(op)) {
				return true;
			}
			inOpQueue.removeIf(queuedOp -> queuedOp == op);
			return false;
		} finally {
			dispatchLock.unlock();
		}
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

	/**
	 * Bounds one submission call to the operations which may acquire transport capacity now.
	 * The dispatch task uses the same bound for its shutdown-recovery snapshot, avoiding work
	 * proportional to a much larger queued batch on every small permit release.
	 */
	int dispatchAttemptLimit(final int bufferedOperationCount) {
		return Math.max(
						0,
						Math.min(bufferedOperationCount, concurrencyThrottle.availablePermits()));
	}

	/**
	 * Returns whether completion-driven direct dispatch is enabled for this driver. Read once from
	 * {@value #DIRECT_DISPATCH_PROPERTY}; off by default so the dispatcher path is unchanged.
	 */
	protected final boolean directDispatchEnabled() {
		return directDispatchEnabled;
	}

	/** Operations the dispatch task has drained but not yet submitted. */
	protected final int dispatcherBacklog() {
		final var task = this.opDispatchTask;
		return task == null ? 0 : task.backlog();
	}

	/**
	 * Takes the next plain operation for a caller which already holds a concurrency permit and a
	 * transport channel, crossing the dispatch boundary under the admission lock. Returns
	 * {@code null} when the caller must instead release its permit and wake the dispatcher: the
	 * queue is empty, the head is composite, partial or NOOP work, child operations are pending,
	 * the dispatcher retains a backlog, or admission has closed. A returned operation is already
	 * {@code DISPATCHED}; the caller owns starting its transport or completing it as failed.
	 */
	protected final O pollForDirectDispatch() {
		if (admissionLock == null || !isStarted() || dispatcherBacklog() > 0) {
			return null;
		}
		admissionLock.lock();
		try {
			if (!admissionOpen || !childOpQueue.isEmpty()) {
				return null;
			}
			final O head = inOpQueue.peek();
			if (head == null || !isDirectDispatchEligible(head)) {
				return null;
			}
			inOpQueue.poll();
			if (markDispatchOwnership(head)) {
				return head;
			}
			operationLifecycle().unattempted(head);
			return null;
		} finally {
			admissionLock.unlock();
		}
	}

	/** Plain operations only: composite and partial work keeps the dispatcher's MPU accounting. */
	protected static boolean isDirectDispatchEligible(final Operation<?> op) {
		return !(op instanceof CompositeOperation)
						&& !(op instanceof PartialOperation)
						&& !OpType.NOOP.equals(op.type());
	}

	/** Returns whether new transport dispatch may begin. */
	protected final boolean isAdmissionOpen() {
		// Constructor-bypassing test doubles predate the gate and have no admission lock.
		return admissionLock == null || admissionOpen;
	}

	/** Atomically crosses the actual dispatch boundary while admission remains open. */
	protected final boolean beginDispatch(final O op) {
		if (admissionLock == null) {
			return markDispatchOwnership(op);
		}
		admissionLock.lock();
		try {
			return admissionOpen && markDispatchOwnership(op);
		} finally {
			admissionLock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	private boolean markDispatchOwnership(final O op) {
		final var lifecycle = operationLifecycle();
		final var state = lifecycle.stateOf(op);
		if (op instanceof CompositeOperation && state == OperationLifecycleState.DISPATCHED) {
			// A composite remains the authoritative in-flight owner while its children run.
			// Its finalization request is continuation of that attempt, not a second attempt.
			return true;
		}
		if (op instanceof PartialOperation<?> partialOp) {
			final O parent = (O) partialOp.parent();
			final var parentState = lifecycle.stateOf(parent);
			if (parentState == OperationLifecycleState.DRIVER_QUEUED) {
				if (!lifecycle.dispatched(parent)) {
					return false;
				}
			} else if (parentState != OperationLifecycleState.DISPATCHED
							&& parentState != OperationLifecycleState.NEW) {
				return false;
			}
		}
		return markOperationDispatched(op);
	}

	/**
	 * Returns whether a successful {@link #submit(Operation)} return proves that the submitted
	 * operation itself started transport. The compatibility default also treats a submit call
	 * which outlives dispatcher shutdown as indeterminate unless another member of that call
	 * crossed the explicit {@link #beginDispatch(Operation)} boundary. Drivers whose successful
	 * submission does not itself start transport return {@code false}.
	 */
	protected boolean successfulSubmitStartsTransport(final O op) {
		return true;
	}

	protected abstract boolean submit(final O op) throws IllegalStateException;

	/**
	 * Submits a contiguous prefix of the operations in the half-open range
	 * {@code [from, to)}. Implementations process the range in encounter order and stop before
	 * the first operation they cannot accept.
	 *
	 * @return the number {@code n} of accepted operations, where
	 *         {@code 0 <= n <= to - from}; the accepted operations must be exactly
	 *         {@code [from, from + n)}, and the remaining suffix stays caller-owned
	 */
	protected abstract int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException;

	/**
	 * Submits a contiguous prefix of {@code ops} under the same ordering and ownership contract
	 * as {@link #submit(List, int, int)}.
	 *
	 * @return the number of operations accepted from the start of the list
	 */
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
		if (admissionLock == null) {
			if (!claimCompositeParentOwnership(childOp, permitOwner)
							|| !claimChildQueueOwnership(childOp)) {
				return false;
			}
			return tryEnqueueChildOp(childOp)
							|| scheduleChildOpEnqueue(childOp, permitOwner, failureContext);
		}
		admissionLock.lock();
		try {
			if (!claimCompositeParentOwnership(childOp, permitOwner)) {
				return false;
			}
			if (!admissionOpen) {
				claimChildQueueOwnership(childOp);
				operationLifecycle().unattempted(childOp);
				recoverQueuedCompositeParent(childOp, permitOwner);
				safeReleaseMpuObjectPermit(permitOwner);
				return false;
			}
			if (!claimChildQueueOwnership(childOp)) {
				return false;
			}
			if (tryEnqueueChildOp(childOp)) {
				return true;
			}
		} finally {
			admissionLock.unlock();
		}
		return scheduleChildOpEnqueue(childOp, permitOwner, failureContext);
	}

	/**
	 * Enqueues extension-created composite work through the lifecycle-aware admission gate.
	 * Existing subclasses which create child operations should use this seam instead of writing
	 * {@link #childOpQueue} directly so shutdown can always recover undispatched ownership.
	 */
	protected final boolean enqueueChildOperation(
					final O childOp, final O permitOwner, final String failureContext) {
		return enqueueChildOp(childOp, permitOwner, failureContext);
	}

	/**
	 * Atomically claims lifecycle ownership for a composite expansion before any child can cross
	 * dispatch. Queue pressure may defer individual enqueue operations, but no executor task is
	 * ever the sole owner of a child identity.
	 */
	protected final boolean enqueueChildOperations(
					final List<O> childOps, final O permitOwner, final String failureContext) {
		if (childOps.isEmpty()) {
			return true;
		}
		if (admissionLock == null) {
			return claimAndEnqueueChildOperations(childOps, permitOwner, failureContext);
		}
		admissionLock.lock();
		try {
			if (!claimCompositeParentOwnership(childOps.get(0), permitOwner)) {
				return false;
			}
			if (!admissionOpen) {
				for (final O childOp : childOps) {
					claimChildQueueOwnership(childOp);
					operationLifecycle().unattempted(childOp);
				}
				recoverQueuedCompositeParent(childOps.get(0), permitOwner);
				safeReleaseMpuObjectPermit(permitOwner);
				return false;
			}
			return claimAndEnqueueChildOperations(childOps, permitOwner, failureContext);
		} finally {
			admissionLock.unlock();
		}
	}

	private boolean claimAndEnqueueChildOperations(
					final List<O> childOps, final O permitOwner, final String failureContext) {
		if (!claimCompositeParentOwnership(childOps.get(0), permitOwner)) {
			return false;
		}
		final var claimed = new ArrayList<O>(childOps.size());
		var allClaimed = true;
		for (final O childOp : childOps) {
			if (claimChildQueueOwnership(childOp)) {
				claimed.add(childOp);
			} else {
				allClaimed = false;
			}
		}
		if (!allClaimed) {
			for (final O claimedChild : claimed) {
				operationLifecycle().unattempted(claimedChild);
			}
			safeReleaseMpuObjectPermit(permitOwner);
			return false;
		}
		for (var i = 0; i < claimed.size(); i++) {
			if (!tryEnqueueChildOp(claimed.get(i))) {
				return scheduleChildOpsEnqueue(
								List.copyOf(claimed.subList(i, claimed.size())), permitOwner, failureContext);
			}
		}
		return true;
	}

	private boolean scheduleChildOpEnqueue(
					final O childOp, final O permitOwner, final String failureContext) {
		return scheduleChildOpsEnqueue(List.of(childOp), permitOwner, failureContext);
	}

	private boolean scheduleChildOpsEnqueue(
					final List<O> childOps, final O permitOwner, final String failureContext) {
		try {
			executeChildEnqueue(() -> {
				for (var i = 0; i < childOps.size(); i++) {
					if (!enqueueChildOpWithTimeout(childOps.get(i), permitOwner, failureContext)) {
						for (var j = i + 1; j < childOps.size(); j++) {
							operationLifecycle().unattempted(childOps.get(j));
						}
						return;
					}
				}
			});
			return true;
		} catch (final RejectedExecutionException e) {
			Loggers.ERR.error(
							"{}: Failed to schedule MPU {} enqueue; failing affected MPU",
							toString(), failureContext, e);
			for (final O childOp : childOps) {
				childOp.status(Operation.Status.FAIL_TIMEOUT);
				operationLifecycle().unattempted(childOp);
			}
			safeReleaseMpuObjectPermit(permitOwner);
			return false;
		}
	}

	/** Scheduling seam for testing executor rejection without replacing the shared executor. */
	protected void executeChildEnqueue(final Runnable task) {
		ServiceTaskExecutor.VT_EXECUTOR.submit(task);
	}

	private boolean tryEnqueueChildOp(final O childOp) {
		if (!childOpQueue.offer(childOp)) {
			return false;
		}
		signalDispatch();
		return true;
	}

	private boolean claimChildQueueOwnership(final O childOp) {
		final var lifecycle = operationLifecycle();
		return !lifecycle.isEnabled()
						|| lifecycle.stateOf(childOp) == OperationLifecycleState.DRIVER_QUEUED
						|| childOp instanceof CompositeOperation
										&& lifecycle.stateOf(childOp) == OperationLifecycleState.DISPATCHED
						|| lifecycle.driverQueued(childOp);
	}

	@SuppressWarnings("unchecked")
	private O compositeParent(final O childOp, final O permitOwner) {
		final CompositeOperation<?> parent;
		if (childOp instanceof PartialOperation<?> partialOp) {
			parent = partialOp.parent();
		} else if (permitOwner instanceof PartialOperation<?> partialOwner) {
			parent = partialOwner.parent();
		} else if (permitOwner instanceof CompositeOperation<?> compositeOwner) {
			parent = compositeOwner;
		} else {
			return null;
		}
		return (O) parent;
	}

	private boolean claimCompositeParentOwnership(final O childOp, final O permitOwner) {
		final O parentOp = compositeParent(childOp, permitOwner);
		if (parentOp == null) {
			return true;
		}
		final var lifecycle = operationLifecycle();
		final var state = lifecycle.stateOf(parentOp);
		return !lifecycle.isEnabled()
						|| state == OperationLifecycleState.DRIVER_QUEUED
						|| state == OperationLifecycleState.DISPATCHED
						|| lifecycle.driverQueued(parentOp);
	}

	private void recoverQueuedCompositeParent(final O childOp, final O permitOwner) {
		final O parentOp = compositeParent(childOp, permitOwner);
		if (parentOp != null
						&& operationLifecycle().stateOf(parentOp) == OperationLifecycleState.DRIVER_QUEUED) {
			operationLifecycle().unattempted(parentOp);
		}
	}

	private boolean enqueueChildOpWithTimeout(final O childOp, final O permitOwner, final String failureContext) {
		if (admissionLock == null) {
			try {
				if (childOpQueue.offer(childOp, childOpEnqueueTimeoutMillis(), TimeUnit.MILLISECONDS)) {
					operationLifecycle().driverQueued(childOp);
					signalDispatch();
					return true;
				}
			} catch (final InterruptedException e) {
				operationLifecycle().unattempted(childOp);
				safeReleaseMpuObjectPermit(permitOwner);
				throwUnchecked(e);
				return false;
			}
			failChildOpEnqueue(childOp, permitOwner, failureContext);
			return false;
		}
		final long deadline = System.nanoTime()
						+ TimeUnit.MILLISECONDS.toNanos(childOpEnqueueTimeoutMillis());
		while (System.nanoTime() < deadline) {
			admissionLock.lock();
			try {
				if (!admissionOpen) {
					operationLifecycle().unattempted(childOp);
					safeReleaseMpuObjectPermit(permitOwner);
					return false;
				}
				if (tryEnqueueChildOp(childOp)) {
					return true;
				}
			} finally {
				admissionLock.unlock();
			}
			try {
				TimeUnit.MILLISECONDS.sleep(LIFECYCLE_POLL_INTERVAL_MILLIS);
			} catch (final InterruptedException e) {
				operationLifecycle().unattempted(childOp);
				safeReleaseMpuObjectPermit(permitOwner);
				throwUnchecked(e);
				return false;
			}
		}
		failChildOpEnqueue(childOp, permitOwner, failureContext);
		return false;
	}

	private void failChildOpEnqueue(final O childOp, final O permitOwner, final String failureContext) {
		Loggers.ERR.error(
						"{}: Timed out enqueueing MPU {} after {} ms; failing affected MPU",
						toString(), failureContext, childOpEnqueueTimeoutMillis());
		childOp.status(Operation.Status.FAIL_TIMEOUT);
		operationLifecycle().unattempted(childOp);
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
					enqueueChildOperations(subOps, op, "part operation");
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
						parentOp.put("mpuFailure", op.status().name());
						parentOp.put("mpuAbort", "true");
					}
				} else if (parentOp.get("mpuAbort") == null) {
					// Part succeeded, fetch next part if any
					nextChildOps = (List<O>) parentOp.nextSubOperations(1);
				}
				enqueueFinalization = shouldEnqueueFinalization(parentOp);
			}
			enqueueChildOperations(nextChildOps, op, childFailureContext);
			if (enqueueFinalization) {
				// Execute once again to finalize the composite operation, for example
				// completing the multipart upload. The finalization gate prevents
				// concurrent part completions from enqueueing the same parent twice.
				enqueueChildOp((O) parentOp, op, "completion operation");
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
	 * a completion frees capacity (handleCompleted). Lock-free so transport
	 * event loops never contend on {@code dispatchLock}: the task re-checks its
	 * wait condition before parking and an unpark permit is sticky, so delivery
	 * is guaranteed without holding a lock across the signal.
	 */
	private void signalDispatch() {
		final var task = this.opDispatchTask;
		if (task != null) {
			task.unpark();
		}
	}

	/** Wakes dispatch after a subclass recovery path returns concurrency capacity. */
	protected final void signalDispatchCapacityAvailable() {
		signalDispatch();
	}

	/** @deprecated always returns {@code false}; direct fast recycle was removed */
	@Deprecated
	protected boolean isFastRecycleEnabled() {
		return false;
	}

	/** @deprecated always returns {@code false}; direct fast-recycle quiescing was removed */
	@Deprecated
	protected boolean isFastRecycleQuiesceActive() {
		return false;
	}

	/**
	 * @param op ignored
	 * @deprecated always returns {@code false}; completed operations use shared circulation
	 */
	@Deprecated
	protected boolean isFastRecycleEligible(final O op) {
		return false;
	}

	@Override
	@Deprecated
	public void enableFastRecycle(final int concurrencyThreshold) {
		warnFastRecycleDisabled();
	}

	@Override
	@Deprecated
	public void enableFastRecycleQuiesce() {
		warnFastRecycleDisabled();
	}

	private synchronized void warnFastRecycleDisabled() {
		if (!fastRecycleWarningLogged) {
			fastRecycleWarningLogged = true;
			Loggers.MSG.warn(
							"{}: deprecated fast-recycle request ignored; completed operations use shared generator circulation",
							toString());
		}
	}

	@Override
	protected void doShutdown() {
		closeAdmission();
		recoverQueuedOperations();
		Loggers.MSG.debug("{}: shut down", toString());
	}

	@Override
	public final void closeAdmission() {
		final boolean taskWasRunning = opDispatchTask.isStarted();
		final boolean taskWasAlreadyStopped = opDispatchTask.isStopped();
		admissionLock.lock();
		try {
			admissionOpen = false;
		} finally {
			admissionLock.unlock();
		}
		opDispatchTask.stop();
		signalDispatch();
		if (taskWasRunning || taskWasAlreadyStopped) {
			try {
				if (!opDispatchTask.await(TASK_STOP_WAIT_SECONDS, TimeUnit.SECONDS)) {
					Loggers.MSG.warn(
									"{}: dispatch task did not stop within {} second(s); "
													+ "queued lifecycle ownership will be recovered without touching task buffers",
									toString(), TASK_STOP_WAIT_SECONDS);
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
		}
	}

	@Override
	public final List<O> recoverQueuedOperations() {
		final Set<O> indeterminateSubmissions = Collections.newSetFromMap(new IdentityHashMap<>());
		final var submittingOperations = opDispatchTask.submittingOperations();
		final boolean explicitDispatchObserved = submittingOperations.stream()
						.map(OperationDispatchTask.SubmittingOperation::dispatchToken)
						.anyMatch(operationLifecycle()::hasExplicitDispatchBoundary);
		for (final var submittingOperation : submittingOperations) {
			final O op = submittingOperation.operation();
			if (!explicitDispatchObserved
							&& successfulSubmitStartsTransport(op)
							&& operationLifecycle().unresolvedSubmission(submittingOperation.dispatchToken())) {
				indeterminateSubmissions.add(op);
			}
		}
		final Set<O> recovered = Collections.newSetFromMap(new IdentityHashMap<>());
		// The lifecycle registry is the authoritative handoff. It includes operations in
		// dispatcher-owned local buffers without racing those non-thread-safe buffers when a
		// custom submit implementation ignores interruption beyond the bounded stop wait.
		recovered.addAll(operationLifecycle().driverQueuedOperations());
		childOpQueue.drainTo(recovered);
		inOpQueue.drainTo(recovered);
		recovered.addAll(recoverAdditionalQueuedOperations());
		recovered.removeAll(indeterminateSubmissions);
		final var unattempted = new ArrayList<O>(recovered.size());
		for (final O op : recovered) {
			if (operationLifecycle().unattempted(op)) {
				unattempted.add(op);
			}
		}
		return List.copyOf(unattempted);
	}

	/** Extension hook for cooperative drivers with an additional pre-dispatch queue. */
	protected List<O> recoverAdditionalQueuedOperations() {
		return List.of();
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
			closeAdmission();
			recoverQueuedOperations();
			opDispatchTask.close();
		}
	}
}
