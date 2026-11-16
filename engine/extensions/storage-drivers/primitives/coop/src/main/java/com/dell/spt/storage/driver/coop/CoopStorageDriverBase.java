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
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.CompositeResults;
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
import java.util.function.BooleanSupplier;
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
	/** Completion-driven direct dispatch; set to false to use dispatcher scheduling. */
	static final String DIRECT_DISPATCH_PROPERTY = "spt.dispatch.direct";
	private static final String KEY_FINALIZATION_ENQUEUED = "sptFinalizationEnqueued";
	/** The upload ID a multipart driver records on the upload once its initiation is answered. */
	private static final String KEY_UPLOAD_ID = "uploadId";

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
	private volatile boolean dispatchStoppedInTime = true;
	private final Object mpuSchedulingLock = new Object();
	private final int configuredMpuObjectLimit;
	private final int configuredMpuPartLimit;
	// Evaluated once so the per-operation path reads one field. supportsDirectDispatch() must
	// therefore be a constant for the subclass, which is all the capability declaration is. Only
	// disableDirectDispatch() writes it afterwards, before the first operation.
	private boolean directDispatchEnabled = Boolean.parseBoolean(System.getProperty(DIRECT_DISPATCH_PROPERTY, "true"))
					&& supportsDirectDispatch();
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
		} catch (final NoSuchElementException | InvalidValuePathException ignored) {
			// Older extension schemas omit optional multipart limits; retain the unlimited default.
		} catch (final Exception e) {
			Loggers.ERR.warn("{}: Failed to parse multipart limits: {}. Proceeding with unlimited.", testStepId, e.getMessage());
		}
		try {
			maxParts = storageConfig.intVal("driver-limit-multipart-parts");
		} catch (final NoSuchElementException | InvalidValuePathException ignored) {
			// Older extension schemas omit optional multipart limits; retain the unlimited default.
		} catch (final Exception e) {
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
		onAdmissionOpened();
	}

	/**
	 * Called when admission opens, as the driver starts or restarts, before any operation of the
	 * new cycle is dispatched. A driver that keeps state from {@link #onAdmissionClosed()} resets
	 * it here, so that a stop of an earlier cycle cannot act on the requests of this one.
	 */
	protected void onAdmissionOpened() {}

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
			return offerIncomingOperationLocked(op, inOpQueue);
		} finally {
			dispatchLock.unlock();
		}
	}

	/**
	 * Queue/lifecycle handoff under the driver's admission lock. Retained-attempt adapters may
	 * override this to preserve dispatched custody. They must reject duplicate tokens before
	 * offering, leave ownership unchanged on queue refusal, and never retain the queue reference.
	 */
	protected boolean offerIncomingOperationLocked(final O op, final BlockingQueue<O> queue) {
		if (!queue.offer(op)) {
			return false;
		}
		if (operationLifecycle().driverQueued(op)) {
			return true;
		}
		queue.removeIf(queuedOp -> queuedOp == op);
		return false;
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

	/**
	 * Check if the concurrency throttle has available permits. Used by OperationDispatchTask to
	 * double-check before entering backpressure await. A driver with transport capacity beyond
	 * the throttle overrides it, so that the task parks instead of spinning while that capacity
	 * is exhausted; whatever frees it must then call {@link #signalDispatchCapacityAvailable()}.
	 */
	protected boolean hasAvailableDispatchCapacity() {
		return concurrencyThrottle.availablePermits() > 0;
	}

	/**
	 * Bounds one submission call to the operations which may acquire transport capacity now.
	 * The dispatch task uses the same bound for its shutdown-recovery snapshot, avoiding work
	 * proportional to a much larger queued batch on every small permit release.
	 */
	protected int dispatchAttemptLimit(final int bufferedOperationCount) {
		return Math.max(
						0,
						Math.min(bufferedOperationCount, concurrencyThrottle.availablePermits()));
	}

	/**
	 * Returns whether completion-driven direct dispatch is enabled for this driver: the JVM-wide
	 * {@value #DIRECT_DISPATCH_PROPERTY} defaults to true and the driver declares the capability
	 * through {@link #supportsDirectDispatch()}. Set the property to false before constructing the
	 * driver to retain dispatcher scheduling. Drivers without the capability always use the dispatcher.
	 */
	protected final boolean directDispatchEnabled() {
		return directDispatchEnabled;
	}

	/**
	 * Turns direct dispatch off for a driver that declares the capability but learns only as it
	 * starts that its transport cannot use it. Call it in {@code doStart()} before
	 * {@code super.doStart()} of the first start: that start, which starts the dispatch task,
	 * publishes the change to the task and to the event loops of every later completion.
	 */
	protected final void disableDirectDispatch() {
		directDispatchEnabled = false;
	}

	/**
	 * Declares that this driver's completion path calls {@link #pollForDirectDispatch()} and can
	 * transfer its permit and transport to the returned operation. Must return a constant: it is
	 * read once during {@code CoopStorageDriverBase} construction, before subclass fields exist.
	 */
	protected boolean supportsDirectDispatch() {
		return false;
	}

	/**
	 * Declares whether this driver times a multipart upload's completion or abort as a new exchange
	 * of its own, calling {@link Operation#startRequest()} for it. When this returns {@code true},
	 * the parent's timing is reset before that exchange, so the earlier exchange's response times
	 * do not make the new request look finished after its response started, and the parent's
	 * result carries the completion's or abort's own times.
	 *
	 * <p>The default is {@code false}: the parent keeps its earlier timing through the completion
	 * or abort, as before this hook existed. A driver that calls {@code startRequest()} for the
	 * completion or abort must return {@code true}. A composite READ's finalization is never reset,
	 * because it sends no request.
	 */
	protected boolean timesUploadFinalization() {
		return false;
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
			if (claimDispatchOwnership(head)) {
				return head;
			}
			recoverQueuedOperation(head);
			return null;
		} finally {
			admissionLock.unlock();
		}
	}

	/**
	 * A cooperative driver enqueues a successful, unfinished composite's parts in
	 * {@link #handleCompleted}, and then the composite once more to finalize it.
	 */
	@Override
	public final CompositeResults compositeResults() {
		return CompositeResults.IN_PARTS;
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
			return claimDispatchOwnership(op);
		}
		admissionLock.lock();
		try {
			return admissionOpen && claimDispatchOwnership(op);
		} finally {
			admissionLock.unlock();
		}
	}

	/**
	 * Runs a bounded transport-handoff action while admission is open. Range adapters use this
	 * at the actual execute/write boundary, validating their captured attempt inside the action.
	 * The action must not wait for I/O or invoke result output. Returning from an earlier
	 * beginDispatch call is not permission to hand off after admission closes.
	 */
	protected final boolean withDispatchAdmission(final BooleanSupplier handoff) {
		if (admissionLock == null) {
			return handoff.getAsBoolean();
		}
		admissionLock.lock();
		try {
			return admissionOpen && handoff.getAsBoolean();
		} finally {
			admissionLock.unlock();
		}
	}

	/**
	 * As {@link #beginDispatch}, for a worker that holds {@code op} past its admission: when the
	 * closed admission refuses it, the composite it continues is settled before this returns false,
	 * as {@link #settleClosedDispatch} settles it. A refusal while admission is open, and a dispatch,
	 * run exactly as {@link #beginDispatch} runs them.
	 */
	protected final boolean beginDispatchOrSettle(final O op) {
		if (admissionLock == null) {
			return claimDispatchOwnership(op);
		}
		admissionLock.lock();
		try {
			if (admissionOpen) {
				return claimDispatchOwnership(op);
			}
		} finally {
			admissionLock.unlock();
		}
		settleClosedDispatch(op);
		return false;
	}

	/**
	 * As {@link #withDispatchAdmission(BooleanSupplier)}, for the transport claim of {@code op}: when
	 * the closed admission refuses it, the composite it continues is settled before this returns
	 * false, as {@link #settleClosedDispatch} settles it. A finalization or abort the stop refuses is
	 * in no queue, so nothing else would settle it. A refusal while admission is open, a stale
	 * circulation or a part of an ended upload, and a successful hand-off run exactly as {@link
	 * #withDispatchAdmission(BooleanSupplier)} runs them.
	 */
	protected final boolean withDispatchAdmission(final BooleanSupplier handoff, final O op) {
		if (admissionLock == null) {
			return handoff.getAsBoolean();
		}
		admissionLock.lock();
		try {
			if (admissionOpen) {
				return handoff.getAsBoolean();
			}
		} finally {
			admissionLock.unlock();
		}
		settleClosedDispatch(op);
		return false;
	}

	/**
	 * A transport attempt that names, only when the closed admission refuses its claim, the operation
	 * that claim was for. An attempt that holds its operation in a field passes itself, so that a
	 * claim admission allows reads nothing more than {@link #withDispatchAdmission(BooleanSupplier)}
	 * reads.
	 */
	protected interface DispatchRefusal<O> {

		/** The operation of the refused claim, whose composite the closed admission settles. */
		O refusedOperation();
	}

	/**
	 * As {@link #withDispatchAdmission(BooleanSupplier, Operation)}, with the operation asked of
	 * {@code refusal} only when the closed admission refuses the claim. A refusal while admission is
	 * open, and a successful hand-off, run exactly as {@link #withDispatchAdmission(BooleanSupplier)}
	 * runs them.
	 */
	protected final boolean withDispatchAdmission(final BooleanSupplier handoff, final DispatchRefusal<O> refusal) {
		if (admissionLock == null) {
			return handoff.getAsBoolean();
		}
		admissionLock.lock();
		try {
			if (admissionOpen) {
				return handoff.getAsBoolean();
			}
		} finally {
			admissionLock.unlock();
		}
		settleClosedDispatch(refusal.refusedOperation());
		return false;
	}

	/**
	 * Called under admission lock by queued and direct dispatch. The compatibility default
	 * records logical dispatch; retained-attempt adapters may defer that record until actual
	 * transport handoff and must fence stale/duplicate attempt tokens themselves.
	 */

	protected boolean claimDispatchOwnership(final O op) {
		return claimCapturedDispatch(op, null);
	}

	/**
	 * The compatibility claim for a circulation captured earlier, which a driver deferring custody
	 * from admission to its request write makes at the write, under the admission lock. With a
	 * {@code token} from {@code queuedDispatchToken}, the operation is marked dispatched only if the
	 * token still names its queued circulation; a {@code null} token marks the current one. A
	 * composite's finalization continues its dispatched attempt and a part's parent is claimed
	 * first, as at admission.
	 */
	@SuppressWarnings("unchecked")
	protected final boolean claimCapturedDispatch(final O op, final OperationLifecycleTracker.DispatchToken<O> token) {
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
				if (state == OperationLifecycleState.DRIVER_QUEUED
								&& (hasEnded(parentState)
												|| (parentState == OperationLifecycleState.COMPLETING
																&& isPartOfFinalizedUpload(op, parent)))) {
					// A part still queued when its upload ended, for example one enqueued while the
					// upload's abort ran, is never sent. It is settled here, so the dispatcher drops
					// it instead of offering it again, and the ended upload is not claimed again. An
					// upload whose finalization or abort is enqueued has ended once that request's
					// completion runs; the initiation's completion runs before the finalization flag
					// can be set, so a part of a live upload is only refused there.
					lifecycle.unattempted(op);
				}
				return false;
			}
		}
		return token == null ? markOperationDispatched(op) : lifecycle.explicitlyDispatched(token);
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
		if (op instanceof CompositeOperation compositeOp && OpType.CREATE.equals(op.type())) {
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
				settleRefusedCompositeParent(childOp, permitOwner);
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
				settleRefusedCompositeParent(childOps.get(0), permitOwner);
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
						|| (childOp instanceof CompositeOperation
										&& lifecycle.stateOf(childOp) == OperationLifecycleState.DISPATCHED)
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
						|| (!isPartOfFinalizedUpload(childOp, parentOp) && lifecycle.driverQueued(parentOp));
	}

	/**
	 * Whether {@code childOp} is a part of an upload whose last request, its completion or its
	 * abort, is already enqueued. A sibling that succeeds just before the abort may still enqueue
	 * the part it fetched; claiming the upload for that part would start a lifecycle the upload
	 * never ends, so the part is dropped instead.
	 */
	private static boolean isPartOfFinalizedUpload(final Operation<?> childOp, final Operation<?> parentOp) {
		return childOp instanceof PartialOperation
						&& parentOp instanceof CompositeOperation<?> compositeOp
						&& compositeOp.get(KEY_FINALIZATION_ENQUEUED) != null;
	}

	private static boolean hasEnded(final OperationLifecycleState state) {
		return state == OperationLifecycleState.TERMINAL
						|| state == OperationLifecycleState.UNATTEMPTED
						|| state == OperationLifecycleState.UNRESOLVED;
	}

	/**
	 * Settles the composite operation whose next request, a part, a range or its finalization, the
	 * closed admission refused, or that queue recovery took back. Runs only after admission has
	 * closed, so the open-admission path never reaches it.
	 *
	 * <p>A composite that has not dispatched anything of this pass is unattempted, unless it is an
	 * upload whose initiation was answered: that upload is open on the target, so it is unresolved.
	 * A dispatched composite can no longer finish, so it is unresolved at once instead of at the
	 * drain's deadline, which would hold the step's end for {@code load-op-wait-limit}. Its parts
	 * still in flight end on their own; their completions cannot claim the settled composite again,
	 * because an unresolved operation starts no new lifecycle.
	 *
	 * <p>A refused part of a composite whose finalization or abort is already enqueued leaves the
	 * composite to that request: it is either in flight and ends the composite, or queued, and then
	 * refused or recovered itself.
	 */
	private void settleRefusedCompositeParent(final O childOp, final O permitOwner) {
		final O parentOp = compositeParent(childOp, permitOwner);
		if (parentOp == null) {
			return;
		}
		final var lifecycle = operationLifecycle();
		final var state = lifecycle.stateOf(parentOp);
		if (state == OperationLifecycleState.DRIVER_QUEUED) {
			if (isInitiatedUpload(parentOp)) {
				if (lifecycle.unresolvedSubmission(parentOp)) {
					safeReleaseMpuObjectPermit(parentOp);
				}
			} else {
				lifecycle.unattempted(parentOp);
			}
		} else if (state == OperationLifecycleState.DISPATCHED
						&& (childOp == parentOp || !isPartOfFinalizedUpload(childOp, parentOp))
						&& lifecycle.unresolved(parentOp)) {
			safeReleaseMpuObjectPermit(parentOp);
		}
	}

	/**
	 * Settles, after a stop, the composite that an unsent request leaves: stop-time code, such as a
	 * driver's {@link #onAdmissionClosed()}, calls it for a request the stop keeps from being sent.
	 * While admission is open it does nothing. A transport claim refused by the stop settles through
	 * {@link #withDispatchAdmission(BooleanSupplier, Operation)} or {@link #beginDispatchOrSettle}
	 * instead, which know the refusal's reason from the admission check they make anyway.
	 */
	protected final void settleRefusedDispatch(final O op) {
		if (isAdmissionOpen()) {
			return;
		}
		settleClosedDispatch(op);
	}

	/**
	 * Settles the composite that a request the closed admission refused leaves: a composite's
	 * finalization or abort, or one of its parts. A dispatched composite, or an upload whose
	 * initiation was answered, can no longer finish: it is settled as {@link
	 * #settleRefusedCompositeParent} settles it, so it is unresolved once and its upload permit comes
	 * back once. A refused finalization or abort is in no queue, so queue recovery never sees it. A
	 * refused part keeps its own state for the caller or queue recovery, and a queued composite that
	 * opened nothing on the target is left to queue recovery. Runs only once admission has closed.
	 */
	@SuppressWarnings("unchecked")
	private void settleClosedDispatch(final O op) {
		final O parentOp;
		if (op instanceof PartialOperation<?> partialOp) {
			parentOp = (O) partialOp.parent();
		} else if (op instanceof CompositeOperation) {
			parentOp = op;
		} else {
			return;
		}
		if (parentOp == null) {
			return;
		}
		final var state = operationLifecycle().stateOf(parentOp);
		if (state == OperationLifecycleState.DISPATCHED
						|| (state == OperationLifecycleState.DRIVER_QUEUED && isInitiatedUpload(parentOp))) {
			settleRefusedCompositeParent(op, parentOp);
		}
	}

	/** Whether {@code op} is a multipart upload whose initiation was answered with an upload ID. */
	private static boolean isInitiatedUpload(final Operation<?> op) {
		return op instanceof CompositeOperation<?> compositeOp
						&& OpType.CREATE.equals(op.type())
						&& compositeOp.get(KEY_UPLOAD_ID) != null;
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
					settleRefusedCompositeParent(childOp, permitOwner);
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
				if (!isAdmissionOpen()) {
					// A stop that arrives as an interrupt refuses the enqueue as the closed admission does.
					settleRefusedCompositeParent(childOp, permitOwner);
				}
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

	@Override
	protected final boolean handleCompleted(final O op) {
		return handleCompleted(op, true);
	}

	/**
	 * Completes {@code op}; {@code wakeDispatcher} is {@code false} only when the caller still owns
	 * the completed operation's permit and will either hand it to the next operation directly or
	 * release it and wake the dispatcher itself, so the per-completion unpark is not paid twice.
	 */
	@SuppressWarnings("unchecked")
	protected final boolean handleCompleted(final O op, final boolean wakeDispatcher) {
		final boolean accepted = super.handleCompleted(op);
		if (!accepted) {
			if (op instanceof CompositeOperation || op instanceof PartialOperation) {
				safeReleaseMpuObjectPermit(op);
			}
			return false;
		}

		completedOpCount.increment();
		if (op instanceof CompositeOperation parentOp) {
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
		} else if (op instanceof PartialOperation subOp) {
			final var parentOp = subOp.parent();
			List<O> nextChildOps = List.of();
			String childFailureContext = "part operation";
			boolean enqueueFinalization;
			synchronized (parentOp) {
				if (op.status() != Operation.Status.SUCC) {
					// A part of an aborted upload is not retried: its retry would take the ended upload's
					// lifecycle back, and the upload gets no second terminal result, so the step's end
					// would wait for it. It falls to the exhausted path, which keeps the first failure.
					if (subOp.retryCount() < MAX_PART_RETRIES && parentOp.get("mpuAbort") == null) {
						// retry the individual part instead of aborting the whole MPU
						final var failStatus = op.status();
						subOp.incrementRetryCount();
						parentOp.undoMarkSubTaskCompleted();
						op.status(Operation.Status.PENDING);
						// The retry is a new request: the failed attempt's response times would make
						// it look finished after its response started.
						op.resetTiming();
						if (op.item() instanceof DataItem dataItem) {
							dataItem.reset();
						}
						// Child operations skip prepare(), so reset() does not run for the retry: without
						// this, the failed attempt's bytes (an error body, or a body cut short) would be
						// added to the retry's, and its data-response start would hide the retry's TTFB.
						if (op instanceof DataOperationImpl<?> dataOp) {
							dataOp.resetAttemptProgress();
						}
						Loggers.ERR.warn(
										"{}: part #{} failed ({}), retry {}/{}",
										toString(), subOp.partNumber(), failStatus,
										subOp.retryCount(), MAX_PART_RETRIES);
						nextChildOps = List.of(op);
						childFailureContext = "part retry";
					} else {
						final boolean rangeRead = OpType.READ.equals(op.type());
						// An upload keeps the failure of its first part to run out of retries, the
						// reason it is aborted: a sibling that fails after the abort, for example with
						// 404 on the aborted upload, must not replace it.
						if (rangeRead || parentOp.get("mpuFailure") == null) {
							parentOp.put("mpuFailure", op.status().name());
						}
						if (rangeRead) {
							// Range reads are stateless, so there is nothing to abort: the other
							// ranges finish, and finalization reports this failure.
							nextChildOps = (List<O>) parentOp.nextSubOperations(1);
						} else {
							// retries exhausted — abort the MPU
							parentOp.put("mpuAbort", "true");
						}
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
				// See timesUploadFinalization(). A composite READ's finalization sends no request, and
				// its duration runs from its initial pass, so its timing is kept.
				if (OpType.CREATE.equals(parentOp.type()) && timesUploadFinalization()) {
					parentOp.resetTiming();
				}
				enqueueChildOp((O) parentOp, op, "completion operation");
			}
		}
		if (wakeDispatcher) {
			signalDispatch();
		}
		return true;
	}

	/**
	 * Range adapters call this only after the retained coordinator accepts a completion
	 * callback once. This is the legacy driver callback count, not logical range accounting;
	 * queue recovery does not invent a completion callback. Release attempt resources before
	 * requesting a wake-up, or pass false if that release already signaled capacity.
	 */
	protected final void recordRetainedAttemptCompletion(final boolean wakeDispatcher) {
		completedOpCount.increment();
		if (wakeDispatcher)
			signalDispatch();
	}

	/**
	 * The status a multipart upload ends with when it is aborted because a part ran out of retries: the
	 * failure of the first part that did, whether or not the abort itself succeeds, so that under
	 * {@code load-op-retry} the whole upload is retried only when that failure is retryable, and an auth
	 * or not-found failure is not. {@code FAIL_UNKNOWN} when no part failure is recorded.
	 */
	protected static Operation.Status abortedUploadStatus(final CompositeOperation<?> parentOp) {
		final String partFailure = parentOp.get("mpuFailure");
		return partFailure == null ? Operation.Status.FAIL_UNKNOWN : Operation.Status.valueOf(partFailure);
	}

	static boolean shouldEnqueueFinalization(final CompositeOperation parentOp) {
		if (parentOp.get("mpuAbort") != null) {
			return markFinalizationEnqueued(parentOp);
		}
		if (!parentOp.allSubOperationsDone()) {
			return false;
		}
		// A part settles before its completion takes the parent's lock, so a count of zero can include a
		// failed part that is about to retry. Once a part has recorded a failure, the only failed parts
		// that can end the operation are those out of retries.
		final boolean failed = parentOp.get("mpuFailure") != null;
		for (final var rawSubOp : parentOp.subOperations()) {
			final var subOp = (PartialOperation) rawSubOp;
			if (subOp.status() != Operation.Status.SUCC && !(failed && subOp.retryCount() >= MAX_PART_RETRIES)) {
				return false;
			}
		}
		return markFinalizationEnqueued(parentOp);
	}

	private static boolean markFinalizationEnqueued(final CompositeOperation parentOp) {
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
	// Preserve the overridable extension ABI; inlining would require making this method final.
	@SuppressWarnings("InlineMeSuggester")
	protected boolean isFastRecycleEnabled() {
		return false;
	}

	/** @deprecated always returns {@code false}; direct fast-recycle quiescing was removed */
	@Deprecated
	// Preserve the overridable extension ABI; inlining would require making this method final.
	@SuppressWarnings("InlineMeSuggester")
	protected boolean isFastRecycleQuiesceActive() {
		return false;
	}

	/**
	 * @param op ignored
	 * @deprecated always returns {@code false}; completed operations use shared circulation
	 */
	@Deprecated
	// Preserve the overridable extension ABI; inlining would require making this method final.
	@SuppressWarnings("InlineMeSuggester")
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
			dispatchStoppedInTime = awaitDispatchStopped();
			if (!dispatchStoppedInTime) {
				Loggers.MSG.warn(
								"{}: dispatch task did not stop within {} second(s); "
												+ "queued lifecycle ownership will be recovered without touching task buffers",
								toString(), TASK_STOP_WAIT_SECONDS);
			}
		}
		onAdmissionClosed();
	}

	/**
	 * Waits for the dispatch task to stop, also on an interrupted thread: a step's stop or a close
	 * can arrive as an interrupt, and the rest of the close must still run.
	 *
	 * <p>Invariant: the wait ends by the {@link com.dell.spt.base.Constants#TASK_STOP_WAIT_SECONDS}
	 * deadline whether or not the thread is interrupted, before the call or during the wait, so a
	 * close on an interrupted thread takes at most that bound longer and never waits unbounded. An
	 * interrupt neither ends the wait early nor skips it, so the result is the task's real state
	 * and not a stale earlier one; the interrupt flag is set again before this returns.
	 */
	private boolean awaitDispatchStopped() {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TASK_STOP_WAIT_SECONDS);
		var interrupted = Thread.interrupted();
		try {
			while (true) {
				final long remaining = deadline - System.nanoTime();
				try {
					return opDispatchTask.await(Math.max(0L, remaining), TimeUnit.NANOSECONDS);
				} catch (final InterruptedException e) {
					interrupted = true;
					if (remaining <= 0) {
						// Another interrupt hit the last check at the deadline: count the task late.
						return false;
					}
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/**
	 * Called after admission has been closed, which is the first thing a load step does when it
	 * stops, before it waits for the operations in flight. A driver that can end its requests
	 * in flight on its own may bound that wait here. May be called more than once.
	 */
	protected void onAdmissionClosed() {}

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
		recovered.addAll(opDispatchTask.takeStoppedContinuations());
		recovered.removeAll(indeterminateSubmissions);
		final var unattempted = new ArrayList<O>(recovered.size());
		for (final O op : recovered) {
			if (op instanceof CompositeOperation || op instanceof PartialOperation) {
				recoverQueuedCompositeWork(op, unattempted);
			} else if (recoverQueuedOperation(op)) {
				unattempted.add(op);
			}
		}
		return List.copyOf(unattempted);
	}

	/**
	 * Recovers a queued part, range or composite, then settles its composite: the composite cannot
	 * finish once queued work of it is taken back (see {@link #settleRefusedCompositeParent}). An
	 * initiated upload waiting for its first parts is unresolved, not unattempted, because it is open
	 * on the target; a composite whose queued finalization is taken back is unresolved too.
	 */
	@SuppressWarnings("unchecked")
	private void recoverQueuedCompositeWork(final O op, final List<O> unattempted) {
		if (op instanceof CompositeOperation
						&& operationLifecycle().stateOf(op) == OperationLifecycleState.DRIVER_QUEUED
						&& isInitiatedUpload(op)) {
			settleRefusedCompositeParent(op, op);
			return;
		}
		if (recoverQueuedOperation(op)) {
			unattempted.add(op);
		}
		if (op instanceof PartialOperation<?> partialOp) {
			settleRefusedCompositeParent(op, (O) partialOp.parent());
		} else {
			settleRefusedCompositeParent(op, op);
		}
	}

	/**
	 * Settles a recovered queue entry. Return true only when it became unattempted and should
	 * enter the legacy recovery output. Retained retries instead settle their known failure
	 * and return false; no result output or transport action may run here.
	 */
	protected boolean recoverQueuedOperation(final O op) {
		return operationLifecycle().unattempted(op);
	}

	/** Extension hook for cooperative drivers with an additional pre-dispatch queue. */
	protected List<O> recoverAdditionalQueuedOperations() {
		return List.of();
	}

	/** False when the last {@link #closeAdmission()} gave up waiting for the dispatch task. */
	protected final boolean dispatchStoppedInTime() {
		return dispatchStoppedInTime;
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
