package com.dell.spt.storage.driver.coop;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Constants.LIFECYCLE_POLL_INTERVAL_MILLIS;
import static com.dell.spt.base.Constants.TASK_STOP_WAIT_SECONDS;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationFailureMark;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.LogUtil;
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
import org.apache.logging.log4j.Level;

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
		try {
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
		} catch (final Throwable t) {
			if (i == from) {
				throw t;
			}
			return keepAcceptedPrefix(i - from, t);
		}
		final var n = i - from;
		scheduledOpCount.add(n);
		if (n > 0) {
			signalDispatch();
		}
		return n;
	}

	/**
	 * A failure preparing a later operation of a batch must not hide the operations this call has
	 * already queued: the caller would offer them again. The call reports the accepted prefix; the
	 * failing operation is first in the caller's next call, which then reports the failure itself.
	 * An {@link Error} or a terminal failure is thrown once the prefix is counted and dispatched: it
	 * ends the step.
	 */
	private int keepAcceptedPrefix(final int accepted, final Throwable t) {
		scheduledOpCount.add(accepted);
		signalDispatch();
		if (t instanceof Error error) {
			throw error;
		}
		final var terminal = IntegrityTerminalException.find(t);
		if (terminal != null) {
			throw terminal;
		}
		if (t instanceof InterruptedException) {
			Thread.currentThread().interrupt();
			return accepted;
		}
		LogUtil.trace(Loggers.ERR, Level.WARN, t,
						"{}: an operation failed to prepare after part of its batch was queued; it is offered again",
						stepId);
		return accepted;
	}

	@Override
	public final int put(final List<O> ops) {
		if (!isStarted() || !admissionOpen) {
			throwUnchecked(new EOFException());
		}
		var n = 0;
		try {
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
		} catch (final Throwable t) {
			if (n == 0) {
				throw t;
			}
			return keepAcceptedPrefix(n, t);
		}
		scheduledOpCount.add(n);
		if (n > 0) {
			signalDispatch();
		}
		return n;
	}

	/**
	 * Records the failure that ended {@link OperationDispatchTask} as the step's terminal failure
	 * while admission is open. A failure while admission closes, such as a transport refusing work
	 * as it shuts down, does not fail a step that ends normally. Runs on the dispatch thread, so it
	 * must not close admission or stop the driver: that waits for this thread to stop.
	 */
	void dispatchTaskFailed(final Throwable t) {
		if (!admissionOpen || !isStarted()) {
			Loggers.MSG.debug("{}: dispatch task ended on {} while admission closes", stepId, t.toString());
			return;
		}
		final var found = IntegrityTerminalException.find(t);
		recordTerminalFailure(found != null
						? found
						: new IntegrityTerminalException(
										IntegrityTerminalException.Category.EXECUTION,
										stepId,
										"Storage driver dispatch stopped on an unexpected " + t.getClass().getName(),
										t));
		LogUtil.trace(Loggers.ERR, Level.ERROR, t, "{}: dispatch stopped on an unexpected failure", stepId);
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
	 * double-check before entering backpressure await, and before waiting while uploads wait for
	 * a multipart slot. A driver with transport capacity beyond
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
	 * {@link #handleCompleted}, and then finalizes the composite: it enqueues it once more, or for a
	 * read may end it at its last range, see {@link #finishCompositeRead}.
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
							|| !claimChildQueueOwnership(childOp, permitOwner)) {
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
				claimChildQueueOwnership(childOp, permitOwner);
				operationLifecycle().unattempted(childOp);
				settleRefusedCompositeParent(childOp, permitOwner);
				safeReleaseMpuObjectPermit(permitOwner);
				return false;
			}
			if (!claimChildQueueOwnership(childOp, permitOwner)) {
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
		final int queued;
		if (admissionLock == null) {
			queued = claimAndQueueChildOperations(childOps, permitOwner);
		} else {
			admissionLock.lock();
			try {
				if (!claimCompositeParentOwnership(childOps.get(0), permitOwner)) {
					return false;
				}
				if (!admissionOpen) {
					for (final O childOp : childOps) {
						claimChildQueueOwnership(childOp, permitOwner);
						operationLifecycle().unattempted(childOp);
					}
					settleRefusedCompositeParent(childOps.get(0), permitOwner);
					safeReleaseMpuObjectPermit(permitOwner);
					return false;
				}
				queued = claimAndQueueChildOperations(childOps, permitOwner);
			} finally {
				admissionLock.unlock();
			}
		}
		if (queued == childOps.size()) {
			return true;
		}
		// The rest runs off the admission lock, because a composite that cannot go on is published and
		// result output must not run under that lock. A stop in between recovers the claimed children.
		if (queued == PARENT_REFUSED) {
			return false;
		}
		if (queued == CHILDREN_UNCLAIMED) {
			// A child is refused its queue claim at the dispatch deadline, which would refuse an abort's
			// dispatch too: the composite is published at once instead.
			final O parentOp = compositeParent(childOps.get(0), permitOwner);
			if (parentOp != null && childOps.get(0) != parentOp) {
				publishCompositeEnqueueFailure(parentOp, "could not claim MPU " + failureContext + " for the queue", null);
			}
			safeReleaseMpuObjectPermit(permitOwner);
			return false;
		}
		return scheduleChildOpsEnqueue(List.copyOf(childOps.subList(queued, childOps.size())), permitOwner, failureContext);
	}

	/** {@link #claimAndQueueChildOperations}: the composite refused the expansion. */
	private static final int PARENT_REFUSED = -1;
	/** {@link #claimAndQueueChildOperations}: a child could not be claimed, and none was queued. */
	private static final int CHILDREN_UNCLAIMED = -2;

	/**
	 * Claims the composite and every child, then queues the children in order while the child queue
	 * has room. Returns how many leading children it queued, all of them on the usual route, or
	 * {@link #PARENT_REFUSED} or {@link #CHILDREN_UNCLAIMED}, when it queued none.
	 */
	private int claimAndQueueChildOperations(final List<O> childOps, final O permitOwner) {
		if (!claimCompositeParentOwnership(childOps.get(0), permitOwner)) {
			return PARENT_REFUSED;
		}
		final var claimed = new ArrayList<O>(childOps.size());
		var allClaimed = true;
		for (final O childOp : childOps) {
			if (claimChildQueueOwnership(childOp, permitOwner)) {
				claimed.add(childOp);
			} else {
				allClaimed = false;
			}
		}
		if (!allClaimed) {
			for (final O claimedChild : claimed) {
				operationLifecycle().unattempted(claimedChild);
			}
			return CHILDREN_UNCLAIMED;
		}
		for (var i = 0; i < claimed.size(); i++) {
			if (!tryEnqueueChildOp(claimed.get(i))) {
				return i;
			}
		}
		return claimed.size();
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
					final var outcome = enqueueChildOpWithTimeout(childOps.get(i), permitOwner);
					if (outcome == ChildEnqueue.QUEUED) {
						continue;
					}
					if (outcome == ChildEnqueue.TIMED_OUT) {
						// The children after the timed-out one are not queued either: the composite is
						// settled once for all of them.
						failChildOpsEnqueue(
										childOps.subList(i, childOps.size()), permitOwner,
										"timed out enqueueing MPU " + failureContext + " after "
														+ childOpEnqueueTimeoutMillis() + " ms",
										null);
					} else {
						for (var j = i + 1; j < childOps.size(); j++) {
							operationLifecycle().unattempted(childOps.get(j));
						}
					}
					return;
				}
			});
			return true;
		} catch (final RejectedExecutionException e) {
			failChildOpsEnqueue(childOps, permitOwner, "failed to schedule MPU " + failureContext + " enqueue", e);
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

	private boolean claimChildQueueOwnership(final O childOp, final O permitOwner) {
		final var lifecycle = operationLifecycle();
		return !lifecycle.isEnabled()
						|| lifecycle.stateOf(childOp) == OperationLifecycleState.DRIVER_QUEUED
						|| (childOp instanceof CompositeOperation
										&& lifecycle.stateOf(childOp) == OperationLifecycleState.DISPATCHED)
						|| queueChild(lifecycle, childOp, permitOwner);
	}

	/**
	 * Takes queue custody of {@code childOp}. A composite's own finalization or abort is the
	 * operation its load generator dispatched; any other child, a part or an extension's ordinary
	 * child, the driver created, and the tracker records it so (see
	 * {@link OperationLifecycleTracker#driverCreatedQueued}).
	 */
	private boolean queueChild(final OperationLifecycleTracker<O> lifecycle, final O childOp, final O permitOwner) {
		return compositeParent(childOp, permitOwner) == childOp
						? lifecycle.driverQueued(childOp)
						: lifecycle.driverCreatedQueued(childOp);
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

	/** How a deferred child enqueue ended. */
	private enum ChildEnqueue {
		QUEUED,
		/** The closed admission or an interrupt refused it; the refusal settled the child. */
		REFUSED,
		/** The queue stayed full until the enqueue timeout; nothing is settled yet. */
		TIMED_OUT
	}

	private ChildEnqueue enqueueChildOpWithTimeout(final O childOp, final O permitOwner) {
		if (admissionLock == null) {
			try {
				if (childOpQueue.offer(childOp, childOpEnqueueTimeoutMillis(), TimeUnit.MILLISECONDS)) {
					queueChild(operationLifecycle(), childOp, permitOwner);
					signalDispatch();
					return ChildEnqueue.QUEUED;
				}
			} catch (final InterruptedException e) {
				operationLifecycle().unattempted(childOp);
				safeReleaseMpuObjectPermit(permitOwner);
				throwUnchecked(e);
				return ChildEnqueue.REFUSED;
			}
			return ChildEnqueue.TIMED_OUT;
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
					return ChildEnqueue.REFUSED;
				}
				if (tryEnqueueChildOp(childOp)) {
					return ChildEnqueue.QUEUED;
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
				return ChildEnqueue.REFUSED;
			}
		}
		return ChildEnqueue.TIMED_OUT;
	}

	/**
	 * Settles children that could not be queued, which {@code cause} names, and the composite they
	 * belong to, once for all of them. Runs off the admission lock, while admission may be open.
	 *
	 * <ul>
	 * <li>Parts of an upload: the upload records {@code FAIL_TIMEOUT} as its failure unless a part
	 * failure came first, and is aborted as when a part runs out of retries. The abort is queued
	 * once, through {@link #shouldEnqueueFinalization}; its own enqueue can fail the same way.
	 * <li>Ranges of a composite READ: each is a failure out of retries, as are the ranges not started
	 * yet, so the read's finalization, after its ranges in flight end, reports {@code FAIL_TIMEOUT}.
	 * <li>The composite's own finalization or abort: the composite is published as a failure, see
	 * {@link #publishCompositeEnqueueFailure}.
	 * </ul>
	 * Children of no composite are only settled, as before.
	 */
	@SuppressWarnings("unchecked")
	private void failChildOpsEnqueue(
					final List<O> childOps, final O permitOwner, final String cause, final Throwable error) {
		final O parentOp = compositeParent(childOps.get(0), permitOwner);
		for (final O childOp : childOps) {
			if (childOp != parentOp) {
				childOp.status(Operation.Status.FAIL_TIMEOUT);
				operationLifecycle().unattempted(childOp);
			}
		}
		if (parentOp == null) {
			Loggers.ERR.error("{}: {}; the operation is not sent", toString(), cause, error);
			safeReleaseMpuObjectPermit(permitOwner);
			return;
		}
		final var compositeOp = (CompositeOperation<?>) parentOp;
		if (childOps.get(0) == parentOp) {
			publishCompositeEnqueueFailure(parentOp, cause, error);
			return;
		}
		final boolean rangeRead = OpType.READ.equals(parentOp.type());
		final boolean enqueueFinalization;
		synchronized (compositeOp) {
			if (rangeRead) {
				// As a range out of retries records its failure (handleCompleted).
				compositeOp.put("mpuFailure", Operation.Status.FAIL_TIMEOUT.name());
				for (final O childOp : childOps) {
					settleRangeAsExhausted(compositeOp, childOp);
				}
				// No later range is started: the read has failed, and the queue is full.
				for (final var notStarted : compositeOp.nextSubOperations(Integer.MAX_VALUE)) {
					notStarted.status(Operation.Status.FAIL_TIMEOUT);
					settleRangeAsExhausted(compositeOp, notStarted);
				}
			} else {
				if (compositeOp.get("mpuFailure") == null) {
					compositeOp.put("mpuFailure", Operation.Status.FAIL_TIMEOUT.name());
				}
				compositeOp.put("mpuAbort", "true");
			}
			enqueueFinalization = shouldEnqueueFinalization(compositeOp);
		}
		if (rangeRead) {
			Loggers.ERR.error(
							"{}: {}; failing the composite read once its ranges in flight end", toString(), cause, error);
		} else if (enqueueFinalization) {
			Loggers.ERR.error("{}: {}; aborting the upload", toString(), cause, error);
		} else {
			Loggers.ERR.error(
							"{}: {}; failing the upload, whose completion or abort is already queued",
							toString(), cause, error);
		}
		if (enqueueFinalization) {
			if (OpType.CREATE.equals(parentOp.type()) && timesUploadFinalization()) {
				parentOp.resetTiming();
			}
			enqueueChildOp(parentOp, parentOp, rangeRead ? "completion operation" : "abort operation");
		}
	}

	/**
	 * Under the composite's monitor: {@code childOp}, a range of the composite READ that is not sent,
	 * ends as a failure out of retries, so {@link #shouldEnqueueFinalization} lets the read finish
	 * without it. {@link PartialOperation} implementations that do not count retries never run out
	 * of them, as in {@link #handleCompleted}; the engine's own, {@link PartialDataOperationImpl},
	 * counts them and is also marked as having recorded its failure.
	 */
	private static void settleRangeAsExhausted(final CompositeOperation<?> compositeOp, final Operation<?> childOp) {
		if (childOp instanceof PartialOperation<?> range && range.parent() == compositeOp) {
			for (var retries = range.retryCount(); retries < MAX_PART_RETRIES; retries++) {
				range.incrementRetryCount();
			}
			if (range instanceof PartialDataOperationImpl<?> part) {
				PartialDataOperationFailureMark.markFailureRecorded(part);
			}
			// The range never reaches finishResponse(), which settles a sent range.
			compositeOp.markSubTaskCompleted();
		}
	}

	/**
	 * Publishes {@code parentOp}, whose finalization or abort could not be queued, as a failure: its
	 * recorded part failure, or {@code FAIL_TIMEOUT}, through the normal completion route, which
	 * returns its upload permit. Its parts still in flight end without a retry or a next part, and a
	 * queued part of it is settled at dispatch. Nothing is sent, so an initiated upload may stay
	 * open on the target. A completion the driver refuses, after a stop, settles it as queue
	 * recovery does.
	 */
	private void publishCompositeEnqueueFailure(final O parentOp, final String cause, final Throwable error) {
		final var compositeOp = (CompositeOperation<?>) parentOp;
		final boolean aborting;
		synchronized (compositeOp) {
			aborting = compositeOp.get("mpuAbort") != null;
			if (compositeOp.get("mpuFailure") == null) {
				compositeOp.put("mpuFailure", Operation.Status.FAIL_TIMEOUT.name());
			}
			compositeOp.put("mpuAbort", "true");
			compositeOp.put(KEY_FINALIZATION_ENQUEUED, Boolean.TRUE.toString());
		}
		if (isInitiatedUpload(parentOp)) {
			Loggers.ERR.error(
							"{}: {}; failing the upload without sending it, so it may stay open on the target",
							toString(), cause, error);
			// The record goes to this step's files from the thread that gave up on the queue.
			try (final var logCtx = CloseableThreadContext.put(KEY_STEP_ID, stepId)) {
				recordUploadLeftOpen(parentOp, aborting, cause);
			} catch (final RuntimeException e) {
				Loggers.ERR.warn("{}: failed to record an upload left open", toString(), e);
			}
		} else {
			Loggers.ERR.error("{}: {}; failing the composite operation", toString(), cause, error);
		}
		parentOp.status(abortedUploadStatus(compositeOp));
		if (!handleCompleted(parentOp)) {
			settleRefusedCompositeParent(parentOp, parentOp);
		}
	}

	/**
	 * {@code uploadOp}, an initiated upload, is about to be published as a failure without its
	 * completion or abort ({@code aborting}) being sent, because that could not be queued
	 * ({@code cause}): the upload stays open on the target. A driver that records its multipart
	 * uploads records this one here as orphaned, through its one-record-per-upload writer. It runs
	 * before the upload's result is published, with the step's id in the log context, holding neither
	 * the admission lock nor the upload's monitor. The default records nothing.
	 */
	protected void recordUploadLeftOpen(final O uploadOp, final boolean aborting, final String cause) {}

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
						// added to the retry's, its data-response start would hide the retry's TTFB, and
						// the verifier would resume from the range the failed attempt stopped in.
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
						// Only now may the parent end: a sibling's completion that finds this part
						// settled but unmarked waits for this one, which records the final status.
						if (subOp instanceof PartialDataOperationImpl<?> part) {
							PartialDataOperationFailureMark.markFailureRecorded(part);
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
				// its duration runs from its initial pass, so its timing is kept. A driver may end
				// such a read here, see finishCompositeRead(); otherwise it is queued like the rest.
				if (OpType.CREATE.equals(parentOp.type()) && timesUploadFinalization()) {
					parentOp.resetTiming();
				}
				if (!OpType.READ.equals(parentOp.type()) || !finishCompositeRead((O) parentOp)) {
					enqueueChildOp((O) parentOp, op, "completion operation");
				}
			}
		}
		if (wakeDispatcher) {
			signalDispatch();
		}
		return true;
	}

	/**
	 * Ends {@code compositeRead}, whose ranges have all ended, on the thread that completed the last
	 * of them, if this driver's finalization of a read sends no request. Queued, that finalization
	 * would wait behind the work already queued ahead of it, so the read would be counted late, and
	 * a stop would refuse it and leave the read unresolved with all its bytes transferred.
	 *
	 * <p>The read already owns its dispatched attempt, so an implementation neither takes a permit
	 * nor crosses {@link #beginDispatch}, which refuses once admission closes. It sets the read's
	 * status from the failure its ranges recorded, if any, and completes it. It is called at most
	 * once per attempt, holding neither the read's monitor nor the admission lock.
	 *
	 * @return whether the read was ended; by default {@code false}, and the finalization is queued
	 */
	protected boolean finishCompositeRead(final O compositeRead) {
		return false;
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
		// failed part whose completion has not run yet: one about to retry, or one out of retries whose
		// failure is not recorded yet. Only a failed part whose completion recorded its failure lets the
		// operation end, so the operation ends with the failure recorded last, which then stays. Another
		// PartialOperation type, an extension's own, is judged as through 5.17.0: once a part has
		// recorded a failure, a failed part out of retries lets the operation end.
		for (final var rawSubOp : parentOp.subOperations()) {
			final var subOp = (PartialOperation) rawSubOp;
			if (subOp.status() != Operation.Status.SUCC && !failureSettled(parentOp, subOp)) {
				return false;
			}
		}
		return markFinalizationEnqueued(parentOp);
	}

	/** Under {@code parentOp}'s monitor: whether {@code subOp}, a failed part, no longer holds it. */
	private static boolean failureSettled(final CompositeOperation<?> parentOp, final PartialOperation<?> subOp) {
		if (subOp instanceof PartialDataOperationImpl<?> part) {
			return PartialDataOperationFailureMark.failureRecorded(part);
		}
		return parentOp.get("mpuFailure") != null && subOp.retryCount() >= MAX_PART_RETRIES;
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
	 * wait condition before parking and the signal stays set until the task
	 * waits, so delivery is guaranteed without holding a lock across the signal.
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

	/**
	 * Settles every operation still queued before dispatch, once each, and returns those it
	 * settled as unattempted.
	 *
	 * <p>Two passes, so each queued operation is visited once and the registry scan stays small.
	 * The first drains the queues this driver owns, and an extension's extra queue, into plain
	 * lists and settles them: each operation is in at most one of this driver's queues. The
	 * second settles what the registry still holds as driver-queued, which is the dispatch task's
	 * local buffers and its deferred uploads; settling removes an operation from the registry,
	 * so the first pass leaves only those for it. An operation the first pass visited but left
	 * driver-queued, and every extra-queue entry, is skipped there, as is an indeterminate
	 * submission. An extra-queue entry can also be in the input queue and the registry, a range
	 * read is in all three, so it is visited only from the extra queue: its hook may publish a
	 * retained result and must run once. The extra queue itself may list an operation more than
	 * once; it is still visited once.
	 */
	@Override
	public final List<O> recoverQueuedOperations() {
		final var indeterminateSubmissions = indeterminateSubmissions();
		final var queued = new ArrayList<O>(childOpQueue.size() + inOpQueue.size());
		childOpQueue.drainTo(queued);
		inOpQueue.drainTo(queued);
		queued.addAll(opDispatchTask.takeStoppedContinuations());
		final var additional = recoverAdditionalQueuedOperations();
		final Set<O> visitedElsewhere;
		final List<O> extraQueued;
		if (additional.isEmpty() && indeterminateSubmissions.isEmpty()) {
			visitedElsewhere = Set.of();
			extraQueued = List.of();
		} else {
			visitedElsewhere = Collections.newSetFromMap(new IdentityHashMap<>());
			visitedElsewhere.addAll(indeterminateSubmissions);
			// Each extra-queue identity once, and none that is an indeterminate submission.
			extraQueued = new ArrayList<>(additional.size());
			for (final O op : additional) {
				if (visitedElsewhere.add(op)) {
					extraQueued.add(op);
				}
			}
		}
		final var unattempted = new ArrayList<O>(queued.size() + extraQueued.size());
		final Set<O> leftQueued = Collections.newSetFromMap(new IdentityHashMap<>());
		for (final O op : queued) {
			if ((visitedElsewhere.isEmpty() || !visitedElsewhere.contains(op))
							&& !recoverQueuedEntry(op, unattempted)) {
				leftQueued.add(op);
			}
		}
		for (final O op : extraQueued) {
			if (!recoverQueuedEntry(op, unattempted)) {
				leftQueued.add(op);
			}
		}
		for (final O op : operationLifecycle().driverQueuedOperations()) {
			if ((visitedElsewhere.isEmpty() || !visitedElsewhere.contains(op))
							&& (leftQueued.isEmpty() || !leftQueued.contains(op))) {
				recoverQueuedEntry(op, unattempted);
			}
		}
		return Collections.unmodifiableList(unattempted);
	}

	/**
	 * Queued submissions the dispatch task did not finish within its stop wait. They may have
	 * started transport, so they are settled unresolved here and left out of queue recovery.
	 */
	private Set<O> indeterminateSubmissions() {
		final var submittingOperations = opDispatchTask.submittingOperations();
		if (submittingOperations.isEmpty()) {
			return Set.of();
		}
		final Set<O> indeterminateSubmissions = Collections.newSetFromMap(new IdentityHashMap<>());
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
		return indeterminateSubmissions;
	}

	/**
	 * Settles one recovered queue entry. Returns false only when the entry is still driver-queued
	 * afterwards, so the registry pass can skip it rather than visit it a second time.
	 */
	private boolean recoverQueuedEntry(final O op, final List<O> unattempted) {
		if (op instanceof CompositeOperation || op instanceof PartialOperation) {
			if (recoverQueuedCompositeWork(op, unattempted)) {
				return true;
			}
		} else if (recoverQueuedOperation(op)) {
			unattempted.add(op);
			return true;
		}
		return operationLifecycle().stateOf(op) != OperationLifecycleState.DRIVER_QUEUED;
	}

	/**
	 * Recovers a queued part, range or composite, then settles its composite: the composite cannot
	 * finish once queued work of it is taken back (see {@link #settleRefusedCompositeParent}). An
	 * initiated upload waiting for its first parts is unresolved, not unattempted, because it is open
	 * on the target; a composite whose queued finalization is taken back is unresolved too. Returns
	 * whether the entry itself became unattempted.
	 */
	@SuppressWarnings("unchecked")
	private boolean recoverQueuedCompositeWork(final O op, final List<O> unattempted) {
		if (op instanceof CompositeOperation
						&& operationLifecycle().stateOf(op) == OperationLifecycleState.DRIVER_QUEUED
						&& isInitiatedUpload(op)) {
			settleRefusedCompositeParent(op, op);
			return false;
		}
		final boolean settled = recoverQueuedOperation(op);
		if (settled) {
			unattempted.add(op);
		}
		if (op instanceof PartialOperation<?> partialOp) {
			settleRefusedCompositeParent(op, (O) partialOp.parent());
		} else {
			settleRefusedCompositeParent(op, op);
		}
		return settled;
	}

	/**
	 * Settles a recovered queue entry. Return true only when it became unattempted and should
	 * enter the legacy recovery output. Retained retries instead settle their known failure
	 * and return false; the hook may publish that settled failure, once and on the calling thread,
	 * but no success output or transport action may run here. Direct dispatch calls this hook
	 * under {@code admissionLock}, so a hook that publishes must run only with direct dispatch off,
	 * as range drivers do, or move the publication outside that lock. Called at most once
	 * per queued operation in each recovery call, and not for a queued composite that recovery
	 * settles through one of its recovered parts. An operation this hook leaves queued can be
	 * visited again by a later recovery call, so the hook must be safe to repeat.
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
