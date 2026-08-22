package com.dell.spt.storage.driver.coop;

import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import com.github.akurilov.commons.collection.CircularArrayBuffer;
import com.github.akurilov.commons.collection.CircularBuffer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

/**
 * Created by andrey on 23.08.17.
 */
public final class OperationDispatchTask<I extends Item, O extends Operation<I>>
				extends TaskBase {
	static final class SubmittingOperation<O extends Operation<? extends Item>> {
		private final O operation;
		private final OperationLifecycleTracker.DispatchToken<O> dispatchToken;

		private SubmittingOperation(
						final O operation,
						final OperationLifecycleTracker.DispatchToken<O> dispatchToken) {
			this.operation = operation;
			this.dispatchToken = dispatchToken;
		}

		O operation() {
			return operation;
		}

		OperationLifecycleTracker.DispatchToken<O> dispatchToken() {
			return dispatchToken;
		}
	}

	private static final String CLS_NAME = OperationDispatchTask.class.getSimpleName();

	private final String stepId;
	private final int batchSize;
	private final BlockingQueue<O> childOpQueue;
	private final BlockingQueue<O> inOpQueue;
	private final CoopStorageDriverBase<I, O> storageDriver;
	private final CircularBuffer<O> buff;
	private final List<O> tempInOps;
	private final Lock dispatchLock;
	private final Condition dispatchReady;
	private final int deferredQueueCapacity;

	private final Queue<O> deferredMpuQueue;
	private volatile List<SubmittingOperation<O>> submittingOperations = List.of();

	public OperationDispatchTask(
					final ThreadTaskExecutor executor, final CoopStorageDriverBase<I, O> storageDriver,
					final BlockingQueue<O> inOpQueue, final BlockingQueue<O> childOpQueue, final String stepId,
					final int batchSize, final Lock dispatchLock, final Condition dispatchReady, final int deferredQueueCapacity) {
		super(executor);
		this.buff = new CircularArrayBuffer<>(batchSize);
		this.tempInOps = new ArrayList<>(batchSize);
		this.storageDriver = storageDriver;
		this.inOpQueue = inOpQueue;
		this.childOpQueue = childOpQueue;
		this.stepId = stepId;
		this.batchSize = batchSize;
		this.dispatchLock = dispatchLock;
		this.dispatchReady = dispatchReady;
		this.deferredQueueCapacity = deferredQueueCapacity;
		this.deferredMpuQueue = new java.util.ArrayDeque<>(deferredQueueCapacity);
	}

	/** Preserves the constructor descriptor used by existing extensions. */
	public OperationDispatchTask(
					final VirtualThreadExecutor executor, final CoopStorageDriverBase<I, O> storageDriver,
					final BlockingQueue<O> inOpQueue, final BlockingQueue<O> childOpQueue, final String stepId,
					final int batchSize, final Lock dispatchLock, final Condition dispatchReady, final int deferredQueueCapacity) {
		this((ThreadTaskExecutor) executor, storageDriver, inOpQueue, childOpQueue, stepId,
						batchSize, dispatchLock, dispatchReady, deferredQueueCapacity);
	}

	@Override
	protected void doInit() {
		ThreadContext.put(KEY_STEP_ID, stepId);
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
	}

	@Override
	protected final void doWork() throws Exception {
		try {
			while (buff.size() < batchSize && !deferredMpuQueue.isEmpty()
							&& storageDriver.tryAcquireMpuObjectPermit(deferredMpuQueue.peek())) {
				buff.add(deferredMpuQueue.poll());
			}

			// Drain child ops first (partial/composite completions have priority)
			if (buff.size() < batchSize) {
				drainChildOperations(batchSize - buff.size());
			}

			// Then drain incoming ops
			if (buff.size() == 0) {
				drainIncomingOperations(Math.max(0, Math.min(batchSize, deferredQueueCapacity - deferredMpuQueue.size())));
				if (tempInOps.isEmpty() && deferredMpuQueue.isEmpty()) {
					dispatchLock.lock();
					try {
						if (childOpQueue.isEmpty() && inOpQueue.isEmpty()) {
							dispatchReady.await();
						}
					} finally {
						dispatchLock.unlock();
					}
					// Drain again after waking
					while (buff.size() < batchSize && !deferredMpuQueue.isEmpty()
									&& storageDriver.tryAcquireMpuObjectPermit(deferredMpuQueue.peek())) {
						buff.add(deferredMpuQueue.poll());
					}
					if (buff.size() < batchSize) {
						drainChildOperations(batchSize - buff.size());
					}
					if (buff.size() < batchSize) {
						drainIncomingOperations(Math.max(0, Math.min(batchSize - buff.size(), deferredQueueCapacity - deferredMpuQueue.size())));
					}
				} else if (tempInOps.isEmpty() && !deferredMpuQueue.isEmpty()) {
					// We might have no actionable work (MPU permits are exhausted, or deferred queue is full and inOpQueue is blocked).
					dispatchLock.lock();
					try {
						boolean childEmpty = childOpQueue.isEmpty();
						boolean inEmpty = inOpQueue.isEmpty();
						boolean canDrainIn = deferredMpuQueue.size() < deferredQueueCapacity;

						if (childEmpty && (inEmpty || !canDrainIn)) {
							if (!storageDriver.tryAcquireMpuObjectPermit(deferredMpuQueue.peek())) {
								dispatchReady.await();
							} else {
								// We acquired a permit just now, we can process a deferred op
								if (buff.size() < batchSize && !deferredMpuQueue.isEmpty()) {
									buff.add(deferredMpuQueue.poll());
								}
							}
						}
					} finally {
						dispatchLock.unlock();
					}
					if (buff.size() < batchSize) {
						drainChildOperations(batchSize - buff.size());
					}
					if (buff.size() < batchSize) {
						drainIncomingOperations(Math.max(0, Math.min(batchSize - buff.size(), deferredQueueCapacity - deferredMpuQueue.size())));
					}
				}
			} else if (buff.size() < batchSize) {
				drainIncomingOperations(Math.max(0, Math.min(batchSize - buff.size(), deferredQueueCapacity - deferredMpuQueue.size())));
			}

			// Process new incoming ops to respect MPU object limits
			for (int i = 0; i < tempInOps.size(); i++) {
				O op = tempInOps.get(i);
				if (storageDriver.isMpuInit(op)) {
					if (storageDriver.tryAcquireMpuObjectPermit(op)) {
						buff.add(op);
					} else {
						deferredMpuQueue.add(op);
					}
				} else {
					buff.add(op);
				}
			}

			// submit all buffered ops (including retries from prior iterations)
			final int buffSize = buff.size();
			if (buffSize > 0) {
				final var dispatchTokens = captureQueuedDispatchTokens(buffSize);
				submittingOperations = submittingOperationsSnapshot(buffSize, dispatchTokens);
				boolean submitted;
				try {
					if (buffSize == 1) { // non-batch mode
						submitted = storageDriver.submit(buff.get(0));
						if (submitted) {
							markLegacyDispatchFallback(1, dispatchTokens);
							buff.clear();
						} else {
							submitted = removeResolvedPrefix() > 0;
						}
					} else { // batch mode
						final int m = storageDriver.submit(buff, 0, buffSize);
						submitted = m > 0;
						if (submitted) {
							markLegacyDispatchFallback(m, dispatchTokens);
							buff.removeFirst(m);
						} else {
							submitted = removeResolvedPrefix() > 0;
						}
					}
				} finally {
					submittingOperations = List.of();
				}
				// Backpressure: submit made no progress (no permits available).
				// Wait for a completion to free capacity.  The double-check
				// inside the lock prevents a lost-signal race: a completion may
				// release a permit AND call signalDispatch() between submit()
				// returning false and this lock acquisition — the signal is lost
				// because nobody is in await() yet.  Checking availablePermits
				// under the lock detects that case (the permit release happened-
				// before the lock acquisition), so we skip the await and retry.
				if (!submitted) {
					dispatchLock.lock();
					try {
						if (!storageDriver.hasAvailableDispatchCapacity()) {
							dispatchReady.await();
						}
					} finally {
						dispatchLock.unlock();
					}
				}
			}
		} catch (final IllegalStateException e) {
			LogUtil.exception(
							Level.TRACE, e,
							"{}: failed to submit some load operations due to the illegal storage driver state ({})",
							storageDriver.toString(), storageDriver.state());
		} finally {
			tempInOps.clear();
		}
	}

	private void drainIncomingOperations(final int maxCount) {
		if (maxCount <= 0) {
			return;
		}
		dispatchLock.lock();
		try {
			inOpQueue.drainTo(tempInOps, maxCount);
		} finally {
			dispatchLock.unlock();
		}
	}

	private void drainChildOperations(final int maxCount) {
		if (maxCount <= 0) {
			return;
		}
		final int firstDrainedIndex = buff.size();
		childOpQueue.drainTo(buff, maxCount);
		final var tracker = storageDriver.operationLifecycle();
		if (tracker == null) {
			return;
		}
		for (var i = buff.size() - 1; i >= firstDrainedIndex; i--) {
			final O op = buff.get(i);
			final var state = tracker.stateOf(op);
			if (state == OperationLifecycleState.NEW
							|| state == OperationLifecycleState.GENERATOR_BUFFERED) {
				// Existing extensions may still write the protected child queue directly.
				// Claim their compatibility ownership before the task-local buffer is the
				// only place retaining the identity.
				if (!tracker.driverQueued(op)) {
					tracker.unattempted(op);
					buff.remove(i);
				}
			}
		}
	}

	private List<OperationLifecycleTracker.DispatchToken<O>> captureQueuedDispatchTokens(
					final int operationCount) {
		final var tracker = storageDriver.operationLifecycle();
		final var tokens = new ArrayList<OperationLifecycleTracker.DispatchToken<O>>(operationCount);
		for (var i = 0; i < operationCount; i++) {
			tokens.add(tracker == null ? null : tracker.queuedDispatchToken(buff.get(i)));
		}
		return tokens;
	}

	private List<SubmittingOperation<O>> submittingOperationsSnapshot(
					final int operationCount,
					final List<OperationLifecycleTracker.DispatchToken<O>> dispatchTokens) {
		final var operations = new ArrayList<SubmittingOperation<O>>(operationCount);
		for (var i = 0; i < operationCount; i++) {
			operations.add(new SubmittingOperation<>(buff.get(i), dispatchTokens.get(i)));
		}
		return List.copyOf(operations);
	}

	List<SubmittingOperation<O>> submittingOperations() {
		return submittingOperations;
	}

	private void markLegacyDispatchFallback(
					final int submittedCount,
					final List<OperationLifecycleTracker.DispatchToken<O>> dispatchTokens) {
		final var tracker = storageDriver.operationLifecycle();
		if (tracker == null) {
			return;
		}
		// Existing cooperative extensions predate beginDispatch(). A successful submit return
		// is their compatibility boundary. Only operations still owned by the driver queue are
		// eligible: built-in drivers already transitioned at the exact transport handoff. The
		// token prevents a synchronous legacy completion and recycle from making a later
		// circulation appear eligible for this earlier submit return.
		for (var i = 0; i < submittedCount; i++) {
			final var op = buff.get(i);
			if (storageDriver.successfulSubmitStartsTransport(op)) {
				final var token = dispatchTokens.get(i);
				if (!tracker.dispatched(token)) {
					tracker.unresolvedSubmission(token);
				}
			}
		}
	}

	private int removeResolvedPrefix() {
		var removed = 0;
		while (!buff.isEmpty()) {
			final var op = buff.get(0);
			final var tracker = storageDriver.operationLifecycle();
			final OperationLifecycleState state;
			if (tracker == null) {
				final var lifecycle = op.lifecycle();
				if (lifecycle == null) {
					break;
				}
				state = lifecycle.state();
			} else {
				state = tracker.stateOf(op);
			}
			if (state != OperationLifecycleState.TERMINAL
							&& state != OperationLifecycleState.UNATTEMPTED
							&& state != OperationLifecycleState.UNRESOLVED) {
				break;
			}
			buff.remove(0);
			removed++;
		}
		return removed;
	}

	@Override
	protected final void doStop() {
		// Only the task thread owns these non-thread-safe collections. Queue recovery uses the
		// lifecycle registry, so cleanup never needs to race a submit implementation which did
		// not return before the driver's bounded stop wait.
		while (!buff.isEmpty()) {
			buff.remove(0);
		}
		tempInOps.clear();
		while (!deferredMpuQueue.isEmpty()) {
			deferredMpuQueue.poll();
		}
	}
}
