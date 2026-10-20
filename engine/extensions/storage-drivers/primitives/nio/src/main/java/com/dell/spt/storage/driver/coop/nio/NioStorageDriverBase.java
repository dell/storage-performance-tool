package com.dell.spt.storage.driver.coop.nio;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Constants.TASK_STOP_WAIT_SECONDS;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.item.op.Operation.Status.ACTIVE;
import static com.dell.spt.base.item.op.Operation.Status.INTERRUPTED;
import static com.dell.spt.base.item.op.Operation.Status.PENDING;

import com.github.akurilov.commons.collection.CircularArrayBuffer;
import com.github.akurilov.commons.collection.CircularBuffer;
import com.github.akurilov.commons.concurrent.ThreadUtil;

import com.github.akurilov.confuse.Config;

import org.apache.logging.log4j.CloseableThreadContext;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
Created by kurila on 19.07.16.
The multi-threaded non-blocking I/O storage driver.
*/
public abstract class NioStorageDriverBase<I extends Item, O extends Operation<I>>
				extends CoopStorageDriverBase<I, O>
				implements NioStorageDriver<I, O> {

	private final static String CLS_NAME = NioStorageDriverBase.class.getSimpleName();
	private final static VirtualThreadExecutor IO_EXECUTOR = new VirtualThreadExecutor();

	/**
	 * The buffer capacity of a worker that serves one concurrency permit. Such a worker is handed
	 * an operation only while it is idle, so its buffer holds that one operation and whatever it
	 * keeps active between invocations; the rest is slack. A worker blocked in one request must
	 * not strand thousands of permits behind it.
	 */
	static final int PERMIT_WORKER_BUFF_CAPACITY = 4;

	/** How long a worker that serves one permit waits for work before it checks its buffer again. */
	private static final long PERMIT_WORKER_IDLE_WAIT_MILLIS = 100;

	private final int nioWorkerCount;
	private final boolean ioWorkerPerPermit;
	private final long idleWaitMillis;
	private final int opBuffCapacity;
	private final List<Task> ioWorkers;
	private final CircularBuffer<O>[] opBuffs;
	private final Lock[] opBuffLocks;
	private final Condition[] opAvailableConditions;
	private final int[] opBuffReserved;
	private final AtomicLong rrc = new AtomicLong(0);
	private final AtomicBoolean operationFailureReported = new AtomicBoolean(false);

	public NioStorageDriverBase(
					final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) throws IllegalConfigurationException {
		this(testStepId, dataInput, storageConfig, verifyFlag, batchSize, false);
	}

	/**
	 * @param ioWorkerPerPermit whether, when {@code storage.driver.threads} is not set, to run one
	 *     worker per concurrency permit instead of one per hardware thread. A driver whose
	 *     {@link #invokeNio} blocks for the whole request needs this to keep the configured number
	 *     of requests in flight: with fewer workers than permits, the permits beyond the worker
	 *     count wait in the workers' buffers, and a worker blocked in a lost request strands the
	 *     permits queued behind it.
	 */
	@SuppressWarnings("unchecked")
	protected NioStorageDriverBase(
					final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize, final boolean ioWorkerPerPermit) throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
		final var confWorkerCount = storageConfig.intVal("driver-threads");
		if (confWorkerCount > 0) {
			nioWorkerCount = confWorkerCount;
		} else if (concurrencyLimit > 0) {
			nioWorkerCount = ioWorkerPerPermit
							? concurrencyLimit
							: Math.min(concurrencyLimit, ThreadUtil.getHardwareThreadCount());
		} else {
			nioWorkerCount = ThreadUtil.getHardwareThreadCount();
		}
		// One worker per permit holds, by construction, as long as no worker serves several permits.
		this.ioWorkerPerPermit = ioWorkerPerPermit && concurrencyLimit > 0 && nioWorkerCount >= concurrencyLimit;
		this.idleWaitMillis = this.ioWorkerPerPermit ? PERMIT_WORKER_IDLE_WAIT_MILLIS : 1;
		ioWorkers = new ArrayList<>(nioWorkerCount);
		opBuffs = new CircularBuffer[nioWorkerCount];
		opBuffLocks = new Lock[nioWorkerCount];
		opAvailableConditions = new Condition[nioWorkerCount];
		opBuffReserved = new int[nioWorkerCount];
		opBuffCapacity = this.ioWorkerPerPermit
						? PERMIT_WORKER_BUFF_CAPACITY
						: Math.max(MIN_TASK_BUFF_CAPACITY, concurrencyLimit / nioWorkerCount);
		for (var i = 0; i < nioWorkerCount; i++) {
			opBuffs[i] = new CircularArrayBuffer<>(opBuffCapacity);
			opBuffLocks[i] = new ReentrantLock();
			opAvailableConditions[i] = opBuffLocks[i].newCondition();
			ioWorkers.add(new NioWorkerTask(IO_EXECUTOR, i, opBuffs[i], opBuffLocks[i], opAvailableConditions[i]));
		}
	}

	/**
	The class represents the non-blocking load operation execution algorithm.
	The load operation itself may correspond to a large data transfer so it can't be non-blocking.
	So the load operation may be invoked multiple times (in the reentrant manner).
	*/
	private final class NioWorkerTask
					extends TaskBase {

		private final CircularBuffer<O> opBuff;
		private final Lock opBuffLock;
		private final Condition opAvailable;
		private final int workerIdx;
		private final List<O> opLocalBuff;
		private final List<O> opActiveBuff;

		public NioWorkerTask(
						final VirtualThreadExecutor executor, final int workerIdx, final CircularBuffer<O> opBuff,
						final Lock opBuffLock, final Condition opAvailable) {
			super(executor);
			this.workerIdx = workerIdx;
			this.opBuff = opBuff;
			this.opBuffLock = opBuffLock;
			this.opAvailable = opAvailable;
			this.opLocalBuff = new ArrayList<>(opBuffCapacity);
			this.opActiveBuff = new ArrayList<>(opBuffCapacity);
		}

		@Override
		protected void doInit() {
			ThreadContext.put(KEY_STEP_ID, stepId);
			ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
		}

		// The single timed await is a safety net inside the task's own work loop: doWork() returns
		// when the buffer is still empty, and the task calls it again.
		@SuppressWarnings("WaitNotInLoop")
		@Override
		protected final void doWork() throws Exception {

			opBuffLock.lock();
			try {
				var opBuffSize = opBuff.size();
				if (opBuffSize == 0) {
					// Wait for work — VT unmounts during await, freeing the carrier thread.
					// The timeout is a safety net; normal wake-up is via signal from submit(). A
					// worker per permit waits longer, so that many idle workers cost few wake-ups.
					opAvailable.await(idleWaitMillis, TimeUnit.MILLISECONDS);
					opBuffSize = opBuff.size();
					if (opBuffSize == 0) {
						return;
					}
				}
				opBuffReserved[workerIdx] = opBuffSize;
				for (var i = 0; i < opBuffSize; i++) {
					opLocalBuff.add(opBuff.get(i));
				}
				opBuff.clear();
			} finally {
				opBuffLock.unlock();
			}

			var processedCount = 0;
			try {
				final var opLocalBuffSize = opLocalBuff.size();
				for (; processedCount < opLocalBuffSize;) {
					final var op = opLocalBuff.get(processedCount);
					try {
						// check if the op is invoked 1st time
						if (PENDING.equals(op.status())) {
							if (!isStarted() || !beginDispatch(op)) {
								// Permit already acquired in submit() — release it
								concurrencyThrottle.release();
								operationLifecycle().unattempted(op);
								processedCount++;
								continue;
							}
							// Permit already acquired in submit() — mark active
							op.startRequest();
							op.finishRequest();
						}
						// Perform non-blocking I/O for the op. A faulty extension must not strand
						// this permit or discard the remaining worker-local batch.
						invokeNio(op);
					} catch (final Throwable cause) {
						throwUncheckedIfInterrupted(cause);
						logOperationFailure(cause, "I/O operation invocation failure");
						op.status(Operation.Status.FAIL_UNKNOWN);
						releaseCompletedPermit();
						processedCount++;
						handleCompletedSafely(op);
						continue;
					}
					// remove the op from the buffer if it is not active more
					if (!ACTIVE.equals(op.status())) {
						releaseCompletedPermit();
						// Advance ownership before external result publication. If output throws,
						// shutdown recovery must not revisit and release this permit again.
						processedCount++;
						handleCompletedSafely(op);
					} else {
						// the op remains in the buffer for the next iteration
						opActiveBuff.add(op);
						processedCount++;
					}
				}
			} catch (final Throwable cause) {
				throwUncheckedIfInterrupted(cause);
				logOperationFailure(cause, "I/O worker failure");
			} finally {
				var recoveredCapacity = false;
				opBuffLock.lock();
				try {
					// A worker-level failure may leave a suffix in this local buffer even while
					// admission remains open. Transfer or recover every owner before clearing it.
					for (var i = processedCount; i < opLocalBuff.size(); i++) {
						recoveredCapacity |= recoverUnprocessedLocalOperation(opLocalBuff.get(i));
					}
					final var activeSize = opActiveBuff.size();
					if (activeSize > 0) {
						for (var i = 0; i < activeSize; i++) {
							if (!opBuff.add(opActiveBuff.get(i))) {
								final var op = opActiveBuff.get(i);
								op.status(INTERRUPTED);
								concurrencyThrottle.release();
								handleCompletedSafely(op);
							}
						}
						opActiveBuff.clear();
					}
					opBuffReserved[workerIdx] = 0;
				} finally {
					opBuffLock.unlock();
					opLocalBuff.clear();
				}
				if (recoveredCapacity && isAdmissionOpen()) {
					signalDispatchCapacityAvailable();
				}
			}
		}

		/**
		 * Releases the permit of an operation this worker has finished with. With a worker per
		 * permit the worker first gives up its reservation for the operation, so that whoever takes
		 * the permit finds this worker idle and hands it the next operation: all the worker has
		 * left to do is publish the result. Before, the worker counted as busy until it had
		 * published, and the next operation could be placed behind a worker blocked in a request.
		 */
		private void releaseCompletedPermit() {
			if (ioWorkerPerPermit) {
				opBuffLock.lock();
				try {
					if (opBuffReserved[workerIdx] > 0) {
						opBuffReserved[workerIdx]--;
					}
				} finally {
					opBuffLock.unlock();
				}
			}
			concurrencyThrottle.release();
		}

		private boolean recoverUnprocessedLocalOperation(final O op) {
			final var lifecycle = operationLifecycle();
			final var lifecycleState = lifecycle.stateOf(op);
			switch (lifecycleState) {
			case NEW, GENERATOR_BUFFERED, DRIVER_QUEUED:
				concurrencyThrottle.release();
				lifecycle.unattempted(op);
				return true;
			case DISPATCHED:
				return recoverDispatchedLocalOperation(op);
			case COMPLETING:
				concurrencyThrottle.release();
				lifecycle.unresolved(op);
				return true;
			case TERMINAL, UNATTEMPTED, UNRESOLVED:
				// A concurrent lifecycle decision already retained the evidence, but this
				// worker-local owner still holds the permit acquired by submit().
				concurrencyThrottle.release();
				return true;
			}
			throw new AssertionError("Unhandled operation lifecycle state: " + lifecycleState);
		}

		private boolean recoverDispatchedLocalOperation(final O op) {
			final Operation.Status status;
			try {
				status = op.status();
			} catch (final Throwable cause) {
				concurrencyThrottle.release();
				operationLifecycle().unresolved(op);
				logOperationFailure(cause, "I/O operation state recovery failure");
				return true;
			}
			if (ACTIVE.equals(status)) {
				// Transport work from an earlier invocation remains eligible for bounded drain.
				opActiveBuff.add(op);
				return false;
			}
			concurrencyThrottle.release();
			if (PENDING.equals(status)) {
				// Dispatch ownership was published, but worker failure prevented a trustworthy
				// transport/result outcome. It cannot be reclassified as unattempted.
				operationLifecycle().unresolved(op);
				return true;
			}
			handleCompletedSafely(op);
			final var recoveredState = operationLifecycle().stateOf(op);
			if (recoveredState == OperationLifecycleState.DISPATCHED
							|| recoveredState == OperationLifecycleState.COMPLETING) {
				operationLifecycle().unresolved(op);
			}
			return true;
		}

		private void resolveStoppedLocalOperation(final O op) {
			concurrencyThrottle.release();
			if (PENDING.equals(op.status())) {
				operationLifecycle().unattempted(op);
			} else {
				if (ACTIVE.equals(op.status())) {
					op.status(INTERRUPTED);
				}
				handleCompletedSafely(op);
			}
		}

		private void handleCompletedSafely(final O op) {
			try {
				handleCompleted(op);
			} catch (final Throwable cause) {
				throwUncheckedIfInterrupted(cause);
				logOperationFailure(cause, "I/O operation completion failure");
			}
		}

		@Override
		protected void doStop() {
			opBuffLock.lock();
			try {
				final var opBuffSize = opBuff.size();
				Loggers.MSG.debug("Finish {} remaining active load operations finally", opBuffSize);
				for (var i = 0; i < opBuffSize; i++) {
					resolveStoppedLocalOperation(opBuff.get(i));
				}
				opBuff.clear();
				Loggers.MSG.debug("Finish the remaining active load operations done");
			} finally {
				opBuffLock.unlock();
			}
		}

		@Override
		protected final void doClose() {
			opBuffLock.lock();
			try {
				opBuff.clear();
			} finally {
				opBuffLock.unlock();
			}
		}
	}

	private void logOperationFailure(final Throwable cause, final String message) {
		if (operationFailureReported.compareAndSet(false, true)) {
			LogUtil.exception(
							Level.ERROR, cause,
							message + "; further I/O failures are logged at DEBUG");
		} else {
			LogUtil.exception(Level.DEBUG, cause, message);
		}
	}

	/** The number of I/O workers this driver runs. */
	public final int ioWorkerCount() {
		return nioWorkerCount;
	}

	/** Whether every concurrency permit has a worker of its own. */
	public final boolean ioWorkerPerPermit() {
		return ioWorkerPerPermit;
	}

	/**
	Reentrant method which decorates the actual non-blocking create/read/etc I/O operation.
	May change the task status or not change if the I/O operation is not completed during this
	particular invocation
	@param op the operation to invoke
	*/
	protected abstract void invokeNio(final O op);

	@Override
	protected final void doStart()
					throws IllegalStateException {
		super.doStart();
		for (final var ioWorker : ioWorkers) {
			if (ioWorker.isStopped() && ioWorker instanceof TaskBase task) {
				task.restart();
			} else {
				ioWorker.start();
			}
		}
	}

	@Override
	protected final void doShutdown()
					throws IllegalStateException {
		super.doShutdown();
		stopIoWorkers();
	}

	@Override
	protected final void doStop()
					throws IllegalStateException {
		super.doStop();
		stopIoWorkers();
	}

	private void stopIoWorkers() {
		for (final var ioWorker : ioWorkers) {
			ioWorker.stop();
		}
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TASK_STOP_WAIT_SECONDS);
		for (final var ioWorker : ioWorkers) {
			final long remaining = deadline - System.nanoTime();
			if (remaining <= 0) {
				throw new IllegalStateException("Timed out waiting for NIO worker termination");
			}
			try {
				if (!ioWorker.await(remaining, TimeUnit.NANOSECONDS)) {
					throw new IllegalStateException("Timed out waiting for NIO worker termination");
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
		}
	}

	@Override
	protected final boolean successfulSubmitStartsTransport(final O op) {
		return false;
	}

	@Override
	protected final boolean submit(final O op)
					throws IllegalStateException {
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		if (!concurrencyThrottle.tryAcquire()) {
			return false;
		}
		final var startIdx = (int) (rrc.getAndIncrement() % nioWorkerCount);
		if (placeOnWorker(op, startIdx) >= 0) {
			return true;
		}
		concurrencyThrottle.release();
		return false;
	}

	/**
	 * Hands an operation to a worker, starting the round robin at the given worker. With a worker
	 * per permit only an idle worker takes it: a worker busy with a blocking request would hold the
	 * operation until that request ends, with the permit it carries. A worker releases a permit
	 * only after it has given up its reservation for the operation that held it, so a permit in
	 * hand means a worker that shows idle, and the caller retries when a race hides it.
	 *
	 * @return the worker that took the operation, or -1 when none could
	 */
	private int placeOnWorker(final O op, final int startIdx) {
		return placeOnWorker(op, startIdx, ioWorkerPerPermit);
	}

	private int placeOnWorker(final O op, final int startIdx, final boolean idleOnly) {
		for (var i = 0; i < nioWorkerCount && isStarted(); i++) {
			final var workerIdx = (startIdx + i) % nioWorkerCount;
			final var opBuffLock = opBuffLocks[workerIdx];
			// A worker holds its buffer lock only while it takes its batch or releases a permit. The
			// idle pass waits for it, so that an idle worker is never passed over; the pass over all
			// workers keeps trying the lock, as it always has.
			if (idleOnly) {
				opBuffLock.lock();
			} else if (!opBuffLock.tryLock()) {
				continue;
			}
			try {
				final var bufferedCount = opBuffs[workerIdx].size() + opBuffReserved[workerIdx];
				final var hasRoom = idleOnly ? bufferedCount == 0 : bufferedCount < opBuffCapacity;
				if (hasRoom && opBuffs[workerIdx].add(op)) {
					opAvailableConditions[workerIdx].signal();
					return workerIdx;
				}
			} finally {
				opBuffLock.unlock();
			}
		}
		return -1;
	}

	@Override
	protected final int submit(final List<O> ops, final int from, final int to)
					throws IllegalStateException {
		final int numOps = to - from;
		if (numOps <= 0) {
			return 0;
		}
		if (!isStarted()) {
			throw new IllegalStateException();
		}
		// Grab as many permits as available (non-blocking) — the dispatch task
		// handles backpressure via Condition signaling when no permits are free.
		var permits = concurrencyThrottle.drainPermits();
		if (permits == 0) {
			return 0;
		}
		if (permits > numOps) {
			concurrencyThrottle.release(permits - numOps);
			permits = numOps;
		}
		var submitted = 0;
		var opIdx = from;
		var nextStartIdx = (int) (rrc.getAndIncrement() % nioWorkerCount);
		while (submitted < permits && isStarted()) {
			if (placeOnWorker(ops.get(opIdx), nextStartIdx) < 0) {
				break;
			}
			submitted++;
			opIdx++;
			nextStartIdx = (nextStartIdx + 1) % nioWorkerCount;
		}
		if (submitted < permits) {
			concurrencyThrottle.release(permits - submitted);
		}
		return submitted;
	}

	@Override
	protected final int submit(final List<O> ops)
					throws IllegalStateException {
		return submit(ops, 0, ops.size());
	}

	@Override
	protected final List<O> recoverAdditionalQueuedOperations() {
		final var recovered = new ArrayList<O>();
		for (var i = 0; i < nioWorkerCount; i++) {
			final var opBuffLock = opBuffLocks[i];
			opBuffLock.lock();
			try {
				if (opBuffs[i] == null) {
					continue;
				}
				final var retainedActive = new ArrayList<O>(opBuffs[i].size());
				while (!opBuffs[i].isEmpty()) {
					final O op = opBuffs[i].remove(0);
					if (PENDING.equals(op.status())) {
						recovered.add(op);
						concurrencyThrottle.release();
					} else {
						retainedActive.add(op);
					}
				}
				opBuffs[i].addAll(retainedActive);
			} finally {
				opBuffLock.unlock();
			}
		}
		return recovered;
	}

	protected final void finishOperation(final O op) {
		try {
			op.startResponse();
			op.finishResponse();
			op.status(Operation.Status.SUCC);
		} catch (final IllegalStateException e) {
			LogUtil.exception(
							Level.WARN, e, "{}: finishing the load operation which is in an invalid state", op.toString());
			op.status(Operation.Status.FAIL_UNKNOWN);
		}
	}

	@Override
	protected void doClose()
					throws IOException {
		closeAdmission();
		recoverQueuedOperations();

		ioWorkers.forEach(
						worker -> {
							try {
								worker.close();
							} catch (final Exception e) {
								LogUtil.exception(Level.WARN, e, "Failed to close the I/O worker: {}", worker);
							}
						});
		ioWorkers.clear();

		for (var i = 0; i < nioWorkerCount; i++) {
			try (final var logCtx = CloseableThreadContext.put(KEY_CLASS_NAME, CLS_NAME)) {
				if (opBuffLocks[i].tryLock(1, TimeUnit.SECONDS)) {
					try {
						opBuffs[i].clear();
					} finally {
						opBuffLocks[i].unlock();
					}
				} else if (opBuffs[i].size() > 0) {
					Loggers.ERR.debug("Failed to obtain the load operations buff lock in time");
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
			opBuffs[i] = null;
		}

		super.doClose();
	}
}
