package com.dell.spt.storage.driver.coop.nio;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
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

	private final int ioWorkerCount;
	private final int opBuffCapacity;
	private final List<Task> ioWorkers;
	private final CircularBuffer<O>[] opBuffs;
	private final Lock[] opBuffLocks;
	private final Condition[] opAvailableConditions;
	private final int[] opBuffReserved;
	private final AtomicLong rrc = new AtomicLong(0);
	private final AtomicBoolean operationFailureReported = new AtomicBoolean(false);

	@SuppressWarnings("unchecked")
	public NioStorageDriverBase(
					final String testStepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
					final int batchSize) throws IllegalConfigurationException {
		super(testStepId, dataInput, storageConfig, verifyFlag, batchSize);
		final var confWorkerCount = storageConfig.intVal("driver-threads");
		if (confWorkerCount > 0) {
			ioWorkerCount = confWorkerCount;
		} else if (concurrencyLimit > 0) {
			ioWorkerCount = Math.min(concurrencyLimit, ThreadUtil.getHardwareThreadCount());
		} else {
			ioWorkerCount = ThreadUtil.getHardwareThreadCount();
		}
		ioWorkers = new ArrayList<>(ioWorkerCount);
		opBuffs = new CircularBuffer[ioWorkerCount];
		opBuffLocks = new Lock[ioWorkerCount];
		opAvailableConditions = new Condition[ioWorkerCount];
		opBuffReserved = new int[ioWorkerCount];
		opBuffCapacity = Math.max(MIN_TASK_BUFF_CAPACITY, concurrencyLimit / ioWorkerCount);
		for (var i = 0; i < ioWorkerCount; i++) {
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

		@Override
		protected final void doWork() throws Exception {

			opBuffLock.lock();
			try {
				var opBuffSize = opBuff.size();
				if (opBuffSize == 0) {
					// Wait for work — VT unmounts during await, freeing the carrier thread.
					// The 1ms timeout is a safety net; normal wake-up is via signal from submit().
					opAvailable.await(1, TimeUnit.MILLISECONDS);
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
						concurrencyThrottle.release();
						processedCount++;
						handleCompletedSafely(op);
						continue;
					}
					// remove the op from the buffer if it is not active more
					if (!ACTIVE.equals(op.status())) {
						concurrencyThrottle.release();
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
				LogUtil.exception(Level.ERROR, cause, "I/O worker failure");
			} finally {
				opBuffLock.lock();
				try {
					if (!isAdmissionOpen()) {
						for (var i = processedCount; i < opLocalBuff.size(); i++) {
							final var op = opLocalBuff.get(i);
							if (PENDING.equals(op.status())) {
								resolveStoppedLocalOperation(op);
							} else if (ACTIVE.equals(op.status())) {
								opActiveBuff.add(op);
							} else {
								concurrencyThrottle.release();
								handleCompletedSafely(op);
							}
						}
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
			}
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
							message + "; further per-operation failures are logged at DEBUG");
		} else {
			LogUtil.exception(Level.DEBUG, cause, message);
		}
	}

	/**
	Reentrant method which decorates the actual non-blocking create/read/etc I/O operation.
	May change the task status or not change if the I/O operation is not completed during this
	particular invocation
	@param op
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
		final var startIdx = (int) (rrc.getAndIncrement() % ioWorkerCount);
		for (var i = 0; i < ioWorkerCount && isStarted(); i++) {
			final var workerIdx = (startIdx + i) % ioWorkerCount;
			final var opBuffLock = opBuffLocks[workerIdx];
			if (opBuffLock.tryLock()) {
				try {
					final var bufferedCount = opBuffs[workerIdx].size() + opBuffReserved[workerIdx];
					if (bufferedCount < opBuffCapacity && opBuffs[workerIdx].add(op)) {
						opAvailableConditions[workerIdx].signal();
						return true;
					}
				} finally {
					opBuffLock.unlock();
				}
			}
		}
		concurrencyThrottle.release();
		return false;
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
		var nextStartIdx = (int) (rrc.getAndIncrement() % ioWorkerCount);
		while (submitted < permits && isStarted()) {
			var accepted = false;
			for (var i = 0; i < ioWorkerCount; i++) {
				final var workerIdx = (nextStartIdx + i) % ioWorkerCount;
				final var opBuffLock = opBuffLocks[workerIdx];
				if (opBuffLock.tryLock()) {
					try {
						final var opBuff = opBuffs[workerIdx];
						final var bufferedCount = opBuff.size() + opBuffReserved[workerIdx];
						if (bufferedCount < opBuffCapacity && opBuff.add(ops.get(opIdx))) {
							opAvailableConditions[workerIdx].signal();
							accepted = true;
						}
					} finally {
						opBuffLock.unlock();
					}
				}
				if (accepted) {
					break;
				}
			}
			if (!accepted) {
				break;
			}
			submitted++;
			opIdx++;
			nextStartIdx = (nextStartIdx + 1) % ioWorkerCount;
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
		for (var i = 0; i < ioWorkerCount; i++) {
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

		for (var i = 0; i < ioWorkerCount; i++) {
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
