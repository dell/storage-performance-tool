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
	private final AtomicLong rrc = new AtomicLong(0);

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
		opBuffCapacity = Math.max(MIN_TASK_BUFF_CAPACITY, concurrencyLimit / ioWorkerCount);
		for (var i = 0; i < ioWorkerCount; i++) {
			opBuffs[i] = new CircularArrayBuffer<>(opBuffCapacity);
			opBuffLocks[i] = new ReentrantLock();
			opAvailableConditions[i] = opBuffLocks[i].newCondition();
			ioWorkers.add(new NioWorkerTask(IO_EXECUTOR, opBuffs[i], opBuffLocks[i], opAvailableConditions[i]));
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
		private final List<O> opLocalBuff;

		public NioWorkerTask(
						final VirtualThreadExecutor executor, final CircularBuffer<O> opBuff,
						final Lock opBuffLock, final Condition opAvailable) {
			super(executor);
			this.opBuff = opBuff;
			this.opBuffLock = opBuffLock;
			this.opAvailable = opAvailable;
			this.opLocalBuff = new ArrayList<>(opBuffCapacity);
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

				try {
					for (var i = 0; i < opBuffSize; i++) {
						final var op = opBuff.get(i);
						// check if the op is invoked 1st time
						if (PENDING.equals(op.status())) {
							if (!isStarted()) {
								// Permit already acquired in submit() — release it
								concurrencyThrottle.release();
								op.status(INTERRUPTED);
								handleCompleted(op);
								continue;
							}
							// Permit already acquired in submit() — mark active
							op.startRequest();
							op.finishRequest();
						}
						// perform non blocking I/O for the op
						invokeNio(op);
						// remove the op from the buffer if it is not active more
						if (!ACTIVE.equals(op.status())) {
							concurrencyThrottle.release();
							handleCompleted(op);
						} else {
							// the op remains in the buffer for the next iteration
							opLocalBuff.add(op);
						}
					}
				} catch (final Throwable cause) {
					throwUncheckedIfInterrupted(cause);
					LogUtil.exception(Level.ERROR, cause, "I/O worker failure");
				} finally {
					// put the active operations back into the buffer
					opBuff.clear();
					final var localSize = opLocalBuff.size();
					if (localSize > 0) {
						for (var i = 0; i < localSize; i++) {
							opBuff.add(opLocalBuff.get(i));
						}
						opLocalBuff.clear();
					}
				}
			} finally {
				opBuffLock.unlock();
			}
		}

		@Override
		protected void doStop() {
			opBuffLock.lock();
			try {
				final var opBuffSize = opBuff.size();
				Loggers.MSG.debug("Finish {} remaining active load operations finally", opBuffSize);
				for (var i = 0; i < opBuffSize; i++) {
					final var op = opBuff.get(i);
					if (ACTIVE.equals(op.status()) || PENDING.equals(op.status())) {
						op.status(INTERRUPTED);
						concurrencyThrottle.release();
						handleCompleted(op);
					}
				}
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
			ioWorker.start();
		}
	}

	@Override
	protected final void doShutdown()
					throws IllegalStateException {
		super.doShutdown();
		for (final var ioWorker : ioWorkers) {
			ioWorker.stop();
		}
	}

	@Override
	protected final void doStop()
					throws IllegalStateException {
		super.doStop();
		for (final var ioWorker : ioWorkers) {
			ioWorker.stop();
		}
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
		// Round-robin into a worker buffer
		final var j = (int) (rrc.getAndIncrement() % ioWorkerCount);
		opBuffLocks[j].lock();
		try {
			if (opBuffs[j].size() < opBuffCapacity && opBuffs[j].add(op)) {
				opAvailableConditions[j].signal();
				return true;
			}
		} finally {
			opBuffLocks[j].unlock();
		}
		// Buffer full — release permit and fail
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
		// Cap the number of ops we distribute at the number of permits acquired.
		// Each op placed in a worker buffer will eventually release one permit on
		// completion — distributing more ops than permits would inflate the
		// semaphore past concurrencyLimit.
		final int limit = from + permits;
		CircularBuffer<O> opBuff;
		Lock opBuffLock;
		var j = from;
		int k;
		int n;
		int m;
		for (var i = 0; i < ioWorkerCount; i++) {
			if (!isStarted()) {
				break;
			}
			m = (int) (rrc.getAndIncrement() % ioWorkerCount);
			opBuff = opBuffs[m];
			opBuffLock = opBuffLocks[m];
			if (opBuffLock.tryLock()) {
				try {
					n = Math.min(limit - j, opBuffCapacity - opBuff.size());
					for (k = 0; k < n; k++) {
						opBuff.add(ops.get(j + k));
					}
					if (n > 0) {
						opAvailableConditions[m].signal();
					}
					j += n;
				} finally {
					opBuffLock.unlock();
				}
			}
			if (j >= limit) {
				break;
			}
		}
		// Refund permits for any ops that could not be buffered
		final int submitted = j - from;
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
