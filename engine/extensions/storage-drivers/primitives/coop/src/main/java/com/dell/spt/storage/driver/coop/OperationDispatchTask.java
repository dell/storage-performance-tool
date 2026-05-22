package com.dell.spt.storage.driver.coop;

import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

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

	private static final String CLS_NAME = OperationDispatchTask.class.getSimpleName();

	private final String stepId;
	private final int batchSize;
	private final BlockingQueue<O> childOpQueue;
	private final BlockingQueue<O> inOpQueue;
	private final CoopStorageDriverBase<I, O> storageDriver;
	private final CircularBuffer<O> buff;
	private final Lock dispatchLock;
	private final Condition dispatchReady;
	private final int deferredQueueCapacity;

	private final Queue<O> deferredMpuQueue;

	public OperationDispatchTask(
					final VirtualThreadExecutor executor, final CoopStorageDriverBase<I, O> storageDriver,
					final BlockingQueue<O> inOpQueue, final BlockingQueue<O> childOpQueue, final String stepId,
					final int batchSize, final Lock dispatchLock, final Condition dispatchReady, final int deferredQueueCapacity) {
		super(executor);
		this.buff = new CircularArrayBuffer<>(batchSize);
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
				childOpQueue.drainTo(buff, batchSize - buff.size());
			}

			// Then drain incoming ops
			final java.util.List<O> tempInOps = new java.util.ArrayList<>(batchSize);
			if (buff.size() == 0) {
				inOpQueue.drainTo(tempInOps, Math.max(0, Math.min(batchSize, deferredQueueCapacity - deferredMpuQueue.size())));
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
						childOpQueue.drainTo(buff, batchSize - buff.size());
					}
					if (buff.size() < batchSize) {
						inOpQueue.drainTo(tempInOps, Math.max(0, Math.min(batchSize - buff.size(), deferredQueueCapacity - deferredMpuQueue.size())));
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
						childOpQueue.drainTo(buff, batchSize - buff.size());
					}
					if (buff.size() < batchSize) {
						inOpQueue.drainTo(tempInOps, Math.max(0, Math.min(batchSize - buff.size(), deferredQueueCapacity - deferredMpuQueue.size())));
					}
				}
			} else if (buff.size() < batchSize) {
				inOpQueue.drainTo(tempInOps, Math.max(0, Math.min(batchSize - buff.size(), deferredQueueCapacity - deferredMpuQueue.size())));
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
				boolean submitted;
				if (buffSize == 1) { // non-batch mode
					submitted = storageDriver.submit(buff.get(0));
					if (submitted) {
						buff.clear();
					}
				} else { // batch mode
					final int m = storageDriver.submit(buff, 0, buffSize);
					submitted = m > 0;
					if (submitted) {
						buff.removeFirst(m);
					}
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
		}
	}

	@Override
	protected final void doClose() {
		// Design decision: any operations remaining in the buffer are dropped on shutdown.
		// For a benchmarking tool this is acceptable — metrics have already captured the
		// completed operations, and attempting a draining flush here risks hangs if the
		// downstream driver is in a failed or saturated state.
		buff.clear();
	}
}
