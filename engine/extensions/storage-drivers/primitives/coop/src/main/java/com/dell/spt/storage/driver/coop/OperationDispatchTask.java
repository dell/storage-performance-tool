package com.dell.spt.storage.driver.coop;

import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.LogUtil;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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

	public OperationDispatchTask(
					final VirtualThreadExecutor executor, final CoopStorageDriverBase<I, O> storageDriver,
					final BlockingQueue<O> inOpQueue, final BlockingQueue<O> childOpQueue, final String stepId,
					final int batchSize) {
		super(executor);
		this.buff = new CircularArrayBuffer<>(batchSize);
		this.storageDriver = storageDriver;
		this.inOpQueue = inOpQueue;
		this.childOpQueue = childOpQueue;
		this.stepId = stepId;
		this.batchSize = batchSize;
	}

	@Override
	protected void doInit() {
		ThreadContext.put(KEY_STEP_ID, stepId);
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
	}

	@Override
	protected final void doWork() throws Exception {
		try {
			// Drain child ops first (partial/composite completions have priority)
			childOpQueue.drainTo(buff, batchSize - buff.size());
			// Then drain incoming ops — block briefly if nothing available
			if (buff.size() == 0) {
				final O op = inOpQueue.poll(1, TimeUnit.MILLISECONDS);
				if (op != null) {
					buff.add(op);
					inOpQueue.drainTo(buff, batchSize - buff.size());
				}
			} else if (buff.size() < batchSize) {
				inOpQueue.drainTo(buff, batchSize - buff.size());
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
				// Backpressure relief: if submit made no progress, yield the carrier thread.
				// Without this sleep the VT would spin-loop at full CPU because the buffer
				// still has items and the next doWork() iteration would skip the 1ms poll.
				if (!submitted) {
					Thread.sleep(1);
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
