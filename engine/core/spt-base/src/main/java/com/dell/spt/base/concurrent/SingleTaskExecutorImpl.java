package com.dell.spt.base.concurrent;

import com.dell.spt.base.logging.LogContextThreadFactory;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.Level;

public final class SingleTaskExecutorImpl implements Runnable, SingleTaskExecutor {

	private static final ThreadFactory THREAD_FACTORY = new LogContextThreadFactory("single_task_executor_", true);

	private final AtomicReference<Thread> workerRef = new AtomicReference<>(null);
	private final AtomicReference<Runnable> taskRef = new AtomicReference<>(null);

	public SingleTaskExecutorImpl() {
		startWorker();
	}

	private void startWorker() {
		if (workerRef.compareAndSet(null, THREAD_FACTORY.newThread(this))) {
			workerRef.get().start();
		}
	}

	private void stopWorker() {
		final Thread worker = workerRef.getAndSet(null);
		if (null != worker) {
			worker.interrupt();
		}
	}

	@Override
	public final void run() {
		Runnable task;
		while (Thread.currentThread() == workerRef.get()) {
			task = taskRef.get();
			if (null == task) {
				LockSupport.park(); // Sleep until unparked or interrupted
				if (Thread.interrupted()) {
					// Clear interrupt status and continue
				}
			} else {
				try {
					task.run();
				} catch (final Throwable cause) {
					if (!(cause instanceof InterruptedException)) {
						LogUtil.trace(Loggers.ERR, Level.ERROR, cause, "Unexpected task execution failure");
					} else if (Thread.currentThread() == workerRef.get()) {
						// No stop replaced this worker, so it lives on to run the next task. A stopped
						// task's worker was replaced and ends quietly.
						LogUtil.trace(
										Loggers.ERR, Level.ERROR, cause, "A task ended with an interrupt that was not a stop");
					}
				} finally {
					taskRef.compareAndSet(task, null);
				}
				// A flag the ended task left set must not interrupt the next task. The flag is not cleared
				// before a task runs: a stop that lands before then has replaced this worker already.
				Thread.interrupted();
			}
		}
	}

	@Override
	public final void execute(final Runnable task) throws RejectedExecutionException {
		if (!taskRef.compareAndSet(null, task)) {
			throw new RejectedExecutionException();
		}
		final Thread worker = workerRef.get();
		if (worker != null) {
			LockSupport.unpark(worker);
		}
	}

	@Override
	public final Runnable task() {
		return taskRef.get();
	}

	@Override
	public final boolean stop(final Runnable task) {
		if (taskRef.compareAndSet(task, null)) {
			stopWorker();
			startWorker();
			return true;
		} else {
			return false;
		}
	}

	@Override
	public final void close() {
		stopWorker();
		taskRef.set(null);
	}
}
