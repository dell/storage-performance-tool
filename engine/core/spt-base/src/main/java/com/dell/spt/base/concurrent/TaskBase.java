package com.dell.spt.base.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for recurring tasks that run on a Virtual Thread.
 * <p>
 * Subclasses implement {@link #doWork()} with their work loop body. The VirtualThreadExecutor
 * submits this as a Runnable to a Virtual Thread, which calls doWork() in a loop until stopped.
 */
public abstract class TaskBase implements Task, Runnable {

	private volatile boolean started = false;
	private volatile boolean stopped = false;
	private volatile Thread taskThread;
	private final VirtualThreadExecutor executor;
	private volatile CountDownLatch stopLatch = new CountDownLatch(1);

	protected TaskBase(final VirtualThreadExecutor executor) {
		this.executor = executor;
	}

	@Override
	public void start() {
		if (started) {
			throw new IllegalStateException("Task already started");
		}
		started = true;
		executor.submit(this);
	}

	@Override
	public final void run() {
		taskThread = Thread.currentThread();
		try {
			doInit();
			while (started && !stopped && !Thread.currentThread().isInterrupted()) {
				doWork();
			}
		} catch (final Exception e) {
			if (!(e instanceof InterruptedException)) {
				handleError(e);
			}
		} finally {
			stopped = true;
			try {
				doStop();
			} finally {
				stopLatch.countDown();
			}
		}
	}

	/**
	 * Called once on the Virtual Thread before the work loop begins. Override to set up
	 * per-VT state such as Log4j ThreadContext (MDC) keys. With Virtual Threads, thread-local
	 * state persists for the VT's entire lifetime, so initialization done here does not need
	 * to be repeated in each doWork() iteration.
	 */
	protected void doInit() {}

	/**
	 * Subclasses implement their work loop body here. Called repeatedly until the task is
	 * stopped or interrupted. Blocking calls (queue.poll, Thread.sleep, lock.lock) will
	 * automatically unmount the Virtual Thread from its carrier.
	 */
	protected abstract void doWork() throws Exception;

	/**
	 * Called once when the task exits its run loop. Override for cleanup logging, metric
	 * finalization, etc.
	 */
	protected void doStop() {}

	/**
	 * Called when doWork() throws an unexpected (non-interrupt) exception. Override to log
	 * or handle errors. The default implementation does nothing — the task will stop.
	 */
	protected void handleError(final Exception e) {}

	@Override
	public void stop() {
		stopped = true;
		final var t = taskThread;
		if (t != null) {
			t.interrupt();
		}
	}

	/**
	 * Restart a previously-stopped task. Resets internal state and submits a fresh Virtual
	 * Thread so the doInit/doWork loop runs again. Throws if the task is still running.
	 */
	public void restart() {
		if (isStarted()) {
			throw new IllegalStateException("Task is still running");
		}
		started = false;
		stopped = false;
		taskThread = null;
		stopLatch = new CountDownLatch(1);
		start();
	}

	/**
	 * Wait for the task to finish after stop() has been called.
	 */
	public boolean awaitStop(final long timeout, final TimeUnit unit) throws InterruptedException {
		return stopLatch.await(timeout, unit);
	}

	@Override
	public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
		return awaitStop(timeout, unit);
	}

	@Override
	public boolean isStarted() {
		return started && !stopped;
	}

	@Override
	public boolean isStopped() {
		return stopped;
	}

	@Override
	public void close() {
		stop();
		doClose();
	}

	/**
	 * Called once during close(). Override to release resources.
	 */
	protected void doClose() {}
}
