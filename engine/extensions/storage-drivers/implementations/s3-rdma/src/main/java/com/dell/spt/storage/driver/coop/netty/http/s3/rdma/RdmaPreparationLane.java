package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

import com.dell.spt.base.logging.LogUtil;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

/**
 * One NIC's Preparation Lane (item 12a): a platform thread named after the NIC, which prepares that
 * NIC's attempts one at a time. It allocates and first touches its NIC's buffers, copies PUT payloads
 * with its NIC's copier, makes the tokens and hands each attempt to its NIC's Path Pool. So a slow
 * allocation, registration or copy delays only this NIC, and the dispatch task keeps admitting for
 * the others. It keeps to its NIC's NUMA node, so its NIC's pool is first touched there (D77).
 *
 * <p>The dispatch task only hands attempts over. The queue's capacity is the NIC's admission limit,
 * which holds every waiting attempt, so a hand-off never finds it full and the driver's queue is
 * never drained into it past the bound (item 9c, N3).
 *
 * <p>Shutdown (item 12b): {@link #cancel} stops the lane at its next check, wherever it is, and never
 * waits. Whoever closes the driver then waits against one budget for all lanes. A lane past the budget
 * is in a call that cannot be interrupted, so its NIC stays open: {@link #closeWhenDone} leaves the
 * lane to close its NIC when the call returns.
 */
final class RdmaPreparationLane {

	/** What the lane prepares; both methods run at most once per task, and only one of them. */
	interface Task {

		/** Prepares the attempt and hands it on, or ends it. Runs on the lane. */
		void prepare();

		/** Ends an attempt that was not prepared: it gives back what it holds. Runs on any thread. */
		void interrupted();
	}

	private final RdmaNic nic;
	private final String stepId;
	private final BlockingQueue<Task> queue;
	private final Consumer<Throwable> onDeath;
	private final Runnable onStart;
	private final Thread thread;
	private volatile boolean cancelled;
	private volatile boolean exited;
	private volatile boolean closeOnExit;

	/**
	 * @param capacity the attempts the NIC admits at once
	 * @param onDeath told of a failure that ended the lane before it was cancelled
	 * @param onStart runs on the lane before its first task; it places the lane on its NIC's NUMA node (D77)
	 */
	RdmaPreparationLane(final String stepId, final RdmaNic nic, final int capacity, final Consumer<Throwable> onDeath, final Runnable onStart) {
		this.nic = nic;
		this.stepId = stepId;
		this.queue = new ArrayBlockingQueue<>(capacity);
		this.onDeath = onDeath;
		this.onStart = onStart;
		this.thread = Thread.ofPlatform().daemon().name(threadName(stepId, nic.name)).unstarted(this::run);
	}

	static String threadName(final String stepId, final String nic) {
		return stepId + "-rdma-lane-" + nic;
	}

	void start() {
		thread.start();
	}

	/**
	 * Hands a task to the lane; false when the lane no longer takes any, and the task stays with the
	 * caller. A task that races a cancel is ended here or by the lane, once.
	 */
	boolean offer(final Task task) {
		if (cancelled || exited || !queue.offer(task)) {
			return false;
		}
		if (cancelled || exited) {
			drain();
		}
		return true;
	}

	/** Stops the lane at its next check and the NIC's copier at its next chunk; returns at once. */
	void cancel() {
		cancelled = true;
		nic.copier.signalStop();
		thread.interrupt();
	}

	boolean cancelled() {
		return cancelled;
	}

	boolean exited() {
		return exited;
	}

	/** Waits for the lane to end, until {@code deadlineNanos} of {@link System#nanoTime}; an interrupt is kept. */
	boolean awaitExit(final long deadlineNanos) {
		boolean interrupted = false;
		try {
			while (!exited) {
				final long remaining = deadlineNanos - System.nanoTime();
				if (remaining <= 0) {
					return false;
				}
				try {
					TimeUnit.NANOSECONDS.timedJoin(thread, remaining);
				} catch (final InterruptedException e) {
					interrupted = true;
				}
				if (!thread.isAlive()) {
					return true;
				}
			}
			return true;
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/** Ends every task still queued. */
	void drain() {
		Task task;
		while ((task = queue.poll()) != null) {
			task.interrupted();
		}
	}

	/** Leaves it to the lane to close its NIC when it ends, or closes it now if it has ended. */
	void closeWhenDone() {
		closeOnExit = true;
		if (exited) {
			nic.close();
		}
	}

	/** Names the call the lane is in, for the warning of a lane past the budget. */
	String blockedCall() {
		for (final StackTraceElement frame : thread.getStackTrace()) {
			final var call = describe(frame.getClassName(), frame.getMethodName());
			if (call != null) {
				return call;
			}
		}
		return "preparation";
	}

	private static String describe(final String className, final String method) {
		if (method.endsWith("egisterBuffer") && !method.contains("eregister")) {
			return "buffer registration";
		}
		if (method.endsWith("eregisterBuffer")) {
			return "buffer deregistration";
		}
		if (method.endsWith("enerateToken")) {
			return "token generation";
		}
		if (className.endsWith("RdmaPayloadCopier")) {
			return "payload copy";
		}
		if (className.endsWith("RdmaBufferPool")) {
			return "buffer allocation";
		}
		return null;
	}

	private void run() {
		ThreadContext.put(KEY_STEP_ID, stepId);
		ThreadContext.put(KEY_CLASS_NAME, RdmaPreparationLane.class.getSimpleName());
		try {
			if (!cancelled) {
				onStart.run();
			}
			while (!cancelled) {
				final Task task = queue.take();
				if (cancelled) {
					task.interrupted();
					break;
				}
				task.prepare();
			}
		} catch (final InterruptedException e) {
			if (!cancelled) {
				onDeath.accept(e);
			}
		} catch (final Throwable thrown) {
			if (cancelled) {
				LogUtil.exception(Level.DEBUG, thrown, "{}: NIC {}: a cancelled preparation lane ended with a failure", stepId, nic.name);
			} else {
				onDeath.accept(thrown);
			}
		} finally {
			drain();
			exited = true;
			if (closeOnExit) {
				nic.close();
			}
		}
	}
}
