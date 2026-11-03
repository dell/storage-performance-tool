package com.dell.spt.base.load.step.local;

import com.dell.spt.base.load.step.PreparationStatus;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.storage.driver.AdmissionPreparable;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;

/**
 * One slice's preparation, which ends READY, FAILED or FENCED. Only a READY preparation can be
 * released, once, and a fence holds for good, so a late ready or release changes nothing.
 */
final class StepPreparation implements AdmissionPreparable.Preparation {

	private final long id;
	private final long startNanos;
	private final long deadlineNanos;
	private final List<Runnable> fenceActions = new ArrayList<>();
	private PreparationStatus.State state = PreparationStatus.State.RUNNING;
	private String phase = "starting";
	private String message = "";
	private long endNanos;
	private boolean released;
	private boolean stepFailed;

	StepPreparation(final long id, final long budgetNanos, final long nowNanos) {
		if (budgetNanos <= 0) {
			throw new IllegalArgumentException("the preparation budget must be positive: " + budgetNanos);
		}
		this.id = id;
		this.startNanos = nowNanos;
		this.deadlineNanos = nowNanos + budgetNanos;
	}

	long id() {
		return id;
	}

	@Override
	public long deadlineNanos() {
		return deadlineNanos;
	}

	@Override
	public synchronized boolean fenced() {
		return state == PreparationStatus.State.FENCED;
	}

	@Override
	public synchronized void phase(final String phase) {
		if (state == PreparationStatus.State.RUNNING) {
			this.phase = phase;
		}
	}

	@Override
	public void onFence(final Runnable action) {
		synchronized (this) {
			if (state != PreparationStatus.State.FENCED) {
				fenceActions.add(action);
				return;
			}
		}
		action.run();
	}

	synchronized void ready(final long nowNanos) {
		if (state == PreparationStatus.State.RUNNING) {
			state = PreparationStatus.State.READY;
			phase = "ready";
			endNanos = nowNanos;
		}
	}

	synchronized void fail(final String why, final long nowNanos) {
		if (state == PreparationStatus.State.RUNNING) {
			state = PreparationStatus.State.FAILED;
			message = why;
			endNanos = nowNanos;
		}
	}

	/** Fences this preparation unless it has been released; returns whether this call fenced it. */
	boolean fence(final String why, final long nowNanos) {
		final boolean fenced = markFenced(why, nowNanos);
		if (fenced) {
			runFenceActions();
		}
		return fenced;
	}

	/**
	 * The state change of {@link #fence}, for a caller that must make it atomic with a release under
	 * its own lock and then call {@link #runFenceActions()} outside that lock: a driver's action may
	 * close a channel or wait on an event loop.
	 */
	synchronized boolean markFenced(final String why, final long nowNanos) {
		if (released || state == PreparationStatus.State.FENCED) {
			return false;
		}
		if (state == PreparationStatus.State.RUNNING) {
			endNanos = nowNanos;
		}
		state = PreparationStatus.State.FENCED;
		if (message.isEmpty()) {
			message = why;
		}
		return true;
	}

	void runFenceActions() {
		final List<Runnable> actions;
		synchronized (this) {
			actions = List.copyOf(fenceActions);
			fenceActions.clear();
		}
		for (final Runnable action : actions) {
			try {
				action.run();
			} catch (final RuntimeException e) {
				LogUtil.exception(Level.WARN, e, "A preparation's fence action failed");
			}
		}
	}

	/**
	 * Returns whether admission may open: true once this preparation has been released. A preparation
	 * past its deadline is not released, even if the fence has not happened yet.
	 */
	synchronized boolean release(final long nowNanos) {
		if (state == PreparationStatus.State.READY && nowNanos - deadlineNanos < 0) {
			released = true;
		}
		return released;
	}

	synchronized boolean released() {
		return released;
	}

	/** Records that the step failed at the barrier, also when this slice had already opened. */
	synchronized void markStepFailed() {
		stepFailed = true;
	}

	/** Returns whether the step got past the barrier, so its evidence can be complete. */
	synchronized boolean admitted() {
		return released && !stepFailed;
	}

	synchronized PreparationStatus status(final long nowNanos) {
		final long end = state == PreparationStatus.State.RUNNING ? nowNanos : endNanos;
		return new PreparationStatus(state, phase, message, end - startNanos);
	}
}
