package com.dell.spt.base.control.run;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * State scoped to one scenario run, bound to the thread that evaluates the scenario and cleared
 * when the run ends. Load steps start on that thread, so their controller-side setup can see it.
 */
public final class RunScope implements AutoCloseable {

	private static final ThreadLocal<RunScope> CURRENT = new ThreadLocal<>();

	private final RunScope previous;
	private final Set<String> reported = ConcurrentHashMap.newKeySet();

	private RunScope(final RunScope previous) {
		this.previous = previous;
	}

	/** Binds a new scope to the current thread until {@link #close()}. */
	public static RunScope open() {
		final var scope = new RunScope(CURRENT.get());
		CURRENT.set(scope);
		return scope;
	}

	/** Returns the scope of the run evaluating on this thread, or null outside a run. */
	public static RunScope current() {
		return CURRENT.get();
	}

	/**
	 * Returns true the first time the given key is reported in this run, so a message that should
	 * appear once per run can be logged at INFO and its repeats at DEBUG.
	 */
	public boolean firstReport(final String key) {
		return reported.add(key);
	}

	@Override
	public void close() {
		reported.clear();
		if (previous == null) {
			CURRENT.remove();
		} else {
			CURRENT.set(previous);
		}
	}
}
