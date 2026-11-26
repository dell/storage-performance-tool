package com.dell.spt.base.load.step;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The step ids each run has started on this engine, so that a step can tell a file an earlier run
 * left under its id from one an earlier step of its own run left. A run is known by the run id its
 * Entry Node sends. Only the latest {@value #RUNS_KEPT} runs are kept: a run older than those is
 * taken for an earlier run's.
 */
public final class RunStepIds {

	static final int RUNS_KEPT = 16;

	private final Map<Long, Set<String>> stepIdsByRun = new LinkedHashMap<>();

	/**
	 * Records that run {@code runId} starts a step {@code stepId} here, and tells whether it is the
	 * first with that id in that run. A run id of zero or less is not a known run, so no step of it is
	 * ever the first: a file its id finds is refused, as within one run.
	 */
	public synchronized boolean firstInRun(final long runId, final String stepId) {
		if (runId <= 0L) {
			return false;
		}
		var stepIds = stepIdsByRun.remove(runId);
		if (stepIds == null) {
			stepIds = new HashSet<>();
		}
		stepIdsByRun.put(runId, stepIds);
		final Iterator<Long> oldest = stepIdsByRun.keySet().iterator();
		while (stepIdsByRun.size() > RUNS_KEPT) {
			oldest.next();
			oldest.remove();
		}
		return stepIds.add(stepId);
	}
}
