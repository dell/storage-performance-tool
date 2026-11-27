package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface MetricsManager extends Task {

	void register(final MetricsContext metricsCtx);

	void unregister(final MetricsContext metricsCtx);

	/**
	 * Get all currently registered distributed metrics contexts for JSON endpoint access.
	 * @return Set of active distributed metrics contexts
	 */
	Set<DistributedMetricsContext> getDistributedContexts();

	/**
	 * Get all currently registered metrics contexts on this node (both distributed and local).
	 * Intended for local JSON metrics exposure on workers.
	 */
	default java.util.Set<com.dell.spt.base.metrics.context.MetricsContext> getAllContexts() {
		return java.util.Set.of();
	}

	/** Configure retention for terminal entries (milliseconds). */
	default void setTerminalRetentionMillis(long millis) {}

	/** Get retained finished step entries (filtered by retention). */
																		/**
																		 * Records how long a step's preparation took, for its metrics rows: the Entry Node's item
																		 * distribution, and each engine by {@code entry} or node address.
																		 */
	default void recordPreparation(
					final long runId, final String stepId, final long distributionMillis, final Map<String, Long> engineMillis) {}

	/** Returns the step's preparation times as a metrics row shows them, or null without a barrier. */
	default Map<String, Object> preparation(final long runId, final String stepId) {
		return null;
	}

	default List<TerminalStepEntry> getTerminalSteps() {
		return List.of();
	}

	/**
	 * Latest non-zero progress for a step that is still registered. The engine's manager returns an
	 * entry only for a step with one node context; for a step with several (mixed, weighted and
	 * pipeline steps) it returns empty, and each context's entry is read with
	 * {@link #getLastProgressSnapshot(MetricsContext)}.
	 */
	default Optional<TerminalStepEntry> getLastProgressSnapshot(final String stepId) {
		return getLastProgressSnapshot(stepId, false);
	}

	/**
	 * Latest non-zero progress for a step that is still registered, keyed by role. The engine's
	 * manager returns an entry only for a step with one context of that role, and empty for a step
	 * with several.
	 */
	default Optional<TerminalStepEntry> getLastProgressSnapshot(final String stepId, final boolean distributed) {
		return Optional.empty();
	}

	/**
	 * Latest non-zero progress of one metrics context that is still registered. A step with several
	 * contexts (mixed, weighted and pipeline steps) has one entry per context, so each live row reads
	 * its own. This default falls back to the step lookup of the context's scope, the one-argument
	 * lookup for a node context and the two-argument one for a fleet context, as callers used them
	 * before, keeping an entry only when its operation type is the context's.
	 */
	default Optional<TerminalStepEntry> getLastProgressSnapshot(final MetricsContext<?> metricsCtx) {
		if (metricsCtx == null) {
			return Optional.empty();
		}
		final String stepId = metricsCtx.loadStepId();
		final Optional<TerminalStepEntry> stepEntry = metricsCtx instanceof DistributedMetricsContext
						? getLastProgressSnapshot(stepId, true)
						: getLastProgressSnapshot(stepId);
		final OpType opType = metricsCtx.opType();
		return stepEntry.filter(entry -> entry.opType == null || entry.opType == opType);
	}

	/**
	 * Replaces the controller-owned failure-budget outcome in a retained fleet DELETE row.
	 * Node contributor rows deliberately retain their non-authoritative live outcome.
	 */
	default void updateTerminalDeleteFailureOutcome(final String stepId, final String outcome) {}
}
