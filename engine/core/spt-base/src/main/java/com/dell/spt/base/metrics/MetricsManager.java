package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.Task;
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
	default void recordPreparation(final String stepId, final long distributionMillis, final Map<String, Long> engineMillis) {}

	/** Returns the step's preparation times as a metrics row shows them, or null without a barrier. */
	default Map<String, Object> preparation(final String stepId) {
		return null;
	}

	default List<TerminalStepEntry> getTerminalSteps() {
		return List.of();
	}

	/** Latest non-zero progress for a step that is still registered. */
	default Optional<TerminalStepEntry> getLastProgressSnapshot(final String stepId) {
		return getLastProgressSnapshot(stepId, false);
	}

	/** Latest non-zero progress for a step that is still registered, keyed by role. */
	default Optional<TerminalStepEntry> getLastProgressSnapshot(final String stepId, final boolean distributed) {
		return Optional.empty();
	}

	/**
	 * Replaces the controller-owned failure-budget outcome in a retained fleet DELETE row.
	 * Node contributor rows deliberately retain their non-authoritative live outcome.
	 */
	default void updateTerminalDeleteFailureOutcome(final String stepId, final String outcome) {}
}
