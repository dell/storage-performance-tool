package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import java.util.List;
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
