package com.dell.spt.base.metrics;

import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.github.akurilov.fiber4j.Fiber;
import java.util.Set;
import java.util.List;

public interface MetricsManager extends Fiber {

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
}
