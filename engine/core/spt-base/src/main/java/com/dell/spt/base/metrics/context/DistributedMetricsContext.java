package com.dell.spt.base.metrics.context;

import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;

import java.util.List;

public interface DistributedMetricsContext<S extends DistributedAllMetricsSnapshot>
				extends MetricsContext<S> {

	int nodeCount();

	List<String> nodeAddrs();

	/**
	 * Returns the ordered identities corresponding to supplied snapshots, or {@code null} when a
	 * compatibility context cannot provide identity evidence.
	 */
	default List<String> contributorIds() {
		return null;
	}

	/** Returns the legacy public remote-node address presentation. */
	default List<String> nodesPresent() {
		return nodeAddrs();
	}

	/**
	 * Returns fresh contributor identities used for completeness decisions. This additive surface is
	 * separate from {@link #nodesPresent()} so existing fleet API consumers retain the established
	 * remote-address representation.
	 */
	default List<String> contributorsPresent() {
		return List.of();
	}

	/**
	 * Returns whether the current fleet snapshot is incomplete despite any generic counters it
	 * contains.
	 *
	 * <p>The compatibility default preserves the established missing-node calculation for extension
	 * contexts that have not opted in to richer contributor completeness tracking.
	 */
	default boolean partial() {
		final List<String> addrs = nodeAddrs();
		return addrs != null && nodeCount() > addrs.size();
	}

	List<Double> quantileValues();

	@Override
	S lastSnapshot();
}
