package com.dell.spt.base.metrics.context;

import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;

import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/** Builder for distributed metrics contexts. */
public interface DistributedContextBuilder
				extends ContextBuilder<DistributedContextBuilder, DistributedMetricsContextImpl> {

	DistributedContextBuilder quantileValues(final List<Double> quantileValues);

	DistributedContextBuilder nodeAddrs(final List<String> nodeAddrs);

	default DistributedContextBuilder contributorIds(final List<String> contributorIds) {
		return this;
	}

	default DistributedContextBuilder deleteDetailsExpected(final boolean expected) {
		return this;
	}

	DistributedContextBuilder nodeCountSupplier(final IntSupplier nodeCountSupplier);

	DistributedContextBuilder snapshotsSupplier(
					final Supplier<List<AllMetricsSnapshot>> snapshotsSupplier);

	DistributedContextBuilder avgPersistFlag(final boolean avgPersistFlag);

	DistributedContextBuilder sumPersistFlag(final boolean sumPersistFlag);

	DistributedContextBuilder timingPersistFlag(final boolean timingPersistFlag);

	// Optional: for completion percent calculation
	DistributedContextBuilder opCountLimit(final long countLimit);

	DistributedContextBuilder timeLimitSec(final long timeLimitSec);
}
