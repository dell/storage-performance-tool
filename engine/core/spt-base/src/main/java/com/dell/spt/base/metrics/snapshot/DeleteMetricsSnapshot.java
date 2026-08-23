package com.dell.spt.base.metrics.snapshot;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_FIXED;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_IDENTITY_MODE_SINGLE;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_SELECTION_ORDER_CANONICAL;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable DELETE-specific metrics using explicit request and object units.
 *
 * <p>The generic metrics snapshot remains request-based. This additive snapshot is deliberately
 * separate so callers cannot infer request totals from object totals or fabricate byte-oriented
 * measurements for DELETE. Bucket cardinality is bounded to {@value #MAX_BUCKET_METRICS}; excess
 * names are combined under {@value #OVERFLOW_BUCKET}.
 */
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class DeleteMetricsSnapshot implements Serializable {

	/** Maximum number of named buckets retained before overflow aggregation. */
	public static final int MAX_BUCKET_METRICS = 100;

	/** Synthetic bucket holding counts beyond {@link #MAX_BUCKET_METRICS}. */
	public static final String OVERFLOW_BUCKET = "__other__";

	/**
	 * Immutable per-bucket DELETE target counters.
	 *
	 * @param bucket bucket name or {@link #OVERFLOW_BUCKET}
	 * @param selected selected targets
	 * @param attempted dispatched targets
	 * @param accepted storage-API accepted targets
	 * @param failed failed targets
	 */
	public record Bucket(
				String bucket,
				long selected,
				long attempted,
				long accepted,
				long failed) implements Serializable {}

	private final int configuredBatchSize;
	private final String mode;
	private final String selectionOrder;
	private final long requestAttempted;
	private final long requestFullSuccess;
	private final long requestPartial;
	private final long requestFailed;
	private final long requestUnresolved;
	private final double requestsPerSecond;
	private final long objectSelected;
	private final long objectAttempted;
	private final long objectAccepted;
	private final long objectFailed;
	private final long objectUnattempted;
	private final long objectUnresolved;
	private final double objectsPerSecond;
	private final long actualRequestCount;
	private final long actualObjectCount;
	private final long fullBatchCount;
	private final long partialBatchCount;
	private final long currentKeyCount;
	private final long exactVersionCount;
	private final List<Bucket> buckets;
	private final long seedNanos;
	private final long discoveryNanos;
	private final long preValidationNanos;
	private final long scheduledDeleteNanos;
	private final long drainNanos;
	private final long postVerificationNanos;
	private final long cleanupNanos;
	private final long totalWallNanos;
	private final String failurePolicyMode;
	private final String failureOutcome;
	private final long maxFailedObjects;
	private final double maxFailurePercent;
	private final long graceSeconds;
	private final long operationalFailedObjects;
	private final long excludedFailedObjects;
	private final double observedFailurePercent;
	private final boolean reconciled;

	private DeleteMetricsSnapshot(final Builder builder) {
		configuredBatchSize = builder.configuredBatchSize;
		mode = builder.mode;
		selectionOrder = builder.selectionOrder;
		requestAttempted = builder.requestAttempted;
		requestFullSuccess = builder.requestFullSuccess;
		requestPartial = builder.requestPartial;
		requestFailed = builder.requestFailed;
		requestUnresolved = builder.requestUnresolved;
		requestsPerSecond = builder.requestsPerSecond;
		objectSelected = builder.objectSelected;
		objectAttempted = builder.objectAttempted;
		objectAccepted = builder.objectAccepted;
		objectFailed = builder.objectFailed;
		objectUnattempted = builder.objectUnattempted;
		objectUnresolved = builder.objectUnresolved;
		objectsPerSecond = builder.objectsPerSecond;
		actualRequestCount = builder.actualRequestCount;
		actualObjectCount = builder.actualObjectCount;
		fullBatchCount = builder.fullBatchCount;
		partialBatchCount = builder.partialBatchCount;
		currentKeyCount = builder.currentKeyCount;
		exactVersionCount = builder.exactVersionCount;
		buckets = boundedBuckets(builder.buckets.values());
		seedNanos = builder.seedNanos;
		discoveryNanos = builder.discoveryNanos;
		preValidationNanos = builder.preValidationNanos;
		scheduledDeleteNanos = builder.scheduledDeleteNanos;
		drainNanos = builder.drainNanos;
		postVerificationNanos = builder.postVerificationNanos;
		cleanupNanos = builder.cleanupNanos;
		totalWallNanos = builder.totalWallNanos;
		failurePolicyMode = builder.failurePolicyMode;
		failureOutcome = builder.failureOutcome;
		maxFailedObjects = builder.maxFailedObjects;
		maxFailurePercent = builder.maxFailurePercent;
		graceSeconds = builder.graceSeconds;
		operationalFailedObjects = builder.operationalFailedObjects;
		excludedFailedObjects = builder.excludedFailedObjects;
		observedFailurePercent = builder.observedFailurePercent;
		reconciled = builder.reconciled;
	}

	/** Returns a builder for one configured DELETE batch size. */
	public static Builder builder(final int configuredBatchSize) {
		return new Builder(configuredBatchSize);
	}

	/** Returns a builder initialized from this snapshot. */
	public Builder toBuilder() {
		final Builder builder = builder(configuredBatchSize)
						.identity(mode, selectionOrder)
						.requests(
										requestAttempted,
										requestFullSuccess,
										requestPartial,
										requestFailed,
										requestUnresolved,
										requestsPerSecond)
						.objects(
										objectSelected,
										objectAttempted,
										objectAccepted,
										objectFailed,
										objectUnattempted,
										objectUnresolved,
										objectsPerSecond)
						.batches(actualRequestCount, actualObjectCount, fullBatchCount, partialBatchCount)
						.versions(currentKeyCount, exactVersionCount)
						.phases(
										seedNanos,
										discoveryNanos,
										preValidationNanos,
										scheduledDeleteNanos,
										drainNanos,
										postVerificationNanos,
										cleanupNanos,
										totalWallNanos)
						.failurePolicy(
										failurePolicyMode,
										maxFailedObjects,
										maxFailurePercent,
										graceSeconds,
										operationalFailedObjects,
										excludedFailedObjects)
						.failureOutcome(failureOutcome)
						.reconciled(reconciled);
		buckets.forEach(bucket -> builder.bucket(
						bucket.bucket(), bucket.selected(), bucket.attempted(), bucket.accepted(), bucket.failed()));
		return builder;
	}

	/** Aggregates compatible per-node snapshots without averaging percentile distributions. */
	public static DeleteMetricsSnapshot aggregate(final List<DeleteMetricsSnapshot> snapshots) {
		if (snapshots == null || snapshots.isEmpty()) {
			return null;
		}
		final DeleteMetricsSnapshot first = snapshots.get(0);
		final Builder result = builder(first.configuredBatchSize)
						.identity(first.mode, first.selectionOrder);
		double requestRate = 0;
		double objectRate = 0;
		long requestAttempted = 0;
		long requestFullSuccess = 0;
		long requestPartial = 0;
		long requestFailed = 0;
		long requestUnresolved = 0;
		long objectSelected = 0;
		long objectAttempted = 0;
		long objectAccepted = 0;
		long objectFailed = 0;
		long objectUnattempted = 0;
		long objectUnresolved = 0;
		long actualRequestCount = 0;
		long actualObjectCount = 0;
		long fullBatchCount = 0;
		long partialBatchCount = 0;
		long currentKeyCount = 0;
		long exactVersionCount = 0;
		long operationalFailedObjects = 0;
		long excludedFailedObjects = 0;
		long seedNanos = -1;
		long discoveryNanos = -1;
		long preValidationNanos = -1;
		long scheduledNanos = -1;
		long drainNanos = -1;
		long postVerificationNanos = -1;
		long cleanupNanos = -1;
		long totalNanos = -1;
		boolean reconciled = true;
		for (final DeleteMetricsSnapshot snapshot : snapshots) {
			if (snapshot == null) {
				throw new IllegalArgumentException("DELETE metrics snapshot must not be null");
			}
			if (first.configuredBatchSize != snapshot.configuredBatchSize
							|| !Objects.equals(first.mode, snapshot.mode)
							|| !Objects.equals(first.selectionOrder, snapshot.selectionOrder)
							|| !Objects.equals(first.failurePolicyMode, snapshot.failurePolicyMode)
							|| first.maxFailedObjects != snapshot.maxFailedObjects
							|| Double.compare(first.maxFailurePercent, snapshot.maxFailurePercent) != 0
							|| first.graceSeconds != snapshot.graceSeconds) {
				throw new IllegalArgumentException(
								"Cannot aggregate different DELETE result identities or failure policies");
			}
			requestAttempted += snapshot.requestAttempted;
			requestFullSuccess += snapshot.requestFullSuccess;
			requestPartial += snapshot.requestPartial;
			requestFailed += snapshot.requestFailed;
			requestUnresolved += snapshot.requestUnresolved;
			requestRate += snapshot.requestsPerSecond;
			objectSelected += snapshot.objectSelected;
			objectAttempted += snapshot.objectAttempted;
			objectAccepted += snapshot.objectAccepted;
			objectFailed += snapshot.objectFailed;
			objectUnattempted += snapshot.objectUnattempted;
			objectUnresolved += snapshot.objectUnresolved;
			objectRate += snapshot.objectsPerSecond;
			actualRequestCount += snapshot.actualRequestCount;
			actualObjectCount += snapshot.actualObjectCount;
			fullBatchCount += snapshot.fullBatchCount;
			partialBatchCount += snapshot.partialBatchCount;
			currentKeyCount += snapshot.currentKeyCount;
			exactVersionCount += snapshot.exactVersionCount;
			operationalFailedObjects += snapshot.operationalFailedObjects;
			excludedFailedObjects += snapshot.excludedFailedObjects;
			seedNanos = maxApplicable(seedNanos, snapshot.seedNanos);
			discoveryNanos = maxApplicable(discoveryNanos, snapshot.discoveryNanos);
			preValidationNanos = maxApplicable(preValidationNanos, snapshot.preValidationNanos);
			if (!Objects.equals(first.failureOutcome, snapshot.failureOutcome)) {
				throw new IllegalArgumentException(
								"Cannot aggregate different DELETE failure-budget outcomes");
			}
			scheduledNanos = maxApplicable(scheduledNanos, snapshot.scheduledDeleteNanos);
			drainNanos = maxApplicable(drainNanos, snapshot.drainNanos);
			postVerificationNanos = maxApplicable(
							postVerificationNanos, snapshot.postVerificationNanos);
			cleanupNanos = maxApplicable(cleanupNanos, snapshot.cleanupNanos);
			totalNanos = maxApplicable(totalNanos, snapshot.totalWallNanos);
			reconciled &= snapshot.reconciled;
			for (final Bucket bucket : snapshot.buckets) {
				result.bucket(bucket.bucket(), bucket.selected(), bucket.attempted(), bucket.accepted(), bucket.failed());
			}
		}
		return result
						.requests(requestAttempted, requestFullSuccess, requestPartial, requestFailed, requestUnresolved, requestRate)
						.objects(
										objectSelected,
										objectAttempted,
										objectAccepted,
										objectFailed,
										objectUnattempted,
										objectUnresolved,
										objectRate)
						.batches(actualRequestCount, actualObjectCount, fullBatchCount, partialBatchCount)
						.versions(currentKeyCount, exactVersionCount)
						.phases(
										seedNanos,
										discoveryNanos,
										preValidationNanos,
										scheduledNanos,
										drainNanos,
										postVerificationNanos,
										cleanupNanos,
										totalNanos)
						.failurePolicy(
										first.failurePolicyMode,
										first.maxFailedObjects,
										first.maxFailurePercent,
										first.graceSeconds,
										operationalFailedObjects,
										excludedFailedObjects)
						.failureOutcome(first.failureOutcome)
						.reconciled(reconciled)
						.build();
	}

	private static long maxApplicable(final long first, final long second) {
		return first < 0 ? second : second < 0 ? first : Math.max(first, second);
	}

	private static List<Bucket> boundedBuckets(final Iterable<Bucket> source) {
		final List<Bucket> sorted = new ArrayList<>();
		final long[] overflow = new long[4];
		final boolean[] overflowPresent = new boolean[1];
		source.forEach(bucket -> {
			if (OVERFLOW_BUCKET.equals(bucket.bucket())) {
				overflowPresent[0] = true;
				overflow[0] += bucket.selected();
				overflow[1] += bucket.attempted();
				overflow[2] += bucket.accepted();
				overflow[3] += bucket.failed();
			} else {
				sorted.add(bucket);
			}
		});
		sorted.sort(Comparator.comparing(Bucket::bucket));
		if (sorted.size() <= MAX_BUCKET_METRICS) {
			if (overflowPresent[0]) {
				sorted.add(new Bucket(
								OVERFLOW_BUCKET, overflow[0], overflow[1], overflow[2], overflow[3]));
			}
			return List.copyOf(sorted);
		}
		final List<Bucket> bounded = new ArrayList<>(MAX_BUCKET_METRICS + 1);
		bounded.addAll(sorted.subList(0, MAX_BUCKET_METRICS));
		for (int i = MAX_BUCKET_METRICS; i < sorted.size(); i++) {
			final Bucket bucket = sorted.get(i);
			overflow[0] += bucket.selected();
			overflow[1] += bucket.attempted();
			overflow[2] += bucket.accepted();
			overflow[3] += bucket.failed();
		}
		bounded.add(new Bucket(
						OVERFLOW_BUCKET, overflow[0], overflow[1], overflow[2], overflow[3]));
		return List.copyOf(bounded);
	}

	/** Returns the configured targets per logical request. */
	public int configuredBatchSize() {
		return configuredBatchSize;
	}

	/** Returns {@code single} or {@code batch}. */
	public String mode() {
		return mode;
	}

	/** Returns the canonical target selection order. */
	public String selectionOrder() {
		return selectionOrder;
	}

	/** Returns dispatched logical requests. */
	public long requestAttempted() {
		return requestAttempted;
	}

	/** Returns logical requests whose every target was accepted. */
	public long requestFullSuccess() {
		return requestFullSuccess;
	}

	/** Returns logical requests with mixed accepted and failed targets. */
	public long requestPartial() {
		return requestPartial;
	}

	/** Returns logical requests with no accepted targets. */
	public long requestFailed() {
		return requestFailed;
	}

	/** Returns dispatched logical requests without a terminal outcome. */
	public long requestUnresolved() {
		return requestUnresolved;
	}

	/** Returns the mean logical request dispatch rate. */
	public double requestsPerSecond() {
		return requestsPerSecond;
	}

	/** Returns selected object identities. */
	public long objectSelected() {
		return objectSelected;
	}

	/** Returns dispatched object identities. */
	public long objectAttempted() {
		return objectAttempted;
	}

	/** Returns storage-API accepted object identities. */
	public long objectAccepted() {
		return objectAccepted;
	}

	/** Returns failed object identities, including excluded protocol failures. */
	public long objectFailed() {
		return objectFailed;
	}

	/** Returns selected object identities that were never dispatched. */
	public long objectUnattempted() {
		return objectUnattempted;
	}

	/** Returns dispatched object identities without a terminal outcome. */
	public long objectUnresolved() {
		return objectUnresolved;
	}

	/** Returns the mean object-identity dispatch rate. */
	public double objectsPerSecond() {
		return objectsPerSecond;
	}

	/** Returns the observed logical request count. */
	public long actualRequestCount() {
		return actualRequestCount;
	}

	/** Returns the observed dispatched target count. */
	public long actualObjectCount() {
		return actualObjectCount;
	}

	/** Returns requests containing the configured batch size. */
	public long fullBatchCount() {
		return fullBatchCount;
	}

	/** Returns requests smaller than the configured batch size. */
	public long partialBatchCount() {
		return partialBatchCount;
	}

	/** Returns selected current-key targets. */
	public long currentKeyCount() {
		return currentKeyCount;
	}

	/** Returns selected exact-version targets. */
	public long exactVersionCount() {
		return exactVersionCount;
	}

	/** Returns the bounded, immutable per-bucket counters. */
	public List<Bucket> buckets() {
		return buckets;
	}

	/** Returns seed time in nanoseconds, or {@code -1} when not applicable. */
	public long seedNanos() {
		return seedNanos;
	}

	/** Returns discovery time in nanoseconds, or {@code -1} when not applicable. */
	public long discoveryNanos() {
		return discoveryNanos;
	}

	/** Returns pre-validation time in nanoseconds, or {@code -1} when not applicable. */
	public long preValidationNanos() {
		return preValidationNanos;
	}

	/** Returns scheduled DELETE time in nanoseconds. */
	public long scheduledDeleteNanos() {
		return scheduledDeleteNanos;
	}

	/** Returns bounded drain time in nanoseconds. */
	public long drainNanos() {
		return drainNanos;
	}

	/** Returns post-verification time in nanoseconds, or {@code -1} when not applicable. */
	public long postVerificationNanos() {
		return postVerificationNanos;
	}

	/** Returns cleanup time in nanoseconds, or {@code -1} when not applicable. */
	public long cleanupNanos() {
		return cleanupNanos;
	}

	/** Returns total applicable workflow wall time in nanoseconds. */
	public long totalWallNanos() {
		return totalWallNanos;
	}

	/** Returns the selected failed-object policy mode. */
	public String failurePolicyMode() {
		return failurePolicyMode;
	}

	/** Returns the live or controller-owned terminal failure-budget outcome. */
	public String failureOutcome() {
		return failureOutcome;
	}

	/** Returns the fixed operational-failure limit. */
	public long maxFailedObjects() {
		return maxFailedObjects;
	}

	/** Returns the percentage operational-failure limit. */
	public double maxFailurePercent() {
		return maxFailurePercent;
	}

	/** Returns the percentage-policy grace period in seconds. */
	public long graceSeconds() {
		return graceSeconds;
	}

	/** Returns failures included in the controller budget. */
	public long operationalFailedObjects() {
		return operationalFailedObjects;
	}

	/** Returns failures excluded from the controller budget. */
	public long excludedFailedObjects() {
		return excludedFailedObjects;
	}

	/** Returns operational failures divided by accepted plus operational failures. */
	public double observedFailurePercent() {
		return observedFailurePercent;
	}

	/** Returns whether every selected target reached an accounted state. */
	public boolean reconciled() {
		return reconciled;
	}

	/** Mutable builder for an immutable DELETE metrics snapshot. */
	public static final class Builder {
		private final int configuredBatchSize;
		private String mode = DELETE_IDENTITY_MODE_SINGLE;
		private String selectionOrder = DELETE_SELECTION_ORDER_CANONICAL;
		private long requestAttempted;
		private long requestFullSuccess;
		private long requestPartial;
		private long requestFailed;
		private long requestUnresolved;
		private double requestsPerSecond;
		private long objectSelected;
		private long objectAttempted;
		private long objectAccepted;
		private long objectFailed;
		private long objectUnattempted;
		private long objectUnresolved;
		private double objectsPerSecond;
		private long actualRequestCount;
		private long actualObjectCount;
		private long fullBatchCount;
		private long partialBatchCount;
		private long currentKeyCount;
		private long exactVersionCount;
		private final Map<String, Bucket> buckets = new LinkedHashMap<>();
		private long seedNanos = -1;
		private long discoveryNanos = -1;
		private long preValidationNanos = -1;
		private long scheduledDeleteNanos;
		private long drainNanos;
		private long postVerificationNanos = -1;
		private long cleanupNanos = -1;
		private long totalWallNanos;
		private String failurePolicyMode = DELETE_FAILURE_POLICY_MODE_FIXED;
		private String failureOutcome = DELETE_FAILURE_OUTCOME_RUNNING;
		private long maxFailedObjects;
		private double maxFailurePercent;
		private long graceSeconds;
		private long operationalFailedObjects;
		private long excludedFailedObjects;
		private double observedFailurePercent;
		private boolean reconciled;

		private Builder(final int configuredBatchSize) {
			if (configuredBatchSize < 1) {
				throw new IllegalArgumentException("configuredBatchSize must be positive");
			}
			this.configuredBatchSize = configuredBatchSize;
		}

		/** Sets the request mode and canonical target selection order. */
		public Builder identity(final String mode, final String selectionOrder) {
			this.mode = Objects.requireNonNull(mode, "mode");
			this.selectionOrder = Objects.requireNonNull(selectionOrder, "selectionOrder");
			return this;
		}

		/** Sets logical request outcomes and the request dispatch rate. */
		public Builder requests(final long attempted, final long fullSuccess, final long partial,
						final long failed, final long unresolved, final double perSecond) {
			requestAttempted = attempted;
			requestFullSuccess = fullSuccess;
			requestPartial = partial;
			requestFailed = failed;
			requestUnresolved = unresolved;
			requestsPerSecond = perSecond;
			return this;
		}

		/** Sets object-identity lifecycle counters and the object dispatch rate. */
		public Builder objects(final long selected, final long attempted, final long accepted,
						final long failed, final long unattempted, final long unresolved, final double perSecond) {
			objectSelected = selected;
			objectAttempted = attempted;
			objectAccepted = accepted;
			objectFailed = failed;
			objectUnattempted = unattempted;
			objectUnresolved = unresolved;
			objectsPerSecond = perSecond;
			return this;
		}

		/** Sets observed request and target counts by full or partial batch. */
		public Builder batches(final long actualRequests, final long actualObjects,
						final long fullBatches, final long partialBatches) {
			actualRequestCount = actualRequests;
			actualObjectCount = actualObjects;
			fullBatchCount = fullBatches;
			partialBatchCount = partialBatches;
			return this;
		}

		/** Sets current-key and exact-version target counts. */
		public Builder versions(final long currentKey, final long exactVersion) {
			currentKeyCount = currentKey;
			exactVersionCount = exactVersion;
			return this;
		}

		/** Adds counters for one bucket, merging repeated names. */
		public Builder bucket(final String bucket, final long selected, final long attempted,
						final long accepted, final long failed) {
			final String name = bucket == null || bucket.isBlank() ? OVERFLOW_BUCKET : bucket;
			buckets.merge(name, new Bucket(name, selected, attempted, accepted, failed),
							(existing, addition) -> new Bucket(name,
											existing.selected() + addition.selected(),
											existing.attempted() + addition.attempted(),
											existing.accepted() + addition.accepted(),
											existing.failed() + addition.failed()));
			return this;
		}

		/** Sets the always-applicable DELETE phases while leaving optional phases unavailable. */
		public Builder phases(final long scheduledDeleteNanos, final long drainNanos, final long totalWallNanos) {
			return phases(-1, -1, -1, scheduledDeleteNanos, drainNanos, -1, -1, totalWallNanos);
		}

		/** Sets every workflow phase, using {@code -1} for a phase that is not applicable. */
		public Builder phases(
						final long seedNanos,
						final long discoveryNanos,
						final long preValidationNanos,
						final long scheduledDeleteNanos,
						final long drainNanos,
						final long postVerificationNanos,
						final long cleanupNanos,
						final long totalWallNanos) {
			this.seedNanos = seedNanos;
			this.discoveryNanos = discoveryNanos;
			this.preValidationNanos = preValidationNanos;
			this.scheduledDeleteNanos = scheduledDeleteNanos;
			this.drainNanos = drainNanos;
			this.postVerificationNanos = postVerificationNanos;
			this.cleanupNanos = cleanupNanos;
			this.totalWallNanos = totalWallNanos;
			return this;
		}

		/** Sets a compatibility failure-policy view with an externally computed percentage. */
		public Builder failurePolicy(final String mode, final long maxFailedObjects,
						final double maxFailurePercent, final long graceSeconds, final double observedFailurePercent) {
			failurePolicyMode = Objects.requireNonNull(mode, "mode");
			this.maxFailedObjects = maxFailedObjects;
			this.maxFailurePercent = maxFailurePercent;
			this.graceSeconds = graceSeconds;
			operationalFailedObjects = objectFailed;
			excludedFailedObjects = 0;
			this.observedFailurePercent = observedFailurePercent;
			return this;
		}

		/** Sets failure-policy configuration and classified failure counters. */
		public Builder failurePolicy(
						final String mode,
						final long maxFailedObjects,
						final double maxFailurePercent,
						final long graceSeconds,
						final long operationalFailedObjects,
						final long excludedFailedObjects) {
			failurePolicyMode = Objects.requireNonNull(mode, "mode");
			this.maxFailedObjects = maxFailedObjects;
			this.maxFailurePercent = maxFailurePercent;
			this.graceSeconds = graceSeconds;
			this.operationalFailedObjects = operationalFailedObjects;
			this.excludedFailedObjects = excludedFailedObjects;
			final long outcomes = Math.addExact(objectAccepted, operationalFailedObjects);
			observedFailurePercent = outcomes == 0
							? 0.0
							: operationalFailedObjects * 100.0 / outcomes;
			return this;
		}

		/** Sets the live or controller-owned terminal failure-budget outcome. */
		public Builder failureOutcome(final String outcome) {
			failureOutcome = Objects.requireNonNull(outcome, "outcome");
			return this;
		}

		/** Sets whether every selected target reached an accounted state. */
		public Builder reconciled(final boolean reconciled) {
			this.reconciled = reconciled;
			return this;
		}

		/** Builds the immutable, cardinality-bounded snapshot. */
		public DeleteMetricsSnapshot build() {
			return new DeleteMetricsSnapshot(this);
		}
	}
}
