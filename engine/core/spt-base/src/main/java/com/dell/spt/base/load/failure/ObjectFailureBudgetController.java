package com.dell.spt.base.load.failure;

import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import java.time.Duration;
import java.util.List;

/** Aggregates worker counters and is the sole owner of global failure-budget decisions. */
public final class ObjectFailureBudgetController {
	private final ObjectFailureBudgetConfig policy;

	public ObjectFailureBudgetController(final ObjectFailureBudgetConfig policy) {
		this.policy = java.util.Objects.requireNonNull(policy, "policy");
	}

	public ObjectFailureBudgetConfig policy() {
		return policy;
	}

	/** Evaluates cumulative global counters at a live poll or terminal completion. */
	public ObjectFailureBudgetDecision evaluate(
					final List<DeleteObjectLifecycleSnapshot> workerCounters,
					final Duration measuredElapsed,
					final boolean completion) {
		final Aggregation aggregation = aggregate(workerCounters, completion);
		if (aggregation.failureReason != null) {
			return decision(
							ObjectFailureBudgetOutcome.FAILED,
							true,
							aggregation.counters,
							aggregation.failureReason);
		}
		final ObjectFailureBudgetCounters counters = aggregation.counters;
		final double observedPercent = observedPercent(counters);
		if (counters.excludedFailedObjects() > 0) {
			return decision(
							ObjectFailureBudgetOutcome.FAILED,
							true,
							counters,
							"protocol or correctness object failures are outside operational budget room");
		}
		if (completion && !counters.terminalComplete()) {
			return decision(
							ObjectFailureBudgetOutcome.FAILED,
							true,
							counters,
							"terminal object counters are incomplete or do not reconcile");
		}
		if (completion && counters.unresolvedObjects() > 0) {
			return decision(
							ObjectFailureBudgetOutcome.FAILED,
							true,
							counters,
							"unresolved object identities make the result inconclusive");
		}
		final boolean graceExpired = policy.mode() == ObjectFailureBudgetMode.FIXED
						|| policy.maxFailurePercent() == 0
						|| completion
						|| !safeElapsed(measuredElapsed).minus(policy.grace()).isNegative();
		final boolean breached = switch (policy.mode()) {
		case FIXED -> counters.operationalFailedObjects() > policy.maxFailedObjects();
		case PERCENTAGE -> graceExpired && observedPercent > policy.maxFailurePercent();
		};
		if (breached) {
			return new ObjectFailureBudgetDecision(
							ObjectFailureBudgetOutcome.FAILED,
							true,
							policy,
							counters,
							observedPercent,
							"failed-object budget exceeded; scheduling must stop and dispatched requests drain; "
											+ "the threshold is a stop trigger, not a hard cap");
		}
		if (!completion) {
			return new ObjectFailureBudgetDecision(
							ObjectFailureBudgetOutcome.RUNNING,
							false,
							policy,
							counters,
							observedPercent,
							graceExpired ? "within failure budget" : "positive percentage grace is active");
		}
		if (counters.fullSuccessfulRequests() == 0 || counters.acceptedObjects() == 0) {
			return decision(
							ObjectFailureBudgetOutcome.FAILED,
							true,
							counters,
							"zero fully successful requests or zero accepted object deletes makes the run invalid");
		}
		final ObjectFailureBudgetOutcome outcome = counters.operationalFailedObjects() == 0
						? ObjectFailureBudgetOutcome.COMPLETED_CLEANLY
						: ObjectFailureBudgetOutcome.COMPLETED_WITHIN_BUDGET;
		return new ObjectFailureBudgetDecision(
						outcome,
						false,
						policy,
						counters,
						observedPercent,
						outcome == ObjectFailureBudgetOutcome.COMPLETED_CLEANLY
										? "completed cleanly"
										: "completed within failure budget");
	}

	private ObjectFailureBudgetDecision decision(
					final ObjectFailureBudgetOutcome outcome,
					final boolean stopScheduling,
					final ObjectFailureBudgetCounters counters,
					final String reason) {
		return new ObjectFailureBudgetDecision(
						outcome, stopScheduling, policy, counters, observedPercent(counters), reason);
	}

	private static Aggregation aggregate(
					final List<DeleteObjectLifecycleSnapshot> workers, final boolean completion) {
		if (workers == null || workers.isEmpty()) {
			return unavailable(completion, "no worker object counters are available");
		}
		long selected = 0;
		long attempted = 0;
		long accepted = 0;
		long operationalFailed = 0;
		long excludedFailed = 0;
		long unattempted = 0;
		long unresolved = 0;
		long fullSuccessfulRequests = 0;
		boolean terminalComplete = true;
		try {
			for (final DeleteObjectLifecycleSnapshot worker : workers) {
				if (worker == null) {
					return unavailable(completion, "terminal object counters are missing from a participant");
				}
				if (!validWorkerCounters(worker)) {
					return invalid("worker object counters are invalid or do not reconcile as claimed");
				}
				selected = Math.addExact(selected, worker.selected());
				attempted = Math.addExact(attempted, worker.attempted());
				accepted = Math.addExact(accepted, worker.accepted());
				operationalFailed = Math.addExact(
								operationalFailed, worker.failed() - worker.protocolFailed());
				excludedFailed = Math.addExact(excludedFailed, worker.protocolFailed());
				unattempted = Math.addExact(unattempted, worker.unattempted());
				unresolved = Math.addExact(unresolved, worker.unresolved());
				fullSuccessfulRequests = Math.addExact(
								fullSuccessfulRequests, worker.fullSuccessfulRequests());
				terminalComplete &= worker.reconciled();
			}
		} catch (final ArithmeticException failure) {
			return invalid("worker object counters overflowed during validation or aggregation");
		}
		return new Aggregation(
						new ObjectFailureBudgetCounters(
										selected,
										attempted,
										accepted,
										operationalFailed,
										excludedFailed,
										unattempted,
										unresolved,
										fullSuccessfulRequests,
										terminalComplete),
						null);
	}

	private static boolean validWorkerCounters(final DeleteObjectLifecycleSnapshot worker) {
		if (worker.selected() < 0
						|| worker.attempted() < 0
						|| worker.accepted() < 0
						|| worker.failed() < 0
						|| worker.unattempted() < 0
						|| worker.unresolved() < 0
						|| worker.protocolFailed() < 0
						|| worker.fullSuccessfulRequests() < 0
						|| worker.protocolFailed() > worker.failed()) {
			return false;
		}
		final long attempted = Math.addExact(
						Math.addExact(worker.accepted(), worker.failed()), worker.unresolved());
		if (worker.attempted() != attempted) {
			return false;
		}
		final long selected = Math.addExact(attempted, worker.unattempted());
		return worker.reconciled() == (worker.selected() == selected);
	}

	private static Aggregation invalid(final String reason) {
		return new Aggregation(
						new ObjectFailureBudgetCounters(0, 0, 0, 0, 0, 0, 0, 0, false),
						reason);
	}

	private static Aggregation unavailable(final boolean completion, final String reason) {
		return new Aggregation(
						new ObjectFailureBudgetCounters(0, 0, 0, 0, 0, 0, 0, 0, !completion),
						completion ? reason : null);
	}

	private static double observedPercent(final ObjectFailureBudgetCounters counters) {
		final long outcomes;
		try {
			outcomes = counters.attemptedOperationalOutcomes();
		} catch (final ArithmeticException failure) {
			return 100.0;
		}
		return outcomes == 0
						? 0.0
						: 100.0 * counters.operationalFailedObjects() / outcomes;
	}

	private static Duration safeElapsed(final Duration elapsed) {
		return elapsed == null || elapsed.isNegative() ? Duration.ZERO : elapsed;
	}

	private record Aggregation(ObjectFailureBudgetCounters counters, String failureReason) {}
}
