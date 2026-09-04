package com.dell.spt.base.item.op.deletion;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;

/** Cross-view verification classifications joined to timed DELETE target outcomes. */
public record DeleteVerificationSummary(
			boolean preValidationEnabled,
			boolean postVerificationEnabled,
			boolean preValidationComplete,
			boolean postVerificationComplete,
			boolean postVerificationSkipped,
			long timeoutMillis,
			long preValidationFailures,
			long verifiedAbsent,
			long stillPresent,
			long unresolved,
			long acceptedAbsent,
			long acceptedPresent,
			long acceptedUnresolved,
			long failedAbsent,
			long failedPresent,
			long failedUnresolved,
			long operationalUnresolvedAbsent,
			long operationalUnresolvedPresent,
			long operationalUnresolvedUnresolved,
			long unattemptedAbsent,
			long unattemptedPresent,
			long unattemptedUnresolved) implements Serializable {

	private static final long serialVersionUID = 1L;

	/** Disabled compatibility view. */
	public static DeleteVerificationSummary disabled() {
		return new DeleteVerificationSummary(
				false, false, true, true, false,
				StandaloneDeleteConfig.DEFAULT_VERIFICATION_TIMEOUT_MILLIS, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
	}

	/** Aggregates compatible worker classifications. */
	public static DeleteVerificationSummary aggregate(final List<DeleteVerificationSummary> values) {
		if (values == null || values.isEmpty()) {
			return disabled();
		}
		final var first = Objects.requireNonNull(values.get(0), "verification summary");
		long[] totals = new long[16];
		boolean preComplete = true;
		boolean postComplete = true;
		for (final var value : values) {
			Objects.requireNonNull(value, "verification summary");
			if (value.preValidationEnabled != first.preValidationEnabled
					|| value.postVerificationEnabled != first.postVerificationEnabled
					|| value.postVerificationSkipped != first.postVerificationSkipped
					|| value.timeoutMillis != first.timeoutMillis) {
				throw new IllegalArgumentException("Cannot aggregate different DELETE verification settings");
			}
			preComplete &= value.preValidationComplete;
			postComplete &= value.postVerificationComplete;
			final long[] counters = {
				value.preValidationFailures, value.verifiedAbsent, value.stillPresent, value.unresolved,
				value.acceptedAbsent, value.acceptedPresent, value.acceptedUnresolved,
				value.failedAbsent, value.failedPresent, value.failedUnresolved,
				value.operationalUnresolvedAbsent, value.operationalUnresolvedPresent,
				value.operationalUnresolvedUnresolved,
				value.unattemptedAbsent, value.unattemptedPresent, value.unattemptedUnresolved
			};
			for (int index = 0; index < totals.length; index++) {
				totals[index] = Math.addExact(totals[index], counters[index]);
			}
		}
		return new DeleteVerificationSummary(
				first.preValidationEnabled, first.postVerificationEnabled, preComplete, postComplete,
				first.postVerificationSkipped,
				first.timeoutMillis, totals[0], totals[1], totals[2], totals[3], totals[4], totals[5],
				totals[6], totals[7], totals[8], totals[9], totals[10], totals[11], totals[12], totals[13],
				totals[14], totals[15]);
	}

	/** Compatibility overload for small in-memory callers. */
	public static DeleteVerificationSummary classify(
			final boolean preValidationEnabled,
			final boolean postVerificationEnabled,
			final long timeoutMillis,
			final DeleteVerificationReport preReport,
			final DeleteVerificationReport postReport,
			final int[] operationalOutcomes) {
		Objects.requireNonNull(operationalOutcomes, "operationalOutcomes");
		return classify(
				preValidationEnabled, postVerificationEnabled, timeoutMillis, preReport, postReport,
				operationalOutcomes.length, index -> operationalOutcomes[(int) index]);
	}

	/** Builds the phase result while streaming disk-backed operational outcomes. */
	public static DeleteVerificationSummary classify(
			final boolean preValidationEnabled,
			final boolean postVerificationEnabled,
			final long timeoutMillis,
			final DeleteVerificationReport preReport,
			final DeleteVerificationReport postReport,
			final long selectedCount,
			final DeleteOperationalOutcomeLedger operationalOutcomes) {
		if (!postVerificationEnabled) {
			return classify(
					preValidationEnabled, false, timeoutMillis, preReport, postReport,
					selectedCount, ignored -> 0);
		}
		Objects.requireNonNull(operationalOutcomes, "operationalOutcomes");
		if (operationalOutcomes.selected() != selectedCount) {
			throw new IllegalArgumentException("DELETE lifecycle and frozen selection sizes differ");
		}
		try (var outcomes = operationalOutcomes.cursor()) {
			return classify(
					preValidationEnabled, true, timeoutMillis, preReport, postReport,
					selectedCount, ignored -> next(outcomes));
		} catch (final IOException failure) {
			throw new UncheckedIOException("Failed to stream DELETE operational outcomes", failure);
		}
	}

	private static DeleteVerificationSummary classify(
			final boolean preValidationEnabled,
			final boolean postVerificationEnabled,
			final long timeoutMillis,
			final DeleteVerificationReport preReport,
			final DeleteVerificationReport postReport,
			final long selectedCount,
			final OutcomeSource operationalOutcomes) {
		if (timeoutMillis <= 0) {
			throw new IllegalArgumentException("DELETE verification timeout must be positive");
		}
		final boolean preComplete = !preValidationEnabled
				|| complete(preReport, DeleteVerificationPhase.PRE_DELETE, selectedCount);
		final boolean postComplete = !postVerificationEnabled
				|| complete(postReport, DeleteVerificationPhase.POST_DELETE, selectedCount);
		final long[] counters = new long[12];
		if (postComplete && postVerificationEnabled) {
			try (var post = postReport.cursor()) {
				for (long index = 0; index < selectedCount; index++) {
					observe(counters, operationalOutcomes.next(index), post.next());
				}
			} catch (final IOException failure) {
				throw new UncheckedIOException("Failed to stream DELETE verification outcomes", failure);
			}
		}
		final long verifiedAbsent = Math.addExact(
				Math.addExact(Math.addExact(counters[0], counters[3]), counters[6]), counters[9]);
		final long stillPresent = Math.addExact(
				Math.addExact(Math.addExact(counters[1], counters[4]), counters[7]), counters[10]);
		final long unresolved = Math.addExact(
				Math.addExact(Math.addExact(counters[2], counters[5]), counters[8]), counters[11]);
		return new DeleteVerificationSummary(
				preValidationEnabled, postVerificationEnabled, preComplete, postComplete,
				preValidationEnabled && postVerificationEnabled && preComplete && !postComplete
						&& preReport != null && preReport.failureCount() > 0,
				timeoutMillis,
				preReport == null ? 0 : preReport.failureCount(), verifiedAbsent, stillPresent, unresolved,
				counters[0], counters[1], counters[2], counters[3], counters[4], counters[5],
				counters[6], counters[7], counters[8], counters[9], counters[10], counters[11]);
	}

	private static boolean complete(
			final DeleteVerificationReport report,
			final DeleteVerificationPhase phase,
			final long selectedCount) {
		return report != null && report.completePass() && report.phase() == phase
				&& report.selected() == selectedCount;
	}

	private static int next(final DeleteOperationalOutcomeLedger.Cursor cursor) {
		try {
			return cursor.next();
		} catch (final IOException failure) {
			throw new UncheckedIOException(failure);
		}
	}

	private static void observe(
			final long[] counters,
			final int outcome,
			final DeleteVerificationProbe.Presence presence) {
		final int outcomeOffset = switch (outcome) {
		case 1 -> 0;
		case 2 -> 3;
		case 3 -> 6;
		default -> 9;
		};
		final int presenceOffset = switch (presence) {
		case ABSENT -> 0;
		case PRESENT -> 1;
		case UNRESOLVED -> 2;
		};
		counters[outcomeOffset + presenceOffset]++;
	}

	/** Accepted targets still present or unresolved are independent correctness failures. */
	public long correctnessFailures() {
		return Math.addExact(acceptedPresent, acceptedUnresolved);
	}

	/** Any attempted target unresolved at the phase deadline is inconclusive. */
	public long inconclusiveFailures() {
		return Math.addExact(
				Math.addExact(acceptedUnresolved, failedUnresolved),
				operationalUnresolvedUnresolved);
	}

	/** Post-verification correctness or inconclusive evidence requires a failed verdict. */
	public boolean requiresFailedTerminalOutcome() {
		return correctnessFailures() > 0 || inconclusiveFailures() > 0;
	}

	/** Only complete strict validation plus clean accepted-target absence proves removal. */
	public boolean removalConfirmed() {
		return preValidationEnabled && postVerificationEnabled
				&& preValidationComplete && postVerificationComplete
				&& preValidationFailures == 0
				&& correctnessFailures() == 0
				&& inconclusiveFailures() == 0
				&& failedAbsent == 0
				&& failedPresent == 0
				&& operationalUnresolvedAbsent == 0
				&& operationalUnresolvedPresent == 0
				&& operationalUnresolvedUnresolved == 0
				&& unattemptedAbsent == 0
				&& unattemptedPresent == 0
				&& unattemptedUnresolved == 0;
	}

	/** Marks that strict pre-validation on this or another distributed slice skipped the post phase. */
	public DeleteVerificationSummary withPostVerificationSkipped() {
		if (postVerificationSkipped) {
			return this;
		}
		if (!preValidationEnabled || !postVerificationEnabled
				|| !preValidationComplete || postVerificationComplete) {
			throw new IllegalStateException("DELETE post-verification cannot be marked skipped");
		}
		return new DeleteVerificationSummary(
				preValidationEnabled, postVerificationEnabled, preValidationComplete,
				postVerificationComplete, true, timeoutMillis, preValidationFailures,
				verifiedAbsent, stillPresent, unresolved, acceptedAbsent, acceptedPresent,
				acceptedUnresolved, failedAbsent, failedPresent, failedUnresolved,
				operationalUnresolvedAbsent, operationalUnresolvedPresent,
				operationalUnresolvedUnresolved, unattemptedAbsent, unattemptedPresent,
				unattemptedUnresolved);
	}

	/** Refined recovery candidates observed present or unresolved after the timed phase. */
	public long residualCount() {
		return Math.addExact(
				Math.addExact(acceptedPresent, acceptedUnresolved),
				Math.addExact(
						Math.addExact(failedPresent, failedUnresolved),
						Math.addExact(
								Math.addExact(
										operationalUnresolvedPresent,
										operationalUnresolvedUnresolved),
								Math.addExact(unattemptedPresent, unattemptedUnresolved))));
	}

	@FunctionalInterface
	private interface OutcomeSource {
		int next(long index);
	}}
