package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class DeleteVerificationSummaryTest {

	@Test
	void classifiesOperationalCorrectnessInconclusiveAndUnattemptedOutcomesIndependently() throws Exception {
		try (final var post = new DeleteVerificationReport(
						DeleteVerificationPhase.POST_DELETE,
						new DeleteVerificationProbe.Presence[]{
								DeleteVerificationProbe.Presence.ABSENT,
								DeleteVerificationProbe.Presence.PRESENT,
								DeleteVerificationProbe.Presence.UNRESOLVED,
								DeleteVerificationProbe.Presence.ABSENT,
								DeleteVerificationProbe.Presence.PRESENT,
								DeleteVerificationProbe.Presence.UNRESOLVED,
								DeleteVerificationProbe.Presence.ABSENT,
								DeleteVerificationProbe.Presence.PRESENT,
								DeleteVerificationProbe.Presence.UNRESOLVED
						},
						true,
						100)) {
			final var summary = DeleteVerificationSummary.classify(
							false, true, 30_000, null, post,
							new int[]{1, 1, 1, 2, 2, 2, 0, 0, 0
							});

			assertEquals(3, summary.verifiedAbsent());
			assertEquals(3, summary.stillPresent());
			assertEquals(3, summary.unresolved());
			assertEquals(2, summary.correctnessFailures());
			assertEquals(2, summary.inconclusiveFailures());
			assertEquals(6, summary.residualCount());
			assertFalse(summary.removalConfirmed());
		}
	}

	@Test
	void confirmsRemovalOnlyWhenStrictPreValidationAndEverySelectedDeleteAreClean() throws Exception {
		try (final var pre = report(DeleteVerificationPhase.PRE_DELETE,
						DeleteVerificationProbe.Presence.PRESENT);
						final var post = report(DeleteVerificationPhase.POST_DELETE,
										DeleteVerificationProbe.Presence.ABSENT)) {
			final var summary = DeleteVerificationSummary.classify(
							true, true, 30_000, pre, post, new int[]{1
							});

			assertTrue(summary.removalConfirmed());
			assertEquals(0, summary.residualCount());
		}
	}

	@Test
	void keepsDispatchedUnresolvedOutcomesSeparateFromNeverAttemptedTargets() throws Exception {
		try (final var post = new DeleteVerificationReport(
						DeleteVerificationPhase.POST_DELETE,
						new DeleteVerificationProbe.Presence[]{
								DeleteVerificationProbe.Presence.ABSENT,
								DeleteVerificationProbe.Presence.PRESENT,
								DeleteVerificationProbe.Presence.UNRESOLVED
						},
						true,
						1)) {
			final var summary = DeleteVerificationSummary.classify(
							false, true, 30_000, null, post, new int[]{3, 3, 3
							});

			assertEquals(1, summary.operationalUnresolvedAbsent());
			assertEquals(1, summary.operationalUnresolvedPresent());
			assertEquals(1, summary.operationalUnresolvedUnresolved());
			assertEquals(0, summary.unattemptedAbsent());
			assertEquals(2, summary.residualCount());
		}
	}

	@Test
	void cannotConfirmRemovalWithoutCompleteEnabledPhaseReports() throws Exception {
		try (final var pre = report(DeleteVerificationPhase.PRE_DELETE,
						DeleteVerificationProbe.Presence.PRESENT);
						final var post = report(DeleteVerificationPhase.POST_DELETE,
										DeleteVerificationProbe.Presence.ABSENT)) {

			assertFalse(DeleteVerificationSummary.classify(
							true, true, 30_000, null, post, new int[]{1
							}).removalConfirmed());
			final var strictAbort = DeleteVerificationSummary.classify(
							true, true, 30_000, pre, null, new int[]{1
							});
			assertFalse(strictAbort.removalConfirmed());
			assertFalse(strictAbort.postVerificationSkipped());
		}
	}

	@Test
	void marksPostVerificationSkippedOnlyAfterStrictPreValidationFailure() throws Exception {
		try (final var pre = report(DeleteVerificationPhase.PRE_DELETE,
						DeleteVerificationProbe.Presence.ABSENT)) {
			final var summary = DeleteVerificationSummary.classify(
							true, true, 30_000, pre, null, new int[]{0
							});

			assertTrue(summary.preValidationComplete());
			assertFalse(summary.postVerificationComplete());
			assertTrue(summary.postVerificationSkipped());
		}
	}

	@Test
	void aggregatesCoordinatorPropagatedSkipFromLocallyPassingAndFailingSlices() throws Exception {
		try (final var passingPre = report(DeleteVerificationPhase.PRE_DELETE,
						DeleteVerificationProbe.Presence.PRESENT);
						final var failingPre = report(DeleteVerificationPhase.PRE_DELETE,
										DeleteVerificationProbe.Presence.ABSENT)) {
			final var passing = DeleteVerificationSummary.classify(
							true, true, 30_000, passingPre, null, new int[]{0
							});
			final var failing = DeleteVerificationSummary.classify(
							true, true, 30_000, failingPre, null, new int[]{0
							});

			assertFalse(passing.postVerificationSkipped());
			final var aggregate = DeleteVerificationSummary.aggregate(
							List.of(passing.withPostVerificationSkipped(), failing));
			assertTrue(aggregate.preValidationComplete());
			assertFalse(aggregate.postVerificationComplete());
			assertTrue(aggregate.postVerificationSkipped());
			assertEquals(1, aggregate.preValidationFailures());
			assertEquals(0, aggregate.verifiedAbsent());
		}
	}

	private static DeleteVerificationReport report(
					final DeleteVerificationPhase phase,
					final DeleteVerificationProbe.Presence presence) {
		return new DeleteVerificationReport(
						phase, new DeleteVerificationProbe.Presence[]{presence
						}, true, 1);
	}
}
