package com.dell.spt.base.load.failure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class ObjectFailureBudgetControllerTest {
	@Test
	void fixedBudgetPermitsTheBoundaryAndTripsOneOver() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.fixed(2));
		assertFalse(controller.evaluate(List.of(worker(3, 2, 1)), Duration.ZERO, false).stopScheduling());
		assertFalse(controller.evaluate(List.of(worker(3, 1, 2)), Duration.ZERO, false).stopScheduling());
		final var breached = controller.evaluate(List.of(worker(4, 1, 3)), Duration.ZERO, false);
		assertTrue(breached.stopScheduling());
		assertEquals(ObjectFailureBudgetOutcome.FAILED, breached.outcome());
	}

	@Test
	void fixedZeroAndPercentageZeroEnforceImmediately() {
		final var fixed = new ObjectFailureBudgetController(ObjectFailureBudgetConfig.fixed(0));
		final var percentage = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.percentage(0, Duration.ofSeconds(30)));
		assertTrue(fixed.evaluate(List.of(worker(1, 0, 1)), Duration.ZERO, false).stopScheduling());
		assertTrue(percentage.evaluate(List.of(worker(1, 0, 1)), Duration.ZERO, false).stopScheduling());
	}

	@Test
	void positivePercentageWaitsForGraceButAlwaysReevaluatesAtCompletion() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.percentage(10, Duration.ofSeconds(30)));
		final var counters = List.of(worker(10, 8, 2));
		assertFalse(controller.evaluate(counters, Duration.ofSeconds(29), false).stopScheduling());
		assertTrue(controller.evaluate(counters, Duration.ofSeconds(30), false).stopScheduling());
		assertTrue(controller.evaluate(counters, Duration.ofSeconds(1), true).stopScheduling());
	}

	@Test
	void controllerAggregatesWorkersAndFailsClosedOnMissingTerminalCounters() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.fixed(2));
		assertFalse(controller.evaluate(
						List.of(worker(5, 4, 1), worker(5, 4, 1)), Duration.ZERO, false).stopScheduling());
		assertTrue(controller.evaluate(
						List.of(worker(5, 4, 1), worker(5, 3, 2)), Duration.ZERO, false).stopScheduling());
		assertEquals(ObjectFailureBudgetOutcome.FAILED,
						controller.evaluate(java.util.Arrays.asList(worker(1, 1, 0), null), Duration.ZERO, true)
										.outcome());
	}

	@Test
	void controllerRejectsParticipantClaimsWithNegativeOrInconsistentCounters() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.fixed(100));
		final var malformed = List.of(
						new DeleteObjectLifecycleSnapshot(1, 1, -1, 0, 2, 0, 0, 1, true),
						new DeleteObjectLifecycleSnapshot(2, 1, 1, 0, 0, 0, 0, 1, true),
						new DeleteObjectLifecycleSnapshot(1, 2, 1, 0, 0, 0, 0, 1, true));

		for (final var counters : malformed) {
			final var decision = controller.evaluate(List.of(counters), Duration.ZERO, true);
			assertEquals(ObjectFailureBudgetOutcome.FAILED, decision.outcome());
			assertTrue(decision.stopScheduling());
			assertTrue(decision.reason().contains("invalid"));
		}
	}

	@Test
	void controllerStopsImmediatelyOnMalformedOrOverflowingLiveCounters() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.fixed(100));
		final var malformed = new DeleteObjectLifecycleSnapshot(
						1, 1, -1, 0, 2, 0, 0, 1, true);
		final var malformedDecision = controller.evaluate(
						List.of(malformed), Duration.ZERO, false);
		assertEquals(ObjectFailureBudgetOutcome.FAILED, malformedDecision.outcome());
		assertTrue(malformedDecision.stopScheduling());
		assertTrue(malformedDecision.reason().contains("invalid"));

		final var overflowingDecision = controller.evaluate(
						List.of(
										workerWithSuccessRequests(Long.MAX_VALUE, Long.MAX_VALUE, 0, 1),
										worker(1, 1, 0)),
						Duration.ZERO,
						false);
		assertEquals(ObjectFailureBudgetOutcome.FAILED, overflowingDecision.outcome());
		assertTrue(overflowingDecision.stopScheduling());
		assertTrue(overflowingDecision.reason().contains("overflowed"));
	}

	@Test
	void completionDistinguishesCleanWithinBudgetAndInvalid() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.fixed(100));
		assertEquals(ObjectFailureBudgetOutcome.COMPLETED_CLEANLY,
						controller.evaluate(List.of(worker(2, 2, 0)), Duration.ZERO, true).outcome());
		assertEquals(ObjectFailureBudgetOutcome.COMPLETED_WITHIN_BUDGET,
						controller.evaluate(List.of(worker(2, 1, 1)), Duration.ZERO, true).outcome());
		assertEquals(ObjectFailureBudgetOutcome.FAILED,
						controller.evaluate(List.of(workerWithSuccessRequests(2, 2, 0, 0)), Duration.ZERO, true)
										.outcome());
		final var protocolFailure = controller.evaluate(
						List.of(workerWithProtocolFailure()), Duration.ZERO, false);
		assertEquals(ObjectFailureBudgetOutcome.FAILED, protocolFailure.outcome());
		assertTrue(protocolFailure.stopScheduling());
		assertTrue(protocolFailure.reason().contains("outside operational budget"));
	}

	@Test
	void hundredPercentStillCannotValidateZeroAcceptedObjects() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.percentage(100, Duration.ZERO));
		assertEquals(ObjectFailureBudgetOutcome.FAILED,
						controller.evaluate(List.of(workerWithSuccessRequests(2, 0, 2, 0)), Duration.ZERO, true)
										.outcome());
	}

	@Test
	void operationalBudgetRoomCannotExcuseVerificationCorrectnessOrInconclusiveFailures() {
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.fixed(100));
		for (final var worker : List.of(
						new DeleteObjectLifecycleSnapshot(1, 1, 1, 0, 0, 0, 0, 1, 0, 1, 0, true),
						new DeleteObjectLifecycleSnapshot(1, 1, 1, 0, 0, 0, 0, 1, 0, 0, 1, true))) {
			final var decision = controller.evaluate(List.of(worker), Duration.ZERO, true);
			assertEquals(ObjectFailureBudgetOutcome.FAILED, decision.outcome());
			assertTrue(decision.reason().contains("outside operational budget"));
		}
	}

	private static DeleteObjectLifecycleSnapshot worker(
					final long attempted, final long accepted, final long operationalFailed) {
		return workerWithSuccessRequests(attempted, accepted, operationalFailed, accepted > 0 ? 1 : 0);
	}

	private static DeleteObjectLifecycleSnapshot workerWithSuccessRequests(
					final long attempted,
					final long accepted,
					final long operationalFailed,
					final long fullSuccessfulRequests) {
		return new DeleteObjectLifecycleSnapshot(
						attempted, attempted, accepted, operationalFailed, 0, 0, 0,
						fullSuccessfulRequests, true);
	}

	private static DeleteObjectLifecycleSnapshot workerWithProtocolFailure() {
		return new DeleteObjectLifecycleSnapshot(2, 2, 1, 1, 0, 0, 1, 1, true);
	}
}
