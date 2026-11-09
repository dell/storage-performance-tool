package com.dell.spt.storage.driver.coop.netty.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.Operation;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

final class NicAdmissionTest {

	private static final long MS = TimeUnit.MILLISECONDS.toNanos(1);

	private final AtomicLong now = new AtomicLong(1_000_000 * MS);
	private final List<Runnable> scheduled = new ArrayList<>();
	private final AtomicInteger rechecks = new AtomicInteger();

	/** An admission on this test's clock, whose re-checks wait in {@link #scheduled}. */
	private NicAdmission admission(final int limit, final List<? extends NicAdmission.Gate> gates) {
		return NicAdmission.of(limit, gates, now::get, (task, delayMillis) -> {
			assertEquals(NicAdmission.TRIAL_RECHECK_MILLIS, delayMillis);
			scheduled.add(task);
		}, rechecks::incrementAndGet);
	}

	private void advanceMillis(final long millis) {
		now.addAndGet(millis * MS);
	}

	@Test
	void sharesCountEachAttemptUntilItEnds() {
		final var admission = NicAdmission.of(10, 4);
		assertEquals(List.of(3, 3, 2, 2), limits(admission));
		assertTrue(admission.countsUntilEnd());
		assertEquals(List.of(0, 1, 2, 3, 0, 1, 2, 3, 0, 1), takeAll(admission));
		assertFalse(admission.hasRoom());
		assertEquals(0, admission.room());

		assertFalse(admission.waitEnded(2), "a connection does not end the attempt");
		assertEquals(2, admission.count(2));
		assertEquals(-1, admission.take());
		admission.attemptEnded(2);
		assertEquals(1, admission.room());
		assertEquals(2, admission.take());
	}

	@Test
	void theSharedModeGivesEachNicOneAttempt() {
		final var admission = NicAdmission.of(3, 4);
		assertEquals(List.of(1, 1, 1, 1), limits(admission));
		assertTrue(admission.countsUntilEnd());
		assertEquals(List.of(0, 1, 2, 3), takeAll(admission));
	}

	@Test
	void withoutALimitEachNicBoundsOnlyItsWaitingAttempts() {
		final var admission = NicAdmission.of(0, 2);
		final var waiting = NicSetSettings.UNLIMITED_WAITING_LIMIT;
		assertEquals(List.of(waiting, waiting), limits(admission));
		assertFalse(admission.countsUntilEnd());
		assertEquals(2 * waiting, takeAll(admission).size());

		assertTrue(admission.waitEnded(0), "the first wait to end on a full NIC is signalled");
		assertFalse(admission.waitEnded(0), "later ones are not");
		admission.attemptEnded(0);
		assertEquals(waiting - 2, admission.count(0), "the request in flight is not counted");
		assertEquals(2, admission.room());
	}

	@Test
	void theRotationSkipsFullNicsAndStartsAfterTheNicTakenLast() {
		final var admission = NicAdmission.of(6, 3);
		assertEquals(List.of(0, 1, 2, 0, 1, 2), takeAll(admission));
		admission.attemptEnded(0);
		admission.attemptEnded(2);
		assertEquals(0, admission.take(), "the cursor wrapped to NIC 0 after NIC 2");
		assertEquals(2, admission.take(), "full NIC 1 is skipped");
		admission.attemptEnded(1);
		admission.attemptEnded(0);
		assertEquals(0, admission.take(), "after NIC 2 the search starts at NIC 0 again");
		assertEquals(1, admission.take());
	}

	@Test
	void aCancelledTakeReturnsTheNicInEveryMode() {
		for (final var limit : List.of(4, 2, 0)) {
			final var admission = NicAdmission.of(limit, 2);
			final int nic = admission.take();
			admission.cancelled(nic);
			assertEquals(0, admission.count(nic), "limit " + limit);
		}
	}

	@Test
	void anUnopenedPoolAdmitsNothing() {
		final var admission = NicAdmission.none();
		assertEquals(0, admission.size());
		assertEquals(-1, admission.take());
		assertFalse(admission.hasRoom());
		assertEquals(0, admission.room());
	}

	@Test
	void aFailingNicTakesNoAttemptWhileItsPoolWouldFailItAndAnotherNicServes() {
		final var serves = new AtomicBoolean(false);
		final var admission = NicAdmission.of(4, List.of(() -> true, serves::get));
		admission.failed(1);
		assertTrue(admission.isFailing(1));
		assertFalse(admission.isFailing(0));

		assertEquals(List.of(0, 0), takeAll(admission), "NIC 1 is skipped while its Path backs off");
		assertFalse(admission.hasRoom());
		assertEquals(0, admission.room());
		admission.attemptEnded(0);
		assertTrue(admission.hasRoom());
		assertEquals(1, admission.room());
		assertEquals(List.of(0), takeAll(admission), "the working NIC takes every attempt");
	}

	@Test
	void aFailingNicTakesOneTrialAtATimeOnceItsIntervalRanAndItsPoolWouldServe() {
		final var serves = new AtomicBoolean(false);
		final var admission = admission(4, List.of(() -> true, serves::get));
		admission.failed(1);
		assertEquals(0, admission.take());
		serves.set(true);
		assertEquals(1, admission.room(), "the first trial waits 100 ms from the failure");
		advanceMillis(99);
		assertEquals(1, admission.room());
		advanceMillis(1);
		assertEquals(2, admission.room(), "the working NIC's room and one trial");
		assertEquals(List.of(1, 0), takeAll(admission), "the trial keeps its place in the rotation");
		assertFalse(admission.hasRoom(), "no second trial while one is counted");
		assertEquals(0, admission.room());

		admission.attemptEnded(0);
		assertEquals(List.of(0), takeAll(admission));
		admission.failed(1);
		admission.attemptEnded(1);
		assertTrue(admission.isFailing(1), "a failed trial keeps the NIC failing");
		advanceMillis(199);
		assertEquals(List.of(), takeAll(admission), "a second failure in a row doubles the interval");
		advanceMillis(1);
		assertEquals(List.of(1), takeAll(admission));
	}

	@Test
	void theTrialIntervalDoublesWithEachFailureInARowToFiveSecondsAndAnAnswerResetsIt() {
		final var admission = admission(4, List.of(() -> true, () -> true));
		assertEquals(List.of(0, 1, 0, 1), takeAll(admission));
		admission.attemptEnded(1);
		admission.attemptEnded(1);
		for (final long interval : List.of(100L, 200L, 400L, 800L, 1600L, 3200L, 5000L, 5000L, 5000L)) {
			admission.failed(1);
			advanceMillis(interval - 1);
			assertEquals(-1, admission.take(), interval + " ms");
			advanceMillis(1);
			assertEquals(1, admission.take(), interval + " ms");
			admission.attemptEnded(1);
		}
		admission.answered(1);
		assertFalse(admission.isFailing(1));
		admission.failed(1);
		advanceMillis(100);
		assertEquals(1, admission.take(), "after an answer the next failure waits 100 ms again");
	}

	@Test
	void aTrialThatGetsAnAnswerGivesTheNicItsShareAgain() {
		final var admission = admission(4, List.of(() -> true, () -> true));
		admission.failed(1);
		advanceMillis(100);
		assertEquals(List.of(0, 1, 0), takeAll(admission));
		admission.answered(1);
		admission.attemptEnded(1);
		assertFalse(admission.isFailing(1));
		assertEquals(2, admission.room());
		assertEquals(List.of(1, 1), takeAll(admission));
	}

	@Test
	void aSuccessOrAnyResponseErrorIsAnAnswerAndNothingElseIs() {
		final var answers = EnumSet.of(Operation.Status.SUCC, Operation.Status.RESP_FAIL_UNKNOWN, Operation.Status.RESP_FAIL_CLIENT,
						Operation.Status.RESP_FAIL_SVC, Operation.Status.RESP_FAIL_NOT_FOUND, Operation.Status.RESP_FAIL_AUTH,
						Operation.Status.RESP_FAIL_CORRUPT, Operation.Status.RESP_FAIL_SPACE);
		for (final var status : Operation.Status.values()) {
			assertEquals(answers.contains(status), NicAdmission.isAnswer(status), status.name());
		}
	}

	@Test
	void whenEveryNicIsFailingTheRotationTakesAnyNicWithRoom() {
		final var admission = NicAdmission.of(4, List.of(() -> false, () -> false));
		admission.failed(0);
		admission.failed(1);
		assertTrue(admission.hasRoom());
		assertEquals(4, admission.room());
		assertEquals(List.of(0, 1, 0, 1), takeAll(admission), "as without failures, so the step fails at the rate of its attempts");
		assertFalse(admission.hasRoom());

		admission.answered(0);
		admission.attemptEnded(0);
		admission.attemptEnded(1);
		assertEquals(1, admission.room(), "a working NIC again: NIC 1 waits for its gate");
		assertEquals(List.of(0), takeAll(admission));
	}

	@Test
	void withoutALimitATrialHoldsTheFailingNicUntilItsAttemptEnds() {
		final var serves = new AtomicBoolean(true);
		final var admission = admission(0, List.of(() -> true, serves::get));
		admission.failed(1);
		advanceMillis(100);
		final var taken = takeAll(admission);
		assertEquals(1, taken.stream().filter(nic -> nic == 1).count(), taken.toString());
		admission.waitEnded(1);
		assertEquals(0, admission.count(1), "the trial's wait ended, so it is not counted as waiting");
		assertEquals(0, admission.room(), "but it still holds the NIC: its request is in flight");
		assertEquals(-1, admission.take());
		admission.attemptEnded(1);
		assertEquals(1, admission.room(), "its attempt ended: the next trial may start");
		serves.set(false);
		assertEquals(0, admission.room());
		assertFalse(admission.hasRoom());
	}

	@Test
	void aCancelledTrialLetsTheNextOneStart() {
		final var admission = admission(4, List.of(() -> true, () -> true));
		admission.failed(1);
		advanceMillis(100);
		assertEquals(List.of(0, 1, 0), takeAll(admission));
		admission.cancelled(1);
		assertEquals(List.of(1), takeAll(admission));
	}

	@Test
	void whileOnlyAFailingNicsLaterTrialCouldTakeAnAttemptTheDispatchIsRecheckedOnceAtATime() {
		final var admission = admission(2, List.of(() -> true, () -> true));
		assertEquals(List.of(0, 1), takeAll(admission));
		assertFalse(admission.hasRoom());
		assertEquals(List.of(), scheduled, "no NIC failing: a completion signals, nothing is scheduled");

		admission.failed(1);
		admission.attemptEnded(1);
		assertFalse(admission.hasRoom(), "NIC 0 is full and NIC 1's trial is not due");
		assertFalse(admission.hasRoom());
		assertEquals(1, scheduled.size(), "one re-check at a time");
		advanceMillis(100);
		scheduled.remove(0).run();
		assertEquals(1, rechecks.get());
		assertTrue(admission.hasRoom(), "the trial is due when the dispatch task looks again");
		assertEquals(List.of(), scheduled);
		assertEquals(1, admission.take());
		assertFalse(admission.hasRoom());
		assertEquals(1, scheduled.size(), "rescheduled after the last one ran");
	}

	@Test
	void aRecheckTheLoopsRejectIsNotLeftScheduled() {
		final var rejecting = new AtomicBoolean(true);
		final var admission = NicAdmission.of(2, List.of(() -> true, () -> true), now::get, (task, delayMillis) -> {
			if (rejecting.get()) {
				throw new RejectedExecutionException("shut down");
			}
			scheduled.add(task);
		}, rechecks::incrementAndGet);
		assertEquals(List.of(0, 1), takeAll(admission));
		admission.failed(1);
		admission.attemptEnded(1);
		assertFalse(admission.hasRoom());
		rejecting.set(false);
		assertFalse(admission.hasRoom());
		assertEquals(1, scheduled.size());
	}

	@Test
	void concurrentEndsNeverLetANicPassItsLimit() throws Exception {
		final var admission = NicAdmission.of(8, 4);
		final var taken = new java.util.concurrent.LinkedBlockingQueue<Integer>();
		var over = 0;
		final var ended = new AtomicInteger();
		final int attempts = 200_000;
		final var releasers = Executors.newFixedThreadPool(4);
		final var done = new CountDownLatch(4);
		try {
			for (var t = 0; t < 4; t++) {
				releasers.execute(() -> {
					try {
						while (ended.get() < attempts) {
							final var nic = taken.poll(10, TimeUnit.MILLISECONDS);
							if (nic != null) {
								admission.attemptEnded(nic);
								ended.incrementAndGet();
							}
						}
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					} finally {
						done.countDown();
					}
				});
			}
			for (var i = 0; i < attempts;) {
				final int nic = admission.take();
				if (nic < 0) {
					Thread.onSpinWait();
					continue;
				}
				if (admission.count(nic) > admission.limit(nic)) {
					over++;
				}
				taken.add(nic);
				i++;
			}
			assertTrue(done.await(30, TimeUnit.SECONDS));
		} finally {
			releasers.shutdownNow();
		}
		assertEquals(0, over);
		for (var nic = 0; nic < admission.size(); nic++) {
			assertEquals(0, admission.count(nic));
		}
	}

	private static List<Integer> limits(final NicAdmission admission) {
		final var limits = new ArrayList<Integer>();
		for (var nic = 0; nic < admission.size(); nic++) {
			limits.add(admission.limit(nic));
		}
		return limits;
	}

	private static List<Integer> takeAll(final NicAdmission admission) {
		final var taken = new ArrayList<Integer>();
		for (int nic = admission.take(); nic >= 0; nic = admission.take()) {
			taken.add(nic);
		}
		return taken;
	}
}
