package com.dell.spt.storage.driver.coop.netty.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class NicAdmissionTest {

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
	void aFailingNicTakesOneTrialAtATimeOnceItsPoolWouldServe() {
		final var serves = new AtomicBoolean(false);
		final var admission = NicAdmission.of(4, List.of(() -> true, serves::get));
		admission.failed(1);
		assertEquals(0, admission.take());
		serves.set(true);
		assertEquals(2, admission.room(), "the working NIC's room and one trial");
		assertEquals(List.of(1, 0), takeAll(admission), "the trial keeps its place in the rotation");
		assertFalse(admission.hasRoom(), "no second trial while one is counted");
		assertEquals(0, admission.room());

		admission.attemptEnded(0);
		assertEquals(List.of(0), takeAll(admission));
		admission.failed(1);
		admission.attemptEnded(1);
		assertTrue(admission.isFailing(1), "a failed trial keeps the NIC failing");
		assertEquals(List.of(1), takeAll(admission), "the next trial waits only for the gate");
	}

	@Test
	void aTrialThatSucceedsGivesTheNicItsShareAgain() {
		final var admission = NicAdmission.of(4, List.of(() -> true, () -> true));
		admission.failed(1);
		assertEquals(List.of(0, 1, 0), takeAll(admission));
		admission.succeeded(1);
		admission.attemptEnded(1);
		assertFalse(admission.isFailing(1));
		assertEquals(2, admission.room());
		assertEquals(List.of(1, 1), takeAll(admission));
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

		admission.succeeded(0);
		admission.attemptEnded(0);
		admission.attemptEnded(1);
		assertEquals(1, admission.room(), "a working NIC again: NIC 1 waits for its gate");
		assertEquals(List.of(0), takeAll(admission));
	}

	@Test
	void withoutALimitAFailingNicStillTakesOneWaitingTrialAtATime() {
		final var serves = new AtomicBoolean(true);
		final var admission = NicAdmission.of(0, List.of(() -> true, serves::get));
		admission.failed(1);
		final var taken = takeAll(admission);
		assertEquals(1, taken.stream().filter(nic -> nic == 1).count(), taken.toString());
		admission.waitEnded(1);
		assertEquals(1, admission.room(), "the trial's wait ended: another may start");
		serves.set(false);
		assertEquals(0, admission.room());
		assertFalse(admission.hasRoom());
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
