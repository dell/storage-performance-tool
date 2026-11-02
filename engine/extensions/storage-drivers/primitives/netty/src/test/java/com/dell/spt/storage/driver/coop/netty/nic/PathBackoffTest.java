package com.dell.spt.storage.driver.coop.netty.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class PathBackoffTest {

	private final AtomicLong now = new AtomicLong(1_000);
	private final PathBackoff backoff = new PathBackoff(now::get);

	@Test
	void intervalsDoubleFromOneHundredMillisToTheFiveSecondCap() {
		final var intervals = List.of(1, 2, 3, 4, 5, 6, 7, 8, 40).stream().map(PathBackoff::intervalMillis).toList();
		assertEquals(List.of(100L, 200L, 400L, 800L, 1_600L, 3_200L, 5_000L, 5_000L, 5_000L), intervals);
		assertEquals(0, PathBackoff.intervalMillis(0));
	}

	@Test
	void aHealthyPathAllowsConcurrentConnects() {
		final var first = backoff.tryConnect();
		final var second = backoff.tryConnect();
		assertNotEquals(PathBackoff.REJECTED, first);
		assertEquals(first, second);
		assertFalse(backoff.inBackoff());
	}

	@Test
	void aFailureRejectsConnectsForTheIntervalThenAllowsOneTrial() {
		backoff.failed(backoff.tryConnect());
		assertTrue(backoff.inBackoff());
		assertEquals(PathBackoff.REJECTED, backoff.tryConnect());

		advanceMillis(99);
		assertEquals(PathBackoff.REJECTED, backoff.tryConnect());
		advanceMillis(1);
		final var trial = backoff.tryConnect();
		assertNotEquals(PathBackoff.REJECTED, trial);
		// Until the trial completes, the Path stays in back-off.
		assertEquals(PathBackoff.REJECTED, backoff.tryConnect());
		advanceMillis(10_000);
		assertEquals(PathBackoff.REJECTED, backoff.tryConnect());

		backoff.failed(trial);
		assertEquals(2, backoff.failures());
		advanceMillis(199);
		assertEquals(PathBackoff.REJECTED, backoff.tryConnect());
		advanceMillis(1);
		assertNotEquals(PathBackoff.REJECTED, backoff.tryConnect());
	}

	@Test
	void aSuccessfulConnectResetsThePath() {
		backoff.failed(backoff.tryConnect());
		advanceMillis(100);
		backoff.tryConnect();
		backoff.connected();

		assertFalse(backoff.inBackoff());
		assertEquals(0, backoff.failures());
		final var next = backoff.tryConnect();
		backoff.failed(next);
		assertEquals(1, backoff.failures(), "the next failure starts again at 100 ms");
	}

	@Test
	void connectsInFlightBeforeTheFirstFailureCountOnce() {
		final var epoch = backoff.tryConnect();
		backoff.tryConnect();
		backoff.tryConnect();

		backoff.failed(epoch);
		backoff.failed(epoch);
		backoff.failed(epoch);

		assertEquals(1, backoff.failures());
		advanceMillis(100);
		assertNotEquals(PathBackoff.REJECTED, backoff.tryConnect());
	}

	@Test
	void aStaleConnectThatSucceedsStillResets() {
		final var stale = backoff.tryConnect();
		backoff.failed(backoff.tryConnect());
		assertTrue(backoff.inBackoff());
		backoff.connected();
		assertFalse(backoff.inBackoff());
		backoff.failed(stale);
		assertFalse(backoff.inBackoff(), "a failure from before the reset is stale");
	}

	@Test
	void aFailingPathStartsAtMostEightConnectsInATenSecondWindow() {
		// The storm bound of the reconnect-storm test, with an attempt every millisecond.
		final var start = now.get();
		var connects = 0;
		while (now.get() - start < TimeUnit.SECONDS.toNanos(10)) {
			final var epoch = backoff.tryConnect();
			if (epoch != PathBackoff.REJECTED) {
				connects++;
				backoff.failed(epoch);
			}
			advanceMillis(1);
		}
		assertTrue(connects <= 8, "connects: " + connects);
	}

	private void advanceMillis(final long millis) {
		now.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
	}
}
