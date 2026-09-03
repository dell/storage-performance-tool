package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class OutputBackoffTest {

	@Test
	void startsShortAndDoublesWhileOutputStaysBlocked() {
		final var backoff = new OutputBackoff();

		assertEquals(50_000L, backoff.nextParkNanos());
		assertEquals(100_000L, backoff.nextParkNanos());
		assertEquals(200_000L, backoff.nextParkNanos());
		assertEquals(400_000L, backoff.nextParkNanos());
		assertEquals(800_000L, backoff.nextParkNanos());
	}

	@Test
	void capsAtOneMillisecond() {
		final var backoff = new OutputBackoff();
		for (var i = 0; i < 10; i++) {
			backoff.nextParkNanos();
		}

		assertEquals(OutputBackoff.MAX_NANOS, backoff.nextParkNanos());
		assertEquals(OutputBackoff.MAX_NANOS, backoff.nextParkNanos());
	}

	@Test
	void fullAcceptanceResetsToTheShortWait() {
		final var backoff = new OutputBackoff();
		for (var i = 0; i < 10; i++) {
			backoff.nextParkNanos();
		}

		backoff.onProgress(32, 32);

		assertEquals(OutputBackoff.INITIAL_NANOS, backoff.nextParkNanos());
	}

	@Test
	void partialAcceptanceHoldsTheCurrentWait() {
		// A full queue frees one slot per completion, so the generator sees one-operation
		// successes between refusals. Those must not restart the short wait.
		final var backoff = new OutputBackoff();
		for (var i = 0; i < 10; i++) {
			backoff.nextParkNanos();
		}

		backoff.onProgress(1, 32);
		assertEquals(OutputBackoff.MAX_NANOS, backoff.nextParkNanos());

		backoff.onProgress(4, 32);
		assertEquals(OutputBackoff.MAX_NANOS, backoff.nextParkNanos());
	}

	@Test
	void partialAcceptanceDoesNotGrowTheWaitEither() {
		final var backoff = new OutputBackoff();
		assertEquals(50_000L, backoff.nextParkNanos()); // next would be 100 us

		backoff.onProgress(1, 32);

		assertEquals(100_000L, backoff.nextParkNanos());
	}

	@Test
	void capStaysWellInsideTheDrainInterval() {
		// The driver drains its input queue thousands of operations at a time, a few times per
		// second; the longest poll must remain a small fraction of that so a refilled queue is
		// never left waiting for the generator.
		assertEquals(1_000_000L, OutputBackoff.MAX_NANOS);
		assertEquals(50_000L, OutputBackoff.INITIAL_NANOS);
	}
}
