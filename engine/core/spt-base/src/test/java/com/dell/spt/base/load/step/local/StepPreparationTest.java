package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.PreparationStatus.State;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StepPreparationTest {

	@Test
	void reportsItsPhaseAndElapsedTimeUntilItEnds() {
		final var preparation = new StepPreparation(1, 100, 1_000);
		assertEquals(1_100, preparation.deadlineNanos());
		preparation.phase("probing paths");
		final var running = preparation.status(1_040);
		assertEquals(State.RUNNING, running.state());
		assertEquals("probing paths", running.phase());
		assertEquals(40, running.elapsedNanos());

		preparation.ready(1_060);
		final var ready = preparation.status(1_090);
		assertEquals(State.READY, ready.state());
		assertEquals("ready", ready.phase());
		assertEquals(60, ready.elapsedNanos(), "the time stops when it ends");
	}

	@Test
	void onlyAReadyPreparationCanBeReleasedAndReleaseIsIdempotent() {
		final var preparation = new StepPreparation(1, 100, 0);
		assertFalse(preparation.release(), "still running");
		preparation.ready(1);
		assertTrue(preparation.release());
		assertTrue(preparation.release());
		assertTrue(preparation.released());
	}

	@Test
	void aFailedPreparationKeepsItsMessageAndCannotBeReleased() {
		final var preparation = new StepPreparation(1, 100, 0);
		preparation.fail("no route", 5);
		preparation.ready(6);
		assertEquals(State.FAILED, preparation.status(7).state());
		assertEquals("no route", preparation.status(7).message());
		assertFalse(preparation.release());
		assertTrue(preparation.fence("stopped", 8));
		assertEquals(State.FENCED, preparation.status(9).state());
		assertEquals("no route", preparation.status(9).message(), "the first reason stays");
	}

	@Test
	void aFenceHoldsForGoodSoALateReadyOrReleaseChangesNothing() {
		final var preparation = new StepPreparation(1, 100, 0);
		final var actions = new AtomicInteger();
		preparation.onFence(actions::incrementAndGet);
		assertTrue(preparation.fence("the deadline", 10));
		assertFalse(preparation.fence("again", 11));
		assertEquals(1, actions.get());
		assertTrue(preparation.fenced());
		preparation.ready(12);
		assertEquals(State.FENCED, preparation.status(13).state());
		assertFalse(preparation.release());
		preparation.onFence(actions::incrementAndGet);
		assertEquals(2, actions.get(), "an action registered after the fence runs at once");
	}

	@Test
	void aReleasedPreparationCannotBeFenced() {
		final var preparation = new StepPreparation(1, 100, 0);
		preparation.ready(1);
		preparation.release();
		assertFalse(preparation.fence("too late", 2));
		assertEquals(State.READY, preparation.status(3).state());
	}

	@Test
	void theBudgetMustBePositive() {
		assertThrows(IllegalArgumentException.class, () -> new StepPreparation(1, 0, 0));
	}
}
