package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadySettings;
import com.github.akurilov.commons.system.SizeInBytes;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BucketStepFenceTest {

	private static final long LONG_BUDGET = TimeUnit.SECONDS.toNanos(30);
	private static final long SHORT_BUDGET = TimeUnit.MILLISECONDS.toNanos(300);
	private static final SteadyResult NO_RESULT = null;
	private static final List<String> MEMBERS = List.of("a", "b");

	private BucketStepFence fence;
	private final CountDownLatch release = new CountDownLatch(1);

	@AfterEach
	void tearDown() {
		release.countDown();
		if (fence != null) {
			fence.close();
		}
	}

	private BucketStepFence fence(final long budget) {
		fence = new BucketStepFence("fence-test", budget, 2, System::nanoTime);
		return fence;
	}

	private BucketStepFence.ContextCuts context(final BucketStepFence stepFence) {
		return stepFence.register(new BucketMeters(OpType.CREATE, MEMBERS));
	}

	private void holdEveryPass(final BucketStepFence stepFence) {
		stepFence.beforeSetPass(slot -> {
			try {
				release.await();
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		});
	}

	private static void awaitStarted(final BucketStepFence.ContextCuts cuts) throws Exception {
		// A context's start pass has no public signal; a short poll on its first set's cut is enough.
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (!cuts.startPassDone()) {
			assertTrue(System.nanoTime() < deadline, "start pass did not finish");
			Thread.sleep(1);
		}
	}

	@Test
	void aWindowIsTheDifferenceBetweenItsTwoCuts() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var cuts = context(stepFence);
		final var memberA = cuts.meters().sets()[0];
		memberA.markSucc(100, 1_000, 500, 250);
		memberA.markFail();

		cuts.onWindowStart(System.nanoTime());
		awaitStarted(cuts);

		memberA.markSucc(10, 2_000, 700, 300);
		memberA.markSucc(20, 4_000, 900, 400);
		memberA.markCorrupt();
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();

		final var outcome = cuts.outcome();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, outcome.kind());
		assertEquals(3, outcome.records().size(), "both members and the catch-all");
		final var values = outcome.records().get(0).values();
		assertEquals("a", values.bucket());
		assertEquals(2, values.succeeded());
		assertEquals(1, values.failed());
		assertEquals(1, values.corrupt());
		assertEquals(30, values.bytes());
		assertEquals(2, values.duration().count());
		assertEquals(BucketKind.CATCH_ALL, outcome.records().get(2).values().kind());
		assertEquals(0, outcome.records().get(2).values().succeeded());
	}

	@Test
	void aRecordsHistogramsDecodeAfterItsCutsAreReleased() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var cuts = context(stepFence);
		final var member = cuts.meters().sets()[1];
		member.markSucc(1, 999_999, 1, 1);
		cuts.onWindowStart(System.nanoTime());
		awaitStarted(cuts);
		final Histogram expected = new Histogram(1L, 3_600_000_000L, 2);
		for (long value = 1_000; value < 200_000; value += 997) {
			member.markSucc(1, value, value / 2, value / 3);
			expected.recordValue(value);
		}
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		System.gc();
		final TimingMetricSnapshot duration = cuts.outcome().records().get(1).values().duration();
		final Histogram decoded = Histogram.decodeFromCompressedByteBuffer(
						ByteBuffer.wrap(duration.compressedHistogram()), 3_600_000_000L);
		assertEquals(expected, decoded);
		assertEquals(expected.getTotalCount(), duration.count());
		assertEquals(expected.getMaxValue(), duration.max());
		assertEquals(expected.getMinValue(), duration.min());
	}

	@Test
	void aWindowThatNeverStartedIsNotReachedAndTakesNoCut() {
		final var stepFence = fence(LONG_BUDGET);
		final var cuts = context(stepFence);
		cuts.onWindowClose(System.nanoTime(), false, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.NOT_REACHED, cuts.outcome().kind());
		assertFalse(stepFence.poolCreated(), "no cut means no pool");
	}

	@Test
	void severalSlowContextsShareOneDeadline() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var one = context(stepFence);
		final var two = context(stepFence);
		final var three = context(stepFence);
		holdEveryPass(stepFence);
		one.onWindowStart(System.nanoTime());
		two.onWindowStart(System.nanoTime());
		three.onWindowStart(System.nanoTime());
		one.onWindowClose(System.nanoTime(), true, NO_RESULT);
		two.onWindowClose(System.nanoTime(), true, NO_RESULT);
		three.onWindowClose(System.nanoTime(), true, NO_RESULT);
		final long before = System.nanoTime();
		stepFence.await();
		final long waited = System.nanoTime() - before;
		assertTrue(waited < 3 * SHORT_BUDGET, "one deadline, not one per context: " + waited);
		for (final var cuts : List.of(one, two, three)) {
			assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, cuts.outcome().kind());
		}
	}

	@Test
	void racingClosersGetOneWaitAndOneOutcome() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var cuts = context(stepFence);
		holdEveryPass(stepFence);
		cuts.onWindowStart(System.nanoTime());
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		final AtomicReference<BucketSteadyOutcome> seen = new AtomicReference<>();
		final Thread other = new Thread(() -> {
			stepFence.await();
			seen.set(cuts.outcome());
		});
		final long before = System.nanoTime();
		other.start();
		stepFence.await();
		other.join();
		assertTrue(System.nanoTime() - before < 3 * SHORT_BUDGET);
		assertNotNull(seen.get());
		assertSame(cuts.outcome(), seen.get());
		// A closer that arrives after the deadline returns at once with the same outcome.
		final long late = System.nanoTime();
		stepFence.await();
		assertTrue(System.nanoTime() - late < SHORT_BUDGET);
		assertSame(seen.get(), cuts.outcome());
	}

	@Test
	void aTaskFinishingAfterTheDeadlinePublishesNothingAndThePoolEnds() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var cuts = context(stepFence);
		holdEveryPass(stepFence);
		cuts.onWindowStart(System.nanoTime());
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		final var outcome = cuts.outcome();
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, outcome.kind());
		release.countDown();
		Thread.sleep(50);
		assertSame(outcome, cuts.outcome());
		final long before = System.nanoTime();
		stepFence.close();
		assertTrue(System.nanoTime() - before < TimeUnit.SECONDS.toNanos(3));
	}

	@Test
	void anotherContextsFinishedRowsSurviveADeadlineMiss() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var fast = context(stepFence);
		fast.onWindowStart(System.nanoTime());
		awaitStarted(fast);
		fast.onWindowClose(System.nanoTime(), true, NO_RESULT);
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (fast.outcome() == null) {
			assertTrue(System.nanoTime() < deadline);
			Thread.sleep(1);
		}
		final var slow = context(stepFence);
		holdEveryPass(stepFence);
		slow.onWindowStart(System.nanoTime());
		slow.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, fast.outcome().kind());
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, slow.outcome().kind());
	}

	@Test
	void aStopWhileTheStartIsPendingNeverEndsBeforeItsStart() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var cuts = context(stepFence);
		final AtomicInteger passed = new AtomicInteger();
		holdEveryPass(stepFence);
		cuts.onWindowStart(System.nanoTime());
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		assertNull(cuts.outcome(), "the end waits for its start");
		stepFence.await();
		assertEquals(0, passed.get());
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, cuts.outcome().kind());
	}

	@Test
	void aPassFailureIsUnmeasuredForThatContextOnly() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var healthy = context(stepFence);
		healthy.onWindowStart(System.nanoTime());
		awaitStarted(healthy);
		healthy.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		final var failing = context(stepFence);
		stepFence.beforeSetPass(slot -> {
			throw new IllegalStateException("injected");
		});
		failing.onWindowStart(System.nanoTime());
		failing.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, healthy.outcome().kind());
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, failing.outcome().kind());
		assertTrue(failing.outcome().reason().contains("injected"), failing.outcome().reason());
	}

	@Test
	void anEndedPoolRejectsAndTheContextIsUnmeasured() {
		final var stepFence = fence(LONG_BUDGET);
		final var cuts = context(stepFence);
		stepFence.close();
		cuts.onWindowStart(System.nanoTime());
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, cuts.outcome().kind());
	}

	/** The manager thread only queues at scheduling exhaustion, so a held end never delays it. */
	@Test
	void aHeldEndTaskDoesNotDelayTheClosingThread() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var cuts = context(stepFence);
		cuts.onWindowStart(System.nanoTime());
		awaitStarted(cuts);
		holdEveryPass(stepFence);
		final long before = System.nanoTime();
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		assertTrue(System.nanoTime() - before < TimeUnit.MILLISECONDS.toNanos(200));
		assertNull(cuts.outcome());
		release.countDown();
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, cuts.outcome().kind());
	}

	/** Item 11: the bucket window is shifted later by each cut's delay, and says so. */
	@Test
	void completionsAtEachEdgeFollowTheStatedInterval() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var cuts = context(stepFence);
		final var set = cuts.meters().sets()[0];
		final CountDownLatch startHeld = new CountDownLatch(1);
		final CountDownLatch startGo = new CountDownLatch(1);
		stepFence.beforeSetPass(slot -> {
			startHeld.countDown();
			try {
				startGo.await();
			} catch (final InterruptedException e) {
				throw new IllegalStateException(e);
			}
		});
		final long aggregateStart = System.nanoTime();
		cuts.onWindowStart(aggregateStart);
		startHeld.await();
		set.markSucc(1, 100, 50, 25); // between aS and bS: before this set's start cut, so excluded
		Thread.sleep(20);
		startGo.countDown();
		awaitStarted(cuts);
		set.markSucc(1, 200, 50, 25); // inside the window

		final CountDownLatch endHeld = new CountDownLatch(1);
		final CountDownLatch endGo = new CountDownLatch(1);
		stepFence.beforeSetPass(slot -> {
			endHeld.countDown();
			try {
				endGo.await();
			} catch (final InterruptedException e) {
				throw new IllegalStateException(e);
			}
		});
		final long aggregateEnd = System.nanoTime();
		cuts.onWindowClose(aggregateEnd, true, NO_RESULT);
		endHeld.await();
		set.markSucc(1, 300, 50, 25); // between aE and bE: before this set's end cut, so included
		Thread.sleep(20);
		endGo.countDown();
		stepFence.await();

		final var record = cuts.outcome().records().get(0);
		assertEquals(2, record.values().succeeded());
		assertTrue(record.startDelayNanos() >= TimeUnit.MILLISECONDS.toNanos(20), "start delay reported");
		assertTrue(record.endDelayNanos() >= TimeUnit.MILLISECONDS.toNanos(20), "end delay reported");
		assertTrue(record.windowNanos() > 0);
		assertTrue(record.passNanos() >= 0);
	}

	private static MetricsContext<AllMetricsSnapshot> steadyContext(final OpType type) throws Exception {
		final MetricsContext<AllMetricsSnapshot> context = MetricsContextImpl.builder()
						.loadStepId("fence-order")
						.runId(1L)
						.opType(type)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 1)
						.steadySettings(new SteadySettings(0.0, 0.0, 0L))
						.build();
		context.start();
		return context;
	}

	/** A weighted or pipeline sub-step can end long after another. Its own pass decides its rows. */
	@Test
	void aLaterContextIsNotLostToAnEarlierContextsDeadline() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var first = context(stepFence);
		final var later = context(stepFence);
		first.onWindowStart(System.nanoTime());
		later.onWindowStart(System.nanoTime());
		awaitStarted(first);
		awaitStarted(later);
		first.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, first.outcome().kind());
		Thread.sleep(TimeUnit.NANOSECONDS.toMillis(SHORT_BUDGET) * 2);
		later.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, later.outcome().kind());
	}

	/** A miss costs the context that missed, and the pool stays usable for the ones after it. */
	@Test
	void aMissLeavesThePoolForLaterContexts() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var slow = context(stepFence);
		final CountDownLatch go = new CountDownLatch(1);
		final AtomicInteger held = new AtomicInteger();
		stepFence.beforeSetPass(slot -> {
			if (held.getAndIncrement() == 0) {
				try {
					go.await();
				} catch (final InterruptedException e) {
					throw new IllegalStateException(e);
				}
			}
		});
		slow.onWindowStart(System.nanoTime());
		slow.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, slow.outcome().kind());
		go.countDown();
		stepFence.beforeSetPass(slot -> {});
		final var later = context(stepFence);
		later.onWindowStart(System.nanoTime());
		awaitStarted(later);
		later.onWindowClose(System.nanoTime(), true, NO_RESULT);
		stepFence.await();
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, later.outcome().kind());
	}

	/** A closer that takes the window's fast path must find the end pass already queued. */
	@Test
	void aRacingCloserCannotPassTheFenceBeforeTheEndIsQueued() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var context = steadyContext(OpType.CREATE);
		final var cuts = stepFence.register(new BucketMeters(OpType.CREATE, MEMBERS));
		final CountDownLatch inListener = new CountDownLatch(1);
		final CountDownLatch go = new CountDownLatch(1);
		assertTrue(context.steadyWindowListener(new com.dell.spt.base.metrics.steady.SteadyWindow.Listener() {
			@Override
			public void onWindowStart(final long startNanos) {
				cuts.onWindowStart(startNanos);
			}

			@Override
			public void onWindowClose(final long endNanos, final boolean started, final SteadyResult result) {
				inListener.countDown();
				try {
					go.await();
				} catch (final InterruptedException e) {
					throw new IllegalStateException(e);
				}
				cuts.onWindowClose(endNanos, started, result);
			}
		}));
		context.markSucc(1, 100, 50, 25);
		context.observeSteadyWindow();
		awaitStarted(cuts);
		final Thread first = new Thread(() -> context.closeSteadyWindow("first"));
		first.start();
		inListener.await();
		final CountDownLatch secondDone = new CountDownLatch(1);
		final Thread second = new Thread(() -> {
			context.closeSteadyWindow("second");
			stepFence.await();
			secondDone.countDown();
		});
		second.start();
		assertFalse(secondDone.await(200, TimeUnit.MILLISECONDS), "the second closer must not pass yet");
		go.countDown();
		assertTrue(secondDone.await(10, TimeUnit.SECONDS));
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, cuts.outcome().kind(), "and it waited for the end pass");
		first.join();
		second.join();
		context.close();
	}

	/** At scheduling exhaustion nobody may be waiting, and the deadline still holds. */
	@Test
	void aPassThatEndsAfterTheDeadlineIsUnmeasuredWithNoCloserWaiting() throws Exception {
		final var stepFence = fence(SHORT_BUDGET);
		final var cuts = context(stepFence);
		cuts.onWindowStart(System.nanoTime());
		awaitStarted(cuts);
		holdEveryPass(stepFence);
		cuts.onWindowClose(System.nanoTime(), true, NO_RESULT);
		Thread.sleep(TimeUnit.NANOSECONDS.toMillis(SHORT_BUDGET) + 100);
		release.countDown();
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (cuts.outcome() == null) {
			assertTrue(System.nanoTime() < deadline);
			Thread.sleep(1);
		}
		assertEquals(BucketSteadyOutcome.Kind.UNMEASURED, cuts.outcome().kind());
	}

	/** R3-1: every aggregate window is closed while the first bucket end pass is still held. */
	@Test
	void everyAggregateClosesWhileABucketEndPassIsHeld() throws Exception {
		final var stepFence = fence(LONG_BUDGET);
		final var creates = steadyContext(OpType.CREATE);
		final var reads = steadyContext(OpType.READ);
		final var createCuts = stepFence.register(new BucketMeters(OpType.CREATE, MEMBERS));
		final var readCuts = stepFence.register(new BucketMeters(OpType.READ, MEMBERS));
		assertTrue(creates.steadyWindowListener(createCuts));
		assertTrue(reads.steadyWindowListener(readCuts));
		creates.markSucc(1, 100, 50, 25);
		reads.markSucc(1, 100, 50, 25);
		creates.observeSteadyWindow();
		reads.observeSteadyWindow();
		awaitStarted(createCuts);
		awaitStarted(readCuts);

		creates.markSucc(1, 100, 50, 25);
		reads.markSucc(1, 100, 50, 25);
		holdEveryPass(stepFence);
		final SteadyResult createResult = creates.closeSteadyWindow("test stop");
		final SteadyResult readResult = reads.closeSteadyWindow("test stop");
		assertNotNull(createResult, "the first aggregate closed with a bucket pass held");
		assertNotNull(readResult, "the second aggregate closed with a bucket pass held");
		assertNull(createCuts.outcome());

		creates.markSucc(1, 100, 50, 25); // arrives during the hold
		assertSame(createResult, creates.closeSteadyWindow("again"), "absent from the closed aggregate window");
		final long aggregateCount = createResult.statistics().successSnapshot().count();
		release.countDown();
		stepFence.await();
		assertEquals(1, aggregateCount);
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, createCuts.outcome().kind());
		assertEquals(BucketSteadyOutcome.Kind.MEASURED, readCuts.outcome().kind());
		creates.close();
		reads.close();
	}
}
