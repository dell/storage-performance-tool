package com.dell.spt.base.metrics.steady;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.metrics.type.TimingMeterImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The Steady-State Window's start rule, close semantics and statistics, on a controllable clock. */
class SteadyWindowTest {

	private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

	private final AtomicLong clock = new AtomicLong(1_000 * SECOND);
	private final FakeSource source = new FakeSource();
	private final ExecutorService threads = Executors.newCachedThreadPool();

	@AfterEach
	void stopThreads() {
		threads.shutdownNow();
	}

	private SteadyWindow window(final double warmup, final double minTurnovers, final long minDurationMillis) {
		final var window = new SteadyWindow(new SteadySettings(warmup, minTurnovers, minDurationMillis), source, clock::get);
		window.markStepStart();
		return window;
	}

	private SteadyWindow defaultWindow() {
		return window(1, 20, 20_000);
	}

	@Test
	void initialZeroObservationDoesNotStartTheWindow() {
		final var window = defaultWindow();
		window.observe();
		assertFalse(window.isStarted(), "0 completions at 0 in flight must not start the window");
		assertEquals(0, window.cutCount());
	}

	@Test
	void saturatedRunStartsAtOneTurnover() {
		final var window = defaultWindow();
		source.inFlight = 64;
		source.succeeded = 63;
		window.observe();
		assertFalse(window.isStarted());
		source.succeeded = 64;
		window.observe();
		assertTrue(window.isStarted());
		assertEquals(1, window.cutCount());
	}

	@Test
	void rateLimitedAndUnlimitedRunsStartOnActualInFlight() {
		final var rateLimited = defaultWindow();
		source.inFlight = 2;
		source.succeeded = 2;
		rateLimited.observe();
		assertTrue(rateLimited.isStarted(), "two in flight need only two completions");

		final var unlimited = new SteadyWindow(SteadySettings.DEFAULTS, source, clock::get);
		unlimited.markStepStart();
		source.inFlight = 5_000;
		unlimited.observe();
		assertFalse(unlimited.isStarted());
		source.succeeded = 5_000;
		unlimited.observe();
		assertTrue(unlimited.isStarted());
	}

	@Test
	void zeroWarmupStartsAtTheFirstCompletion() {
		final var window = window(0, 20, 20_000);
		source.inFlight = 64;
		window.observe();
		assertFalse(window.isStarted());
		source.failed = 1;
		window.observe();
		assertTrue(window.isStarted(), "a failure is a completion too");
	}

	@Test
	void theStartRuleReadsInFlightAtEachObservation() {
		final var window = defaultWindow();
		source.succeeded = 10;
		source.inFlight = 40;
		window.observe();
		assertFalse(window.isStarted(), "a delayed ramp raises the bar");
		source.inFlight = 8;
		window.observe();
		assertTrue(window.isStarted());
	}

	@Test
	void observationsBeforeStepStartAreIgnored() {
		final var window = new SteadyWindow(SteadySettings.DEFAULTS, source, clock::get);
		source.succeeded = 100;
		window.observe();
		assertFalse(window.isStarted());
	}

	@Test
	void closeBeforeTheStartIsNotReachedWithOneCut() {
		final var window = defaultWindow();
		final SteadyResult result = window.close("user-stop");
		assertEquals(SteadyStatus.NOT_REACHED, result.status());
		assertNull(result.statistics());
		assertEquals(SteadyResult.NONE, result.startOffsetMillis());
		assertEquals("user-stop", result.cause());
		assertEquals(1, window.cutCount());
	}

	@Test
	void validWindowMeasuresCountsRatesAndCoverage() {
		final var window = defaultWindow();
		clock.addAndGet(3 * SECOND);
		source.inFlight = 10;
		source.succeeded = 10;
		source.bytes = 10_000;
		source.concurrency(10, 1);
		window.observe();
		clock.addAndGet(25 * SECOND);
		source.succeeded = 10 + 2_500;
		source.failed = 0;
		source.bytes = 10_000 + 2_500_000;
		source.concurrency(10 + 250 * 10, 251); // 250 more samples of 10
		source.succeededLastRate = 99.5;
		source.bytesLastRate = 99_500;
		final SteadyResult result = window.close("time-limit");

		assertEquals(SteadyStatus.VALID, result.status());
		assertEquals(3_000, result.startOffsetMillis());
		assertEquals(25 * SECOND, result.durationNanos());
		assertEquals(250.0, result.turnovers(), 1e-9);
		final var stats = result.statistics();
		assertEquals(2_500, stats.successSnapshot().count());
		assertEquals(100.0, stats.successSnapshot().mean(), 1e-9);
		assertEquals(99.5, stats.successSnapshot().last(), 1e-9, "Last rates are the EWMA at the end cut");
		assertEquals(100_000.0, stats.byteSnapshot().mean(), 1e-9);
		assertEquals(25_000, stats.elapsedTimeMillis());
		assertEquals(10.0, stats.concurrencySnapshot().mean(), 1e-9);
		assertEquals(10, stats.concurrencySnapshot().last());
		assertEquals(2, window.cutCount());
	}

	@Test
	void failuresCountInTurnoversButNotInThroughput() {
		final var window = defaultWindow();
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		clock.addAndGet(20 * SECOND);
		source.succeeded = 11;
		source.failed = 10;
		final SteadyResult result = window.close("time-limit");
		assertEquals(20.0, result.turnovers(), 1e-9);
		assertEquals(SteadyStatus.VALID, result.status());
		assertEquals(10, result.statistics().successSnapshot().count());
		assertEquals(0.5, result.statistics().successSnapshot().mean(), 1e-9);
		assertEquals(10, result.statistics().failsSnapshot().count());
	}

	@Test
	void eachShortCriterion() {
		final var fewTurnovers = startWith(1, 20, 20_000);
		clock.addAndGet(30 * SECOND);
		source.succeeded += 19;
		assertEquals(SteadyStatus.SHORT, fewTurnovers.close("t").status(), "19 turnovers");

		final var tooBrief = startWith(1, 20, 20_000);
		clock.addAndGet(19 * SECOND);
		source.succeeded += 1_000;
		assertEquals(SteadyStatus.SHORT, tooBrief.close("t").status(), "19 s");

		final var zeroLength = startWith(1, 0, 0);
		assertEquals(SteadyStatus.SHORT, zeroLength.close("t").status(), "end equal to start, criteria disabled");

		final var noCompletions = startWith(1, 0, 0);
		clock.addAndGet(30 * SECOND);
		final SteadyResult empty = noCompletions.close("t");
		assertEquals(SteadyStatus.SHORT, empty.status(), "no completions, criteria disabled");
		assertNotNull(empty.statistics(), "a short window still reports its statistics");
	}

	@Test
	void idleGapWithBothEdgeGaugesAtZeroAndNoConcurrencySamples() {
		final var window = window(1, 0, 0);
		source.succeeded = 1;
		source.inFlight = 0;
		window.observe();
		assertTrue(window.isStarted(), "one completion at zero in flight meets max(1, 0)");
		clock.addAndGet(5 * SECOND);
		final SteadyResult result = window.close("time-limit");
		assertEquals(0.0, result.statistics().concurrencySnapshot().mean(), 1e-9);
		assertEquals(SteadyStatus.SHORT, result.status(), "no completions inside the window");
	}

	@Test
	void windowWithoutConcurrencySamplesUsesTheCutGauges() {
		final var window = window(1, 0, 0);
		source.inFlight = 4;
		source.succeeded = 4;
		source.concurrency(40, 10);
		window.observe();
		clock.addAndGet(SECOND / 20); // shorter than the 100 ms gauge refresh
		source.inFlight = 6;
		source.succeeded = 14;
		final SteadyResult result = window.close("exhaustion");
		assertEquals(5.0, result.statistics().concurrencySnapshot().mean(), 1e-9);
		assertEquals(2.0, result.turnovers(), 1e-9);
	}

	@Test
	void concurrencyMeanIsTheWindowsSampleMean() {
		final var window = window(1, 0, 0);
		source.inFlight = 2;
		source.succeeded = 2;
		source.concurrency(1_000, 100); // 100 samples of 10 before the window
		window.observe();
		clock.addAndGet(SECOND);
		source.succeeded = 52;
		source.concurrency(1_000 + 4 * 50, 150); // 50 samples of 4 inside it
		assertEquals(4.0, window.close("t").statistics().concurrencySnapshot().mean(), 1e-9);
	}

	@Test
	void stragglerAndSparseMixedTrafficNeverReachTheStart() {
		final var window = defaultWindow();
		source.inFlight = 64;
		source.succeeded = 1; // one fast slot finished; a slow straggler holds the rest
		window.observe();
		assertFalse(window.isStarted());
		source.inFlight = 3; // sparse traffic for this operation type
		source.succeeded = 2;
		window.observe();
		assertFalse(window.isStarted());
		assertEquals(SteadyStatus.NOT_REACHED, window.close("t").status());
	}

	@Test
	void counterWindowWithoutTimingObservations() {
		final var window = window(1, 0, 0);
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		clock.addAndGet(SECOND);
		source.succeeded = 101;
		final var stats = window.close("t").statistics();
		assertEquals(100, stats.successSnapshot().count());
		assertEquals(0, stats.durationSnapshot().count());
		assertEquals(0, stats.latencySnapshot().count());
		assertEquals(0, stats.ttfbSnapshot().count());
	}

	@Test
	void timingComesFromTheDifferenceHistogramOnly() {
		final var window = window(1, 0, 0);
		source.inFlight = 1;
		source.succeeded = 1;
		source.duration.recordValue(5_000);
		window.observe();
		clock.addAndGet(SECOND);
		for (int i = 0; i < 99; i++) {
			source.duration.recordValue(1_000);
		}
		// An operation counted as succeeded whose timings are not recorded yet: the scalar
		// population and the histogram population differ, and the window follows the histogram.
		source.succeeded = 101;
		final var duration = window.close("t").statistics().durationSnapshot();
		assertEquals(99, duration.count());
		assertEquals(1_000.0, duration.mean(), 1_000 * 0.01);
		assertTrue(duration.max() < 5_000, "the pre-window value must not leak into the window");
	}

	@Test
	void racingClosersAllGetTheFirstResultAndAtMostTwoCuts() throws Exception {
		final var window = defaultWindow();
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		final var go = new CountDownLatch(1);
		final List<CompletableFuture<SteadyResult>> closers = new ArrayList<>();
		for (int i = 0; i < 8; i++) {
			final String cause = "closer-" + i;
			closers.add(CompletableFuture.supplyAsync(() -> {
				await(go);
				return window.close(cause);
			}, threads));
		}
		go.countDown();
		final SteadyResult first = closers.get(0).get(5, TimeUnit.SECONDS);
		for (final var closer : closers) {
			assertSame(first, closer.get(5, TimeUnit.SECONDS));
		}
		assertEquals(2, window.cutCount());
	}

	@Test
	void aLosingCloserWaitsForThePausedWinnersResult() throws Exception {
		final var window = defaultWindow();
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		final var winnerReading = new CountDownLatch(1);
		final var releaseWinner = new CountDownLatch(1);
		source.pauseNextRead(winnerReading, releaseWinner);
		final var winner = CompletableFuture.supplyAsync(() -> window.close("deadline"), threads);
		assertTrue(winnerReading.await(5, TimeUnit.SECONDS));
		final var loserResult = new CompletableFuture<SteadyResult>();
		final Thread loser = Thread.ofPlatform().start(() -> loserResult.complete(window.close("doShutdown")));
		awaitBlocked(loser);
		assertFalse(loserResult.isDone(), "the loser must wait for the published result, not return early");
		assertEquals(2, source.readCount(), "the loser acquires no cut of its own");
		releaseWinner.countDown();
		final SteadyResult result = winner.get(5, TimeUnit.SECONDS);
		assertSame(result, loserResult.get(5, TimeUnit.SECONDS));
		assertEquals("deadline", result.cause());
		assertEquals(2, window.cutCount());
	}

	@Test
	void aCloseWaitsForAStartCutInProgressAndCutsStayInOrder() throws Exception {
		final var window = defaultWindow();
		source.inFlight = 1;
		source.succeeded = 1;
		final var startReading = new CountDownLatch(1);
		final var releaseStart = new CountDownLatch(1);
		source.pauseNextRead(startReading, releaseStart);
		final var observer = CompletableFuture.runAsync(window::observe, threads);
		assertTrue(startReading.await(5, TimeUnit.SECONDS));
		final var closerResult = new CompletableFuture<SteadyResult>();
		final Thread closer = Thread.ofPlatform().start(() -> closerResult.complete(window.close("deadline")));
		awaitBlocked(closer);
		assertEquals(1, source.readCount(), "the end cut must not begin while the start cut is in progress");
		releaseStart.countDown();
		observer.get(5, TimeUnit.SECONDS);
		final SteadyResult result = closerResult.get(5, TimeUnit.SECONDS);
		assertTrue(result.status().hasStatistics(), "the window started before it closed");
		assertEquals(2, source.readCount());
	}

	@Test
	void aCloseThatCannotBeMeasuredPublishesAResultAndTakesNoThirdCut() {
		final var window = defaultWindow();
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		source.failNextRead = true;
		final SteadyResult result = window.close("deadline");
		assertEquals(SteadyStatus.NOT_REACHED, result.status());
		assertTrue(result.cause().startsWith("deadline"));
		assertSame(result, window.close("doShutdown"));
		assertEquals(2, source.readCount());
	}

	/** Waits until the thread blocks on the window's monitor, without a fixed sleep. */
	private static void awaitBlocked(final Thread thread) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (thread.getState() != Thread.State.BLOCKED) {
			assertTrue(System.nanoTime() < deadline, "thread never blocked: " + thread.getState());
			Thread.onSpinWait();
		}
	}

	@Test
	void scheduledEndClosesOnlyOnceTheDriverHasDrained() {
		final var window = window(1, 0, 0);
		source.inFlight = 4;
		source.succeeded = 4;
		window.observe();
		final var drained = new java.util.concurrent.atomic.AtomicBoolean();
		window.markSchedulingEnded(drained::get);
		clock.addAndGet(SECOND);
		source.succeeded = 400;
		window.observe();
		assertNull(window.result(), "queued work still keeps the slots full");
		drained.set(true);
		clock.addAndGet(SECOND);
		source.succeeded = 800;
		window.observe();
		final SteadyResult result = window.result();
		assertEquals("scheduling exhaustion", result.cause());
		assertEquals(796, result.statistics().successSnapshot().count());
		assertEquals(2 * SECOND, result.durationNanos());
	}

	@Test
	void scheduledEndBeforeTheStartIsNotReached() {
		final var window = defaultWindow();
		source.inFlight = 64;
		source.succeeded = 3;
		window.markSchedulingEnded(() -> true);
		window.observe();
		assertEquals(SteadyStatus.NOT_REACHED, window.result().status());
	}

	@Test
	void noStartBeginsAfterAClose() {
		final var window = defaultWindow();
		window.close("user-stop");
		source.inFlight = 1;
		source.succeeded = 1;
		window.observe();
		assertEquals(1, window.cutCount());
		assertEquals(SteadyStatus.NOT_REACHED, window.result().status());
	}

	private SteadyWindow startWith(final double warmup, final double minTurnovers, final long minDurationMillis) {
		final var window = window(warmup, minTurnovers, minDurationMillis);
		source.inFlight = 1;
		source.succeeded = Math.max(source.succeeded, 1);
		window.observe();
		assertTrue(window.isStarted());
		return window;
	}

	private static void await(final CountDownLatch latch) {
		try {
			latch.await();
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	/** A metrics context's meters as plain fields; each read returns histogram copies. */
	static final class FakeSource implements SteadyWindow.Source {
		// Set by the test thread before any worker thread starts.
		long succeeded, failed, corrupt, bytes;
		double succeededLastRate, bytesLastRate;
		long concurrencySum, concurrencyCount;
		int inFlight;
		final Histogram duration = histogram(), latency = histogram(), ttfb = histogram();
		final List<Integer> readOrder = new ArrayList<>();
		boolean failNextRead;
		private int reads;
		private CountDownLatch pausedSignal, pauseRelease;

		static Histogram histogram() {
			return new Histogram(1L, TimingMeterImpl.HIGHEST_TRACKABLE_VALUE_MICROS, TimingMeterImpl.SIGNIFICANT_VALUE_DIGITS);
		}

		void concurrency(final long sum, final long count) {
			concurrencySum = sum;
			concurrencyCount = count;
		}

		synchronized void pauseNextRead(final CountDownLatch signal, final CountDownLatch release) {
			pausedSignal = signal;
			pauseRelease = release;
		}

		@Override
		public long succeeded() {
			return succeeded;
		}

		@Override
		public long failed() {
			return failed;
		}

		@Override
		public int inFlight() {
			return inFlight;
		}

		synchronized int readCount() {
			return reads;
		}

		@Override
		public SteadyCut.Readings read() {
			final CountDownLatch signal, release;
			synchronized (this) {
				readOrder.add(++reads);
				if (failNextRead) {
					failNextRead = false;
					throw new IllegalStateException("simulated meter failure");
				}
				signal = pausedSignal;
				release = pauseRelease;
				pausedSignal = null;
				pauseRelease = null;
			}
			if (signal != null) {
				signal.countDown();
				await(release);
			}
			synchronized (duration) {
				return new SteadyCut.Readings(
								succeeded,
								failed,
								corrupt,
								bytes,
								succeededLastRate,
								0.0,
								0.0,
								bytesLastRate,
								concurrencySum,
								concurrencyCount,
								inFlight,
								duration.copy(),
								latency.copy(),
								ttfb.copy());
			}
		}
	}
}
