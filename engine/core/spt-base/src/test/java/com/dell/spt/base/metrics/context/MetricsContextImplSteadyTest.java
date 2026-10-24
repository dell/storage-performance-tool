package com.dell.spt.base.metrics.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadySettings;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The Steady-State Window wired to a real metrics context's meters. */
class MetricsContextImplSteadyTest {

	private final AtomicInteger gauge = new AtomicInteger();
	private final ExecutorService threads = Executors.newCachedThreadPool();

	@AfterEach
	void stopThreads() {
		threads.shutdownNow();
	}

	private MetricsContextImpl<?> context(final SteadySettings settings) {
		final var ctx = (MetricsContextImpl<?>) MetricsContextImpl.builder()
						.loadStepId("steady-" + System.nanoTime())
						.opType(OpType.CREATE)
						.actualConcurrencyGauge(gauge::get)
						.concurrencyLimit(64)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1024))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(1L)
						.steadySettings(settings)
						.build();
		ctx.start();
		return ctx;
	}

	@Test
	void contextWithoutSettingsMeasuresNoWindow() {
		final var ctx = context(null);
		ctx.markSucc(1, 1_000, 500);
		ctx.observeSteadyWindow();
		assertNull(ctx.closeSteadyWindow("t"));
		assertNull(ctx.steadyResult());
	}

	@Test
	void theStartRuleReadsTheGaugeFreshNotTheCachedSnapshot() {
		final var ctx = context(new SteadySettings(1, 0, 0));
		gauge.set(10);
		for (int i = 0; i < 50; i++) {
			ctx.markSucc(1, 1_000, 500);
		}
		ctx.refreshLastSnapshot(true); // caches 10 in flight
		gauge.set(100);
		ctx.observeSteadyWindow();
		assertNull(ctx.steadyResult());
		gauge.set(40);
		ctx.observeSteadyWindow();
		final SteadyResult result = ctx.closeSteadyWindow("t");
		assertTrue(result.status().hasStatistics(), "50 completions at 40 fresh in flight start the window");
	}

	@Test
	void startCutCopiesBypassTheSnapshotThrottle() {
		final var ctx = context(new SteadySettings(0, 0, 0));
		for (int i = 0; i < 5; i++) {
			ctx.markSucc(1, 1_000, 500);
		}
		ctx.refreshLastSnapshot(true); // timing snapshots cached with 5 values
		for (int i = 0; i < 5; i++) {
			ctx.markSucc(1, 1_000, 500); // within the 100 ms throttle: not in the cached snapshot
		}
		ctx.observeSteadyWindow();
		for (int i = 0; i < 3; i++) {
			ctx.markSucc(1, 2_000, 900);
		}
		final var stats = ctx.closeSteadyWindow("t").statistics();
		assertEquals(3, stats.durationSnapshot().count(), "the start cut must already hold all 10 earlier values");
		assertEquals(3, stats.successSnapshot().count());
	}

	@Test
	void windowFromRealMetersCountsFailuresBytesAndParts() throws Exception {
		final var ctx = context(new SteadySettings(1, 0, 0));
		gauge.set(2);
		ctx.markSucc(10, 1_000, 500);
		ctx.markSucc(10, 1_000, 500);
		ctx.observeSteadyWindow();
		TimeUnit.MILLISECONDS.sleep(20);
		ctx.markSucc(100, 3_000, 1_000);
		ctx.markFail(4_000, 2_000);
		ctx.markPartSucc(50, 2_500, 900);
		ctx.markCorrupt();
		final SteadyResult result = ctx.closeSteadyWindow("time-limit");
		final var stats = result.statistics();
		assertEquals(1, stats.successSnapshot().count());
		assertEquals(2, stats.failsSnapshot().count(), "a corrupt operation is also a failure");
		assertEquals(1, stats.corruptSnapshot().count());
		assertEquals(150, stats.byteSnapshot().count());
		assertEquals(3, stats.durationSnapshot().count(), "success, failure and part timings");
		assertEquals(3.0 / 2.0, result.turnovers(), 1e-9);
		assertTrue(result.durationNanos() >= TimeUnit.MILLISECONDS.toNanos(20));
		assertSame(result, ctx.steadyResult());
		assertSame(result, ctx.closeSteadyWindow("later"), "later closers get the first result");
	}

	@Test
	void closingWhileSnapshotsRefreshConcurrently() throws Exception {
		final var ctx = context(new SteadySettings(0, 0, 0));
		gauge.set(1);
		ctx.markSucc(1, 1_000, 500);
		ctx.observeSteadyWindow();
		final var running = new AtomicBoolean(true);
		final List<CompletableFuture<Void>> refreshers = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			refreshers.add(CompletableFuture.runAsync(() -> {
				while (running.get()) {
					ctx.refreshLastSnapshot(true);
					ctx.markSucc(1, 1_000, 500);
				}
			}, threads));
		}
		TimeUnit.MILLISECONDS.sleep(50);
		final SteadyResult result = ctx.closeSteadyWindow("deadline");
		running.set(false);
		for (final var refresher : refreshers) {
			refresher.get(5, TimeUnit.SECONDS);
		}
		final var stats = result.statistics();
		assertTrue(stats.successSnapshot().count() > 0);
		assertFalse(stats.durationSnapshot().count() < 0);
		assertSame(result, ctx.steadyResult());
	}
}
