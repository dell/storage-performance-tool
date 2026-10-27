package com.dell.spt.base.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DriverStatsFleet;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The retained entry of a step carries the result of its own final refresh. A refresh already in
 * flight when the step ends, such as a scrape, finishes first and cannot publish over it, on a
 * node or on the Entry Node; no later refresh replaces it until the context starts again.
 */
class MetricsManagerImplTerminalSnapshotTest {

	private static final String STEP_ID = "terminal-snapshot";
	private static final String OLDER_REFRESH = "older-refresh";
	private static final long WAIT_SECONDS = 10;

	private final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final List<MetricsContext<?>> contexts = new ArrayList<>();
	private final CountDownLatch releaseOlder = new CountDownLatch(1);

	@BeforeEach
	void retainTerminalEntries() {
		mgr.setTerminalRetentionMillis(30_000);
	}

	@AfterEach
	void closeContexts() {
		releaseOlder.countDown();
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void fleetEntryKeepsItsFinalReadWhenAnOlderRefreshIsInFlight() throws Exception {
		final MetricsContext<?> early = node(() -> stats(10));
		final MetricsContext<?> last = node(() -> stats(20));
		early.markSucc(4096, 1_000, 100);
		last.markSucc(4096, 1_000, 100);
		final AllMetricsSnapshot earlySnapshot = DriverStatsFleet.snapshotOf(early);
		final AtomicReference<List<AllMetricsSnapshot>> answer = new AtomicReference<>(List.of(DriverStatsFleet.snapshotOf(last)));
		final CountDownLatch olderReading = new CountDownLatch(1);
		final AtomicInteger otherReads = new AtomicInteger();
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(STEP_ID, OpType.CREATE, 1, () -> {
			if (Thread.currentThread().getName().equals(OLDER_REFRESH)) {
				// A complete read taken before the last requests ended.
				olderReading.countDown();
				await(releaseOlder);
				return List.of(earlySnapshot);
			}
			otherReads.incrementAndGet();
			return answer.get();
		});
		contexts.add(fleet);
		mgr.register(fleet);
		stopSweeps();

		final Thread older = thread(OLDER_REFRESH, fleet::refreshLastSnapshot);
		older.start();
		assertTrue(olderReading.await(WAIT_SECONDS, TimeUnit.SECONDS));
		final int readsBefore = otherReads.get();
		final Thread unregister = thread("unregister", () -> mgr.unregister(fleet));
		unregister.start();
		awaitWaiting(unregister);
		assertEquals(readsBefore, otherReads.get(), "the final read waits for the refresh in flight");
		releaseOlder.countDown();
		join(older);
		join(unregister);

		final TerminalStepEntry entry = onlyEntry();
		assertFalse(entry.partial);
		assertEquals(List.of(MetricsConstants.FLEET_LOCAL_CONTRIBUTOR_ID), entry.contributorsPresent);
		assertEquals(stats(20), entry.driverStatsTotal.sum());
		assertEquals(1, entry.driverStatsTotal.reporting());
		assertFalse(entry.driverStatsTotal.partial());

		// A contributor that stops answering after the final read changes nothing the step kept.
		answer.set(Collections.singletonList(null));
		fleet.refreshLastSnapshot(true);
		assertFalse(fleet.partial());
		assertEquals(List.of(MetricsConstants.FLEET_LOCAL_CONTRIBUTOR_ID), fleet.contributorsPresent());
		assertEquals(stats(20), fleet.lastSnapshot().driverStatsTotal().sum());
	}

	@Test
	void nodeEntryKeepsItsFinalReadWhenAnOlderRefreshIsInFlight() throws Exception {
		final AtomicLong requests = new AtomicLong(10);
		final CountDownLatch olderReading = new CountDownLatch(1);
		final MetricsContext<?> ctx = node(() -> {
			final DriverStatsSnapshot seen = stats(requests.get());
			if (Thread.currentThread().getName().equals(OLDER_REFRESH)) {
				olderReading.countDown();
				await(releaseOlder);
			}
			return seen;
		});
		ctx.markSucc(4096, 1_000, 100);
		mgr.register(ctx);
		stopSweeps();

		final Thread older = thread(OLDER_REFRESH, () -> ctx.refreshLastSnapshot(true));
		older.start();
		assertTrue(olderReading.await(WAIT_SECONDS, TimeUnit.SECONDS));
		// The last requests end before the step stops.
		requests.set(20);
		final Thread unregister = thread("unregister", () -> mgr.unregister(ctx));
		unregister.start();
		awaitWaiting(unregister);
		releaseOlder.countDown();
		join(older);
		join(unregister);

		assertEquals(stats(20), onlyEntry().driverStats);
		// What the driver changes when it closes reaches neither the entry nor the Entry Node's read.
		requests.set(30);
		assertEquals(stats(20), DriverStatsFleet.snapshotOf(ctx).driverStats());
	}

	@Test
	void contextRefreshesAgainOnceItStartsAgainOrCloses() {
		final AtomicLong requests = new AtomicLong(10);
		final MetricsContext<?> ctx = node(() -> stats(requests.get()));
		final DistributedMetricsContext<?> fleet = DriverStatsFleet.fleet(
						STEP_ID, OpType.CREATE, 1, () -> List.of(DriverStatsFleet.snapshotOf(ctx)));
		contexts.add(fleet);
		assertEquals(stats(10), fleet.terminalSnapshot().driverStatsTotal().sum());
		assertEquals(stats(10), ctx.terminalSnapshot().driverStats());
		requests.set(20);
		fleet.refreshLastSnapshot(true);
		assertEquals(stats(10), fleet.lastSnapshot().driverStatsTotal().sum());

		ctx.start();
		fleet.start();
		fleet.refreshLastSnapshot(true);
		assertEquals(stats(20), fleet.lastSnapshot().driverStatsTotal().sum());

		assertEquals(stats(20), ctx.terminalSnapshot().driverStats());
		requests.set(30);
		ctx.close();
		ctx.refreshLastSnapshot(true);
		assertEquals(stats(30), ctx.lastSnapshot().driverStats());
	}

	private MetricsContext<?> node(final Supplier<DriverStatsSnapshot> stats) {
		final MetricsContext<?> ctx = DriverStatsFleet.node(STEP_ID, stats);
		contexts.add(ctx);
		return ctx;
	}

	private void stopSweeps() throws InterruptedException {
		// Only the refreshes each test starts run from here on.
		mgr.stop();
		assertTrue(mgr.awaitStop(WAIT_SECONDS, TimeUnit.SECONDS));
	}

	private TerminalStepEntry onlyEntry() {
		final List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size(), entries.toString());
		return entries.get(0);
	}

	private static DriverStatsSnapshot stats(final long requests) {
		return DriverStatsSnapshot.builder("terminal-driver").counter("requests", requests).build();
	}

	private static Thread thread(final String name, final Runnable task) {
		final Thread thread = new Thread(task, name);
		thread.setDaemon(true);
		return thread;
	}

	/** Waits until the thread parks, which the unregister thread does on the refresh in flight. */
	private static void awaitWaiting(final Thread thread) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
		while (thread.getState() != Thread.State.WAITING) {
			if (!thread.isAlive() || System.nanoTime() > deadline) {
				fail("\"" + thread.getName() + "\" did not wait for the refresh in flight: " + thread.getState());
			}
			Thread.sleep(1);
		}
	}

	private static void join(final Thread thread) throws InterruptedException {
		thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
		assertFalse(thread.isAlive(), "\"" + thread.getName() + "\" did not finish");
	}

	private static void await(final CountDownLatch latch) {
		try {
			assertTrue(latch.await(WAIT_SECONDS, TimeUnit.SECONDS), "the test never released the refresh");
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
	}
}
