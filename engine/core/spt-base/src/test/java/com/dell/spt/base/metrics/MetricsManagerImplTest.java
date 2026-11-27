package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MetricsManagerImpl} focusing on register/unregister lifecycle
 * and basic periodic output/threshold handling via the internal tick.
 */
public class MetricsManagerImplTest {

	private MetricsManagerImpl mgr;

	@BeforeEach
	void setUp() {
		mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	}

	@AfterEach
	void tearDown() throws Exception {
		// Ensure manager is closed between tests
		mgr.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void quantileValuesPreserveMissingTimingSignal() throws Exception {
		final Method quantileValues = MetricsManagerImpl.class.getDeclaredMethod(
						"quantileValues", TimingMetricSnapshot.class, List.class);
		quantileValues.setAccessible(true);
		final Map<Double, Long> values = (Map<Double, Long>) quantileValues.invoke(
						null,
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						List.of(0.5, 0.9));

		assertTrue(values.containsKey(0.5));
		assertNull(values.get(0.5));
		assertNull(values.get(0.9));
	}

	@Test
	void eachIterationObservesTheSteadyStateWindow() throws Exception {
		final FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-steady");
		mgr.register(ctx);
		final long deadline = System.currentTimeMillis() + 5_000;
		while (ctx.steadyObservations.get() < 3 && System.currentTimeMillis() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(ctx.steadyObservations.get() >= 3, "the 100 Hz loop must observe the window on each iteration");
		mgr.unregister(ctx);
	}

	@Test
	void registerNonDistributedDoesNotExposeDistributedContext() {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-A");
		mgr.register(ctx);

		// Manager should have started
		assertTrue(mgr.isStarted(), "Metrics manager task should be started after first register");

		// Non-distributed contexts must not appear in distributed set
		assertTrue(mgr.getDistributedContexts().isEmpty(), "No distributed contexts expected");

		mgr.unregister(ctx);
	}

	@Test
	void registerAndUnregisterDistributedContext() throws Exception {
		// Prepare a per-test tmp dir for TimingMetricQuantileResultsImpl to scan
		Path tmpBase = Files.createTempDirectory("mmgr-test-");
		String origTmp = System.getProperty("java.io.tmpdir");
		try {
			// Ensure the path java.io.tmpdir + "/spt/" exists to avoid NPE in quantile helper
			Path sptDir = tmpBase.resolve("spt");
			Files.createDirectories(sptDir);
			System.setProperty("java.io.tmpdir", tmpBase.toString());

			FakeDistributedMetricsContext dctx = new FakeDistributedMetricsContext("step-D");
			mgr.register(dctx);
			assertEquals(1, mgr.getDistributedContexts().size(), "Distributed context should be visible after register");

			mgr.unregister(dctx);
			assertTrue(mgr.getDistributedContexts().isEmpty(), "Distributed context should be removed after unregister");
		} finally {
			if (origTmp != null) {
				System.setProperty("java.io.tmpdir", origTmp);
			}
		}
	}

	@Test
	void tickUpdatesLastOutputTimestampWhenDue() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-T");
		ctx.outputPeriodMillis = 1; // make it due immediately
		ctx.lastOutputTs = 0L;
		// Supply a non-null snapshot so periodic output branch executes
		ctx.snapshot = new FakeAllSnapshot(new FakeConcurrencySnapshot(0));

		// Avoid starting the internal task thread to prevent lock races: inject the context directly.
		java.lang.reflect.Field allMetricsField = MetricsManagerImpl.class.getDeclaredField("allMetrics");
		allMetricsField.setAccessible(true);
		@SuppressWarnings("unchecked")
		final java.util.Set<MetricsContext> allMetrics = (java.util.Set<MetricsContext>) allMetricsField.get(mgr);
		allMetrics.add(ctx);

		// Invoke the protected tick method reflectively
		Method tick = MetricsManagerImpl.class.getDeclaredMethod("doWork");
		tick.setAccessible(true);
		tick.invoke(mgr);

		assertTrue(ctx.lastOutputTs > 0, "Expected lastOutputTs to be updated by tick");
	}

	@Test
	void unregisterExitsThresholdStateIfEntered() {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-TH");
		ctx.thresholdEntered = true;
		ctx.thresholdExited = false;
		FakeMetricsContext<AllMetricsSnapshot> inner = new FakeMetricsContext<>("th-metrics");
		inner.snapshot = new FakeAllSnapshot(new FakeConcurrencySnapshot(0));
		ctx.thresholdDelegate = inner;

		mgr.register(ctx);
		mgr.unregister(ctx);

		assertTrue(ctx.exitThresholdCalled, "exitThresholdState should be called on unregister when entered");
	}

	@Test
	void recordProgressSnapshotStoresMetadataLimits() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-meta");
		ctx.metadata.put(MetricsConstants.METADATA_LIMIT_OP_COUNT, 250L);
		ctx.metadata.put(MetricsConstants.METADATA_LIMIT_TIME_SEC, 30L);
		ctx.snapshot = new CountingSnapshot(
						10,
						2,
						1024,
						100.0,
						200.0,
						new FakeConcurrencySnapshot(4),
						5000L);

		invokeRecordProgressSnapshot(ctx, ctx.snapshot);

		Collection<TerminalStepEntry> entries = lastProgressEntries();
		assertEquals(1, entries.size(), "Expected single progress entry");
		TerminalStepEntry entry = entries.iterator().next();
		assertEquals(250L, entry.countLimit);
		assertEquals(30L, entry.timeLimitSec);
		assertEquals(10L, entry.successCount);
		assertEquals(2L, entry.failedCount);
		assertEquals(1024L, entry.bytesTotal);
		assertEquals(100.0, entry.latencyMeanUs);
		assertEquals(200.0, entry.durationMeanUs);
		assertEquals(4L, entry.concurrencyLast);
		assertEquals(5000L, entry.elapsedTimeMillis);
	}

	@Test
	void recordProgressSnapshotIgnoresMalformedMetadata() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-meta-bad");
		ctx.metadata.put(MetricsConstants.METADATA_LIMIT_OP_COUNT, "not-a-number");
		ctx.metadata.put(MetricsConstants.METADATA_LIMIT_TIME_SEC, new Object());
		ctx.snapshot = new CountingSnapshot(
						5,
						0,
						512,
						50.0,
						75.0,
						new FakeConcurrencySnapshot(1),
						2000L);

		invokeRecordProgressSnapshot(ctx, ctx.snapshot);

		Collection<TerminalStepEntry> entries = lastProgressEntries();
		assertEquals(1, entries.size());
		TerminalStepEntry entry = entries.iterator().next();
		assertEquals(0L, entry.countLimit);
		assertEquals(0L, entry.timeLimitSec);
	}

	@Test
	void hasProgressHandlesSnapshotExceptions() throws Exception {
		Method hasProgress = MetricsManagerImpl.class.getDeclaredMethod("hasProgress", AllMetricsSnapshot.class);
		hasProgress.setAccessible(true);
		boolean result = (boolean) hasProgress.invoke(null, new ThrowingSnapshot());
		assertFalse(result, "Snapshot throwing should be treated as no progress");
	}

	/**
	 * Reproduces a bug where the MetricsManager's doWork() loop cannot restart after all
	 * contexts from a previous load step have been unregistered. In a multi-step scenario
	 * (e.g. Write then Read), the first step's unregister empties allMetrics which triggers
	 * stop(). The second step's register calls startIfNotStarted(), but the task refuses to
	 * restart because isStopped() is true. This means doWork() never runs for step 2, so
	 * periodic metrics.csv output is never produced.
	 *
	 * Regression introduced in commit e3d72cd when MetricsManagerImpl was migrated from
	 * ExclusiveFiberBase (fiber4j) to TaskBase (Virtual Threads).
	 */
	@Test
	void metricsManagerRestartsAfterAllContextsUnregistered() throws Exception {
		// --- Step 1: register and unregister a context (simulates first load step completing) ---
		FakeMetricsContext<AllMetricsSnapshot> step1Ctx = new FakeMetricsContext<>("WRITE-step");
		mgr.register(step1Ctx);
		assertTrue(mgr.isStarted(), "Manager should be started after first register");

		mgr.unregister(step1Ctx);
		// The unregister empties allMetrics, which calls stop() inside MetricsManagerImpl.
		// Wait for the task's virtual thread to actually finish.
		Thread.sleep(200);
		assertTrue(mgr.isStopped(), "Manager should be stopped after all contexts unregistered");

		// --- Step 2: register a new context (simulates second load step starting) ---
		FakeMetricsContext<AllMetricsSnapshot> step2Ctx = new FakeMetricsContext<>("READ-step");
		step2Ctx.outputPeriodMillis = 1; // due immediately
		step2Ctx.lastOutputTs = 0;
		step2Ctx.snapshot = new FakeAllSnapshot(new FakeConcurrencySnapshot(0));

		mgr.register(step2Ctx);

		// The manager must restart its doWork() loop for the new context.
		assertTrue(mgr.isStarted(),
						"Manager should be re-started after registering a context for a new step");

		// Give the doWork() loop time to run at least one iteration (it sleeps 10ms per cycle)
		Thread.sleep(200);

		assertTrue(step2Ctx.lastOutputTs > 0,
						"doWork() should have updated lastOutputTs for the second step's context, "
										+ "proving the periodic metrics output loop is running");

		mgr.unregister(step2Ctx);
	}

	/**
	 * Simulates a realistic multi-step scenario (Write → Read → Delete) as executed by the
	 * CLI's scenario templates.  Each step follows the exact lifecycle driven by
	 * {@code LoadStepBase}: register metrics contexts, let the doWork loop run (producing
	 * periodic metrics.csv output), then unregister — which empties allMetrics and stops the
	 * manager task.  The next step's register must transparently restart the manager.
	 * <p>
	 * This is the integration-level complement to
	 * {@link #metricsManagerRestartsAfterAllContextsUnregistered()}, which proves the restart
	 * mechanism itself.  Here we additionally verify:
	 * <ul>
	 *   <li>Three full stop→restart cycles work (not just one)</li>
	 *   <li>Each step's periodic output loop actually runs (lastOutputTs updated)</li>
	 *   <li>Terminal entries are recorded for each step and queryable via getTerminalSteps()</li>
	 *   <li>Different OpTypes (CREATE, READ, DELETE) are preserved in the terminal entries</li>
	 * </ul>
	 */
	@Test
	void multiStepScenarioWriteReadDelete() throws Exception {
		// Keep terminal entries for the duration of the test
		mgr.setTerminalRetentionMillis(60_000);

		final OpType[] opTypes = {OpType.CREATE, OpType.READ, OpType.DELETE
		};
		final String[] stepIds = {"write-step", "read-step", "delete-step"
		};
		final long[][] operationData = {
				// {successCount, bytes, latencyMean, durationMean}
				{500, 512_000, 120, 80
				},
				{1000, 1_024_000, 95, 60
				},
				{500, 0, 50, 30
				}
		};

		for (int step = 0; step < 3; step++) {
			final String stepId = stepIds[step];
			final OpType opType = opTypes[step];
			final long[] data = operationData[step];

			// --- Replicate LoadStepBase.doStart(): create context, start, register ---
			FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>(stepId);
			ctx.opType = opType;
			ctx.outputPeriodMillis = 1; // due immediately so doWork() updates lastOutputTs
			ctx.lastOutputTs = 0;
			ctx.snapshot = new CountingSnapshot(
							data[0], 0, data[1], data[2], data[3],
							new FakeConcurrencySnapshot(4), 1000L * (step + 1));
			ctx.metadata.put(MetricsConstants.METADATA_LIMIT_OP_COUNT, data[0]);

			ctx.start(); // MetricsContext::start (no-op for fake, but mirrors real lifecycle)
			mgr.register(ctx);

			assertTrue(mgr.isStarted(),
							"Manager should be running during step " + (step + 1) + " (" + stepId + ")");

			// Let the doWork() loop run a few iterations (10ms per cycle)
			Thread.sleep(200);

			assertTrue(ctx.lastOutputTs > 0,
							"doWork() should have produced periodic output for " + stepId);

			// --- Replicate LoadStepBase.doStop(): unregister ---
			mgr.unregister(ctx);

			// Wait for the virtual thread to finish (allMetrics is now empty → stop())
			Thread.sleep(200);
			assertTrue(mgr.isStopped(),
							"Manager should be stopped after " + stepId + " unregistered");
		}

		// --- Verify terminal entries captured for all three steps ---
		final List<TerminalStepEntry> terminal = mgr.getTerminalSteps();
		assertEquals(3, terminal.size(),
						"Should have terminal entries for all three steps (Write, Read, Delete)");

		// Verify each step's entry has the correct OpType and step id
		final Map<String, TerminalStepEntry> byId = new HashMap<>();
		for (final TerminalStepEntry entry : terminal) {
			byId.put(entry.stepId, entry);
		}

		for (int step = 0; step < 3; step++) {
			final TerminalStepEntry entry = byId.get(stepIds[step]);
			assertNotNull(entry, "Terminal entry missing for " + stepIds[step]);
			assertEquals(opTypes[step], entry.opType,
							"OpType mismatch for " + stepIds[step]);
			assertEquals(operationData[step][0], entry.successCount,
							"Success count mismatch for " + stepIds[step]);
		}
	}

	@Test
	void invokeTimedExclusivelyIgnoresConcurrentModification() throws Exception {
		ThrowingRefreshMetricsContext ctx = new ThrowingRefreshMetricsContext("step-cme");
		mgr.register(ctx);

		Method tick = MetricsManagerImpl.class.getDeclaredMethod("doWork");
		tick.setAccessible(true);

		assertDoesNotThrow(() -> tick.invoke(mgr));
	}

	// --- Test fakes ---

	private static class FakeMetricsContext<S extends AllMetricsSnapshot> implements MetricsContext<S> {
		final java.util.concurrent.atomic.AtomicInteger steadyObservations = new java.util.concurrent.atomic.AtomicInteger();
		final String id;
		long runId = 1L;
		OpType opType = OpType.READ;
		int concurrencyLimit = 1;
		SizeInBytes size = new SizeInBytes(0);
		long startTs = System.currentTimeMillis();
		String comment = "";
		boolean thresholdEntered = false;
		boolean thresholdExited = false;
		boolean exitThresholdCalled = false;
		int threshold = 0;
		long outputPeriodMillis = 0;
		long lastOutputTs = 0;
		boolean avgPersist = false;
		boolean sumPersist = false;
		boolean timingPersist = false;
		S snapshot = null;
		MetricsContext thresholdDelegate = this;
		final Map<String, Object> metadata = new HashMap<>();

		FakeMetricsContext(String id) {
			this.id = id;
		}

		@Override
		public Map metadata() {
			return metadata;
		}

		@Override
		public String loadStepId() {
			return id;
		}

		@Override
		public long runId() {
			return runId;
		}

		@Override
		public OpType opType() {
			return opType;
		}

		@Override
		public int concurrencyLimit() {
			return concurrencyLimit;
		}

		@Override
		public SizeInBytes itemDataSize() {
			return size;
		}

		@Override
		public void markSucc(long bytes, long duration, long latency) {}

		@Override
		public void markSucc(long bytes, long duration, long latency, long ttfb) {}

		@Override
		public void markPartSucc(long bytes, long duration, long latency) {}

		@Override
		public void markPartSucc(long bytes, long duration, long latency, long ttfb) {}

		@Override
		public void markSucc(long count, long bytes, long[] durationValues, long[] latencyValues) {}

		@Override
		public void markSucc(long count, long bytes, long[] durationValues, long[] latencyValues, long[] ttfbValues) {}

		@Override
		public void markPartSucc(long bytes, long[] durationValues, long[] latencyValues) {}

		@Override
		public void markPartSucc(long bytes, long[] durationValues, long[] latencyValues, long[] ttfbValues) {}

		@Override
		public void markFail() {}

		@Override
		public void markFail(long count) {}

		@Override
		public void start() {}

		@Override
		public boolean isStarted() {
			return true;
		}

		@Override
		public long startTimeStamp() {
			return startTs;
		}

		@Override
		public void refreshLastSnapshot() { /* no-op */ }

		@Override
		public void observeSteadyWindow() {
			steadyObservations.incrementAndGet();
		}

		@Override
		public S lastSnapshot() {
			return snapshot;
		}

		@Override
		public int concurrencyThreshold() {
			return threshold;
		}

		@Override
		public boolean thresholdStateEntered() {
			return thresholdEntered;
		}

		@Override
		public void enterThresholdState() throws IllegalStateException {
			thresholdEntered = true;
		}

		@Override
		public boolean thresholdStateExited() {
			return thresholdExited;
		}

		@Override
		public MetricsContext thresholdMetrics() {
			return thresholdDelegate;
		}

		@Override
		public void exitThresholdState() throws IllegalStateException {
			thresholdExited = true;
			exitThresholdCalled = true;
		}

		@Override
		public boolean stdOutColorEnabled() {
			return false;
		}

		@Override
		public boolean avgPersistEnabled() {
			return avgPersist;
		}

		@Override
		public boolean sumPersistEnabled() {
			return sumPersist;
		}

		@Override
		public boolean timingPersistEnabled() {
			return timingPersist;
		}

		@Override
		public long outputPeriodMillis() {
			return outputPeriodMillis;
		}

		@Override
		public long lastOutputTs() {
			return lastOutputTs;
		}

		@Override
		public void lastOutputTs(long ts) {
			lastOutputTs = ts;
		}

		@Override
		public long elapsedTimeMillis() {
			return 0;
		}

		@Override
		public String comment() {
			return comment;
		}

		@Override
		public void close() {}

		@Override
		public int compareTo(MetricsContext<S> o) {
			if (o == null)
				return 1;
			return this.loadStepId().compareTo(o.loadStepId());
		}

		@Override
		public String toString() {
			return "FakeMetricsContext(" + id + ")";
		}
	}

	private static class FakeDistributedMetricsContext extends FakeMetricsContext<DistributedAllMetricsSnapshot> implements DistributedMetricsContext<DistributedAllMetricsSnapshot> {
		List<String> addrs = List.of("127.0.0.1");
		List<Double> quantiles = List.of(0.5, 0.9);
		int nodeCount = 1;

		FakeDistributedMetricsContext(String id) {
			super(id);
		}

		@Override
		public int nodeCount() {
			return nodeCount;
		}

		@Override
		public List<String> nodeAddrs() {
			return addrs;
		}

		@Override
		public List<Double> quantileValues() {
			return quantiles;
		}

		@Override
		public DistributedAllMetricsSnapshot lastSnapshot() {
			return (DistributedAllMetricsSnapshot) super.lastSnapshot();
		}
	}

	private static class FakeAllSnapshot implements AllMetricsSnapshot, DistributedAllMetricsSnapshot {
		final ConcurrencyMetricSnapshot conc;
		final com.dell.spt.base.metrics.snapshot.RateMetricSnapshot succ = new StubRate("success");
		final com.dell.spt.base.metrics.snapshot.RateMetricSnapshot fail = new StubRate("fails");
		final com.dell.spt.base.metrics.snapshot.RateMetricSnapshot bytes = new StubRate("byte");
		final com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot lat = new StubTiming("latency");
		final com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot dur = new StubTiming("duration");

		FakeAllSnapshot(ConcurrencyMetricSnapshot conc) {
			this.conc = conc;
		}

		@Override
		public ConcurrencyMetricSnapshot concurrencySnapshot() {
			return conc;
		}

		@Override
		public long elapsedTimeMillis() {
			return 0;
		}

		@Override
		public com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot durationSnapshot() {
			return dur;
		}

		@Override
		public com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot latencySnapshot() {
			return lat;
		}

		@Override
		public com.dell.spt.base.metrics.snapshot.RateMetricSnapshot byteSnapshot() {
			return bytes;
		}

		@Override
		public com.dell.spt.base.metrics.snapshot.RateMetricSnapshot successSnapshot() {
			return succ;
		}

		@Override
		public com.dell.spt.base.metrics.snapshot.RateMetricSnapshot failsSnapshot() {
			return fail;
		}

		@Override
		public int nodeCount() {
			return 1;
		}
	}

	private static class StubRate implements com.dell.spt.base.metrics.snapshot.RateMetricSnapshot {
		private final String name;
		private final long count;
		private final double mean;
		private final double last;

		StubRate(String name) {
			this(name, 0L, 0.0, 0.0);
		}

		StubRate(String name, long count, double mean, double last) {
			this.name = name;
			this.count = count;
			this.mean = mean;
			this.last = last;
		}

		@Override
		public long count() {
			return count;
		}

		@Override
		public double last() {
			return last;
		}

		@Override
		public long elapsedTimeMillis() {
			return 0L;
		}

		@Override
		public double mean() {
			return mean;
		}

		@Override
		public String name() {
			return name;
		}
	}

	private static class StubTiming implements com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot {
		private final String name;
		private final long count;
		private final double mean;
		private final long sum;
		private final long min;
		private final long max;

		StubTiming(String name) {
			this(name, 0L, 0.0, 0L, 0L, 0L);
		}

		StubTiming(String name, long count, double mean, long sum, long min, long max) {
			this.name = name;
			this.count = count;
			this.mean = mean;
			this.sum = sum;
			this.min = min;
			this.max = max;
		}

		@Override
		public long count() {
			return count;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public double mean() {
			return mean;
		}

		@Override
		public long sum() {
			return sum;
		}

		@Override
		public long min() {
			return min;
		}

		@Override
		public long max() {
			return max;
		}
	}

	private static class FakeConcurrencySnapshot implements ConcurrencyMetricSnapshot {
		final long last;

		FakeConcurrencySnapshot(long last) {
			this.last = last;
		}

		@Override
		public long last() {
			return last;
		}

		@Override
		public double mean() {
			return last;
		}

		@Override
		public String name() {
			return "concurrency";
		}
	}

	private static class CountingSnapshot implements AllMetricsSnapshot {
		private final RateMetricSnapshot success;
		private final RateMetricSnapshot fails;
		private final RateMetricSnapshot bytes;
		private final TimingMetricSnapshot latency;
		private final TimingMetricSnapshot duration;
		private final ConcurrencyMetricSnapshot concurrency;
		private final long elapsedMillis;

		CountingSnapshot(
						long successCount,
						long failCount,
						long bytesTotal,
						double latencyMean,
						double durationMean,
						ConcurrencyMetricSnapshot concurrency,
						long elapsedMillis) {
			this.success = new StubRate("success", successCount, 0.0, 0.0);
			this.fails = new StubRate("fails", failCount, 0.0, 0.0);
			this.bytes = new StubRate("byte", bytesTotal, 0.0, 0.0);
			this.latency = new StubTiming("latency", successCount + failCount, latencyMean, 0L, 0L, 0L);
			this.duration = new StubTiming("duration", successCount + failCount, durationMean, 0L, 0L, 0L);
			this.concurrency = concurrency;
			this.elapsedMillis = elapsedMillis;
		}

		@Override
		public ConcurrencyMetricSnapshot concurrencySnapshot() {
			return concurrency;
		}

		@Override
		public long elapsedTimeMillis() {
			return elapsedMillis;
		}

		@Override
		public TimingMetricSnapshot durationSnapshot() {
			return duration;
		}

		@Override
		public TimingMetricSnapshot latencySnapshot() {
			return latency;
		}

		@Override
		public RateMetricSnapshot byteSnapshot() {
			return bytes;
		}

		@Override
		public RateMetricSnapshot successSnapshot() {
			return success;
		}

		@Override
		public RateMetricSnapshot failsSnapshot() {
			return fails;
		}
	}

	private static class ThrowingSnapshot implements AllMetricsSnapshot {
		@Override
		public ConcurrencyMetricSnapshot concurrencySnapshot() {
			throw new UnsupportedOperationException("concurrencySnapshot");
		}

		@Override
		public long elapsedTimeMillis() {
			return 0;
		}

		@Override
		public TimingMetricSnapshot durationSnapshot() {
			throw new UnsupportedOperationException("durationSnapshot");
		}

		@Override
		public TimingMetricSnapshot latencySnapshot() {
			throw new UnsupportedOperationException("latencySnapshot");
		}

		@Override
		public RateMetricSnapshot byteSnapshot() {
			throw new UnsupportedOperationException("byteSnapshot");
		}

		@Override
		public RateMetricSnapshot successSnapshot() {
			throw new RuntimeException("snapshot failure");
		}

		@Override
		public RateMetricSnapshot failsSnapshot() {
			throw new UnsupportedOperationException("failsSnapshot");
		}
	}

	private static class ThrowingRefreshMetricsContext extends FakeMetricsContext<AllMetricsSnapshot> {
		ThrowingRefreshMetricsContext(String id) {
			super(id);
		}

		@Override
		public void refreshLastSnapshot() {
			throw new ConcurrentModificationException("simulated CME");
		}
	}

	@Test
	void eachContextOfAStepKeepsItsOwnProgress() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> read = progressContext("step-mixed", OpType.READ, 1_000, 4_096_000);
		FakeMetricsContext<AllMetricsSnapshot> create = progressContext("step-mixed", OpType.CREATE, 10, 40_960);
		invokeRecordProgressSnapshot(read, read.snapshot);
		invokeRecordProgressSnapshot(create, create.snapshot);

		assertEquals(2, lastProgressEntries().size(), "one entry per context");
		assertEquals(1_000L, mgr.getLastProgressSnapshot(read).orElseThrow().successCount);
		assertEquals(10L, mgr.getLastProgressSnapshot(create).orElseThrow().successCount);
		assertEquals(40_960L, mgr.getLastProgressSnapshot(create).orElseThrow().bytesTotal);
	}

	@Test
	void contextsThatCompareEqualKeepSeparateProgress() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> first = new EqualFakeMetricsContext("step-equal");
		FakeMetricsContext<AllMetricsSnapshot> second = new EqualFakeMetricsContext("step-equal");
		first.snapshot = new CountingSnapshot(300, 0, 3_000, 10.0, 20.0, new FakeConcurrencySnapshot(1), 1_000L);
		second.snapshot = new CountingSnapshot(7, 0, 70, 30.0, 40.0, new FakeConcurrencySnapshot(1), 1_000L);
		assertEquals(first, second);
		assertEquals(first.hashCode(), second.hashCode());
		invokeRecordProgressSnapshot(first, first.snapshot);
		invokeRecordProgressSnapshot(second, second.snapshot);

		assertEquals(2, lastProgressEntries().size(), "contexts that compare equal must not share an entry");
		assertEquals(300L, mgr.getLastProgressSnapshot(first).orElseThrow().successCount);
		assertEquals(7L, mgr.getLastProgressSnapshot(second).orElseThrow().successCount);
	}

	@Test
	void unregisteringOneContextKeepsTheProgressOfItsSiblings() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> read = progressContext("step-siblings", OpType.READ, 1_000, 4_096_000);
		FakeMetricsContext<AllMetricsSnapshot> create = progressContext("step-siblings", OpType.CREATE, 10, 40_960);
		mgr.register(read);
		mgr.register(create);
		invokeRecordProgressSnapshot(read, read.snapshot);
		invokeRecordProgressSnapshot(create, create.snapshot);

		mgr.unregister(create);

		final Collection<TerminalStepEntry> entries = lastProgressEntries();
		assertEquals(1, entries.size(), "the sibling's entry stays, the unregistered context's goes");
		assertEquals(OpType.READ, entries.iterator().next().opType);
		assertEquals(1_000L, entries.iterator().next().successCount);

		mgr.unregister(read);
		assertTrue(lastProgressEntries().isEmpty(), "unregister leaves no entry behind");
	}

	@Test
	void closeClearsTheProgressCache() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> ctx = progressContext("step-close", OpType.READ, 5, 5_120);
		invokeRecordProgressSnapshot(ctx, ctx.snapshot);
		assertEquals(1, lastProgressEntries().size());

		mgr.close();

		assertTrue(lastProgressEntries().isEmpty(), "close leaves no entry behind");
	}

	@Test
	void stepLookupStillReturnsTheEntryOfASingleContextStep() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> ctx = progressContext("step-single", OpType.CREATE, 42, 43_008);
		invokeRecordProgressSnapshot(ctx, ctx.snapshot);

		assertEquals(42L, mgr.getLastProgressSnapshot("step-single").orElseThrow().successCount);
		assertEquals(42L, mgr.getLastProgressSnapshot("step-single", false).orElseThrow().successCount);
		assertTrue(mgr.getLastProgressSnapshot("step-single", true).isEmpty(), "the node entry is not a fleet entry");
		assertTrue(mgr.getLastProgressSnapshot("other-step").isEmpty());
		assertTrue(mgr.getLastProgressSnapshot((String) null).isEmpty());
		assertTrue(mgr.getLastProgressSnapshot((MetricsContext<?>) null).isEmpty());
	}

	@Test
	void stepLookupOfAStepWithSeveralContextsReturnsNoSiblingsEntry() throws Exception {
		FakeMetricsContext<AllMetricsSnapshot> read = progressContext("step-several", OpType.READ, 1_000, 4_096_000);
		FakeMetricsContext<AllMetricsSnapshot> create = progressContext("step-several", OpType.CREATE, 10, 40_960);
		invokeRecordProgressSnapshot(read, read.snapshot);
		invokeRecordProgressSnapshot(create, create.snapshot);

		assertTrue(
						mgr.getLastProgressSnapshot("step-several").isEmpty(),
						"no single entry stands for a step of several contexts");
	}

	@Test
	void aSweepBlockedBeforeItRecordsLeavesNoEntryOfAContextUnregisteredMeanwhile() throws Exception {
		final Thread testThread = Thread.currentThread();
		final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicBoolean armed = new java.util.concurrent.atomic.AtomicBoolean(true);
		final FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-race-refresh") {
			@Override
			public void refreshLastSnapshot() {
				if (Thread.currentThread() != testThread && armed.compareAndSet(true, false)) {
					entered.countDown();
					awaitQuietly(release);
				}
			}
		};
		ctx.snapshot = new CountingSnapshot(50, 0, 51_200, 100.0, 110.0, new FakeConcurrencySnapshot(1), 2_000L);
		// Keeps the manager running once the raced context is gone.
		mgr.register(progressContext("step-race-keeper", OpType.READ, 0, 0));
		mgr.register(ctx);
		assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS), "the sweep reached the context");

		// The sweep holds the output lock, so the unregister times out on it and runs alongside.
		mgr.unregister(ctx);
		release.countDown();
		awaitSweep();

		assertTrue(lastProgressEntries().isEmpty(), "no entry outlives its unregistered context");
	}

	@Test
	void aSweepRecordingWhileAContextIsUnregisteredLeavesNoEntryOfIt() throws Exception {
		final Thread testThread = Thread.currentThread();
		final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicBoolean armed = new java.util.concurrent.atomic.AtomicBoolean(true);
		final FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-race-record");
		ctx.snapshot = new CountingSnapshot(50, 0, 51_200, 100.0, 110.0, new FakeConcurrencySnapshot(1), 2_000L) {
			@Override
			public RateMetricSnapshot successSnapshot() {
				// The sweep blocks here inside its record, after any check made before it.
				if (Thread.currentThread() != testThread && armed.compareAndSet(true, false)) {
					entered.countDown();
					awaitQuietly(release);
				}
				return super.successSnapshot();
			}
		};
		mgr.register(progressContext("step-race-keeper", OpType.READ, 0, 0));
		mgr.register(ctx);
		assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS), "the sweep reached the record");

		mgr.unregister(ctx);
		release.countDown();
		awaitSweep();

		assertTrue(lastProgressEntries().isEmpty(), "no entry outlives its unregistered context");
	}

	@Test
	void aSweepFinishingAContextWhoseUnregisterWaitsForTheLockKeepsItsEntryForTheTerminalRow() throws Exception {
		final Thread testThread = Thread.currentThread();
		final java.util.concurrent.CountDownLatch entered = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
		final java.util.concurrent.atomic.AtomicBoolean armed = new java.util.concurrent.atomic.AtomicBoolean(true);
		final FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-race-waiting");
		// No progress in the final snapshot, so the terminal row comes from the cached entry.
		ctx.snapshot = new CountingSnapshot(0, 0, 0, 100.0, 110.0, new FakeConcurrencySnapshot(1), 2_000L) {
			@Override
			public RateMetricSnapshot successSnapshot() {
				// The sweep blocks here inside its record of the context, while it is still registered.
				if (Thread.currentThread() != testThread && armed.compareAndSet(true, false)) {
					entered.countDown();
					awaitQuietly(release);
				}
				return super.successSnapshot();
			}
		};
		mgr.register(progressContext("step-race-keeper", OpType.READ, 0, 0));
		mgr.register(ctx);
		assertTrue(entered.await(10, java.util.concurrent.TimeUnit.SECONDS), "the sweep reached the record");
		invokeRecordProgressSnapshot(ctx, progressSnapshot(6_000, 6_144_000));

		final Thread unregister = Thread.ofPlatform().start(() -> mgr.unregister(ctx));
		// The unregister has taken the context out of the set and waits for the sweep's lock.
		while (mgr.getAllContexts().stream().anyMatch(c -> c == ctx)) {
			Thread.onSpinWait();
		}
		release.countDown();
		unregister.join(10_000);

		final TerminalStepEntry terminal = mgr.getTerminalSteps().stream()
						.filter(e -> "step-race-waiting".equals(e.stepId))
						.findFirst()
						.orElseThrow(() -> new AssertionError("the cached entry became the terminal row"));
		assertEquals(6_000L, terminal.successCount);
		assertTrue(lastProgressEntries().isEmpty(), "unregister leaves no entry behind");
	}

	@Test
	void registerClearsTheContextsOwnEntry() throws Exception {
		// No progress in its own snapshot, so the manager's sweep records nothing after the register.
		final FakeMetricsContext<AllMetricsSnapshot> ctx = progressContext("step-own", OpType.READ, 0, 0);
		invokeRecordProgressSnapshot(ctx, progressSnapshot(5, 5_120));
		assertEquals(1, lastProgressEntries().size());

		mgr.register(ctx);

		assertTrue(lastProgressEntries().isEmpty(), "a context registers with no cached progress");
		assertTrue(mgr.getLastProgressSnapshot(ctx).isEmpty());
	}

	@Test
	void registerClearsAStaleEntryOfItsStepAndScope() throws Exception {
		// Left as a sweep that raced an unregister could leave it: the context is not registered.
		final FakeMetricsContext<AllMetricsSnapshot> stale = progressContext("step-reused", OpType.CREATE, 900, 921_600);
		invokeRecordProgressSnapshot(stale, stale.snapshot);
		final FakeMetricsContext<AllMetricsSnapshot> next = progressContext("step-reused", OpType.CREATE, 0, 0);

		mgr.register(next);

		assertTrue(lastProgressEntries().isEmpty(), "the stale entry of the step goes");
		invokeRecordProgressSnapshot(next, progressSnapshot(3, 3_072));
		assertEquals(3L, mgr.getLastProgressSnapshot("step-reused").orElseThrow().successCount);
		assertEquals(3L, mgr.getLastProgressSnapshot("step-reused", false).orElseThrow().successCount);
	}

	@Test
	void registerKeepsTheEntriesOfRegisteredSiblingsAndOfOtherScopesAndSteps() throws Exception {
		final FakeMetricsContext<AllMetricsSnapshot> read = progressContext("step-keep", OpType.READ, 1_000, 4_096_000);
		mgr.register(read);
		invokeRecordProgressSnapshot(read, read.snapshot);
		final FakeDistributedMetricsContext fleet = distributedProgressContext("step-keep", OpType.READ, 70, 71_680);
		invokeRecordProgressSnapshot(fleet, fleet.snapshot);
		final FakeMetricsContext<AllMetricsSnapshot> other = progressContext("step-other", OpType.READ, 8, 8_192);
		invokeRecordProgressSnapshot(other, other.snapshot);

		mgr.register(progressContext("step-keep", OpType.CREATE, 0, 0));

		assertEquals(3, lastProgressEntries().size(), "only the step's unregistered entries of the scope go");
		assertEquals(1_000L, mgr.getLastProgressSnapshot(read).orElseThrow().successCount);
		assertEquals(70L, mgr.getLastProgressSnapshot("step-keep", true).orElseThrow().successCount);
		assertEquals(8L, mgr.getLastProgressSnapshot("step-other").orElseThrow().successCount);
	}

	@Test
	void unregisteringAContextWithoutProgressRetainsNoSiblingsEntryAsItsTerminalRow() throws Exception {
		final FakeMetricsContext<AllMetricsSnapshot> read = progressContext("step-idle-sibling", OpType.READ, 1_000, 4_096_000);
		final FakeMetricsContext<AllMetricsSnapshot> create = progressContext("step-idle-sibling", OpType.CREATE, 0, 0);
		mgr.register(read);
		mgr.register(create);
		invokeRecordProgressSnapshot(read, read.snapshot);

		mgr.unregister(create);

		assertTrue(
						mgr.getTerminalSteps().stream().noneMatch(e -> "step-idle-sibling".equals(e.stepId)),
						"a context with no progress leaves no terminal row, least of all its sibling's");
		assertEquals(1_000L, mgr.getLastProgressSnapshot(read).orElseThrow().successCount, "the sibling keeps its entry");
	}

	@Test
	void stepLookupOfADistributedContextReturnsItsEntry() throws Exception {
		final FakeDistributedMetricsContext fleet = distributedProgressContext("step-fleet", OpType.READ, 70, 71_680);
		invokeRecordProgressSnapshot(fleet, fleet.snapshot);

		assertEquals(70L, mgr.getLastProgressSnapshot("step-fleet", true).orElseThrow().successCount);
		assertEquals(70L, mgr.getLastProgressSnapshot(fleet).orElseThrow().successCount);
		assertTrue(mgr.getLastProgressSnapshot("step-fleet").isEmpty(), "the fleet entry is not a node entry");
	}

	@Test
	void stepLookupOfTwoDistributedContextsOfAStepReturnsNoEntry() throws Exception {
		final FakeDistributedMetricsContext read = distributedProgressContext("step-fleets", OpType.READ, 70, 71_680);
		final FakeDistributedMetricsContext create = distributedProgressContext("step-fleets", OpType.CREATE, 6, 6_144);
		invokeRecordProgressSnapshot(read, read.snapshot);
		invokeRecordProgressSnapshot(create, create.snapshot);

		assertTrue(mgr.getLastProgressSnapshot("step-fleets", true).isEmpty(), "no single fleet entry stands for the step");
		assertEquals(70L, mgr.getLastProgressSnapshot(read).orElseThrow().successCount);
		assertEquals(6L, mgr.getLastProgressSnapshot(create).orElseThrow().successCount);
	}

	@Test
	void stepLookupOfANodeAndAFleetContextOfAStepReturnsEachScopesEntry() throws Exception {
		final FakeMetricsContext<AllMetricsSnapshot> node = progressContext("step-scopes", OpType.READ, 35, 35_840);
		final FakeDistributedMetricsContext fleet = distributedProgressContext("step-scopes", OpType.READ, 70, 71_680);
		invokeRecordProgressSnapshot(node, node.snapshot);
		invokeRecordProgressSnapshot(fleet, fleet.snapshot);

		assertEquals(35L, mgr.getLastProgressSnapshot("step-scopes").orElseThrow().successCount);
		assertEquals(35L, mgr.getLastProgressSnapshot("step-scopes", false).orElseThrow().successCount);
		assertEquals(70L, mgr.getLastProgressSnapshot("step-scopes", true).orElseThrow().successCount);
		assertEquals(35L, mgr.getLastProgressSnapshot(node).orElseThrow().successCount);
		assertEquals(70L, mgr.getLastProgressSnapshot(fleet).orElseThrow().successCount);
	}

	private static AllMetricsSnapshot progressSnapshot(final long successCount, final long bytes) {
		return new CountingSnapshot(successCount, 0, bytes, 100.0, 110.0, new FakeConcurrencySnapshot(1), 2_000L);
	}

	private static FakeDistributedMetricsContext distributedProgressContext(
					final String stepId, final OpType opType, final long successCount, final long bytes) {
		final FakeDistributedMetricsContext ctx = new FakeDistributedMetricsContext(stepId);
		ctx.opType = opType;
		ctx.snapshot = new DistributedCountingSnapshot(successCount, bytes);
		return ctx;
	}

	/** A fleet snapshot with counts, as a distributed context publishes. */
	private static final class DistributedCountingSnapshot extends CountingSnapshot implements DistributedAllMetricsSnapshot {
		DistributedCountingSnapshot(final long successCount, final long bytes) {
			super(successCount, 0, bytes, 100.0, 110.0, new FakeConcurrencySnapshot(1), 2_000L);
		}

		@Override
		public int nodeCount() {
			return 1;
		}
	}

	private static void awaitQuietly(final java.util.concurrent.CountDownLatch latch) {
		boolean interrupted = false;
		try {
			while (true) {
				try {
					if (latch.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
						return;
					}
					throw new IllegalStateException("never released");
				} catch (final InterruptedException e) {
					interrupted = true;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	/** Waits for the sweep in progress to release the output lock. */
	private void awaitSweep() throws Exception {
		final java.lang.reflect.Field lockField = MetricsManagerImpl.class.getDeclaredField("outputLock");
		lockField.setAccessible(true);
		final java.util.concurrent.locks.ReentrantLock lock = (java.util.concurrent.locks.ReentrantLock) lockField.get(mgr);
		assertTrue(lock.tryLock(10, java.util.concurrent.TimeUnit.SECONDS), "the sweep finished");
		lock.unlock();
	}

	private static FakeMetricsContext<AllMetricsSnapshot> progressContext(
					final String stepId, final OpType opType, final long successCount, final long bytes) {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new OrderedFakeMetricsContext(stepId);
		ctx.opType = opType;
		ctx.snapshot = new CountingSnapshot(successCount, 0, bytes, 100.0, 110.0, new FakeConcurrencySnapshot(1), 2_000L);
		return ctx;
	}

	/** Orders apart from the other contexts of its step, as the manager's set of contexts needs. */
	private static final class OrderedFakeMetricsContext extends FakeMetricsContext<AllMetricsSnapshot> {
		private static final java.util.concurrent.atomic.AtomicInteger SEQUENCE = new java.util.concurrent.atomic.AtomicInteger();
		private final int order = SEQUENCE.incrementAndGet();

		OrderedFakeMetricsContext(String id) {
			super(id);
		}

		@Override
		public int compareTo(MetricsContext<AllMetricsSnapshot> o) {
			final int byStep = super.compareTo(o);
			if (byStep != 0 || !(o instanceof OrderedFakeMetricsContext other)) {
				return byStep;
			}
			return Integer.compare(order, other.order);
		}
	}

	/** Compares equal to every other instance, as two contexts started in the same nanosecond tick could. */
	private static final class EqualFakeMetricsContext extends FakeMetricsContext<AllMetricsSnapshot> {
		EqualFakeMetricsContext(String id) {
			super(id);
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof EqualFakeMetricsContext;
		}

		@Override
		public int hashCode() {
			return 42;
		}
	}

	private void invokeRecordProgressSnapshot(MetricsContext<?> ctx, AllMetricsSnapshot snapshot) throws Exception {
		Method record = MetricsManagerImpl.class.getDeclaredMethod("recordProgressSnapshot", MetricsContext.class, AllMetricsSnapshot.class);
		record.setAccessible(true);
		record.invoke(mgr, ctx, snapshot);
	}

	@SuppressWarnings("unchecked")
	private Collection<TerminalStepEntry> lastProgressEntries() throws Exception {
		java.lang.reflect.Field lastProgressField = MetricsManagerImpl.class.getDeclaredField("lastProgressByContext");
		lastProgressField.setAccessible(true);
		Map<?, TerminalStepEntry> map = (Map<?, TerminalStepEntry>) lastProgressField.get(mgr);
		return new ArrayList<>(map.values());
	}
}
