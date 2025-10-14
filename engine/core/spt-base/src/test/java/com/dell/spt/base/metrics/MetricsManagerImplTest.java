package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
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
		mgr = new MetricsManagerImpl(ServiceTaskExecutor.INSTANCE);
	}

	@AfterEach
	void tearDown() throws Exception {
		// Ensure manager is closed between tests
		mgr.close();
	}

	@Test
	void registerNonDistributedDoesNotExposeDistributedContext() {
		FakeMetricsContext<AllMetricsSnapshot> ctx = new FakeMetricsContext<>("step-A");
		mgr.register(ctx);

		// Manager should have started
		assertTrue(mgr.isStarted(), "Metrics manager fiber should be started after first register");

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

		// Avoid starting the internal fiber thread to prevent lock races: inject the context directly.
		java.lang.reflect.Field allMetricsField = MetricsManagerImpl.class.getDeclaredField("allMetrics");
		allMetricsField.setAccessible(true);
		@SuppressWarnings("unchecked")
		final java.util.Set<MetricsContext> allMetrics = (java.util.Set<MetricsContext>) allMetricsField.get(mgr);
		allMetrics.add(ctx);

		// Invoke the protected tick method reflectively
		Method tick = MetricsManagerImpl.class.getDeclaredMethod("invokeTimedExclusively", long.class);
		tick.setAccessible(true);
		tick.invoke(mgr, System.nanoTime());

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

	// --- Test fakes ---

	private static class FakeMetricsContext<S extends AllMetricsSnapshot> implements MetricsContext<S> {
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

		FakeMetricsContext(String id) {
			this.id = id;
		}

		@Override
		public Map metadata() {
			return Collections.emptyMap();
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
		public void markPartSucc(long bytes, long duration, long latency) {}

		@Override
		public void markSucc(long count, long bytes, long[] durationValues, long[] latencyValues) {}

		@Override
		public void markPartSucc(long bytes, long[] durationValues, long[] latencyValues) {}

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

		StubRate(String name) {
			this.name = name;
		}

		@Override
		public long count() {
			return 0L;
		}

		@Override
		public double last() {
			return 0.0;
		}

		@Override
		public long elapsedTimeMillis() {
			return 0L;
		}

		@Override
		public double mean() {
			return 0.0;
		}

		@Override
		public String name() {
			return name;
		}
	}

	private static class StubTiming implements com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot {
		private final String name;

		StubTiming(String name) {
			this.name = name;
		}

		@Override
		public long count() {
			return 0L;
		}

		@Override
		public String name() {
			return name;
		}

		@Override
		public double mean() {
			return 0.0;
		}

		@Override
		public long sum() {
			return 0L;
		}

		@Override
		public long min() {
			return 0L;
		}

		@Override
		public long max() {
			return 0L;
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
}
