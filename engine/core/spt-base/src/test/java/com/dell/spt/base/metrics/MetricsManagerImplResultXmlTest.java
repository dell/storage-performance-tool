package com.dell.spt.base.metrics;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that {@link MetricsManagerImpl} emits well-formed XML wrapper tags
 * ({@code <result>} and {@code </result>}) around the {@code ExtResultsXmlLogMessage}
 * entries written to the result.xml logger.
 *
 * <p>Uses an in-memory appender attached to {@link Loggers#METRICS_EXT_RESULTS_FILE}
 * to capture log events without file I/O.
 */
public class MetricsManagerImplResultXmlTest {

	private MetricsManagerImpl mgr;
	private CapturingAppender appender;

	@BeforeEach
	void setUp() {
		mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		appender = new CapturingAppender();
		appender.start();
		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.METRICS_EXT_RESULTS_FILE.getName());
		logger.addAppender(appender);
	}

	@AfterEach
	void tearDown() throws Exception {
		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.METRICS_EXT_RESULTS_FILE.getName());
		logger.removeAppender(appender);
		appender.stop();
		mgr.close();
	}

	/**
	 * A single distributed context produces: {@code <result>}, entry, {@code </result>}.
	 */
	@Test
	void singleContextProducesWrappedXml() throws Exception {
		var ctx = distributedContext("step-single", OpType.CREATE);
		ctx.sumPersist = true;
		ctx.snapshot = nonZeroDistributedSnapshot();

		mgr.register(ctx);
		mgr.unregister(ctx);
		Thread.sleep(200);

		List<String> messages = appender.messages();
		assertEquals(3, messages.size(),
						"Expected <result>, entry, </result>; got: " + messages);
		assertEquals("<result>", messages.get(0));
		assertTrue(messages.get(1).startsWith("<result id=\"step-single\""),
						"Entry should be a self-closing <result .../> element");
		assertTrue(messages.get(1).endsWith("/>"));
		assertEquals("</result>", messages.get(2));
	}

	/**
	 * Multiple distributed contexts for the same step (mixed workload) produce
	 * one opening tag, N entries, one closing tag.
	 */
	@Test
	void multipleContextsSameStepProducesSingleWrapper() throws Exception {
		OpType[] ops = {OpType.CREATE, OpType.READ, OpType.DELETE, OpType.LIST
		};
		var contexts = new FakeDistributedContext[ops.length];
		for (int i = 0; i < ops.length; i++) {
			contexts[i] = distributedContext("mixed-step", ops[i]);
			contexts[i].sumPersist = true;
			contexts[i].snapshot = nonZeroDistributedSnapshot();
			mgr.register(contexts[i]);
		}

		// Unregister all (sequential, like LoadStepBase.doStop)
		for (var ctx : contexts) {
			mgr.unregister(ctx);
		}
		Thread.sleep(200);

		List<String> messages = appender.messages();
		// 1 open + 4 entries + 1 close = 6
		assertEquals(6, messages.size(),
						"Expected 1 open + 4 entries + 1 close; got: " + messages);
		assertEquals("<result>", messages.get(0));
		for (int i = 1; i <= 4; i++) {
			assertTrue(messages.get(i).startsWith("<result id=\"mixed-step\""),
							"Entry " + i + " should be a result element");
		}
		assertEquals("</result>", messages.get(5));
	}

	/**
	 * Two sequential steps each get their own wrapper.
	 */
	@Test
	void sequentialStepsGetSeparateWrappers() throws Exception {
		for (String stepId : List.of("step-A", "step-B")) {
			var ctx = distributedContext(stepId, OpType.READ);
			ctx.sumPersist = true;
			ctx.snapshot = nonZeroDistributedSnapshot();
			mgr.register(ctx);
			mgr.unregister(ctx);
			Thread.sleep(200);
		}

		List<String> messages = appender.messages();
		// step-A: <result>, entry, </result>  then  step-B: <result>, entry, </result>
		assertEquals(6, messages.size(),
						"Expected 2 × (open + entry + close); got: " + messages);
		assertEquals("<result>", messages.get(0));
		assertTrue(messages.get(1).contains("step-A"));
		assertEquals("</result>", messages.get(2));
		assertEquals("<result>", messages.get(3));
		assertTrue(messages.get(4).contains("step-B"));
		assertEquals("</result>", messages.get(5));
	}

	/**
	 * When sumPersist is false, no result XML is emitted at all (no wrapper, no entry).
	 */
	@Test
	void sumPersistDisabledProducesNoOutput() throws Exception {
		var ctx = distributedContext("step-quiet", OpType.READ);
		ctx.sumPersist = false;
		ctx.snapshot = nonZeroDistributedSnapshot();

		mgr.register(ctx);
		mgr.unregister(ctx);
		Thread.sleep(200);

		List<String> messages = appender.messages();
		assertTrue(messages.isEmpty(),
						"No result XML should be emitted when sumPersist is disabled; got: " + messages);
	}

	// --- In-memory appender ---

	private static class CapturingAppender extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());

		CapturingAppender() {
			super("testResultXmlCapture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(LogEvent event) {
			captured.add(event.getMessage().getFormattedMessage());
		}

		List<String> messages() {
			return List.copyOf(captured);
		}
	}

	// --- Helpers ---

	/** Unique-per-instance counter so ConcurrentSkipListSet treats each context as distinct. */
	private static final AtomicLong SEQ = new AtomicLong();

	private static FakeDistributedContext distributedContext(String stepId, OpType op) {
		var ctx = new FakeDistributedContext(stepId, SEQ.incrementAndGet());
		ctx.opType = op;
		ctx.concurrencyLimit = 4;
		ctx.size = new SizeInBytes(1024);
		return ctx;
	}

	private static FakeDistSnapshot nonZeroDistributedSnapshot() {
		return new FakeDistSnapshot(
						new StubRate("success", 100, 50.0, 50.0),
						new StubRate("fails", 0, 0.0, 0.0),
						new StubRate("byte", 102400, 1024.0, 1024.0),
						new StubTiming("latency", 100, 500.0, 50000, 100, 900),
						new StubTiming("duration", 100, 600.0, 60000, 200, 1000),
						new FakeConcurrency(4),
						5000L);
	}

	// --- Test fakes (self-contained; uses seq for unique compareTo) ---

	private static class FakeDistributedContext implements DistributedMetricsContext<DistributedAllMetricsSnapshot> {
		final String id;
		final long seq;
		OpType opType = OpType.READ;
		int concurrencyLimit = 1;
		SizeInBytes size = new SizeInBytes(0);
		long startTs = System.currentTimeMillis();
		boolean sumPersist = false;
		DistributedAllMetricsSnapshot snapshot = null;
		final Map<String, Object> metadata = new HashMap<>();

		FakeDistributedContext(String id, long seq) {
			this.id = id;
			this.seq = seq;
		}

		@Override
		public String loadStepId() {
			return id;
		}

		@Override
		public long runId() {
			return 1;
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
		public void markSucc(long b, long d, long l) {}

		@Override
		public void markPartSucc(long b, long d, long l) {}

		@Override
		public void markSucc(long c, long b, long[] d, long[] l) {}

		@Override
		public void markPartSucc(long b, long[] d, long[] l) {}

		@Override
		public void markFail() {}

		@Override
		public void markFail(long c) {}

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
		public void refreshLastSnapshot() {}

		@Override
		public DistributedAllMetricsSnapshot lastSnapshot() {
			return snapshot;
		}

		@Override
		public int concurrencyThreshold() {
			return 0;
		}

		@Override
		public boolean thresholdStateEntered() {
			return false;
		}

		@Override
		public void enterThresholdState() {}

		@Override
		public boolean thresholdStateExited() {
			return false;
		}

		@Override
		public MetricsContext thresholdMetrics() {
			return this;
		}

		@Override
		public void exitThresholdState() {}

		@Override
		public boolean stdOutColorEnabled() {
			return false;
		}

		@Override
		public boolean avgPersistEnabled() {
			return false;
		}

		@Override
		public boolean sumPersistEnabled() {
			return sumPersist;
		}

		@Override
		public boolean timingPersistEnabled() {
			return false;
		}

		@Override
		public long outputPeriodMillis() {
			return 0;
		}

		@Override
		public long lastOutputTs() {
			return 0;
		}

		@Override
		public void lastOutputTs(long ts) {}

		@Override
		public long elapsedTimeMillis() {
			return 0;
		}

		@Override
		public String comment() {
			return "";
		}

		@Override
		public void close() {}

		@Override
		public Map metadata() {
			return metadata;
		}

		// DistributedMetricsContext
		@Override
		public int nodeCount() {
			return 1;
		}

		@Override
		public List<String> nodeAddrs() {
			return List.of("127.0.0.1");
		}

		@Override
		public List<Double> quantileValues() {
			return List.of(0.5, 0.9);
		}

		@Override
		public int compareTo(MetricsContext o) {
			if (o == null)
				return 1;
			if (o instanceof FakeDistributedContext other) {
				return Long.compare(this.seq, other.seq);
			}
			return this.loadStepId().compareTo(o.loadStepId());
		}
	}

	private static class FakeDistSnapshot implements DistributedAllMetricsSnapshot {
		final RateMetricSnapshot succ, fail, bytes;
		final TimingMetricSnapshot lat, dur;
		final ConcurrencyMetricSnapshot conc;
		final long elapsed;

		FakeDistSnapshot(RateMetricSnapshot succ, RateMetricSnapshot fail, RateMetricSnapshot bytes,
						TimingMetricSnapshot lat, TimingMetricSnapshot dur,
						ConcurrencyMetricSnapshot conc, long elapsed) {
			this.succ = succ;
			this.fail = fail;
			this.bytes = bytes;
			this.lat = lat;
			this.dur = dur;
			this.conc = conc;
			this.elapsed = elapsed;
		}

		@Override
		public ConcurrencyMetricSnapshot concurrencySnapshot() {
			return conc;
		}

		@Override
		public long elapsedTimeMillis() {
			return elapsed;
		}

		@Override
		public TimingMetricSnapshot durationSnapshot() {
			return dur;
		}

		@Override
		public TimingMetricSnapshot latencySnapshot() {
			return lat;
		}

		@Override
		public RateMetricSnapshot byteSnapshot() {
			return bytes;
		}

		@Override
		public RateMetricSnapshot successSnapshot() {
			return succ;
		}

		@Override
		public RateMetricSnapshot failsSnapshot() {
			return fail;
		}

		@Override
		public int nodeCount() {
			return 1;
		}
	}

	private record StubRate(String name, long count, double mean, double last) implements RateMetricSnapshot {

	@Override
	public long elapsedTimeMillis() {
		return 0;
	}

	}

	private record StubTiming(String name, long count, double mean, long sum, long min,
							  long max) implements TimingMetricSnapshot {}

	private record FakeConcurrency(long last) implements ConcurrencyMetricSnapshot {

	@Override
	public double mean() {
		return last;
	}

	@Override
	public String name() {
		return "concurrency";
	}
}}
