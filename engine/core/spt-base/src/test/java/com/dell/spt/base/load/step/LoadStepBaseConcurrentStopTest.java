package com.dell.spt.base.load.step;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.xml.parsers.DocumentBuilderFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * A signal stops a step from two threads: the shutdown hook through {@code close()}, and the scenario
 * thread through {@code doStop()} from {@code await}'s {@code finally}. Each test holds the first
 * caller inside its unregister loop and lets the second run into it.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class LoadStepBaseConcurrentStopTest {

	private static final long WAIT_SECONDS = 10;
	private static final String SCENARIO_THREAD = "scenario";
	private static final List<OpType> MIXED_OPS = List.of(OpType.READ, OpType.STAT, OpType.CREATE, OpType.DELETE);

	@Test
	void aSecondStopWaitsUntilTheFirstHasUnregisteredEveryContext() throws Exception {
		final MetricsManager manager = mock(MetricsManager.class);
		final MetricsContext<?> first = mock(MetricsContext.class);
		final MetricsContext<?> second = mock(MetricsContext.class);
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		final AtomicBoolean firstCall = new AtomicBoolean(true);
		final List<String> calls = new CopyOnWriteArrayList<>();
		doAnswer(invocation -> {
			final String context = invocation.getArgument(0) == first ? "first" : "second";
			calls.add(Thread.currentThread().getName() + ":" + context);
			if (firstCall.compareAndSet(true, false)) {
				entered.countDown();
				assertTrue(release.await(WAIT_SECONDS, TimeUnit.SECONDS), "the test never released the first stop");
			}
			return null;
		}).when(manager).unregister(any());
		final TestStep step = new TestStep(config("concurrent-stop-order"), manager, List.of(first, second));

		final Caller scenario = new Caller(SCENARIO_THREAD, step::doStop);
		final Caller hook;
		try {
			assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
			hook = new Caller("hook", step::doStop);
			assertTrue(hook.reachesTheStopLock(), "the second stop returned or went on while the first was unregistering");
			assertEquals(List.of("scenario:first"), calls);
		} finally {
			release.countDown();
		}

		scenario.assertFinished();
		hook.assertFinished();
		assertEquals(List.of("scenario:first", "scenario:second", "hook:first", "hook:second"), calls);
	}

	@Test
	void closeWaitsForARunningUnregisterBeforeClosingTheOnlyContext() throws Exception {
		final MetricsManager manager = mock(MetricsManager.class);
		final MetricsContext<?> context = mock(MetricsContext.class);
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		final AtomicBoolean firstCall = new AtomicBoolean(true);
		doAnswer(invocation -> {
			if (firstCall.compareAndSet(true, false)) {
				entered.countDown();
				assertTrue(release.await(WAIT_SECONDS, TimeUnit.SECONDS), "the test never released the first stop");
			}
			return null;
		}).when(manager).unregister(context);
		final TestStep step = new TestStep(config("concurrent-stop-close"), manager, List.of(context));
		step.start();

		final Caller scenario = new Caller(SCENARIO_THREAD, step::doStop);
		final Caller hook;
		try {
			assertTrue(entered.await(WAIT_SECONDS, TimeUnit.SECONDS));
			hook = new Caller("hook", step::close);
			assertTrue(hook.reachesTheStopLock(), "close went on while the context was still being unregistered");
			verify(context, never()).close();
		} finally {
			release.countDown();
		}

		scenario.assertFinished();
		hook.assertFinished();
		final var order = inOrder(manager, context);
		order.verify(manager, times(2)).unregister(context);
		order.verify(context).close();
	}

	@Test
	void twoConcurrentStopsWriteEachResultFileOnce(@TempDir final Path home) throws Exception {
		runConcurrentStop("concurrent-stop-files", MIXED_OPS, home, false);
	}

	@Test
	void closeDuringAStopKeepsEveryRow(@TempDir final Path home) throws Exception {
		runConcurrentStop("concurrent-stop-close-rows", MIXED_OPS, home, true);
	}

	@Test
	void closeDuringAStopKeepsTheOnlyRowOfAOneContextStep(@TempDir final Path home) throws Exception {
		runConcurrentStop("concurrent-stop-one-context", List.of(OpType.READ), home, true);
	}

	/**
	 * Runs a real metrics manager. The scenario thread is held after it has claimed the first context
	 * and before it has written that context's output, which is the window the signal hits.
	 */
	private static void runConcurrentStop(
					final String stepId,
					final List<OpType> ops,
					final Path home,
					final boolean hookCloses)
					throws Exception {
		final Capture total = new Capture(Loggers.METRICS_FILE_TOTAL, stepId);
		final Capture steady = new Capture(Loggers.METRICS_STEADY_FILE_TOTAL, stepId);
		final Capture xml = new Capture(Loggers.METRICS_EXT_RESULTS_FILE, stepId);
		final MetricsManagerImpl manager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final CountDownLatch claimed = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		ThreadContext.put(KEY_HOME_DIR, home.toString());
		try {
			final List<DistributedMetricsContext> contexts = new ArrayList<>();
			for (final OpType op : ops) {
				contexts.add(context(stepId, op, contexts.isEmpty() ? claimed : null, release));
			}
			orderBySequence(contexts);
			final TestStep step = new TestStep(config(stepId), manager, List.copyOf(contexts));
			step.start();

			final Caller scenario = new Caller(SCENARIO_THREAD, home, step::doStop);
			final Caller hook;
			try {
				assertTrue(claimed.await(WAIT_SECONDS, TimeUnit.SECONDS));
				hook = new Caller("hook", home, hookCloses ? step::close : step::doStop);
				// Without the step's lock the hook runs to its end here, ahead of the held context.
				hook.awaitStopLockOrExit();
			} finally {
				release.countDown();
			}
			scenario.assertFinished();
			hook.assertFinished();

			assertCsv("metrics.total.csv", total.await(ops.size()), ops);
			assertCsv("metrics.steady.total.csv", steady.await(ops.size()), ops);
			assertResultXml(xml.await(ops.size() + 2), stepId, ops.size());
		} finally {
			release.countDown();
			ThreadContext.remove(KEY_HOME_DIR);
			total.close();
			steady.close();
			xml.close();
			manager.close();
		}
	}

	private static void assertCsv(final String file, final List<String> messages, final List<OpType> ops)
					throws IOException {
		final String csv = String.join(System.lineSeparator(), messages);
		final List<CSVRecord> records;
		try (final CSVParser parser = CSVFormat.DEFAULT.parse(new StringReader(csv))) {
			records = parser.getRecords();
		}
		assertFalse(records.isEmpty(), file + " has no lines");
		final CSVRecord header = records.get(0);
		assertEquals("DateTimeISO8601", header.get(0), file + " does not start with its header: " + csv);
		assertEquals(1L, records.stream().filter(record -> "DateTimeISO8601".equals(record.get(0))).count(), file + ": " + csv);
		final int opColumn = header.toList().indexOf("OpType");
		final List<String> rows = records.stream().skip(1).map(record -> record.get(opColumn)).sorted().toList();
		assertEquals(ops.stream().map(OpType::name).sorted().toList(), rows, file + ": " + csv);
	}

	private static void assertResultXml(final List<String> messages, final String stepId, final int entries)
					throws Exception {
		final String xml = String.join("\n", messages);
		final var document = DocumentBuilderFactory.newInstance()
						.newDocumentBuilder()
						.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
		final Element root = document.getDocumentElement();
		assertEquals("result", root.getTagName(), xml);
		int found = 0;
		for (Node node = root.getFirstChild(); node != null; node = node.getNextSibling()) {
			if (node instanceof Element entry) {
				assertEquals(stepId, entry.getAttribute("id"), xml);
				found++;
			}
		}
		assertEquals(entries, found, xml);
	}

	@SuppressWarnings("unchecked")
	private static DistributedMetricsContext context(
					final String stepId,
					final OpType op,
					final CountDownLatch claimed,
					final CountDownLatch release) {
		final DistributedMetricsContext ctx = mock(DistributedMetricsContext.class);
		final AtomicReference<AllMetricsSnapshot> snapshot = new AtomicReference<>(snapshot());
		final AtomicBoolean firstRead = new AtomicBoolean(true);
		when(ctx.lastSnapshot()).thenAnswer(invocation -> {
			// The manager's periodic sweep reads the snapshot too: only the stop is held.
			if (claimed != null
							&& SCENARIO_THREAD.equals(Thread.currentThread().getName())
							&& firstRead.compareAndSet(true, false)) {
				claimed.countDown();
				assertTrue(release.await(WAIT_SECONDS, TimeUnit.SECONDS), "the test never released the first stop");
			}
			return snapshot.get();
		});
		doAnswer(invocation -> {
			snapshot.set(null);
			return null;
		}).when(ctx).close();
		when(ctx.terminalSnapshot()).thenCallRealMethod();
		when(ctx.loadStepId()).thenReturn(stepId);
		when(ctx.opType()).thenReturn(op);
		when(ctx.quantileValues()).thenReturn(List.of(0.5, 0.9, 0.99, 0.999));
		when(ctx.nodeCount()).thenReturn(1);
		when(ctx.metadata()).thenReturn(Map.of(MetricsConstants.METADATA_LIMIT_OP_COUNT, 0L));
		when(ctx.concurrencyLimit()).thenReturn(4);
		when(ctx.itemDataSize()).thenReturn(new SizeInBytes(4_096L));
		when(ctx.startTimeStamp()).thenReturn(System.currentTimeMillis());
		when(ctx.nodeAddrs()).thenReturn(List.of("127.0.0.1:1099"));
		when(ctx.comment()).thenReturn("");
		when(ctx.runId()).thenReturn(1L);
		when(ctx.sumPersistEnabled()).thenReturn(true);
		return ctx;
	}

	@SuppressWarnings("unchecked")
	private static void orderBySequence(final List<DistributedMetricsContext> contexts) {
		final IdentityHashMap<DistributedMetricsContext, Integer> order = new IdentityHashMap<>();
		for (int i = 0; i < contexts.size(); i++) {
			order.put(contexts.get(i), i);
		}
		for (final DistributedMetricsContext ctx : contexts) {
			when(ctx.compareTo(any())).thenAnswer(
							invocation -> Integer.compare(order.get(ctx), order.get(invocation.getArgument(0))));
		}
	}

	private static DistributedAllMetricsSnapshotImpl snapshot() {
		final long elapsedMillis = 1_000;
		return new DistributedAllMetricsSnapshotImpl(
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_DUR, List.of(1_000L, 2_000L)),
						TimingMetricSnapshotImpl.fromSamples(MetricsConstants.METRIC_NAME_LAT, List.of(500L, 900L)),
						new TimingMetricSnapshotImpl(0, 0, 0, 0, 0.0, MetricsConstants.METRIC_NAME_TTFB),
						new ConcurrencyMetricSnapshotImpl(MetricsConstants.METRIC_NAME_CONC, 4, 4.0),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_FAIL, 0, elapsedMillis),
						new RateMetricSnapshotImpl(0.0, 0.0, MetricsConstants.METRIC_NAME_CORRUPT, 0, elapsedMillis),
						new RateMetricSnapshotImpl(5.0, 5.0, MetricsConstants.METRIC_NAME_SUCC, 5, elapsedMillis),
						new RateMetricSnapshotImpl(20.0, 20.0, MetricsConstants.METRIC_NAME_BYTE, 20, elapsedMillis),
						1,
						elapsedMillis,
						null,
						SteadyResult.incomplete("the step was stopped"));
	}

	private static Config config(final String stepId) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("run-id", 123L);
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "none");
		return config;
	}

	/** One stopping thread, and what it is doing as seen from the test. */
	private static final class Caller {
		private final Thread thread;
		private final AtomicReference<Throwable> failure = new AtomicReference<>();

		Caller(final String name, final Action action) {
			this(name, null, action);
		}

		Caller(final String name, final Path home, final Action action) {
			thread = new Thread(() -> {
				if (home != null) {
					ThreadContext.put(KEY_HOME_DIR, home.toString());
				}
				try {
					action.run();
				} catch (final Throwable cause) {
					failure.set(cause);
				} finally {
					ThreadContext.remove(KEY_HOME_DIR);
				}
			}, name);
			thread.setDaemon(true);
			thread.start();
		}

		boolean reachesTheStopLock() throws InterruptedException {
			awaitStopLockOrExit();
			return waitsInDoStop();
		}

		void awaitStopLockOrExit() throws InterruptedException {
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_SECONDS);
			while (thread.isAlive() && !waitsInDoStop()) {
				assertTrue(System.nanoTime() < deadline, () -> thread.getName() + " neither waited nor ended");
				Thread.sleep(1);
			}
		}

		/** Parked without a timeout directly under {@code doStop}, so not in anything it calls. */
		private boolean waitsInDoStop() {
			if (thread.getState() != Thread.State.WAITING) {
				return false;
			}
			for (final StackTraceElement frame : thread.getStackTrace()) {
				final String type = frame.getClassName();
				if (!type.startsWith("java.") && !type.startsWith("jdk.")) {
					return LoadStepBase.class.getName().equals(type) && "doStop".equals(frame.getMethodName());
				}
			}
			return false;
		}

		void assertFinished() throws InterruptedException {
			thread.join(TimeUnit.SECONDS.toMillis(WAIT_SECONDS));
			assertFalse(thread.isAlive(), () -> thread.getName() + " did not finish after the first stop ended");
			assertNull(failure.get(), () -> thread.getName() + " failed: " + failure.get());
		}
	}

	@FunctionalInterface
	private interface Action {
		void run() throws Exception;
	}

	/** Collects what one logger writes for the step. */
	private static final class Capture extends AbstractAppender implements AutoCloseable {
		private final org.apache.logging.log4j.core.Logger logger;
		private final String stepId;
		private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

		Capture(final Logger source, final String stepId) {
			super("concurrentStopCapture-" + source.getName(), null, null, true, Property.EMPTY_ARRAY);
			this.stepId = stepId;
			logger = LoggerContext.getContext(false).getLogger(source.getName());
			start();
			logger.addAppender(this);
		}

		@Override
		public void append(final LogEvent event) {
			if (stepId.equals(event.getContextData().getValue(KEY_STEP_ID))) {
				messages.add(event.getMessage().getFormattedMessage());
			}
		}

		/** What arrived once the expected count is there, or all there is when it never comes. */
		List<String> await(final int expected) throws InterruptedException {
			for (int attempt = 0; attempt < 40; attempt++) {
				LogUtil.flushAll();
				if (messages.size() >= expected) {
					break;
				}
				Thread.sleep(50);
			}
			LogUtil.flushAll();
			synchronized (messages) {
				return List.copyOf(messages);
			}
		}

		@Override
		public void close() {
			logger.removeAppender(this);
			stop();
		}
	}

	private static final class TestStep extends LoadStepBase {

		@SuppressWarnings({"rawtypes", "unchecked"
		})
		TestStep(final Config config, final MetricsManager manager, final List<? extends MetricsContext> contexts) {
			super(config, Collections.emptyList(), Collections.emptyList(), manager);
			metricsContexts.addAll((List) contexts);
		}

		@Override
		protected void doStartWrapped() {}

		@Override
		protected void init() {}

		@Override
		protected void initMetrics(
						final int originIndex,
						final OpType opType,
						final int concurrency,
						final Config metricsConfig,
						final SizeInBytes itemDataSize,
						final boolean outputColorFlag) {}

		@Override
		public boolean await(final long timeout, final TimeUnit timeUnit) {
			return true;
		}

		@Override
		public String getTypeName() {
			return "test";
		}
	}
}
