package com.dell.spt.storage.driver.coop.nio.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * An {@code fs} read split by the size threshold reads the whole file in the parent's own pass, then
 * reads its ranges. The parent's read is a real one, so the step keeps its timing sample next to its
 * ranges', with or without a first byte: the driver never marks it requestless.
 */
final class FsSplitReadTimingStepTest {

	private static final int OBJECTS = 3;
	private static final long OBJECT_SIZE = 1024 * 1024;
	private static final long THRESHOLD = 512 * 1024;
	private static final long WAIT_SECONDS = 60;

	private Path dir;
	private Config config;
	private Config loadConfig;
	private DataInput dataInput;
	private MetricsContext<AllMetricsSnapshot> metrics;

	@BeforeEach
	void setUp() throws Exception {
		dir = Files.createTempDirectory("fs-split-read-");
		config = TestConfigBuilder.config();
		config.val("load-batch-size", 16);
		config.val("storage-driver-limit-concurrency", 4);
		config.val("storage-driver-threads", 0);
		config.val("storage-driver-limit-queue-input", 128);
		// the fs tests' config builder holds the driver's settings only; the step's come from the defaults
		loadConfig = new BundledDefaultsProvider().config("-", InitialConfigSchemaProvider.provider().schema());
		loadConfig.val("load-op-retry", false);
		loadConfig.val("load-op-recycle-mode", false);
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
		metrics = MetricsContextImpl.builder()
						.loadStepId("fs-split-read-timing")
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(OBJECT_SIZE))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		metrics.start();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (metrics != null) {
			metrics.close();
		}
		if (dataInput != null) {
			dataInput.close();
		}
		try (var paths = Files.walk(dir)) {
			for (final var path : paths.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList()) {
				Files.deleteIfExists(path);
			}
		}
	}

	@Test
	void aSplitReadThatReadsTheFileItselfKeepsItsSample() throws Exception {
		assertParentAndRangeSamples(false, OBJECT_SIZE);
	}

	@Test
	void aSplitReadThatReadsTheFileItselfKeepsItsSampleWithBatchedResults() throws Exception {
		assertParentAndRangeSamples(true, OBJECT_SIZE);
	}

	/** A file listed at its size and found empty: the parent's read reaches the end at once and succeeds. */
	@Test
	void aSplitReadOfAnEmptyFileKeepsItsSample() throws Exception {
		assertParentAndRangeSamples(false, 0);
	}

	@Test
	void aSplitReadOfAnEmptyFileKeepsItsSampleWithBatchedResults() throws Exception {
		assertParentAndRangeSamples(true, 0);
	}

	private void assertParentAndRangeSamples(final boolean batch, final long fileSize) throws Exception {
		for (var object = 0; object < OBJECTS; object++) {
			Files.write(dir.resolve(name(object)), new byte[(int) fileSize]);
		}
		final var samples = new Samples();
		try (var driver = new FileStorageDriver<DataItemImpl, Operation<DataItemImpl>>(
						"test-fs", dataInput, config.configVal("storage"), false, config.intVal("load-batch-size"))) {
			final var context = new LoadStepContextImpl<>(
							"fs-split-read-timing", inertGenerator(), driver, metrics, loadConfig.configVal("load"), false);
			final var output = new StepOutput(context, batch, samples);
			driver.operationResultOutput(output);
			driver.start();
			for (var object = 0; object < OBJECTS; object++) {
				final var read = new CompositeDataOperationImpl<DataItemImpl>(
								0, OpType.READ, new DataItemImpl(name(object), 0, OBJECT_SIZE), dir.toString(), null,
								null, null, 0, THRESHOLD);
				assertTrue(driver.put(read));
			}
			assertTrue(output.finished.await(WAIT_SECONDS, TimeUnit.SECONDS), "the split reads did not finish");
			driver.stop();
		}

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(OBJECTS, snapshot.successSnapshot().count(), "objects read");
		assertEquals(0, snapshot.failsSnapshot().count(), "failed reads");
		assertEquals(OBJECTS, samples.parents, "finished parents");
		assertEquals(0, samples.markedRequestless, "parents the driver marked requestless");
		// a nonempty file stamps the parent's first byte; an empty one reaches the end with none
		assertEquals(fileSize > 0 ? OBJECTS : 0, samples.parentsThatRead, "parents with a first byte");
		assertEquals(OBJECTS * (OBJECT_SIZE / THRESHOLD), samples.ranges, "ranges read");
		samples.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		// the meter takes a first byte that is positive and within the duration, which the results show
		assertEquals(samples.firstBytes, snapshot.ttfbSnapshot().count(), "first-byte samples");
	}

	private static String name(final int object) {
		return "split-" + object + ".bin";
	}

	@SuppressWarnings("unchecked")
	private static LoadGenerator<DataItemImpl, Operation<DataItemImpl>> inertGenerator() {
		return (LoadGenerator<DataItemImpl, Operation<DataItemImpl>>) Proxy.newProxyInstance(
						LoadGenerator.class.getClassLoader(), new Class<?>[]{LoadGenerator.class
						},
						(proxy, method, args) -> {
							switch (method.getName()) {
							case "hashCode":
								return System.identityHashCode(proxy);
							case "equals":
								return proxy == args[0];
							case "toString":
								return "generator";
							default:
								break;
							}
							final Class<?> type = method.getReturnType();
							if (type == boolean.class) {
								return false;
							}
							if (type == int.class) {
								return 0;
							}
							if (type == long.class) {
								return 0L;
							}
							return null;
						});
	}

	/**
	 * The samples the step's meters must hold: one for each successful range, and one for each
	 * finished parent, which read the file itself. Exact, because each meter keeps its sum and extremes.
	 */
	private static final class Samples {

		private long parents;
		private long parentsThatRead;
		private long markedRequestless;
		private long ranges;
		private long firstBytes;
		private long count;
		private long durationSum;
		private long minDuration = Long.MAX_VALUE;
		private long maxDuration;
		private long latencies;
		private long latencySum;
		private long minLatency = Long.MAX_VALUE;
		private long maxLatency;

		synchronized void add(final Operation<?> result) {
			if (result.status() != Operation.Status.SUCC) {
				return;
			}
			if (result instanceof CompositeOperation<?> parent) {
				if (!parent.allSubOperationsDone()) {
					return;
				}
				parents++;
				if (((DataOperation<?>) result).respDataTimeStart() > 0) {
					parentsThatRead++;
				}
				if (parent instanceof CompositeDataOperationImpl<?> impl && impl.rangesSentAsRequests()) {
					markedRequestless++;
				}
			} else if (result instanceof PartialOperation) {
				ranges++;
			} else {
				return;
			}
			if (result.duration() <= 0) {
				return;
			}
			count++;
			durationSum += result.duration();
			minDuration = Math.min(minDuration, result.duration());
			maxDuration = Math.max(maxDuration, result.duration());
			if (result.latency() > 0) {
				latencies++;
				latencySum += result.latency();
				minLatency = Math.min(minLatency, result.latency());
				maxLatency = Math.max(maxLatency, result.latency());
			}
			final long dataLatency = ((DataOperation<?>) result).dataLatency();
			if (dataLatency > 0 && dataLatency <= result.duration()) {
				firstBytes++;
			}
		}

		synchronized void assertHeldBy(final TimingMetricSnapshot duration, final TimingMetricSnapshot latency) {
			assertEquals(count, duration.count(), "duration samples");
			assertEquals(durationSum, duration.sum(), "duration sum");
			assertEquals(minDuration, duration.min(), "shortest duration");
			assertEquals(maxDuration, duration.max(), "longest duration");
			assertEquals(latencies, latency.count(), "latency samples");
			assertEquals(latencySum, latency.sum(), "latency sum");
			assertEquals(minLatency, latency.min(), "shortest latency");
			assertEquals(maxLatency, latency.max(), "longest latency");
		}
	}

	/** Gives the step each result the driver ends, one at a time or as batches, and notes it first. */
	private static final class StepOutput implements Output<Operation<DataItemImpl>> {

		private final LoadStepContextImpl<DataItemImpl, Operation<DataItemImpl>> context;
		private final boolean batch;
		private final Samples samples;
		final CountDownLatch finished = new CountDownLatch(OBJECTS);

		StepOutput(final LoadStepContextImpl<DataItemImpl, Operation<DataItemImpl>> context, final boolean batch,
						final Samples samples) {
			this.context = context;
			this.batch = batch;
			this.samples = samples;
		}

		@Override
		public boolean put(final Operation<DataItemImpl> result) {
			note(result);
			final boolean accepted = batch ? context.put(List.of(result), 0, 1) == 1 : context.put(result);
			countFinished(result);
			return accepted;
		}

		@Override
		public int put(final List<Operation<DataItemImpl>> results, final int from, final int to) {
			final var given = new ArrayList<>(results.subList(from, to));
			given.forEach(this::note);
			final int accepted;
			if (batch) {
				accepted = context.put(results, from, to);
			} else {
				var n = 0;
				for (final var result : given) {
					if (context.put(result)) {
						n++;
					}
				}
				accepted = n;
			}
			given.forEach(this::countFinished);
			return accepted;
		}

		@Override
		public int put(final List<Operation<DataItemImpl>> results) {
			return put(results, 0, results.size());
		}

		private void note(final Operation<DataItemImpl> result) {
			samples.add(result);
		}

		private void countFinished(final Operation<DataItemImpl> result) {
			if (result instanceof CompositeOperation<?> parent
							&& (parent.allSubOperationsDone() || result.status() != Operation.Status.SUCC)) {
				finished.countDown();
			}
		}

		@Override
		public Input<Operation<DataItemImpl>> getInput() {
			throw new AssertionError("not read");
		}

		@Override
		public void close() {}
	}
}
