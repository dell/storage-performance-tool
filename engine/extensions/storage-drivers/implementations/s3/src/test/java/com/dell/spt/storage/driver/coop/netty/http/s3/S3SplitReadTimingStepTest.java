package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import io.netty.channel.epoll.Epoll;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Load steps of reads split into ranges, on the {@code s3} driver against the loopback stub: the
 * step's duration and latency statistics hold one sample per range request. The shipped driver gives
 * the step one result at a time; each case also runs with its results passed as batches.
 */
@Timeout(180)
final class S3SplitReadTimingStepTest {

	private static final int RANGE_SIZE = 1024;
	private static final int RANGES = 4;
	private static final int OBJECTS = 16;
	private static final int CONCURRENCY = 4;
	/** One object in this many has a range the target always refuses. */
	private static final int REFUSED_EVERY = 4;

	private final NicSetLoopback stub = new NicSetLoopback();
	private final RangeSamples rangeSamples = new RangeSamples();

	@TempDir
	Path tempDir;

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
		stub.absentObjectSize = RANGES * RANGE_SIZE;
	}

	@AfterEach
	void stop() throws Exception {
		stub.close();
		NicSetLoopback.awaitNoNicIoThreads();
	}

	@Test
	void aSplitReadStepHasOneTimingSamplePerRange() throws Exception {
		assertOneTimingSamplePerRange(false);
	}

	@Test
	void aSplitReadStepWithBatchedResultsHasOneTimingSamplePerRange() throws Exception {
		assertOneTimingSamplePerRange(true);
	}

	private void assertOneTimingSamplePerRange(final boolean batch) throws Exception {
		final var snapshot = readStep(batch);

		assertEquals(OBJECTS, snapshot.successSnapshot().count());
		assertEquals(0, snapshot.failsSnapshot().count());
		assertEquals((long) OBJECTS * RANGES * RANGE_SIZE, snapshot.byteSnapshot().count());
		assertRangeSamples(snapshot, OBJECTS * RANGES);
	}

	@Test
	void aFailedSplitReadAddsOnlyItsRangesThatSucceeded() throws Exception {
		assertOnlySucceededRangesAdded(false);
	}

	@Test
	void aFailedSplitReadWithBatchedResultsAddsOnlyItsRangesThatSucceeded() throws Exception {
		assertOnlySucceededRangesAdded(true);
	}

	/** A refused range's attempts and the failed read that ends them add no sample. */
	private void assertOnlySucceededRangesAdded(final boolean batch) throws Exception {
		stub.failRequest = request -> "GET".equals(request.method()) && request.range() != null
						&& request.range().startsWith("bytes=" + RANGE_SIZE + "-")
						&& objectIndex(request) % REFUSED_EVERY == 0;
		final var refused = OBJECTS / REFUSED_EVERY;

		final var snapshot = readStep(batch);

		assertEquals(OBJECTS - refused, snapshot.successSnapshot().count());
		assertEquals(refused, snapshot.failsSnapshot().count());
		assertRangeSamples(snapshot, (OBJECTS - refused) * RANGES + refused * (RANGES - 1));
	}

	/** The step's statistics are exactly those of its {@code ranges} successful range results. */
	private void assertRangeSamples(final AllMetricsSnapshot snapshot, final int ranges) {
		final var duration = snapshot.durationSnapshot();
		assertEquals(ranges, duration.count(), "duration samples");
		rangeSamples.assertHeldBy(duration, snapshot.latencySnapshot());
		// The sum equals the ranges' exactly (above); this bounds it by the time the step had. A parent's
		// queue time once made it many times that. Twice the budget, plus a millisecond for truncation,
		// leaves room for the wall clock that times the step against the monotonic one that times requests.
		final var busyMicros = 2 * (snapshot.elapsedTimeMillis() + 1) * 1_000 * CONCURRENCY;
		assertTrue(duration.sum() <= busyMicros, () -> duration.sum() + " us of durations in " + busyMicros + " us");
	}

	private static int objectIndex(final NicSetLoopback.Request request) {
		final var path = request.path();
		return Integer.parseInt(path.substring(path.lastIndexOf('-') + 1));
	}

	private AllMetricsSnapshot readStep(final boolean batch) throws Exception {
		final var items = tempDir.resolve("items.csv");
		Files.writeString(items, IntStream.range(0, OBJECTS)
						.mapToObj(i -> "/bucket/split-" + i + "," + Long.toHexString(0x1000L + i) + "," + RANGES * RANGE_SIZE + ",0/")
						.collect(Collectors.joining("\n", "", "\n")));
		final var stepId = "split-read-timing-" + System.nanoTime();
		final var root = NicSetLoopback.root(stub.listeners(1), List.of(), 1);
		final Config config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		// The loopback answers at once; a long inactivity timeout keeps a paused, loaded worker from
		// failing a good range.
		config.val("storage-net-timeoutMilliSec", 60_000);
		config.val("run-id", 779L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-weight", 1);
		config.val("item-output-path", "/bucket");
		config.val("output-color", false);
		config.val("storage-driver-type", "s3");
		final var scenario = """
						Load.config({
							"load": {"step": {"limit": {"time": "120s"}}, "op": {"type": "read", "limit": {"count": %d}}},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"ranges": {"threshold": "%d"}}, "input": {"file": "%s"}}
						}).run();
						""".formatted(OBJECTS, CONCURRENCY, RANGE_SIZE, items);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = new ArrayList<>(Extension.load(classLoader));
		final var batches = new LongAdder();
		extensions.replaceAll(extension -> "s3".equals(extension.id())
						? new ObservedResultsFactory<>((StorageDriverFactory<?, ?, ?>) extension, batch, batches, rangeSamples)
						: extension);
		final var snapshot = new AtomicReference<AllMetricsSnapshot>();
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			public void unregister(final MetricsContext metricsCtx) {
				super.unregister(metricsCtx);
				if (!(metricsCtx instanceof DistributedMetricsContext) && stepId.equals(metricsCtx.loadStepId())) {
					snapshot.set((AllMetricsSnapshot) metricsCtx.lastSnapshot());
				}
			}
		}) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("split read timing", scenario, engine, 779L).run();
		}
		assertNotNull(snapshot.get(), "the step reported its metrics");
		assertEquals(batch, batches.sum() > 0, "the results took the route under test");
		return snapshot.get();
	}

	/** The timings of the successful range results the step was given, as its meters take them. */
	private static final class RangeSamples {

		private long durations;
		private long durationSum;
		private long minDuration = Long.MAX_VALUE;
		private long maxDuration;
		private long latencies;
		private long latencySum;
		private long minLatency = Long.MAX_VALUE;
		private long maxLatency;

		synchronized void add(final Operation<?> result) {
			if (!(result instanceof PartialOperation) || result.status() != Operation.Status.SUCC || result.duration() <= 0) {
				return;
			}
			durations++;
			durationSum += result.duration();
			minDuration = Math.min(minDuration, result.duration());
			maxDuration = Math.max(maxDuration, result.duration());
			if (result.latency() > 0) {
				latencies++;
				latencySum += result.latency();
				minLatency = Math.min(minLatency, result.latency());
				maxLatency = Math.max(maxLatency, result.latency());
			}
		}

		synchronized void assertHeldBy(final TimingMetricSnapshot duration, final TimingMetricSnapshot latency) {
			assertEquals(durations, duration.count(), "duration samples");
			assertEquals(durationSum, duration.sum(), "duration sum");
			assertEquals(minDuration, duration.min(), "shortest duration");
			assertEquals(maxDuration, duration.max(), "longest duration");
			assertEquals(latencies, latency.count(), "latency samples");
			assertEquals(latencySum, latency.sum(), "latency sum");
			assertEquals(minLatency, latency.min(), "shortest latency");
			assertEquals(maxLatency, latency.max(), "longest latency");
		}
	}

	/** Shows the test each result the delegate's drivers give the step, one at a time or as batches. */
	private static final class ObservedResultsFactory<I extends Item, O extends Operation<I>>
					implements StorageDriverFactory<I, O, StorageDriver<I, O>> {

		private final StorageDriverFactory<I, O, ?> delegate;
		private final boolean batch;
		private final LongAdder batches;
		private final RangeSamples rangeSamples;

		@SuppressWarnings("unchecked")
		ObservedResultsFactory(final StorageDriverFactory<?, ?, ?> delegate, final boolean batch, final LongAdder batches,
						final RangeSamples rangeSamples) {
			this.delegate = (StorageDriverFactory<I, O, ?>) delegate;
			this.batch = batch;
			this.batches = batches;
			this.rangeSamples = rangeSamples;
		}

		@Override
		@SuppressWarnings("unchecked")
		public StorageDriver<I, O> create(final String stepId, final DataInput dataInput, final Config storageConfig,
						final boolean verifyFlag, final int batchSize) throws InterruptedException {
			final StorageDriver<I, O> driver = delegate.create(stepId, dataInput, storageConfig, verifyFlag, batchSize);
			return (StorageDriver<I, O>) Proxy.newProxyInstance(StorageDriver.class.getClassLoader(),
							new Class<?>[]{StorageDriver.class
							}, (proxy, method, args) -> {
								if ("operationResultOutput".equals(method.getName())) {
									driver.operationResultOutput(new ObservedOutput<>((Output<O>) args[0], batch, batches, rangeSamples));
									return null;
								}
								try {
									return method.invoke(driver, args);
								} catch (final InvocationTargetException e) {
									throw e.getCause();
								}
							});
		}

		@Override
		public String id() {
			return delegate.id();
		}

		@Override
		public Config defaults(final Path appHomePath) {
			return delegate.defaults(appHomePath);
		}

		@Override
		public SchemaProvider schemaProvider() {
			return delegate.schemaProvider();
		}

		@Override
		public void install(final Path dstPath) {
			delegate.install(dstPath);
		}
	}

	private static final class ObservedOutput<O extends Operation<?>> implements Output<O> {

		private final Output<O> output;
		private final boolean batch;
		private final LongAdder batches;
		private final RangeSamples rangeSamples;

		ObservedOutput(final Output<O> output, final boolean batch, final LongAdder batches, final RangeSamples rangeSamples) {
			this.output = output;
			this.batch = batch;
			this.batches = batches;
			this.rangeSamples = rangeSamples;
		}

		@Override
		public boolean put(final O result) {
			rangeSamples.add(result);
			if (!batch) {
				return output.put(result);
			}
			batches.increment();
			return output.put(List.of(result), 0, 1) == 1;
		}

		@Override
		public int put(final List<O> results, final int from, final int to) {
			results.subList(from, to).forEach(rangeSamples::add);
			batches.increment();
			return output.put(results, from, to);
		}

		@Override
		public int put(final List<O> results) {
			return put(results, 0, results.size());
		}

		@Override
		public Input<O> getInput() {
			return output.getInput();
		}

		@Override
		public void close() throws Exception {
			output.close();
		}
	}
}
