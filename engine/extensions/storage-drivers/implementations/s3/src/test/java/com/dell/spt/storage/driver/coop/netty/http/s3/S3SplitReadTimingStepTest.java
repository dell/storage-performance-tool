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
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
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
	/** Every object request waits this long, so a real request's latency stands out from a parent's. */
	private static final int ANSWER_DELAY_MILLIS = 2;
	private static final long LEAST_REQUEST_LATENCY_MICROS = 1_000;
	/** One object in this many has a range the target always refuses. */
	private static final int REFUSED_EVERY = 4;

	private final NicSetLoopback stub = new NicSetLoopback();

	@TempDir
	Path tempDir;

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
		stub.absentObjectSize = RANGES * RANGE_SIZE;
		stub.answerDelayMillis = request -> ANSWER_DELAY_MILLIS;
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

	private static void assertRangeSamples(final AllMetricsSnapshot snapshot, final int ranges) {
		final var duration = snapshot.durationSnapshot();
		final var latency = snapshot.latencySnapshot();
		assertEquals(ranges, duration.count(), "duration samples");
		assertEquals(ranges, latency.count(), "latency samples");
		assertEquals(ranges, snapshot.ttfbSnapshot().count(), "time-to-first-byte samples");
		assertTrue(latency.min() >= LEAST_REQUEST_LATENCY_MICROS, () -> "the least latency is " + latency.min() + " us");
		final var busyMicros = snapshot.elapsedTimeMillis() * 1_000 * CONCURRENCY;
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
		if (batch) {
			extensions.replaceAll(extension -> "s3".equals(extension.id())
							? new BatchResultsFactory<>((StorageDriverFactory<?, ?, ?>) extension, batches)
							: extension);
		}
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

	/** Makes the delegate's drivers give the step each result through its batch {@code put}. */
	private static final class BatchResultsFactory<I extends Item, O extends Operation<I>>
					implements StorageDriverFactory<I, O, StorageDriver<I, O>> {

		private final StorageDriverFactory<I, O, ?> delegate;
		private final LongAdder batches;

		@SuppressWarnings("unchecked")
		BatchResultsFactory(final StorageDriverFactory<?, ?, ?> delegate, final LongAdder batches) {
			this.delegate = (StorageDriverFactory<I, O, ?>) delegate;
			this.batches = batches;
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
									driver.operationResultOutput(new BatchOutput<>((Output<O>) args[0], batches));
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

	private record BatchOutput<O>(Output<O> output, LongAdder batches)implements Output<O>
	{

	@Override
	public boolean put(final O result) {
		batches.increment();
		return output.put(List.of(result), 0, 1) == 1;
	}

	@Override
	public int put(final List<O> results, final int from, final int to) {
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
}}
