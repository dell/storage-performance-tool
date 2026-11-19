package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Load steps of reads split into ranges, on the {@code s3-aws} driver against a loopback S3: the
 * step's duration and latency statistics hold one sample per range request. The driver gives the step
 * one result at a time; each case also runs with its results passed as batches.
 */
@Timeout(value = 240, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsSplitReadTimingStepTest {

	private static final long MIB = 1024 * 1024;
	private static final long PART_SIZE = 5 * MIB;
	private static final long OBJECT_SIZE = 12 * MIB;
	private static final int RANGES = 3;
	private static final int OBJECTS = 6;
	private static final int CONCURRENCY = 2;
	/** One object in this many has a range the target always refuses. */
	private static final int REFUSED_EVERY = 3;

	private final RangeTarget target = new RangeTarget();
	private final RangeSamples rangeSamples = new RangeSamples();
	/** How long the last {@link #readStep} took, setup included, on the requests' monotonic clock. */
	private long stepNanos;

	@TempDir
	Path home;

	@AfterEach
	void stop() {
		target.close();
	}

	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aSplitReadStepHasOneTimingSamplePerRange(final boolean batch) throws Exception {
		final var snapshot = readStep(batch);

		assertEquals(OBJECTS, snapshot.successSnapshot().count());
		assertEquals(0, snapshot.failsSnapshot().count());
		assertEquals(OBJECTS * OBJECT_SIZE, snapshot.byteSnapshot().count());
		assertRangeSamples(snapshot, OBJECTS * RANGES);
	}

	/** A refused range's attempts and the failed read that ends them add no sample. */
	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aFailedSplitReadAddsOnlyItsRangesThatSucceeded(final boolean batch) throws Exception {
		target.refuseMiddleRange = true;
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
		// The sum equals the ranges' exactly (above); this bounds it by the time the step had, on the
		// monotonic clock that times requests too. A parent's queue time once made it many times that;
		// twice the budget leaves room for a request that starts its clock before it holds a permit.
		final var busyMicros = 2 * (stepNanos / 1_000 + 1) * CONCURRENCY;
		assertTrue(duration.sum() <= busyMicros, () -> duration.sum() + " us of durations in " + busyMicros + " us");
	}

	private AllMetricsSnapshot readStep(final boolean batch) throws Exception {
		final long start = System.nanoTime();
		try {
			return runReadStep(batch);
		} finally {
			stepNanos = System.nanoTime() - start;
		}
	}

	private AllMetricsSnapshot runReadStep(final boolean batch) throws Exception {
		final var items = home.resolve("items.csv");
		Files.writeString(items, IntStream.range(0, OBJECTS)
						.mapToObj(i -> "/bucket/split-" + i + "," + Long.toHexString(0x1000L + i) + "," + OBJECT_SIZE + ",0/")
						.collect(Collectors.joining("\n", "", "\n")));
		final var stepId = "s3-aws-split-read-timing-" + System.nanoTime();
		final var config = S3AwsMergedConfig.create(home, target.port(), Map.of(
						"load-step-id", stepId,
						"load-step-idAutoGenerated", false));
		final var scenario = """
						Load.config({
							"load": {"step": {"limit": {"time": "120s"}}, "op": {"type": "read", "limit": {"count": %d}}},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"input": {"file": "%s"}, "data": {"ranges": {"threshold": "%d"}}}
						}).run();
						""".formatted(OBJECTS, CONCURRENCY, items.toString().replace("\\", "/"), PART_SIZE);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = new ArrayList<>(Extension.load(classLoader));
		final var batches = new LongAdder();
		extensions.replaceAll(extension -> "s3-aws".equals(extension.id())
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
			new RunImpl("s3-aws split read timing", scenario, engine, 1L).run();
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
		public boolean put(final O result) throws IOException {
			rangeSamples.add(result);
			if (!batch) {
				return output.put(result);
			}
			batches.increment();
			return output.put(List.of(result), 0, 1) == 1;
		}

		@Override
		public int put(final List<O> results, final int from, final int to) throws IOException {
			results.subList(from, to).forEach(rangeSamples::add);
			batches.increment();
			return output.put(results, from, to);
		}

		@Override
		public int put(final List<O> results) throws IOException {
			return put(results, 0, results.size());
		}

		@Override
		public Input<O> getInput() throws IOException {
			return output.getInput();
		}

		@Override
		public void close() throws IOException {
			output.close();
		}
	}

	/** A loopback S3 serving {@link #OBJECT_SIZE}-byte objects by range. */
	private static final class RangeTarget implements AutoCloseable {

		private static final byte[] CHUNK = new byte[64 * 1024];

		private final HttpServer server;
		private final ExecutorService threads = Executors.newCachedThreadPool();
		volatile boolean refuseMiddleRange;

		RangeTarget() {
			try {
				server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			} catch (final IOException e) {
				throw new IllegalStateException(e);
			}
			server.setExecutor(threads);
			server.createContext("/", this::handle);
			server.start();
		}

		int port() {
			return server.getAddress().getPort();
		}

		private void handle(final HttpExchange exchange) throws IOException {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				final var headers = exchange.getResponseHeaders();
				headers.set("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)));
				final var range = exchange.getRequestHeaders().getFirst("Range");
				if (!"GET".equals(exchange.getRequestMethod()) || range == null) {
					headers.set("Content-Length", "0");
					exchange.sendResponseHeaders(200, -1);
					return;
				}
				final var first = Long.parseLong(range.substring("bytes=".length(), range.indexOf('-')));
				final var last = Math.min(Long.parseLong(range.substring(range.indexOf('-') + 1)), OBJECT_SIZE - 1);
				final var path = exchange.getRequestURI().getPath();
				final var object = Integer.parseInt(path.substring(path.lastIndexOf('-') + 1));
				if (refuseMiddleRange && first == PART_SIZE && object % REFUSED_EVERY == 0) {
					final var xml = "<Error><Code>AccessDenied</Code><Message>scripted</Message></Error>"
									.getBytes(StandardCharsets.UTF_8);
					headers.set("Content-Type", "application/xml");
					exchange.sendResponseHeaders(403, xml.length);
					exchange.getResponseBody().write(xml);
					return;
				}
				headers.set("ETag", "\"stub\"");
				headers.set("Accept-Ranges", "bytes");
				headers.set("Content-Type", "application/octet-stream");
				headers.set("Content-Range", "bytes " + first + "-" + last + "/" + OBJECT_SIZE);
				final var length = last - first + 1;
				exchange.sendResponseHeaders(206, length);
				final var body = exchange.getResponseBody();
				for (var left = length; left > 0; left -= CHUNK.length) {
					body.write(CHUNK, 0, (int) Math.min(CHUNK.length, left));
				}
			}
		}

		@Override
		public void close() {
			server.stop(0);
			threads.shutdownNow();
		}
	}
}
