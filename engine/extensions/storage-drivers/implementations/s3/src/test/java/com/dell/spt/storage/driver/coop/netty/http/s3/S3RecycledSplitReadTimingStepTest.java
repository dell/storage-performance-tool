package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A split read recycled by {@code load.op.recycle.mode}: the step hands the finished result to the
 * generator before it is done with it, and the generator can admit the same object again at once,
 * which resets it. The step must still record the attempt that finished, as a read whose ranges were
 * its only requests. The test forces that order: the step's timing output waits until the driver has
 * prepared the same object again, and that admission waits, just after {@code prepare()}, until the
 * step has taken the result. Two reads of one object then have eight ranges and eight samples.
 */
@Timeout(180)
final class S3RecycledSplitReadTimingStepTest {

	private static final int RANGE_SIZE = 1024;
	private static final int RANGES = 4;
	private static final int READS = 2;
	private static final long WAIT_SECONDS = 60;

	private final NicSetLoopback stub = new NicSetLoopback();
	private final Ordering ordering = new Ordering();

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
	void aRecycledSplitReadAddsOnlyItsRangesSamples() throws Exception {
		assertOnlyRangeSamples(false);
	}

	@Test
	void aRecycledSplitReadWithBatchedResultsAddsOnlyItsRangesSamples() throws Exception {
		assertOnlyRangeSamples(true);
	}

	private void assertOnlyRangeSamples(final boolean batch) throws Exception {
		final var snapshot = readStep(batch);

		assertNull(ordering.failure, () -> "the forced order did not happen: " + ordering.failure);
		assertTrue(ordering.forced, "the step took the result after its object was admitted again");
		assertEquals(READS, snapshot.successSnapshot().count(), "reads");
		assertEquals(0, snapshot.failsSnapshot().count(), "failed reads");
		assertEquals((long) READS * RANGES * RANGE_SIZE, snapshot.byteSnapshot().count(), "bytes");
		assertEquals(READS * RANGES, ordering.ranges.sum(), "ranges read");
		assertEquals(READS * RANGES, snapshot.durationSnapshot().count(), "duration samples");
		assertEquals(READS * RANGES, snapshot.latencySnapshot().count(), "latency samples");
	}

	private AllMetricsSnapshot readStep(final boolean batch) throws Exception {
		final var items = tempDir.resolve("items.csv");
		Files.writeString(items, "/bucket/recycled-0,1000," + RANGES * RANGE_SIZE + ",0/\n");
		final var stepId = "recycled-split-read-timing-" + System.nanoTime();
		final var root = NicSetLoopback.root(stub.listeners(1), List.of(), 1);
		final Config config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		config.val("storage-net-timeoutMilliSec", 60_000);
		config.val("run-id", 781L);
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-weight", 1);
		config.val("item-output-path", "/bucket");
		config.val("output-color", false);
		config.val("storage-driver-type", "s3");
		final var scenario = """
						Load.config({
							"load": {
								"step": {"limit": {"time": "120s"}},
								"op": {"type": "read", "limit": {"count": %d}, "recycle": {"mode": true}}
							},
							"storage": {"driver": {"limit": {"concurrency": 1}}},
							"item": {"data": {"ranges": {"threshold": "%d"}}, "input": {"file": "%s"}}
						}).run();
						""".formatted(READS, RANGE_SIZE, items);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = new ArrayList<>(Extension.load(classLoader));
		extensions.replaceAll(extension -> "s3".equals(extension.id())
						? new OrderedFactory<>((StorageDriverFactory<?, ?, ?>) extension, batch, ordering)
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
			new RunImpl("recycled split read timing", scenario, engine, 781L).run();
		}
		assertNotNull(snapshot.get(), "the step reported its metrics");
		return snapshot.get();
	}

	/**
	 * Holds the first finished split read in the step until the driver has admitted the same object
	 * again, and holds that admission until the step is done with the result. Waits are bounded, and
	 * a missed step is noted for the test thread, since a driver thread's assertion would be lost.
	 */
	private static final class Ordering {

		private final CountDownLatch readmitted = new CountDownLatch(1);
		private final CountDownLatch taken = new CountDownLatch(1);
		private final LongAdder ranges = new LongAdder();
		private volatile Object first;
		private volatile boolean forced;
		private volatile String failure;

		/**
		 * Called before the step is given {@code result}, so before it can recycle it: notes the first
		 * finished split read.
		 */
		void giving(final Operation<?> result) {
			if (first == null && result instanceof CompositeOperation<?> parent && parent.allSubOperationsDone()
							&& result.status() == Operation.Status.SUCC) {
				first = result;
			}
		}

		/**
		 * The step's timing output, which the step calls after it hands a result to the generator. The
		 * generator may have admitted the object again already; if not, this waits until it has.
		 */
		void timed(final Operation<?> result) {
			if (result == first && first != null) {
				if (await(readmitted)) {
					forced = true;
				} else {
					failure = "the generator did not admit the read again";
				}
			}
		}

		/**
		 * Called by the driver just after it prepared an operation for admission. The first finished
		 * read's admission again waits here until the step has taken its result.
		 */
		void prepared(final Operation<?> op) {
			if (op == first && readmitted.getCount() > 0) {
				readmitted.countDown();
				if (!await(taken)) {
					failure = "the step did not take the result";
				}
			}
		}

		/** Called once the step returned from a put that held {@code result}. */
		void taken(final Operation<?> result) {
			if (result instanceof PartialOperation && result.status() == Operation.Status.SUCC) {
				ranges.increment();
			}
			if (result == first) {
				taken.countDown();
			}
		}

		private static boolean await(final CountDownLatch latch) {
			try {
				return latch.await(WAIT_SECONDS, TimeUnit.SECONDS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return false;
			}
		}
	}

	/** Makes the step's {@code s3} driver one that reports each admission it prepares. */
	private static final class OrderedFactory<I extends Item, O extends Operation<I>>
					implements StorageDriverFactory<I, O, StorageDriver<I, O>> {

		private final StorageDriverFactory<I, O, ?> delegate;
		private final boolean batch;
		private final Ordering ordering;

		@SuppressWarnings("unchecked")
		OrderedFactory(final StorageDriverFactory<?, ?, ?> delegate, final boolean batch, final Ordering ordering) {
			this.delegate = (StorageDriverFactory<I, O, ?>) delegate;
			this.batch = batch;
			this.ordering = ordering;
		}

		@Override
		@SuppressWarnings("unchecked")
		public StorageDriver<I, O> create(final String stepId, final DataInput dataInput, final Config storageConfig,
						final boolean verifyFlag, final int batchSize) throws InterruptedException {
			final StorageDriver<I, O> driver;
			try {
				driver = new S3StorageDriver<I, O>(stepId, dataInput, storageConfig, verifyFlag, batchSize) {
					@Override
					protected boolean prepare(final O op) {
						final boolean prepared = super.prepare(op);
						ordering.prepared(op);
						return prepared;
					}
				};
			} catch (final InterruptedException e) {
				throw e;
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
			return (StorageDriver<I, O>) Proxy.newProxyInstance(StorageDriver.class.getClassLoader(),
							new Class<?>[]{StorageDriver.class
							}, (proxy, method, args) -> {
								if ("operationResultOutput".equals(method.getName())) {
									driver.operationResultOutput(new OrderedOutput<>((Output<O>) args[0], batch, ordering));
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

	/**
	 * Gives the step the driver's results, one at a time or as batches, and sets the step's timing
	 * output before the first one.
	 */
	private static final class OrderedOutput<O extends Operation<?>> implements Output<O> {

		private final Output<O> output;
		private final boolean batch;
		private final Ordering ordering;
		private volatile boolean hooked;

		OrderedOutput(final Output<O> output, final boolean batch, final Ordering ordering) {
			this.output = output;
			this.batch = batch;
			this.ordering = ordering;
		}

		@SuppressWarnings("unchecked")
		private void hook() {
			if (!hooked) {
				hooked = true;
				((LoadStepContext<?, O>) output).operationsMetricsOutput(new TimingOutput<>(ordering));
			}
		}

		@Override
		public boolean put(final O result) {
			hook();
			ordering.giving(result);
			final boolean accepted = batch ? output.put(List.of(result), 0, 1) == 1 : output.put(result);
			ordering.taken(result);
			return accepted;
		}

		@Override
		public int put(final List<O> results, final int from, final int to) {
			hook();
			final var given = new ArrayList<>(results.subList(from, to));
			given.forEach(ordering::giving);
			final int accepted;
			if (batch) {
				accepted = output.put(results, from, to);
			} else {
				var n = 0;
				for (final var result : given) {
					if (output.put(result)) {
						n++;
					}
				}
				accepted = n;
			}
			given.forEach(ordering::taken);
			return accepted;
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

	/** The step's per-operation timing output: it holds the first finished split read. */
	private static final class TimingOutput<O extends Operation<?>> implements Output<O> {

		private final Ordering ordering;

		TimingOutput(final Ordering ordering) {
			this.ordering = ordering;
		}

		@Override
		public boolean put(final O result) {
			if (result != null) {
				ordering.timed(result);
			}
			return true;
		}

		@Override
		public int put(final List<O> results, final int from, final int to) {
			results.subList(from, to).forEach(this::put);
			return to - from;
		}

		@Override
		public int put(final List<O> results) {
			return put(results, 0, results.size());
		}

		@Override
		public Input<O> getInput() {
			throw new AssertionError("not read");
		}

		@Override
		public void close() {}
	}
}
