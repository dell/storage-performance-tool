package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorBuilder;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** End-to-end in-process coverage for duration-style recycled workload circulation. */
@SuppressWarnings({"deprecation", "unchecked"
})
class RecycleCirculationIntegrationTest {

	private static final int MANIFEST_ITEM_COUNT = 50;
	private static final int COMPLETION_COUNT = 200;
	private static final long ITEM_SIZE = 64;
	private static final int COMPLETION_TIMEOUT_SECONDS = 10;

	/**
	 * Proves the product invariant at every low-concurrency shape affected by the former
	 * fast-recycle optimization: all manifest items enter circulation before any item is
	 * repeated. The test driver models the old public fast-recycle contract so replaying
	 * this test against v5.14.1 activates the broken direct-resubmission path and fails.
	 */
	@ParameterizedTest
	@ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	})
	void completeManifestCirculatesBeforeAnyRepeat(final int concurrency) throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-data-ranges-concat", null);
		config.val("load-op-type", "read");
		config.val("load-op-retry", false);
		config.val("load-op-recycle-mode", true);
		config.val("load-op-recycle-content-update", false);
		config.val("load-op-limit-count", COMPLETION_COUNT);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", COMPLETION_TIMEOUT_SECONDS);

		final var itemInput = new ManifestItemInput(MANIFEST_ITEM_COUNT);
		final var driver = new CirculationCanaryDriver(concurrency, COMPLETION_COUNT);
		final ItemType itemType = ItemType.DATA;
		final ItemFactory<DataItem> itemFactory = (ItemFactory<DataItem>) ItemType.getItemFactory(itemType);
		final LoadGeneratorBuilder<DataItem, Operation<DataItem>, LoadGeneratorImpl<DataItem, Operation<DataItem>>> generatorBuilder = new LoadGeneratorBuilderImpl<DataItem, Operation<DataItem>, LoadGeneratorImpl<DataItem, Operation<DataItem>>>()
						.itemConfig(config.configVal("item"))
						.loadConfig(config.configVal("load"))
						.itemType(itemType)
						.itemFactory(itemFactory)
						.itemInput(itemInput)
						.loadOperationsOutput(driver)
						.authConfig(config.configVal("storage").configVal("auth"))
						.originIndex(0);
		final LoadGenerator<DataItem, Operation<DataItem>> generator = generatorBuilder.build();
		final MetricsContext<AllMetricsSnapshot> metrics = buildMetrics(concurrency);
		final var stepContext = new LoadStepContextImpl<>(
						"recycle-circulation-t" + concurrency,
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		stepContext.start();
		try {
			assertTrue(
							driver.awaitCompletions(COMPLETION_TIMEOUT_SECONDS, TimeUnit.SECONDS),
							"timed out after " + driver.completedOpCount() + " completions at T" + concurrency);

			final List<String> observed = driver.observedItemNames();
			assertEquals(COMPLETION_COUNT, observed.size());
			assertEquals(
							MANIFEST_ITEM_COUNT,
							new HashSet<>(observed.subList(0, MANIFEST_ITEM_COUNT)).size(),
							"an item repeated before the complete manifest entered circulation at T" + concurrency);

			final Set<String> expectedNames = ManifestItemInput.expectedNames(MANIFEST_ITEM_COUNT);
			assertEquals(expectedNames, new HashSet<>(observed), "the driver saw an unexpected or missing item");
			final Map<String, Integer> counts = new HashMap<>();
			observed.forEach(name -> counts.merge(name, 1, Integer::sum));
			final int minCount = counts.values().stream().mapToInt(Integer::intValue).min().orElseThrow();
			final int maxCount = counts.values().stream().mapToInt(Integer::intValue).max().orElseThrow();
			assertTrue(
							maxCount - minCount <= concurrency,
							"circulation imbalance exceeds the in-flight boundary at T" + concurrency + ": " + counts);

			assertEquals(COMPLETION_COUNT, driver.scheduledOpCount());
			assertEquals(COMPLETION_COUNT, driver.completedOpCount());
			assertEquals(0, driver.activeOpCount(), "all bounded concurrency permits must be released");
		} finally {
			stepContext.stop();
			stepContext.shutdown();
			stepContext.close();
		}
	}

	private static MetricsContext<AllMetricsSnapshot> buildMetrics(final int concurrency) {
		final MetricsContext<AllMetricsSnapshot> metrics = MetricsContextImpl.builder()
						.loadStepId("recycle-circulation-t" + concurrency)
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(concurrency)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(ITEM_SIZE))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		metrics.start();
		return metrics;
	}

	private static final class ManifestItemInput implements Input<DataItem> {
		private final int itemCount;
		private int nextIndex;

		ManifestItemInput(final int itemCount) {
			this.itemCount = itemCount;
		}

		static Set<String> expectedNames(final int itemCount) {
			final Set<String> names = new HashSet<>();
			for (int i = 0; i < itemCount; i++) {
				names.add(itemName(i));
			}
			return names;
		}

		private static String itemName(final int index) {
			return String.format("manifest-item-%03d", index);
		}

		@Override
		public DataItem get() {
			return nextIndex < itemCount ? new DataItemImpl(itemName(nextIndex++), 0, ITEM_SIZE) : null;
		}

		@Override
		public int get(final List<DataItem> buffer, final int limit) {
			if (nextIndex >= itemCount) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			final int start = nextIndex;
			while (nextIndex < itemCount && nextIndex - start < limit) {
				buffer.add(new DataItemImpl(itemName(nextIndex++), 0, ITEM_SIZE));
			}
			return nextIndex - start;
		}

		@Override
		public long skip(final long itemsCount) {
			final long skipped = Math.min(itemsCount, itemCount - nextIndex);
			nextIndex += (int) skipped;
			return skipped;
		}

		@Override
		public void reset() {
			nextIndex = 0;
		}

		@Override
		public void close() {}

		@Override
		public String toString() {
			return "ManifestItemInput";
		}
	}

	/** Protocol-free asynchronous driver with a bounded, observable concurrency contract. */
	private static final class CirculationCanaryDriver extends AsyncRunnableBase
					implements StorageDriver<DataItem, Operation<DataItem>> {
		private final int concurrency;
		private final int completionLimit;
		private final Semaphore permits;
		private final ExecutorService executor;
		private final AtomicInteger scheduled = new AtomicInteger();
		private final AtomicInteger completed = new AtomicInteger();
		private final CountDownLatch completionLatch;
		private final List<String> observedNames = new CopyOnWriteArrayList<>();
		private volatile Output<Operation<DataItem>> resultOutput;
		private volatile boolean legacyDirectRecycle;

		CirculationCanaryDriver(final int concurrency, final int completionLimit) {
			this.concurrency = concurrency;
			this.completionLimit = completionLimit;
			this.permits = new Semaphore(concurrency, true);
			this.executor = Executors.newFixedThreadPool(concurrency);
			this.completionLatch = new CountDownLatch(completionLimit);
		}

		@Override
		public boolean put(final Operation<DataItem> op) {
			if (!isStarted() || !permits.tryAcquire()) {
				return false;
			}
			if (!claimCompletion()) {
				permits.release();
				return false;
			}
			recordDispatch(op);
			executor.execute(() -> complete(op));
			return true;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops, final int from, final int to) {
			int i = from;
			while (i < to && put(ops.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops) {
			return put(ops, 0, ops.size());
		}

		private boolean claimCompletion() {
			int current;
			do {
				current = scheduled.get();
				if (current >= completionLimit) {
					return false;
				}
			} while (!scheduled.compareAndSet(current, current + 1));
			return true;
		}

		private void recordDispatch(final Operation<DataItem> op) {
			final String itemName = op.item().name();
			observedNames.add(itemName.startsWith("/") ? itemName.substring(1) : itemName);
		}

		private void complete(final Operation<DataItem> op) {
			op.reset();
			op.startRequest();
			op.finishRequest();
			op.startResponse();
			if (op instanceof DataOperation<?> dataOperation) {
				dataOperation.startDataResponse();
				try {
					dataOperation.countBytesDone(op.item().size());
				} catch (final IOException e) {
					throw new AssertionError(e);
				}
			}
			op.finishResponse();
			op.status(Operation.Status.SUCC);
			completed.incrementAndGet();

			if (legacyDirectRecycle) {
				op.driverRecycled(true);
				resultOutput.put(op.result());
				if (claimCompletion()) {
					recordDispatch(op);
					executor.execute(() -> complete(op));
				} else {
					permits.release();
				}
			} else {
				permits.release();
				resultOutput.put(op.result());
			}
			completionLatch.countDown();
		}

		@Override
		public void operationResultOutput(final Output<Operation<DataItem>> resultOutput) {
			this.resultOutput = resultOutput;
		}

		@Override
		public List<DataItem> list(
						final ItemFactory<DataItem> itemFactory,
						final String path,
						final String prefix,
						final int idRadix,
						final DataItem lastPrevItem,
						final int count) {
			return List.of();
		}

		@Override
		public List<DataItem> list(
						final ItemFactory<DataItem> itemFactory,
						final String path,
						final String prefix,
						final int idRadix,
						final DataItem lastPrevItem,
						final int count,
						final ListOptions options) {
			return List.of();
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			throw new AssertionError();
		}

		@Override
		public int concurrencyLimit() {
			return concurrency;
		}

		@Override
		public int activeOpCount() {
			return concurrency - permits.availablePermits();
		}

		@Override
		public long scheduledOpCount() {
			return scheduled.get();
		}

		@Override
		public long completedOpCount() {
			return completed.get();
		}

		@Override
		public boolean isIdle() {
			return activeOpCount() == 0;
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

		@Override
		public void enableFastRecycle(final int concurrencyThreshold) {
			legacyDirectRecycle = concurrencyThreshold > 0;
		}

		boolean awaitCompletions(final long timeout, final TimeUnit unit) throws InterruptedException {
			return completionLatch.await(timeout, unit);
		}

		List<String> observedItemNames() {
			return new ArrayList<>(observedNames);
		}

		@Override
		protected void doShutdown() {
			executor.shutdown();
		}

		@Override
		protected void doClose() {
			executor.shutdownNow();
		}
	}
}
