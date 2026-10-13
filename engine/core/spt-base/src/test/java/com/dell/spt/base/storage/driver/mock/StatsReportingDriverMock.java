package com.dell.spt.base.storage.driver.mock;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.concurrent.AsyncRunnable;
import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Test-only storage driver whose statistics have nothing in common with any shipped driver.
 *
 * <p>It is the dummy mock driver with {@link StorageDriver#driverStats()} answered by the test, so
 * the core can be shown to carry statistics it knows nothing about.
 */
public final class StatsReportingDriverMock {

	public static final String DRIVER = "stats-mock";
	public static final String COUNTER_WIDGETS = "widgets_made";
	public static final String COUNTER_SPROCKETS = "sprockets_bent";
	public static final String GAUGE_SHELF = "shelf_load";

	private static final String STATS_METHOD = "driverStats";

	private StatsReportingDriverMock() {}

	/** Statistics with the given values under this driver's names. */
	public static DriverStatsSnapshot stats(final long widgets, final long sprockets, final long shelf) {
		return DriverStatsSnapshot.builder(DRIVER)
						.counter(COUNTER_WIDGETS, widgets)
						.counter(COUNTER_SPROCKETS, sprockets)
						.gauge(GAUGE_SHELF, shelf)
						.build();
	}

	/** A working driver that answers {@code driverStats()} from the supplier. */
	@SuppressWarnings("unchecked")
	public static <I extends Item, O extends Operation<I>> StorageDriver<I, O> create(
					final Supplier<DriverStatsSnapshot> stats) {
		final StorageDriver<I, O> delegate = DummyStorageDriverMock.create();
		return (StorageDriver<I, O>) Proxy.newProxyInstance(
						StorageDriver.class.getClassLoader(),
						new Class<?>[]{StorageDriver.class
						},
						(proxy, method, args) -> {
							if (STATS_METHOD.equals(method.getName()) && method.getParameterCount() == 0) {
								return stats.get();
							}
							try {
								return method.invoke(delegate, args);
							} catch (final InvocationTargetException e) {
								throw e.getCause();
							}
						});
	}

	/**
	 * A working driver that counts for itself, to show which of its values a finished step keeps.
	 *
	 * <p>{@code widgets_made} is the operations it accepted, {@code sprockets_bent} the results it
	 * gave and {@code shelf_load} the operations it still holds. It gives a result for the first
	 * {@code resultLimit} operations and holds every later one, like a driver whose storage stopped
	 * answering. Closing it drops what it holds, so the gauge of a closed driver is 0.
	 */
	public static final class SelfCounting<I extends Item, O extends Operation<I>> extends AsyncRunnableBase
					implements StorageDriver<I, O> {

		/** What the driver answered to one {@code driverStats()} call, and in which state. */
		public record Answer(AsyncRunnable.State state, DriverStatsSnapshot stats) {}

		private final DummyStorageDriverMock<I, O> completing = DummyStorageDriverMock.create();
		private final long resultLimit;
		private final AtomicLong accepted = new AtomicLong();
		private final AtomicLong resultsGiven = new AtomicLong();
		private final List<Answer> answers = new CopyOnWriteArrayList<>();

		public SelfCounting(final long resultLimit) {
			this.resultLimit = resultLimit;
		}

		public long accepted() {
			return accepted.get();
		}

		public long resultsGiven() {
			return resultsGiven.get();
		}

		/** Every answer to {@code driverStats()} so far, in order. */
		public List<Answer> answers() {
			return answers;
		}

		@Override
		public DriverStatsSnapshot driverStats() {
			final Answer answer = new Answer(state(), currentStats());
			answers.add(answer);
			return answer.stats();
		}

		/** The statistics now, without recording an answer. */
		public DriverStatsSnapshot currentStats() {
			final long given = resultsGiven.get();
			return stats(accepted.get(), given, isClosed() ? 0 : accepted.get() - given);
		}

		@Override
		public synchronized boolean put(final O op) {
			if (!isStarted()) {
				throwUnchecked(new EOFException());
			}
			if (resultsGiven.get() >= resultLimit) {
				// Held: accepted, and never answered.
				accepted.incrementAndGet();
				return true;
			}
			if (completing.put(op)) {
				accepted.incrementAndGet();
				resultsGiven.incrementAndGet();
				return true;
			}
			return false;
		}

		@Override
		public synchronized int put(final List<O> ops, final int from, final int to) {
			int next = from;
			while (next < to && put(ops.get(next))) {
				next++;
			}
			return next - from;
		}

		@Override
		public int put(final List<O> ops) {
			return put(ops, 0, ops.size());
		}

		@Override
		public Input<O> getInput() {
			throw new AssertionError();
		}

		@Override
		public void operationResultOutput(final Output<O> opResultOut) {
			completing.operationResultOutput(opResultOut);
		}

		@Override
		public List<I> list(
						final ItemFactory<I> itemFactory,
						final String path,
						final String prefix,
						final int idRadix,
						final I lastPrevItem,
						final int count) {
			return List.of();
		}

		@Override
		public int concurrencyLimit() {
			return completing.concurrencyLimit();
		}

		@Override
		public int activeOpCount() {
			return (int) (accepted.get() - resultsGiven.get());
		}

		@Override
		public long scheduledOpCount() {
			return accepted.get();
		}

		@Override
		public long completedOpCount() {
			return resultsGiven.get();
		}

		@Override
		public boolean isIdle() {
			return activeOpCount() == 0;
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

		@Override
		protected void doStart() {
			completing.start();
		}

		@Override
		protected void doShutdown() {
			completing.shutdown();
		}

		@Override
		protected void doStop() {
			completing.stop();
		}

		@Override
		protected void doClose() throws IOException {
			completing.close();
		}
	}

	/** Makes the driver selectable as {@code storage.driver.type = stats-mock}. */
	public static final class Extension<I extends Item, O extends Operation<I>> extends ExtensionBase
					implements StorageDriverFactory<I, O, StorageDriver<I, O>> {

		private final Supplier<StorageDriver<I, O>> drivers;
		private final List<StorageDriver<I, O>> created = new CopyOnWriteArrayList<>();

		/** Every driver created answers {@code driverStats()} from the supplier. */
		public Extension(final Supplier<DriverStatsSnapshot> stats) {
			this.drivers = () -> StatsReportingDriverMock.create(stats);
		}

		/** The one driver created is the given one. */
		public Extension(final StorageDriver<I, O> driver) {
			this.drivers = () -> driver;
		}

		/** The drivers created so far, in order. */
		public List<StorageDriver<I, O>> created() {
			return created;
		}

		@Override
		public String id() {
			return DRIVER;
		}

		@Override
		public StorageDriver<I, O> create(
						final String stepId,
						final DataInput dataInput,
						final Config storageConfig,
						final boolean verifyFlag,
						final int batchSize)
						throws IllegalConfigurationException {
			final StorageDriver<I, O> driver = drivers.get();
			created.add(driver);
			return driver;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		protected String defaultsFileName() {
			return null;
		}

		@Override
		protected List<String> resourceFilesToInstall() {
			return List.of();
		}
	}
}
