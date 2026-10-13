package com.dell.spt.base.storage.driver.mock;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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

	/** Makes the driver selectable as {@code storage.driver.type = stats-mock}. */
	public static final class Extension<I extends Item, O extends Operation<I>> extends ExtensionBase
					implements StorageDriverFactory<I, O, StorageDriver<I, O>> {

		private final Supplier<DriverStatsSnapshot> stats;
		private final List<StorageDriver<I, O>> created = new CopyOnWriteArrayList<>();

		public Extension(final Supplier<DriverStatsSnapshot> stats) {
			this.stats = stats;
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
			final StorageDriver<I, O> driver = StatsReportingDriverMock.create(stats);
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
