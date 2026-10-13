package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Which metrics context reports a driver's statistics.
 *
 * <p>Every local step type builds one {@link LoadStepContextImpl} per storage driver, and that is
 * the only place where the statistics are attached to a metrics context. Linear, pipeline and
 * weighted steps give it one metrics context per driver; a mixed step gives it one driver, the
 * first operation type's context as the primary and all of them by operation type.
 */
class LoadStepContextDriverStatsTest {

	private final Config config = TestConfigBuilder.config();
	private final List<MetricsContext<AllMetricsSnapshot>> contexts = new ArrayList<>();
	private final List<AutoCloseable> closeables = new ArrayList<>();

	@AfterEach
	void close() throws Exception {
		for (final AutoCloseable closeable : closeables) {
			closeable.close();
		}
		contexts.forEach(MetricsContext::close);
	}

	@Test
	void stepWithOneMetricsContextPerDriverReportsEachDriverOnItsOwnContext() throws Exception {
		// The shape of linear steps (one driver) and of pipeline and weighted steps (one per sub-step).
		final AtomicLong firstWidgets = new AtomicLong(3);
		final AtomicLong secondWidgets = new AtomicLong(50);
		final MetricsContext<AllMetricsSnapshot> first = metricsContext(OpType.CREATE);
		final MetricsContext<AllMetricsSnapshot> second = metricsContext(OpType.READ);

		stepContext(StatsReportingDriverMock.create(() -> StatsReportingDriverMock.stats(firstWidgets.get(), 0, 0)), first, null);
		stepContext(StatsReportingDriverMock.create(() -> StatsReportingDriverMock.stats(secondWidgets.get(), 0, 0)), second, null);

		assertEquals(3, widgets(first));
		assertEquals(50, widgets(second));
		firstWidgets.set(4);
		assertEquals(4, widgets(first));
		assertEquals(50, widgets(second));
	}

	@Test
	void mixedStepReportsItsOneDriverOnThePrimaryContextOnly() throws Exception {
		final Map<OpType, MetricsContext> byOpType = new EnumMap<>(OpType.class);
		final MetricsContext<AllMetricsSnapshot> read = metricsContext(OpType.READ);
		final MetricsContext<AllMetricsSnapshot> create = metricsContext(OpType.CREATE);
		final MetricsContext<AllMetricsSnapshot> delete = metricsContext(OpType.DELETE);
		final MetricsContext<AllMetricsSnapshot> stat = metricsContext(OpType.STAT);
		byOpType.put(OpType.READ, read);
		byOpType.put(OpType.CREATE, create);
		byOpType.put(OpType.DELETE, delete);
		byOpType.put(OpType.STAT, stat);

		stepContext(StatsReportingDriverMock.create(() -> StatsReportingDriverMock.stats(8, 0, 0)), read, byOpType);

		assertEquals(8, widgets(read));
		for (final MetricsContext<AllMetricsSnapshot> other : List.of(create, delete, stat)) {
			assertFalse(other.metadata().containsKey(MetricsConstants.METADATA_DRIVER_STATS), other.toString());
			other.refreshLastSnapshot(true);
			assertNull(other.lastSnapshot().driverStats(), other.toString());
		}
	}

	@Test
	void driverWithoutStatisticsLeavesItsSnapshotsWithoutThem() throws Exception {
		final MetricsContext<AllMetricsSnapshot> ctx = metricsContext(OpType.CREATE);

		stepContext(DummyStorageDriverMock.create(), ctx, null);

		ctx.refreshLastSnapshot(true);
		assertNull(ctx.lastSnapshot().driverStats());
	}

	@Test
	void linearStepReportsItsDriverOnItsOneContext() throws Exception {
		final var extension = new StatsReportingDriverMock.Extension<>(() -> StatsReportingDriverMock.stats(12, 1, 6));
		final Path itemInput = Files.createTempFile("spt-driver-stats-items-", ".csv");
		closeables.add(() -> Files.deleteIfExists(itemInput));
		config.val("load-step-id", "driver-stats-linear-" + System.nanoTime());
		config.val("storage-driver-type", StatsReportingDriverMock.DRIVER);
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("item-input-file", itemInput.toString());
		final ProbeLinearLoadStepLocal step = new ProbeLinearLoadStepLocal(
						config, List.of(extension, new DummyStorageDriverMockExtension<>()), mock(MetricsManager.class));
		closeables.add(0, step);

		step.initForTest();

		assertEquals(1, extension.created().size());
		final List<MetricsContext<? extends AllMetricsSnapshot>> carriers = step.metricsContextsForTest().stream()
						.filter(ctx -> ctx.metadata().containsKey(MetricsConstants.METADATA_DRIVER_STATS))
						.toList();
		assertEquals(1, carriers.size(), "one driver must report on one metrics context");
		carriers.get(0).refreshLastSnapshot(true);
		assertEquals(StatsReportingDriverMock.stats(12, 1, 6), carriers.get(0).lastSnapshot().driverStats());
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private void stepContext(
					final StorageDriver<DataItem, Operation<DataItem>> driver,
					final MetricsContext<AllMetricsSnapshot> primary,
					final Map<OpType, MetricsContext> byOpType) throws Exception {
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		final LoadStepContextImpl<DataItem, Operation<DataItem>> stepContext = new LoadStepContextImpl<>(
						"driver-stats-step", generator, driver, primary, byOpType, config.configVal("load"), false,
						ListShardMetricsRecorder.NO_OP);
		closeables.add(stepContext);
		assertTrue(primary.metadata().containsKey(MetricsConstants.METADATA_DRIVER_STATS));
	}

	private static long widgets(final MetricsContext<AllMetricsSnapshot> ctx) {
		ctx.refreshLastSnapshot(true);
		final DriverStatsSnapshot stats = ctx.lastSnapshot().driverStats();
		return stats.counters().get(StatsReportingDriverMock.COUNTER_WIDGETS);
	}

	private MetricsContext<AllMetricsSnapshot> metricsContext(final OpType opType) {
		final MetricsContext<AllMetricsSnapshot> ctx = MetricsContextImpl.builder()
						.loadStepId("driver-stats-step")
						.opType(opType)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		ctx.start();
		contexts.add(ctx);
		return ctx;
	}

	private static final class ProbeLinearLoadStepLocal extends LinearLoadStepLocal {
		ProbeLinearLoadStepLocal(
						final Config baseConfig, final List<Extension> extensions, final MetricsManager metricsManager) {
			super(baseConfig, extensions, List.of(), metricsManager);
		}

		void initForTest() {
			init();
		}

		List<MetricsContext<? extends AllMetricsSnapshot>> metricsContextsForTest() {
			return metricsContexts;
		}
	}
}
