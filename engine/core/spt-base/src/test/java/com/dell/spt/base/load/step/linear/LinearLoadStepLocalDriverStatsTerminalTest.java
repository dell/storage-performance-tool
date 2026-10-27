package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.AsyncRunnable;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock;
import com.dell.spt.base.storage.driver.mock.StatsReportingDriverMock.SelfCounting;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Which storage driver statistics a linear step keeps when it ends.
 *
 * <p>The step, its load generator, its metrics context and the metrics manager are the real ones;
 * only the storage driver is a test driver that counts for itself. The values kept must be the
 * ones the driver reported when the step stopped: after the wait for operations in flight, with
 * the driver stopped and not yet closed.
 */
@Timeout(60)
class LinearLoadStepLocalDriverStatsTerminalTest {

	private static final long WAIT_MILLIS = 20_000;

	private final Config config = TestConfigBuilder.config();
	private final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private LinearLoadStepLocal step;

	@BeforeEach
	void configure() {
		mgr.setTerminalRetentionMillis(30_000);
		config.val("load-step-id", "driver-stats-terminal-" + System.nanoTime());
		config.val("storage-driver-type", StatsReportingDriverMock.DRIVER);
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		// New items: the step creates them, it reads none.
		config.val("item-input-file", null);
		config.val("item-input-path", null);
		config.val("output-metrics-trace-persist", false);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 0);
	}

	@AfterEach
	void closeStep() throws Exception {
		if (step != null) {
			step.close();
		}
	}

	@Test
	void cancelledStepKeepsWhatItsDriverCountedWhenItStopped() throws Exception {
		final SelfCounting<DataItem, Operation<DataItem>> driver = new SelfCounting<>(Long.MAX_VALUE);
		config.val("load-op-limit-rate", 200);
		start(driver);
		await(() -> driver.resultsGiven() >= 5);

		// Cancelled by its caller while the generator still produces.
		step.close();

		final TerminalStepEntry entry = onlyEntry();
		final long given = driver.resultsGiven();
		assertTrue(given >= 5, "results given: " + given);
		assertEquals(StatsReportingDriverMock.stats(given, given, 0), entry.driverStats);
		assertEquals(given, entry.successCount, "every result the driver gave was counted by the step");
		assertReportedWhenStopped(driver, entry);
	}

	@Test
	void stepEndedByTheShutdownTimeoutKeepsWhatItsDriverCountedBeforeItClosed() throws Exception {
		// 8 operations, of which the driver answers 3 and holds 5 beyond the wait for operations in flight.
		final SelfCounting<DataItem, Operation<DataItem>> driver = new SelfCounting<>(3);
		config.val("load-op-limit-count", 8);
		config.val("load-op-wait-limit", 1);
		start(driver);
		await(() -> driver.accepted() == 8);

		final long stopStarted = System.nanoTime();
		step.stop();
		final long stopMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopStarted);

		assertTrue(stopMillis >= 900, "the step waited for the operations in flight: " + stopMillis + " ms");
		final TerminalStepEntry stopped = onlyEntry();
		assertEquals(StatsReportingDriverMock.stats(8, 3, 5), stopped.driverStats);
		assertEquals(3, stopped.successCount);
		assertReportedWhenStopped(driver, stopped);

		step.close();

		// The closed driver holds nothing; the step keeps the level from before the close.
		assertEquals(StatsReportingDriverMock.stats(8, 3, 0), driver.currentStats());
		assertEquals(StatsReportingDriverMock.stats(8, 3, 5), onlyEntry().driverStats);
	}

	@Test
	void stepInWhichNoOperationCompletedKeepsWhatItsDriverCounted() throws Exception {
		final SelfCounting<DataItem, Operation<DataItem>> driver = new SelfCounting<>(0);
		config.val("load-op-limit-count", 5);
		config.val("load-op-wait-limit", 0);
		start(driver);
		await(() -> driver.accepted() == 5);

		step.close();

		final TerminalStepEntry entry = onlyEntry();
		assertEquals(StatsReportingDriverMock.stats(5, 0, 5), entry.driverStats);
		assertEquals(0, entry.successCount);
		assertEquals(0, entry.failedCount);
		assertEquals(0, entry.bytesTotal);
		assertEquals(config.stringVal("load-step-id"), entry.stepId);
		assertReportedWhenStopped(driver, entry);
	}

	private void start(final SelfCounting<DataItem, Operation<DataItem>> driver) {
		step = new LinearLoadStepLocal(
						config,
						List.of(new StatsReportingDriverMock.Extension<>(driver), new DummyStorageDriverMockExtension<>()),
						List.of(),
						mgr);
		step.start();
		assertTrue(step.isStarted(), "the step must start: " + step.state());
	}

	/** The entry holds the driver's last answer, and that answer came from the stopped driver. */
	private static void assertReportedWhenStopped(
					final SelfCounting<DataItem, Operation<DataItem>> driver, final TerminalStepEntry entry) {
		final List<SelfCounting.Answer> answers = driver.answers();
		final SelfCounting.Answer last = answers.get(answers.size() - 1);
		assertEquals(AsyncRunnable.State.STOPPED, last.state(), answers.toString());
		assertEquals(last.stats(), entry.driverStats);
		final DriverStatsSnapshot now = driver.currentStats();
		assertEquals(now.counters(), entry.driverStats.counters(), "nothing was counted after the last answer");
	}

	private TerminalStepEntry onlyEntry() {
		final List<TerminalStepEntry> entries = mgr.getTerminalSteps();
		assertEquals(1, entries.size(), entries.toString());
		return entries.get(0);
	}

	private static void await(final BooleanSupplier condition) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
		while (!condition.getAsBoolean()) {
			assertTrue(System.nanoTime() < deadline, "condition not reached in " + WAIT_MILLIS + " ms");
			Thread.sleep(10);
		}
	}
}
