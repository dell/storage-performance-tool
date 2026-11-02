package com.dell.spt.base.nic.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.pool.PoolPreparation.Participant;
import com.dell.spt.base.storage.driver.AdmissionPreparable;
import com.dell.spt.base.storage.driver.PoolDemandDriver;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A step's pool work against a stub driver: the checks, the policy, T3, and what each case pre-warms. */
final class PoolPreparationTest {

	private static final long MIB = 1L << 20;
	private static final long GIB = 1L << 30;
	private static final PoolPreparation.Environment NO_ESTIMATE = new PoolPreparation.Environment(System::nanoTime, name -> null);

	private static class StubDriver implements PoolDemandDriver {
		final Layout layout;
		final List<PrewarmOrder> orders = new ArrayList<>();
		String failNic;
		boolean stop;

		StubDriver(final int concurrency, final long limit, final boolean fail, final String... nics) {
			this(MIB, concurrency, limit, fail, nics);
		}

		StubDriver(final long threshold, final int concurrency, final long limit, final boolean fail, final String... nics) {
			layout = new Layout("host-a", limit, concurrency, threshold, 0, 0, fail, List.of(nics));
		}

		@Override
		public Layout poolLayout() {
			return layout;
		}

		@Override
		public List<CompletableFuture<PrewarmResult>> startPrewarm(final List<PrewarmOrder> given) {
			orders.addAll(given);
			return given.stream().map(order -> CompletableFuture.completedFuture(result(order))).toList();
		}

		private PrewarmResult result(final PrewarmOrder order) {
			if (stop) {
				return new PrewarmResult(order.nic(), 0, 0, null, true);
			}
			if (order.nic().equals(failNic)) {
				return new PrewarmResult(order.nic(), 1, 3, "buffer registration failed", false);
			}
			return new PrewarmResult(order.nic(), order.buffers(), 7, null, false);
		}
	}

	private static class StubPreparation implements AdmissionPreparable.Preparation {
		final List<String> phases = new ArrayList<>();

		@Override
		public long deadlineNanos() {
			return System.nanoTime() + 60_000_000_000L;
		}

		@Override
		public boolean fenced() {
			return false;
		}

		@Override
		public void phase(final String phase) {
			phases.add(phase);
		}

		@Override
		public void onFence(final Runnable action) {}
	}

	private static final class MessageLog extends AbstractAppender implements AutoCloseable {
		final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final org.apache.logging.log4j.core.Logger logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		private final Level previous = logger.getLevel();

		MessageLog() {
			super("poolPreparationLog", null, null, true, Property.EMPTY_ARRAY);
			start();
			logger.addAppender(this);
			logger.setLevel(Level.INFO);
		}

		@Override
		public void append(final LogEvent event) {
			captured.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
		}

		long count(final String text) throws InterruptedException {
			// The engine's loggers are asynchronous.
			final long deadline = System.nanoTime() + 3_000_000_000L;
			Loggers.MSG.warn("pool-preparation-test-marker");
			while (System.nanoTime() < deadline && captured.stream().noneMatch(line -> line.contains("pool-preparation-test-marker"))) {
				Thread.sleep(5);
			}
			synchronized (captured) {
				return captured.stream().filter(line -> line.contains(text)).count();
			}
		}

		@Override
		public void close() {
			logger.removeAppender(this);
			logger.setLevel(previous);
			stop();
		}
	}

	private static Config root(final String op, final String size) {
		final Config config = TestConfigBuilder.config();
		config.val("load-op-type", op);
		config.val("item-type", "data");
		config.val("item-data-size", size);
		config.val("item-data-ranges-threshold", 0);
		config.val("item-data-ranges-random", 0);
		return config;
	}

	private static PoolPreparation.Report run(final StubDriver driver, final Config config) throws Exception {
		return PoolPreparation.run("step-1", List.of(new Participant(0, driver, config)), new StubPreparation(), NO_ESTIMATE);
	}

	@Test
	void aZeroByteTransferUsesTheSmallestClassAsThePoolDoes() throws Exception {
		final var driver = new StubDriver(0, 96, 192 * GIB, true, "a", "b");
		run(driver, root("create", "0"));
		assertEquals(2, driver.orders.size());
		driver.orders.forEach(order -> assertEquals(PoolSizeClass.MIN_BYTES, order.sizeClass()));
	}

	@Test
	void aFixedSizeWarmsEachNicsMinimumInItsClassOnEveryLane() throws Exception {
		final var driver = new StubDriver(96, 192 * GIB, true, "a", "b", "c", "d");
		final var report = run(driver, root("create", "1GiB"));
		assertEquals(4, driver.orders.size());
		driver.orders.forEach(order -> {
			assertEquals(24, order.buffers());
			assertEquals((int) GIB, order.sizeClass());
		});
		final var nic = report.drivers().get(0).nics().get(0);
		assertEquals(24, nic.minimum());
		assertEquals(25, nic.cap());
		assertEquals(1, nic.headroom());
		assertEquals(24, nic.prewarmed());
		assertEquals(7, nic.prewarmMillis());
		assertEquals(24 * GIB, nic.budgetBytes());
		assertEquals(7, report.prewarmMillis(), "the record keeps the pre-warm time");
		assertTrue(report.check().passed());
		assertEquals(192 * GIB, report.requiredLimitBytes());
	}

	@Test
	void aRangeInOneClassAndListedSizesInOneClassAreWarmed(@TempDir final Path dir) throws Exception {
		final var range = new StubDriver(4, 64 * GIB, true, "a", "b");
		run(range, root("create", "3MiB-4MiB"));
		assertEquals(List.of(new PoolDemandDriver.PrewarmOrder("a", 2, 4 << 20), new PoolDemandDriver.PrewarmOrder("b", 2, 4 << 20)), range.orders);

		final var file = dir.resolve("items.csv");
		Files.writeString(file, "x,0,3145728,0/0\ny,0,4194304,0/0\n");
		final var config = root("read", "1MiB");
		config.val("item-input-file", file.toString());
		final var listed = new StubDriver(4, 64 * GIB, true, "a", "b");
		final var report = run(listed, config);
		assertEquals(2, listed.orders.size());
		assertEquals(4 << 20, listed.orders.get(0).sizeClass());
		assertEquals(file.toString(), report.drivers().get(0).demand().contributions().get(0).source());
	}

	@Test
	void zeroDemandRegistersNothing() throws Exception {
		final var driver = new StubDriver(4, 64 * GIB, true, "a", "b");
		final var report = run(driver, root("delete", "1GiB"));
		assertTrue(driver.orders.isEmpty());
		assertEquals(0, report.requiredLimitBytes());
		assertEquals("no demand", report.drivers().get(0).nics().get(0).prewarmNote());
	}

	@Test
	void aRangeSpanningClassesUnknownDemandAndTheSharedModeSkipWithALineSayingColdPools() throws Exception {
		try (final var log = new MessageLog()) {
			final var spanning = new StubDriver(4, 64 * GIB, true, "a", "b");
			run(spanning, root("create", "2MiB-1GiB"));
			assertTrue(spanning.orders.isEmpty());

			final var shared = new StubDriver(1, 64 * GIB, true, "a", "b");
			run(shared, root("create", "4MiB"));
			assertTrue(shared.orders.isEmpty());

			final var unlimited = new StubDriver(0, 64 * GIB, true, "a", "b");
			run(unlimited, root("create", "4MiB"));
			assertTrue(unlimited.orders.isEmpty());

			assertTrue(log.count("its sizes span size classes; the load starts with cold pools") >= 2, log.captured::toString);
			assertTrue(log.count("the shared mode; the load starts with cold pools") >= 2, log.captured::toString);
			assertTrue(log.count("its demand is unknown; the load starts with cold pools") >= 2, log.captured::toString);
		}
	}

	@Test
	void aShortBudgetFailsTheStepNamingTheHostTheNicBothAmountsAndTheSetting() {
		final var driver = new StubDriver(96, 64 * GIB, true, "a", "b", "c", "d");
		final var failure = assertThrows(RunTerminalException.class, () -> run(driver, root("create", "1GiB")));
		final var message = failure.getMessage();
		assertTrue(message.contains("host-a") && message.contains("NIC a") && message.contains("24GiB") && message.contains("8GiB"), message);
		assertTrue(message.contains("storage.rdma.poolShortfall=warn") && message.contains("MaxDirectMemorySize"), message);
		assertTrue(driver.orders.isEmpty(), "nothing registers before a step that fails its check");
	}

	@Test
	void aShortBudgetWarnsOnceUnderWarnAndTheRunGoesOnWarmingWhatFits() throws Exception {
		final var driver = new StubDriver(96, 64 * GIB, false, "a", "b", "c", "d");
		try (final var log = new MessageLog()) {
			final var report = run(driver, root("create", "1GiB"));
			assertFalse(report.check().passed());
			assertEquals(1, log.count("WARN Step step-1 on host host-a: NIC a's pool needs 24GiB but has 8GiB"), log.captured::toString);
			assertEquals(4, driver.orders.size());
			driver.orders.forEach(order -> assertEquals(8, order.buffers(), "its budget refuses the rest"));
		}
	}

	@Test
	void aPipelineOfTwoDriversReportsTheTwoResultsApart() throws Exception {
		final var one = new StubDriver(24, 48 * GIB, true, "a", "b");
		final var two = new StubDriver(24, 48 * GIB, true, "c", "d");
		final var participants = List.of(new Participant(0, one, root("create", "1GiB")), new Participant(1, two, root("read", "1GiB")));
		final var failure = assertThrows(RunTerminalException.class,
						() -> PoolPreparation.run("step-1", participants, new StubPreparation(), NO_ESTIMATE));
		assertTrue(failure.getMessage().contains("concurrent RDMA drivers need 48GiB plus 8GiB headroom"), failure.getMessage());
		assertTrue(failure.getMessage().contains("-XX:MaxDirectMemorySize=" + 56 * GIB), failure.getMessage());

		final var warned = List.of(
						new Participant(0, new StubDriver(24, 48 * GIB, false, "a", "b"), root("create", "1GiB")),
						new Participant(1, new StubDriver(24, 48 * GIB, false, "c", "d"), root("read", "1GiB")));
		final var report = PoolPreparation.run("step-1", warned, new StubPreparation(), NO_ESTIMATE);
		assertTrue(report.check().poolsShort().isEmpty());
		assertTrue(report.check().processShort());
		assertEquals(56 * GIB, report.requiredLimitBytes());
		assertEquals(2, report.drivers().size());
	}

	@Test
	void aKnownDriverBesideAnUnknownOneKeepsItsRequirementAndReportsAPartialCheck() throws Exception {
		final var known = new StubDriver(24, 192 * GIB, true, "a", "b");
		final var unknown = new StubDriver(24, 192 * GIB, true, "c", "d");
		final var listing = root("read", "1GiB");
		listing.val("item-input-path", "/bucket");
		try (final var log = new MessageLog()) {
			final var report = PoolPreparation.run("step-1",
							List.of(new Participant(0, known, root("create", "1GiB")), new Participant(1, unknown, listing)),
							new StubPreparation(), NO_ESTIMATE);
			assertTrue(report.partial());
			assertEquals(48 * GIB, report.requiredLimitBytes());
			assertEquals(List.of("GET: its items come from the bucket listing /bucket"), report.unknown());
			assertEquals(1, log.count("the RDMA pool check on host-a is partial"), log.captured::toString);
			assertEquals(2, known.orders.size(), "the known driver still warms");
			assertTrue(unknown.orders.isEmpty());
		}
	}

	@Test
	void aKnownStepFollowedByAnUnknownOneKeepsTheKnownStepsResult() throws Exception {
		final var first = new StubDriver(96, 64 * GIB, true, "a", "b", "c", "d");
		assertThrows(RunTerminalException.class, () -> run(first, root("create", "1GiB")), "the known step is checked and fails");
		final var second = new StubDriver(96, 64 * GIB, true, "a", "b", "c", "d");
		final var listing = root("read", "1GiB");
		listing.val("item-input-path", "/bucket");
		try (final var log = new MessageLog()) {
			final var report = run(second, listing);
			assertEquals(0, report.requiredLimitBytes(), "nothing known in this step");
			// Narrowed to this step's reason: the engine's loggers are asynchronous, so another test's line can land here.
			assertEquals(1, log.count("cannot check the RDMA pool budget on host-a: GET: its"), log.captured::toString);
		}
	}

	@Test
	void anItemFileThatCannotBeReadWithinItsBudgetIsUnknown(@TempDir final Path dir) throws Exception {
		final var file = dir.resolve("items.csv");
		Files.writeString(file, "x,0,4194304,0/0\n");
		final var config = root("read", "1MiB");
		config.val("item-input-file", file.toString());
		final var driver = new StubDriver(4, 64 * GIB, true, "a", "b");
		final var preparation = new StubPreparation() {
			@Override
			public long deadlineNanos() {
				return System.nanoTime() - 1;
			}
		};
		final var report = PoolPreparation.run("step-1", List.of(new Participant(0, driver, config)), preparation, NO_ESTIMATE);
		assertTrue(report.partial());
		assertTrue(report.unknown().get(0).contains("its read budget expired"), report.unknown()::toString);
		assertTrue(driver.orders.isEmpty());
	}

	@Test
	void aLocalEngineReadsItsFileOnceAndLogsTheTime(@TempDir final Path dir) throws Exception {
		final var file = dir.resolve("items.csv");
		Files.writeString(file, "x,0,4194304,0/0\ny,0,3145728,0/0\n");
		final var config = root("read", "1MiB");
		config.val("item-input-file", file.toString());
		try (final var log = new MessageLog()) {
			run(new StubDriver(4, 64 * GIB, true, "a", "b"), config);
			assertEquals(1, log.count("read the sizes in item file \"" + file), log.captured::toString);
			assertTrue(log.count("2 items, 3MiB to 4MiB") >= 1, log.captured::toString);
		}
	}

	@Test
	void aSliceWithASummaryIsNotReadAgain(@TempDir final Path dir) throws Exception {
		final var missing = dir.resolve("never-read.bin");
		final var recorder = new ItemSizeSummary.Recorder("items.csv");
		recorder.record(4 * MIB);
		recorder.finish();
		final var config = root("read", "1MiB");
		config.val("item-input-file", missing.toString());
		config.val(ItemSizeSummary.CONFIG_PATH, recorder.summary().encode());
		final var driver = new StubDriver(4, 64 * GIB, true, "a", "b");
		final var report = run(driver, config);
		assertEquals("items.csv", report.drivers().get(0).demand().contributions().get(0).source(), "from the summary; the slice was not opened");
		assertEquals(2, driver.orders.size());
	}

	@Test
	void anEstimateThatDiffersFromTheEnginesPlanIsLoggedAndTheEnginesFigureDecides() throws Exception {
		final var driver = new StubDriver(96, 192 * GIB, true, "a", "b", "c", "d");
		final var estimate = new PoolPreparation.Environment(System::nanoTime,
						Map.of(PoolPreparation.ESTIMATE_ENV, "other=1,step-1=" + 100 * GIB)::get);
		try (final var log = new MessageLog()) {
			final var report = PoolPreparation.run("step-1", List.of(new Participant(0, driver, root("create", "1GiB"))), new StubPreparation(), estimate);
			assertEquals(1, log.count("pool demand discrepancy"), log.captured::toString);
			assertTrue(log.count("the CLI estimated this step needs 100GiB of direct memory, this engine's plan needs 192GiB") >= 1, log.captured::toString);
			assertEquals(192 * GIB, report.requiredLimitBytes());
			assertEquals(100 * GIB, report.estimateBytes());
		}
		final var same = new PoolPreparation.Environment(System::nanoTime, Map.of(PoolPreparation.ESTIMATE_ENV, "step-1=" + 192 * GIB)::get);
		try (final var log = new MessageLog()) {
			PoolPreparation.run("step-1",
							List.of(new Participant(0, new StubDriver(96, 192 * GIB, true, "a", "b", "c", "d"), root("create", "1GiB"))),
							new StubPreparation(), same);
			assertEquals(0, log.count("pool demand discrepancy"), log.captured::toString);
		}
	}

	@Test
	void aRegistrationFailureDuringPreWarmIsAShortPool() throws Exception {
		final var failing = new StubDriver(4, 64 * GIB, true, "a", "b");
		failing.failNic = "b";
		final var failure = assertThrows(RunTerminalException.class, () -> run(failing, root("create", "4MiB")));
		assertTrue(failure.getMessage().contains("NIC b's pool pre-warm stopped at 1 buffers: buffer registration failed"), failure.getMessage());

		final var warned = new StubDriver(4, 64 * GIB, false, "a", "b");
		warned.failNic = "b";
		final var report = run(warned, root("create", "4MiB"));
		assertEquals(1, report.drivers().get(0).nics().get(1).prewarmed(), "under warn it keeps what was registered");
		assertEquals("buffer registration failed", report.drivers().get(0).nics().get(1).prewarmNote());
	}

	@Test
	void aStopDuringPreWarmEndsThePreparation() {
		final var stopped = new StubDriver(4, 64 * GIB, true, "a", "b");
		stopped.stop = true;
		assertThrows(InterruptedException.class, () -> run(stopped, root("create", "4MiB")));
	}

	@Test
	void aDriverWithoutNicPoolsAddsNothing() throws Exception {
		final PoolDemandDriver none = new StubDriver(4, 64 * GIB, true, "a") {
			@Override
			public Layout poolLayout() {
				return null;
			}
		};
		assertNull(PoolPreparation.run("step-1", List.of(new Participant(0, none, root("create", "1GiB"))), new StubPreparation(), NO_ESTIMATE));
	}

	@Test
	void theEstimateParserIgnoresMalformedEntries() {
		assertEquals(5, PoolPreparation.estimateFor("s", "x=y,s=5"));
		assertEquals(-1, PoolPreparation.estimateFor("s", "s=oops"));
		assertEquals(-1, PoolPreparation.estimateFor("s", null));
	}
}
