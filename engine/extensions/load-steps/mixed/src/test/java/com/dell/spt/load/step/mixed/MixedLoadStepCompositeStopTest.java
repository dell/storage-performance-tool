package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A timed mixed step that stops with a plain READ dispatched and never answered, next to a split
 * (composite) READ, ends with that one READ counted active, whatever became of the composite: the
 * result wrapper retires the composite once, at its last result, and the stop counts out only
 * operations the generator dispatched and the driver never sent. The first three cases are the
 * ones re-review 2 of {@code mixed-final-active-at-stop} probed, and the last three the ones
 * re-review 3 probed, each through a route an extension driver may take; on the release base each
 * ended at 1.
 */
@Timeout(120)
class MixedLoadStepCompositeStopTest {

	private static final long SMALL = 64;
	private static final long LARGE = 1024;

	@TempDir
	Path home;

	@BeforeEach
	void setUp() {
		ThreadContext.put("home_dir", home.toString());
	}

	@AfterEach
	void tearDown() {
		ThreadContext.remove("home_dir");
	}

	/** The composite publishes its kickoff, and the stop takes back its unsent ranges. */
	@Test
	void aCompositeLeftUnsentAfterItsKickoffCountsOutOnce() throws Exception {
		final ScriptedDriverFactory drivers = new ScriptedDriverFactory(Scenario.KICKOFF_ONLY);
		final Map<OpType, Long> concurrency = run(drivers);

		assertEquals(1, drivers.kickoffs.get(), "the composite's kickoff results");
		assertEquals(0, drivers.rangeResults.get(), "no range was sent");
		final OperationLifecycleCounters counters = drivers.counters();
		assertEquals(1, counters.unresolved(), "the dispatched plain READ is unresolved: " + counters);
		assertTrue(counters.unattempted() >= 3, "the composite and its two ranges are unattempted: " + counters);
		assertEquals(1L, concurrency.get(OpType.READ), "the unresolved plain READ stays active: " + concurrency);
	}

	/** The composite ends normally while three plain READs stay queued unsent. */
	@Test
	void aCompletedCompositeNextToHeldReadsLeavesTheUnresolvedReadActive() throws Exception {
		final ScriptedDriverFactory drivers = new ScriptedDriverFactory(Scenario.COMPLETED_COMPOSITE);
		final Map<OpType, Long> concurrency = run(drivers);

		assertEquals(1, drivers.kickoffs.get(), "the composite's kickoff results");
		assertEquals(2, drivers.rangeResults.get(), "the composite's range results");
		assertEquals(1, drivers.finalizations.get(), "the composite's final results");
		final OperationLifecycleCounters counters = drivers.counters();
		assertEquals(1, counters.unresolved(), "the dispatched plain READ is unresolved: " + counters);
		assertEquals(3, counters.unattempted(), "the three queued plain READs are unattempted: " + counters);
		assertEquals(1L, concurrency.get(OpType.READ), "the unresolved plain READ stays active: " + concurrency);
	}

	/**
	 * After the kickoff, the driver queues an ordinary READ of its own with the composite as its
	 * permit owner, through the cooperative enqueue API, and sends nothing more.
	 */
	@Test
	void aDriverCreatedFlatChildLeftUnsentDoesNotCountOut() throws Exception {
		final ScriptedDriverFactory drivers = new ScriptedDriverFactory(Scenario.FLAT_EXTENSION_CHILD);
		final Map<OpType, Long> concurrency = run(drivers);

		assertEquals(1, drivers.kickoffs.get(), "the composite's kickoff results");
		assertEquals(0, drivers.rangeResults.get(), "no range was sent");
		assertTrue(drivers.childQueued.get(), "the driver queued its own READ");
		final OperationLifecycleCounters counters = drivers.counters();
		assertEquals(1, counters.unresolved(), "the dispatched plain READ is unresolved: " + counters);
		assertTrue(counters.unattempted() >= 4, "the composite, its ranges and the child are unattempted: " + counters);
		assertEquals(1L, concurrency.get(OpType.READ), "the unresolved plain READ stays active: " + concurrency);
	}

	/**
	 * Before the kickoff, the driver queues an ordinary READ of its own with the composite as its
	 * permit owner, through the cooperative enqueue API, and sends it; it succeeds. A second plain
	 * READ stays queued, unsent. The child's result must neither count a READ out nor return a
	 * permit: the generator never counted it in.
	 */
	@Test
	void aDriverCreatedFlatChildThatCompletesDoesNotCountOut() throws Exception {
		final ScriptedDriverFactory drivers = new ScriptedDriverFactory(Scenario.COMPLETED_EXTENSION_CHILD);
		final Map<OpType, Long> concurrency = run(drivers);

		assertEquals(1, drivers.kickoffs.get(), "the composite's kickoff results");
		assertEquals(0, drivers.rangeResults.get(), "no range was sent");
		assertTrue(drivers.childQueued.get(), "the driver queued its own READ");
		assertEquals(1, drivers.childResults.get(), "the driver's own READ was sent and answered");
		final OperationLifecycleCounters counters = drivers.counters();
		assertEquals(1, counters.unresolved(), "the dispatched plain READ is unresolved: " + counters);
		assertEquals(4, counters.unattempted(),
						"the queued plain READ, the composite and its two ranges are unattempted: " + counters);
		assertEquals(1L, concurrency.get(OpType.READ), "the unresolved plain READ stays active: " + concurrency);
	}

	/**
	 * Before the kickoff, the driver writes an ordinary READ of its own straight into the protected
	 * child queue, as an extension written before the cooperative enqueue API may, and the
	 * dispatcher takes custody of it; it is never sent. Recovered at the stop, it must not count out.
	 */
	@Test
	void aFlatChildFromTheProtectedQueueLeftUnsentDoesNotCountOut() throws Exception {
		final ScriptedDriverFactory drivers = new ScriptedDriverFactory(Scenario.LEGACY_QUEUE_CHILD);
		final Map<OpType, Long> concurrency = run(drivers);

		assertEquals(1, drivers.kickoffs.get(), "the composite's kickoff results");
		assertEquals(0, drivers.rangeResults.get(), "no range was sent");
		assertTrue(drivers.childQueued.get(), "the driver queued its own READ");
		assertTrue(drivers.childSubmitted.get(), "the dispatcher took custody of the driver's own READ");
		assertEquals(0, drivers.childResults.get(), "the driver's own READ was never sent");
		final OperationLifecycleCounters counters = drivers.counters();
		assertEquals(1, counters.unresolved(), "the dispatched plain READ is unresolved: " + counters);
		assertEquals(4, counters.unattempted(), "the composite, its ranges and the child are unattempted: " + counters);
		assertEquals(1L, concurrency.get(OpType.READ), "the unresolved plain READ stays active: " + concurrency);
	}

	/**
	 * As the kickoff case, through a decorator that does not declare how the driver publishes
	 * composite results ({@code UNDECLARED}, the interface default). The result wrapper retires the
	 * composite at its kickoff, as before, so the stop must not count it out a second time.
	 */
	@Test
	void anUndeclaredDriverCompositeRetiredAtItsKickoffDoesNotCountOutAgain() throws Exception {
		final ScriptedDriverFactory drivers = new ScriptedDriverFactory(Scenario.UNDECLARED_KICKOFF);
		final Map<OpType, Long> concurrency = run(drivers);

		assertEquals(1, drivers.kickoffs.get(), "the composite's kickoff results");
		assertEquals(0, drivers.rangeResults.get(), "no range was sent");
		final OperationLifecycleCounters counters = drivers.counters();
		assertEquals(1, counters.unresolved(), "the dispatched plain READ is unresolved: " + counters);
		assertEquals(3, counters.unattempted(), "the composite and its two ranges are unattempted: " + counters);
		assertEquals(1L, concurrency.get(OpType.READ), "the unresolved plain READ stays active: " + concurrency);
	}

	private Map<OpType, Long> run(final ScriptedDriverFactory drivers) throws Exception {
		final String stepId = "mixed-composite-stop-" + UUID.randomUUID();
		final FinalCountsMetricsManager metrics = new FinalCountsMetricsManager();
		new MixedLoadStepClient(stepConfig(stepId, drivers.scenario), List.of(new MixedLoadStepExtension(), drivers), null,
						metrics).run();
		assertEquals(drivers.scenario.flats + 1, drivers.admitted.get(), "READs the driver admitted");
		return metrics.finalConcurrency(stepId);
	}

	private Config stepConfig(final String stepId, final Scenario scenario) throws Exception {
		final Config config = defaultConfig();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", "2s");
		config.val("load-op-limit-count", 0L);
		config.val("load-op-wait-limit", 1);
		config.val("load-batch-size", 1);
		config.val("load-op-weights-get", 10_000);
		config.val("load-op-weights-put", 0);
		config.val("load-op-weights-delete", 0);
		config.val("load-op-weights-stat", 1);
		config.val("storage-driver-type", ScriptedDriverFactory.TYPE);
		config.val("storage-driver-limit-concurrency", 16);
		config.val("storage-targetBucket-provision", false);
		config.val("item-data-size", "1KB");
		config.val("item-data-ranges-threshold", "512");
		config.val("item-output-path", "/bucket");
		final List<String> seedItems = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			seedItems.add(new DataItemImpl("/bucket/seed-" + i, 0, i % 2 == 0 ? SMALL : LARGE).toString());
		}
		config.val("item-input-file", Files.write(home.resolve("seeds-" + scenario + ".csv"), seedItems).toString());
		return config;
	}

	/** The shipped defaults of the engine and of this extension, as the engine merges them. */
	static Config defaultConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		return ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedLoadStepCompositeStopTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	enum Scenario {
		/** One plain READ, then one composite READ; no range is sent. */
		KICKOFF_ONLY(1),
		/** One plain READ, one composite READ, then three plain READs kept queued; the composite ends. */
		COMPLETED_COMPOSITE(4),
		/** As {@link #KICKOFF_ONLY}, and the driver queues a plain READ child after the kickoff. */
		FLAT_EXTENSION_CHILD(1),
		/**
		 * One plain READ, one composite READ, then one plain READ kept queued; before the kickoff the
		 * driver queues a plain READ child through the cooperative enqueue API and sends it.
		 */
		COMPLETED_EXTENSION_CHILD(2),
		/**
		 * As {@link #KICKOFF_ONLY}, and before the kickoff the driver writes a plain READ child into
		 * its protected child queue directly; the child is never sent.
		 */
		LEGACY_QUEUE_CHILD(1),
		/** As {@link #KICKOFF_ONLY}, through a decorator that leaves the composite results undeclared. */
		UNDECLARED_KICKOFF(1);

		/** The plain READs the driver admits: the first is sent and never answered. */
		final int flats;

		Scenario(final int flats) {
			this.flats = flats;
		}
	}

	/**
	 * The cooperative mock, admitting a fixed set of READs before lifecycle custody and refusing
	 * every other operation there. It sends the first plain READ and never answers it, and keeps
	 * every later plain READ queued, unsent.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	static final class ScriptedDriverFactory implements StorageDriverFactory {

		static final String TYPE = "composite-stop-scripted-mock";
		private static final long PARK_NANOS = TimeUnit.MILLISECONDS.toNanos(1);

		final Scenario scenario;
		final AtomicInteger admitted = new AtomicInteger();
		final AtomicInteger kickoffs = new AtomicInteger();
		final AtomicInteger rangeResults = new AtomicInteger();
		final AtomicInteger finalizations = new AtomicInteger();
		final AtomicReference<Boolean> childQueued = new AtomicReference<>(false);
		final AtomicReference<Boolean> childSubmitted = new AtomicReference<>(false);
		final AtomicInteger childResults = new AtomicInteger();
		private final AtomicReference<StorageDriver<?, ?>> created = new AtomicReference<>();

		ScriptedDriverFactory(final Scenario scenario) {
			this.scenario = scenario;
		}

		OperationLifecycleCounters counters() {
			return created.get().operationLifecycle().counters();
		}

		@Override
		public String id() {
			return TYPE;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}

		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public StorageDriver create(
						final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
						final int batchSize) {
			final StorageDriver driver = new CoopStorageDriverMock(stepId, dataInput, storageConfig, verifyFlag, batchSize) {

				private final AtomicInteger flatsAdmitted = new AtomicInteger();
				private final AtomicInteger compositesAdmitted = new AtomicInteger();
				private final AtomicInteger flatsSubmitted = new AtomicInteger();
				private final AtomicReference<Operation> child = new AtomicReference<>();

				@Override
				protected boolean offerIncomingOperationLocked(final Operation op, final BlockingQueue queue) {
					if (op.type() != OpType.READ) {
						return false;
					}
					if (op instanceof CompositeOperation) {
						// The composite comes second, after the first plain READ.
						if (flatsAdmitted.get() < 1 || compositesAdmitted.get() >= 1) {
							return false;
						}
						if (!super.offerIncomingOperationLocked(op, queue)) {
							return false;
						}
						compositesAdmitted.incrementAndGet();
					} else {
						final boolean next = flatsAdmitted.get() == 0
										|| (compositesAdmitted.get() == 1 && flatsAdmitted.get() < scenario.flats);
						if (!next || !super.offerIncomingOperationLocked(op, queue)) {
							return false;
						}
						flatsAdmitted.incrementAndGet();
					}
					admitted.incrementAndGet();
					return true;
				}

				@Override
				protected boolean submit(final Operation op) {
					if (op instanceof PartialOperation) {
						if (scenario != Scenario.COMPLETED_COMPOSITE) {
							return hold();
						}
						final boolean sent = super.submit(op);
						if (sent) {
							rangeResults.incrementAndGet();
						}
						return sent;
					}
					if (op instanceof CompositeOperation composite) {
						if (composite.allSubOperationsDone()) {
							// The read's finalization sends no request: publish it, keeping its timing.
							if (!beginDispatch(op)) {
								return false;
							}
							op.status(Operation.Status.SUCC);
							finalizations.incrementAndGet();
							handleCompleted(op);
							return true;
						}
						if (scenario == Scenario.COMPLETED_COMPOSITE && flatsAdmitted.get() < scenario.flats) {
							// Kick off only once the later plain READs are queued behind it.
							return hold();
						}
						if (child.get() == null) {
							// Queued before the kickoff, so that the child reaches the dispatcher ahead of
							// the ranges the kickoff queues.
							if (scenario == Scenario.COMPLETED_EXTENSION_CHILD) {
								final Operation flatChild = flatChild(op);
								child.set(flatChild);
								childQueued.set(enqueueChildOperation(flatChild, op, "extension child"));
							} else if (scenario == Scenario.LEGACY_QUEUE_CHILD) {
								final Operation flatChild = flatChild(op);
								child.set(flatChild);
								childQueued.set(childOpQueue.offer(flatChild));
							}
						}
						final boolean sent = super.submit(op);
						if (sent) {
							kickoffs.incrementAndGet();
							if (scenario == Scenario.FLAT_EXTENSION_CHILD) {
								childQueued.set(enqueueChildOperation(flatChild(op), op, "extension child"));
							}
						}
						return sent;
					}
					if (op == child.get()) {
						childSubmitted.set(true);
						if (scenario != Scenario.COMPLETED_EXTENSION_CHILD) {
							return hold();
						}
						final boolean sent = super.submit(op);
						if (sent) {
							childResults.incrementAndGet();
						}
						return sent;
					}
					if (flatsSubmitted.get() == 0 && op.item() instanceof DataItem item && sizeOf(item) == SMALL) {
						// The first plain READ crosses dispatch and is never answered.
						if (!beginDispatch(op)) {
							return false;
						}
						flatsSubmitted.incrementAndGet();
						return true;
					}
					return hold();
				}

				@Override
				protected int submit(final List ops, final int from, final int to) {
					var i = from;
					while (i < to && submit((Operation) ops.get(i))) {
						i++;
					}
					return i - from;
				}
			};
			created.set(driver);
			return scenario == Scenario.UNDECLARED_KICKOFF ? undeclared(driver) : driver;
		}

		/**
		 * An interface decorator, as a driver author might write one: it forwards every method to
		 * {@code driver} except the composite results declaration, so it has the interface's default,
		 * {@code UNDECLARED}.
		 */
		private static StorageDriver undeclared(final StorageDriver driver) {
			return (StorageDriver) Proxy.newProxyInstance(
							StorageDriver.class.getClassLoader(), new Class<?>[]{StorageDriver.class
							},
							(proxy, method, args) -> {
								if ("compositeResults".equals(method.getName())) {
									return InvocationHandler.invokeDefault(proxy, method, args);
								}
								try {
									return method.invoke(driver, args);
								} catch (final InvocationTargetException e) {
									throw e.getCause();
								}
							});
		}

		/** Declines a dispatch, parked, so the dispatcher retrying it does not spin. */
		private static boolean hold() {
			LockSupport.parkNanos(PARK_NANOS);
			return false;
		}

		private static long sizeOf(final DataItem item) {
			try {
				return item.size();
			} catch (final IOException e) {
				return -1;
			}
		}

		private static Operation flatChild(final Operation composite) {
			return new DataOperationImpl<>(composite.originIndex(), OpType.READ, (DataItem) composite.item(),
							composite.srcPath(), composite.dstPath(), composite.credential(), null, 0);
		}
	}
}
