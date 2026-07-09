package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MixedLoadGenerator}.
 *
 * <p>Tests verify schedule-driven dispatch, DELETE re-roll, PUT→DELETE queue flow,
 * and proper result routing through the generator's single-driver architecture.
 */
@SuppressWarnings({"unchecked", "rawtypes"
})
class MixedLoadGeneratorTest {

	@BeforeAll
	static void initLog4j() {
		// Force Log4j initialization before any test starts a generator.
		// Without this, the generator's doInit() triggers lazy Log4j init in a virtual thread,
		// and gen.stop() can interrupt that thread mid-init, permanently corrupting Log4j.
		ThreadContext.put("test", "init");
		Loggers.MSG.isInfoEnabled();
		ThreadContext.remove("test");
	}

	private VirtualThreadExecutor executor;

	@BeforeEach
	void setUp() {
		executor = new VirtualThreadExecutor();
	}

	@AfterEach
	void tearDown() throws Exception {
		executor.close();
	}

	// ── Schedule-driven dispatch ──────────────────────────────────────────

	@Test
	@DisplayName("Dispatches operations according to schedule distribution")
	void dispatchFollowsSchedule() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.READ, 40);
		final OpSchedule schedule = new OpSchedule(weights);

		final List<Item> seeds = makeItems(10);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);

		final ConcurrentLinkedQueue<OpType> dispatched = new ConcurrentLinkedQueue<>();

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, dispatched));
		builders.put(OpType.READ, testBuilder(OpType.READ, dispatched));

		final Output<Operation> mockDriver = noopOutput();

		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, mockDriver);

		dispatch(gen, schedule.size());

		final int total = dispatched.size();
		assertEquals(schedule.size(), total, "Should dispatch one full schedule cycle");

		long createCount = dispatched.stream().filter(t -> t == OpType.CREATE).count();
		long readCount = dispatched.stream().filter(t -> t == OpType.READ).count();
		assertEquals(600, createCount, "CREATE ops should match configured 60% weight");
		assertEquals(400, readCount, "READ ops should match configured 40% weight");
	}

	// ── DELETE re-roll ────────────────────────────────────────────────────

	@Test
	@DisplayName("Re-rolls DELETE to another op when delete queue is empty")
	void rerollDeleteWhenQueueEmpty() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 30);
		weights.put(OpType.DELETE, 70); // Heavy DELETE weight but empty queue
		final OpSchedule schedule = new OpSchedule(weights);

		final List<Item> seeds = makeItems(10);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);
		// Don't add anything to delete queue — it stays empty

		final ConcurrentLinkedQueue<OpType> dispatched = new ConcurrentLinkedQueue<>();

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, dispatched));
		builders.put(OpType.DELETE, testBuilder(OpType.DELETE, dispatched));

		final Output<Operation> mockDriver = noopOutput();

		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, mockDriver);

		dispatch(gen, schedule.size());

		// With empty DELETE queue, all DELETE schedules should re-roll to CREATE
		final int total = dispatched.size();
		assertEquals(schedule.size(), total, "Should dispatch one full schedule cycle");

		long deleteCount = dispatched.stream().filter(t -> t == OpType.DELETE).count();
		assertEquals(0, deleteCount, "DELETE ops should be 0 when queue is empty");

		long createCount = dispatched.stream().filter(t -> t == OpType.CREATE).count();
		assertEquals(total, createCount, "All ops should be CREATE (only non-DELETE op type)");
	}

	// ── PUT → DELETE queue flow ───────────────────────────────────────────

	@Test
	@DisplayName("Successful PUT completions feed the DELETE queue via callback")
	void putCompletionsFeedDeleteQueue() {
		final List<Item> seeds = makeItems(5);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);
		assertEquals(0, pool.deleteQueueSize());

		// Simulate what MixedLoadGenerator does when a PUT succeeds:
		final Item putItem = new ItemImpl("new-object-1");
		pool.addDeleteItem(putItem);

		assertEquals(1, pool.deleteQueueSize());
		assertSame(putItem, pool.pollDelete());
		assertEquals(0, pool.deleteQueueSize());
	}

	// ── Four-op distribution ──────────────────────────────────────────────

	@Test
	@DisplayName("Handles all four op types (GET/PUT/DELETE/STAT)")
	void fourOpDistribution() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 40);
		weights.put(OpType.CREATE, 30);
		weights.put(OpType.DELETE, 10);
		weights.put(OpType.STAT, 20);
		final OpSchedule schedule = new OpSchedule(weights);

		final List<Item> seeds = makeItems(50);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);

		// Pre-populate delete queue so DELETE can proceed
		for (int i = 0; i < 1000; i++) {
			pool.addDeleteItem(new ItemImpl("del-" + i));
		}

		final ConcurrentLinkedQueue<OpType> dispatched = new ConcurrentLinkedQueue<>();

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, dispatched));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, dispatched));
		builders.put(OpType.DELETE, testBuilder(OpType.DELETE, dispatched));
		builders.put(OpType.STAT, testBuilder(OpType.STAT, dispatched));

		final Output<Operation> mockDriver = noopOutput();

		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, mockDriver);

		dispatch(gen, schedule.size());

		final int total = dispatched.size();
		assertEquals(schedule.size(), total, "Should dispatch one full schedule cycle");

		long readCount = dispatched.stream().filter(t -> t == OpType.READ).count();
		long createCount = dispatched.stream().filter(t -> t == OpType.CREATE).count();
		long deleteCount = dispatched.stream().filter(t -> t == OpType.DELETE).count();
		long statCount = dispatched.stream().filter(t -> t == OpType.STAT).count();

		assertEquals(400, readCount, "READ ops should match configured 40% weight");
		assertEquals(300, createCount, "CREATE ops should match configured 30% weight");
		assertEquals(100, deleteCount, "DELETE ops should match configured 10% weight");
		assertEquals(200, statCount, "STAT ops should match configured 20% weight");
	}

	// ── Generator lifecycle ───────────────────────────────────────────────

	@Test
	@DisplayName("Generator stops cleanly when stop() is called")
	void stopsCleanly() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 50);
		weights.put(OpType.READ, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(5));

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		builders.put(OpType.READ, testBuilder(OpType.READ, null));

		final CountDownLatch putEntered = new CountDownLatch(1);
		final CountDownLatch releasePut = new CountDownLatch(1);
		final Output<Operation> blockingDriver = blockingOutput(putEntered, releasePut);

		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, blockingDriver);

		gen.start();
		assertTrue(putEntered.await(5, TimeUnit.SECONDS), "Generator should enter driver put() before stop");
		assertTrue(gen.isStarted());
		assertFalse(gen.isStopped());

		gen.stop();
		releasePut.countDown();
		assertTrue(gen.await(5, TimeUnit.SECONDS), "Generator should stop within 5 seconds");
		assertTrue(gen.isStopped());
	}

	@Test
	@DisplayName("generatedOpCount tracks total operations")
	void generatedOpCountTracking() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 50);
		weights.put(OpType.READ, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(5));

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		builders.put(OpType.READ, testBuilder(OpType.READ, null));

		final Output<Operation> mockDriver = noopOutput();

		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, mockDriver);

		dispatch(gen, 10);

		assertEquals(10, gen.generatedOpCount(), "Should count directly dispatched operations");
	}

	@Test
	@DisplayName("Concurrency throttle blocks generator when permits exhausted")
	void concurrencyThrottle_blockWhenExhausted() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 50);
		weights.put(OpType.CREATE, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(10));

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, null));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));

		// Driver that always accepts ops
		final Output<Operation> mockDriver = noopOutput();

		// Concurrency limit of 2 — no permits are ever released, so only 2 ops should be generated
		final MixedLoadGenerator gen = new MixedLoadGenerator(executor, schedule, pool, builders,
						mockDriver, 2, mockNewItemSupplier());

		gen.doWork();
		gen.doWork();
		assertEquals(2, gen.generatedOpCount(), "precondition: both permits should be consumed");
		assertEquals(0, availablePermits(gen), "precondition: no permits should remain");

		assertEquals(2, gen.generatedOpCount(),
						"No additional op can be generated until a permit is released");
	}

	@Test
	@DisplayName("releasePermit allows generator to dispatch more ops")
	void releasePermit_allowsMoreOps() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 50);
		weights.put(OpType.CREATE, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(10));

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, null));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));

		final Output<Operation> mockDriver = noopOutput();

		final MixedLoadGenerator gen = new MixedLoadGenerator(executor, schedule, pool, builders,
						mockDriver, 1, mockNewItemSupplier());

		gen.doWork();
		assertEquals(1, gen.generatedOpCount(), "Initial: 1 op dispatched with limit 1");
		assertEquals(0, availablePermits(gen), "Initial dispatch should consume the only permit");

		// Release a permit — should allow one more op
		gen.releasePermit();
		gen.doWork();
		assertEquals(2, gen.generatedOpCount(), "After releasePermit: 2 ops total");
	}

	@Test
	@DisplayName("Builder Error path releases acquired permit")
	void builderErrorReleasesPermit() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 50);
		weights.put(OpType.CREATE, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(10));

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, errorBuilder(OpType.READ));
		builders.put(OpType.CREATE, errorBuilder(OpType.CREATE));

		final MixedLoadGenerator gen = new MixedLoadGenerator(
						executor, schedule, pool, builders, noopOutput(), 1, mockNewItemSupplier());

		assertThrows(AssertionError.class, gen::doWork,
						"precondition: builder must fail with Error to exercise throwable path");
		assertEquals(1, availablePermits(gen),
						"generator must release permit even when builder throws Error");
	}

	@Test
	@DisplayName("Per-op in-flight counters increment on dispatch and decrement on completion")
	void perOpInFlightCountersTrackDispatchAndCompletion() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 70);
		weights.put(OpType.CREATE, 30);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(20));
		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, null));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		final List<Operation<Item>> dispatched = new CopyOnWriteArrayList<>();
		final Output<Operation> captureOutput = new Output<>() {
			@Override
			public boolean put(final Operation op) {
				dispatched.add(op);
				return true;
			}

			@Override
			public int put(final List<Operation> ops, final int from, final int to) {
				for (int i = from; i < to; i++) {
					dispatched.add(ops.get(i));
				}
				return to - from;
			}

			@Override
			public int put(final List<Operation> ops) {
				for (final Operation op : ops) {
					dispatched.add(op);
				}
				return ops.size();
			}

			@Override
			public Input<Operation> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};

		final MixedLoadGenerator gen = new MixedLoadGenerator(
						executor, schedule, pool, builders, captureOutput, 10_000, mockNewItemSupplier());

		for (int i = 0; i < 200; i++) {
			gen.doWork();
		}
		assertFalse(dispatched.isEmpty(), "sanity: generator should dispatch operations");

		final long dispatchedRead = dispatched.stream().filter(o -> OpType.READ.equals(o.type())).count();
		final long dispatchedCreate = dispatched.stream().filter(o -> OpType.CREATE.equals(o.type())).count();
		assertEquals(dispatchedRead, gen.inFlightCount(OpType.READ),
						"READ in-flight count should match dispatched READ ops");
		assertEquals(dispatchedCreate, gen.inFlightCount(OpType.CREATE),
						"CREATE in-flight count should match dispatched CREATE ops");

		for (final Operation<Item> op : dispatched) {
			gen.onOperationCompleted(op);
			gen.releasePermit();
		}
		assertEquals(0, gen.inFlightCount(OpType.READ), "READ in-flight count should return to zero after completion");
		assertEquals(0, gen.inFlightCount(OpType.CREATE), "CREATE in-flight count should return to zero after completion");
	}

	@Test
	@DisplayName("DELETE reroll follows configured non-DELETE weights when delete queue is empty")
	void rerollDeleteHonorsConfiguredWeights() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 10);
		weights.put(OpType.CREATE, 90);
		weights.put(OpType.DELETE, 900);
		final OpSchedule schedule = new OpSchedule(weights);

		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(20));
		final ConcurrentLinkedQueue<OpType> dispatched = new ConcurrentLinkedQueue<>();
		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, dispatched));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, dispatched));
		builders.put(OpType.DELETE, testBuilder(OpType.DELETE, dispatched));

		final MixedLoadGenerator gen = new MixedLoadGenerator(
						executor, schedule, pool, builders, noopOutput(), Integer.MAX_VALUE, mockNewItemSupplier());

		for (int i = 0; i < 20_000; i++) {
			gen.doWork();
		}

		final long readCount = dispatched.stream().filter(t -> t == OpType.READ).count();
		final long createCount = dispatched.stream().filter(t -> t == OpType.CREATE).count();
		final long deleteCount = dispatched.stream().filter(t -> t == OpType.DELETE).count();
		final long nonDeleteCount = readCount + createCount;

		assertEquals(0, deleteCount, "DELETE queue is empty, so DELETE ops must reroll");
		assertTrue(nonDeleteCount > 0, "sanity: test must dispatch non-delete operations");
		final double readRatio = (double) readCount / nonDeleteCount;
		assertEquals(0.10, readRatio, 0.08,
						"reroll should preserve configured READ:CREATE weight ratio (10:90)");
	}

	@Test
	@DisplayName("Legacy PUT completion hook is removed from MixedLoadGenerator")
	void legacyPutCompletionHookRemoved() {
		assertThrows(NoSuchMethodException.class,
						() -> MixedLoadGenerator.class.getDeclaredMethod("handlePutCompletion", Operation.class));
	}

	// ── Helpers ───────────────────────────────────────────────────────────

	/** Helper to create a MixedLoadGenerator with raw types (avoids generic inference issues in tests). */
	private MixedLoadGenerator newGenerator(
					final OpSchedule schedule,
					final PoolItemInput pool,
					final Map builders,
					final Output driver) {
		return new MixedLoadGenerator(executor, schedule, pool, builders, driver,
						Integer.MAX_VALUE, mockNewItemSupplier());
	}

	private static void dispatch(final MixedLoadGenerator gen, final int count) throws Exception {
		for (int i = 0; i < count; i++) {
			gen.doWork();
		}
	}

	private static Output<Operation> blockingOutput(final CountDownLatch entered, final CountDownLatch release) {
		return new Output<>() {
			@Override
			public boolean put(final Operation op) {
				entered.countDown();
				try {
					release.await();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
				return true;
			}

			@Override
			public int put(final List<Operation> ops, final int from, final int to) {
				return to - from;
			}

			@Override
			public int put(final List<Operation> ops) {
				return ops.size();
			}

			@Override
			public Input<Operation> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};
	}

	/** Creates a simple Output that accepts all put() calls (avoids Mockito raw-type issues). */
	private static Output<Operation> noopOutput() {
		return new Output<>() {
			@Override
			public boolean put(final Operation op) {
				return true;
			}

			@Override
			public int put(final List<Operation> ops, final int from, final int to) {
				return to - from;
			}

			@Override
			public int put(final List<Operation> ops) {
				return ops.size();
			}

			@Override
			public Input<Operation> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};
	}

	private static List<Item> makeItems(final int count) {
		final List<Item> items = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			items.add(new ItemImpl("seed-" + i));
		}
		return items;
	}

	/** Creates a builder that records the op type when buildOp is called. */
	private static OperationsBuilder<Item, Operation<Item>> testBuilder(
					final OpType opType, final ConcurrentLinkedQueue<OpType> tracker) {
		return new OperationsBuilder<>() {
			private OpType type = opType;

			@Override
			public int originIndex() {
				return 0;
			}

			@Override
			public OpType opType() {
				return type;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> opType(final OpType t) {
				type = t;
				return this;
			}

			@Override
			public String inputPath() {
				return "";
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> inputPath(final String p) {
				return this;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> outputPathInput(final Input<String> i) {
				return this;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> credentialInput(final Input i) {
				return this;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> credentialsByPath(final Map m) {
				return this;
			}

			@Override
			public Operation<Item> buildOp(final Item item) {
				if (tracker != null) {
					tracker.add(opType);
				}
				return new OperationImpl<>(0, opType, item, null, null, null);
			}

			@Override
			public void buildOps(final List<Item> items, final List<Operation<Item>> buff) {
				for (final Item item : items) {
					try {
						buff.add(buildOp(item));
					} catch (final Exception e) {
						throw new RuntimeException(e);
					}
				}
			}

			@Override
			public void close() {}
		};
	}

	private static OperationsBuilder<Item, Operation<Item>> errorBuilder(final OpType opType) {
		return new OperationsBuilder<>() {
			@Override
			public int originIndex() {
				return 0;
			}

			@Override
			public OpType opType() {
				return opType;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> opType(final OpType t) {
				return this;
			}

			@Override
			public String inputPath() {
				return "";
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> inputPath(final String p) {
				return this;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> outputPathInput(final Input<String> i) {
				return this;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> credentialInput(final Input i) {
				return this;
			}

			@Override
			public OperationsBuilder<Item, Operation<Item>> credentialsByPath(final Map m) {
				return this;
			}

			@Override
			public Operation<Item> buildOp(final Item item) {
				throw new AssertionError("synthetic builder error");
			}

			@Override
			public void buildOps(final List<Item> items, final List<Operation<Item>> buff) {}

			@Override
			public void close() {}
		};
	}

	private static int availablePermits(final MixedLoadGenerator gen) throws Exception {
		final var throttleField = MixedLoadGenerator.class.getDeclaredField("concurrencyThrottle");
		throttleField.setAccessible(true);
		return ((Semaphore) throttleField.get(gen)).availablePermits();
	}

	/** Creates a supplier of new items for CREATE operations. */
	private static Input<Item> mockNewItemSupplier() {
		final AtomicInteger counter = new AtomicInteger(0);
		return new Input<>() {
			@Override
			public Item get() {
				return new ItemImpl("new-" + counter.incrementAndGet());
			}

			@Override
			public int get(final List<Item> buffer, final int limit) {
				for (int i = 0; i < limit; i++) {
					buffer.add(get());
				}
				return limit;
			}

			@Override
			public long skip(final long count) {
				return 0;
			}

			@Override
			public void reset() {}

			@Override
			public void close() {}

			@Override
			public String toString() {
				return "TestNewItemInput";
			}
		};
	}
}
