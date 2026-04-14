package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link MixedLoadGenerator}.
 *
 * <p>Tests verify schedule-driven dispatch, DELETE re-roll, PUT→DELETE queue flow,
 * and proper result routing through the generator's single-driver architecture.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
class MixedLoadGeneratorTest {

	@BeforeAll
	static void initLog4j() {
		// Force Log4j initialization before any test starts a generator.
		// Without this, the generator's doInit() triggers lazy Log4j init in a virtual thread,
		// and gen.stop() can interrupt that thread mid-init, permanently corrupting Log4j.
		ThreadContext.put("test", "init");
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

		gen.start();
		// Let it run briefly
		Thread.sleep(200);
		gen.stop();
		gen.await(5, TimeUnit.SECONDS);

		// Should have dispatched a mix of CREATE and READ
		final int total = dispatched.size();
		assertTrue(total > 0, "Should have dispatched some operations");

		long createCount = dispatched.stream().filter(t -> t == OpType.CREATE).count();
		long readCount = dispatched.stream().filter(t -> t == OpType.READ).count();
		assertTrue(createCount > 0, "Should have dispatched CREATE ops");
		assertTrue(readCount > 0, "Should have dispatched READ ops");

		// Distribution should be approximately 60/40 (within tolerance for short run)
		double createRatio = (double) createCount / total;
		assertEquals(0.6, createRatio, 0.15, "CREATE ratio should be ~60%");
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

		gen.start();
		Thread.sleep(200);
		gen.stop();
		gen.await(5, TimeUnit.SECONDS);

		// With empty DELETE queue, all DELETE schedules should re-roll to CREATE
		final int total = dispatched.size();
		assertTrue(total > 0, "Should have dispatched some operations");

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

		gen.start();
		Thread.sleep(300);
		gen.stop();
		gen.await(5, TimeUnit.SECONDS);

		final int total = dispatched.size();
		assertTrue(total > 100, "Should have dispatched many operations");

		long readCount = dispatched.stream().filter(t -> t == OpType.READ).count();
		long createCount = dispatched.stream().filter(t -> t == OpType.CREATE).count();
		long deleteCount = dispatched.stream().filter(t -> t == OpType.DELETE).count();
		long statCount = dispatched.stream().filter(t -> t == OpType.STAT).count();

		assertTrue(readCount > 0, "Should have READ ops");
		assertTrue(createCount > 0, "Should have CREATE ops");
		assertTrue(deleteCount > 0, "Should have DELETE ops");
		assertTrue(statCount > 0, "Should have STAT ops");

		// Verify approximate distribution
		double readRatio = (double) readCount / total;
		double createRatio = (double) createCount / total;
		assertEquals(0.4, readRatio, 0.1, "READ ratio should be ~40%");
		assertEquals(0.3, createRatio, 0.1, "CREATE ratio should be ~30%");
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

		final Output<Operation> mockDriver = noopOutput();

		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, mockDriver);

		gen.start();
		assertTrue(gen.isStarted());
		assertFalse(gen.isStopped());

		gen.stop();
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

		gen.start();
		Thread.sleep(200);
		gen.stop();
		gen.await(5, TimeUnit.SECONDS);

		assertTrue(gen.generatedOpCount() > 0, "Should have generated some operations");
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
						mockDriver, 2, mockNewItemSupplier(), null, null);

		gen.start();
		Thread.sleep(200);
		gen.stop();
		gen.await(5, TimeUnit.SECONDS);

		assertEquals(2, gen.generatedOpCount(),
						"Only 2 ops should be generated when concurrency limit is 2 and no permits released");
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
						mockDriver, 1, mockNewItemSupplier(), null, null);

		gen.start();
		Thread.sleep(50);
		assertEquals(1, gen.generatedOpCount(), "Initial: 1 op dispatched with limit 1");

		// Release a permit — should allow one more op
		gen.releasePermit();
		Thread.sleep(50);
		assertEquals(2, gen.generatedOpCount(), "After releasePermit: 2 ops total");

		gen.stop();
		gen.await(5, TimeUnit.SECONDS);
	}

	// ── Helpers ───────────────────────────────────────────────────────────

	/** Helper to create a MixedLoadGenerator with raw types (avoids generic inference issues in tests). */
	private MixedLoadGenerator newGenerator(
					final OpSchedule schedule,
					final PoolItemInput pool,
					final Map builders,
					final Output driver) {
		return new MixedLoadGenerator(executor, schedule, pool, builders, driver,
						Integer.MAX_VALUE, mockNewItemSupplier(), null, null);
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

			@Override public int originIndex() { return 0; }
			@Override public OpType opType() { return type; }
			@Override public OperationsBuilder<Item, Operation<Item>> opType(final OpType t) { type = t; return this; }
			@Override public String inputPath() { return ""; }
			@Override public OperationsBuilder<Item, Operation<Item>> inputPath(final String p) { return this; }
			@Override public OperationsBuilder<Item, Operation<Item>> outputPathInput(final Input<String> i) { return this; }
			@Override public OperationsBuilder<Item, Operation<Item>> credentialInput(final Input i) { return this; }
			@Override public OperationsBuilder<Item, Operation<Item>> credentialsByPath(final Map m) { return this; }

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
					try { buff.add(buildOp(item)); } catch (final Exception e) { throw new RuntimeException(e); }
				}
			}

			@Override public void close() {}
		};
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
