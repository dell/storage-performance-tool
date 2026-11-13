package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Method;
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
import java.util.concurrent.atomic.AtomicReference;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
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

	@Test
	void mixedPutItemInputUsesConfiguredPrefixShards() throws Exception {
		final Map<String, Object> namingSchema = new java.util.HashMap<>();
		namingSchema.put("length", Integer.class);
		namingSchema.put("seed", Object.class);
		namingSchema.put("prefix", String.class);
		namingSchema.put("shards", Integer.class);
		namingSchema.put("radix", Integer.class);
		namingSchema.put("step", Integer.class);
		namingSchema.put("type", String.class);
		final Map<String, Object> namingValues = new java.util.HashMap<>();
		namingValues.put("length", 12);
		namingValues.put("seed", 0L);
		namingValues.put("prefix", "base/");
		namingValues.put("shards", 3);
		namingValues.put("radix", 10);
		namingValues.put("step", 1);
		namingValues.put("type", "serial");
		final Config itemConfig = new BasicConfig(
						"-",
						Map.of("naming", namingSchema, "data", Map.of("size", Object.class)),
						Map.of("naming", namingValues, "data", Map.of("size", "1KB")));
		final Method factoryMethod = MixedLoadStepLocal.class.getDeclaredMethod(
						"createNewItemInput", Config.class, com.dell.spt.base.item.ItemFactory.class);
		factoryMethod.setAccessible(true);

		try (final Input<Item> input = (Input<Item>) factoryMethod.invoke(
						null, itemConfig, new DataItemFactoryImpl<>())) {
			assertEquals("base/s0000001/1", input.get().name(),
							"mixed PUT item generation must consume item.naming.shards");
		}
	}

	@Test
	void legacyVirtualThreadConstructorDescriptorRemainsAvailable() throws Exception {
		assertNotNull(MixedLoadGenerator.class.getConstructor(
						VirtualThreadExecutor.class,
						OpSchedule.class,
						PoolItemInput.class,
						Map.class,
						Output.class,
						int.class,
						Input.class));
	}

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

	@Test
	@DisplayName("A PUT and DELETE mix runs with an empty read pool and deletes only what it created")
	void aPutAndDeleteMixRunsWithAnEmptyReadPool() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 50);
		weights.put(OpType.DELETE, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(List.of());

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		builders.put(OpType.DELETE, testBuilder(OpType.DELETE, null));
		final List<String> created = new ArrayList<>();
		final List<String> deleted = new ArrayList<>();
		// Completes each operation at once, and feeds each created item to the delete queue as the step does.
		final Output<Operation> driver = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				final Item item = (Item) op.item();
				if (op.type() == OpType.CREATE) {
					created.add(item.name());
					pool.addDeleteItem(item);
				} else {
					assertEquals(OpType.DELETE, op.type(), "only PUT and DELETE are in the mix");
					deleted.add(item.name());
				}
				return true;
			}
		};
		final MixedLoadGenerator gen = newGenerator(schedule, pool, builders, driver);

		dispatch(gen, 2 * schedule.size());

		assertEquals(2 * schedule.size(), gen.generatedOpCount());
		assertEquals(gen.generatedOpCount(), created.size() + deleted.size());
		assertFalse(deleted.isEmpty(), "DELETE ran once PUT completions fed its queue");
		assertTrue(created.containsAll(deleted), "a DELETE deletes only items the step created");
		assertEquals(created.size() - deleted.size(), pool.deleteQueueSize());
	}

	@Test
	@DisplayName("A mix with GET or STAT is refused when built over an empty read pool")
	void aMixWithGetOrStatIsRefusedOverAnEmptyReadPool() {
		for (final OpType drawing : List.of(OpType.READ, OpType.STAT)) {
			final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
			weights.put(drawing, 50);
			weights.put(OpType.CREATE, 50);
			final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
			builders.put(drawing, testBuilder(drawing, null));
			builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));

			final var failure = assertThrows(
							IllegalArgumentException.class,
							() -> newGenerator(new OpSchedule(weights), new PoolItemInput<>(List.of()), builders, noopOutput()),
							drawing.toString());
			assertEquals("MixedLoad: GET and STAT draw from the seed items, and the read pool has none", failure.getMessage());
		}
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

	@Test
	@DisplayName("Mixed DELETE retains the legacy single-item data operation")
	void mixedDeleteRetainsLegacySingleItemDataOperation() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 1);
		weights.put(OpType.DELETE, 999);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput pool = new PoolItemInput(List.of(new DataItemImpl("seed", 0, 1)));
		for (int i = 0; i < 1000; i++) {
			pool.addDeleteItem(new DataItemImpl("delete-" + i, 0, 1));
		}
		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		builders.put(
						OpType.DELETE,
						new DataOperationsBuilderImpl<>(0)
										.opType(OpType.DELETE)
										.inputPath("/bucket")
										.fixedRanges(List.of())
										.randomRangesCount(0)
										.sizeThreshold(Long.MAX_VALUE));
		final List<Operation> captured = new ArrayList<>();
		final Output<Operation> output = new Output<>() {
			@Override
			public boolean put(final Operation operation) {
				captured.add(operation);
				return true;
			}

			@Override
			public int put(final List<Operation> operations, final int from, final int to) {
				captured.addAll(operations.subList(from, to));
				return to - from;
			}

			@Override
			public int put(final List<Operation> operations) {
				return put(operations, 0, operations.size());
			}

			@Override
			public Input<Operation> getInput() {
				return null;
			}

			@Override
			public void close() {}
		};
		final MixedLoadGenerator generator = newGenerator(schedule, pool, builders, output);

		dispatch(generator, schedule.size());

		final var deleteOperations = captured.stream()
						.filter(operation -> operation.type() == OpType.DELETE)
						.toList();
		assertFalse(deleteOperations.isEmpty());
		assertTrue(deleteOperations.stream().allMatch(DataOperation.class::isInstance));
		assertTrue(deleteOperations.stream().noneMatch(DeleteRequestOperation.class::isInstance));
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

	// ── The driver's EOF at stop ──────────────────────────────────────────

	@Test
	@DisplayName("A DELETE the driver refuses with EOF keeps its item and releases its permit")
	void driverEofKeepsTheDeleteItemAndReleasesItsPermit() throws Exception {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 50);
		weights.put(OpType.DELETE, 50);
		final OpSchedule schedule = new OpSchedule(weights);
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(10));
		pool.addDeleteItem(new ItemImpl("to-delete"));

		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, null));
		builders.put(OpType.DELETE, testBuilder(OpType.DELETE, null));

		final int limit = schedule.size();
		final MixedLoadGenerator gen = new MixedLoadGenerator(executor, schedule, pool, builders,
						refusingOutput(OpType.DELETE, new EOFException()), limit, mockNewItemSupplier());

		EOFException refused = null;
		for (int i = 0; i < schedule.size() && refused == null; i++) {
			try {
				gen.doWork();
			} catch (final EOFException e) {
				refused = e;
			}
		}

		assertNotNull(refused, "precondition: the schedule reaches a DELETE");
		assertEquals(1, pool.deleteQueueSize(), "the refused DELETE's item is back in the queue");
		assertEquals(limit - gen.generatedOpCount(), availablePermits(gen),
						"only the accepted operations hold permits");
		assertEquals(0, gen.inFlightCount(OpType.DELETE));
	}

	@Test
	@DisplayName("The driver's EOF ends the generator without an ERROR")
	void driverEofEndsTheGeneratorWithoutAnError() throws Exception {
		try (ErrorLogCapture log = new ErrorLogCapture()) {
			final MixedLoadGenerator gen = startReadCreateGenerator(
							refusingOutput(null, new EOFException()), mockNewItemSupplier());

			assertTrue(gen.await(5, TimeUnit.SECONDS), "the generator ends");
			assertTrue(gen.isStopped());
			assertEquals(4, availablePermits(gen), "the refused operation's permit is released");
			assertEquals(List.of(), log.errorsContaining("MixedLoadGenerator"));
		}
	}

	@Test
	@DisplayName("Another driver failure is still an ERROR")
	void anotherDriverFailureIsStillAnError() throws Exception {
		try (ErrorLogCapture log = new ErrorLogCapture()) {
			final MixedLoadGenerator gen = startReadCreateGenerator(
							refusingOutput(null, new IOException("synthetic driver failure")), mockNewItemSupplier());

			assertTrue(gen.await(5, TimeUnit.SECONDS), "the generator ends");
			final List<String> errors = log.errorsContaining("MixedLoadGenerator error in doWork");
			assertEquals(1, errors.size(), errors.toString());
			assertTrue(errors.get(0).contains("synthetic driver failure"), errors.toString());
		}
	}

	@Test
	@DisplayName("An EOF from the CREATE item input, not the driver, is still an ERROR")
	void anItemInputEofIsStillAnError() throws Exception {
		// The delete queue stays empty, so every DELETE re-rolls to CREATE.
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 50);
		weights.put(OpType.DELETE, 50);
		final Input<Item> failingInput = new Input<>() {
			@Override
			public Item get() {
				throwUnchecked(new EOFException("synthetic item input end"));
				return null;
			}

			@Override
			public int get(final List<Item> buffer, final int limit) {
				return 0;
			}

			@Override
			public long skip(final long count) {
				return 0;
			}

			@Override
			public void reset() {}

			@Override
			public void close() {}
		};
		try (ErrorLogCapture log = new ErrorLogCapture()) {
			final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
			builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
			builders.put(OpType.DELETE, testBuilder(OpType.DELETE, null));
			final MixedLoadGenerator gen = new MixedLoadGenerator(executor, new OpSchedule(weights),
							new PoolItemInput<>(makeItems(5)), builders, noopOutput(), 4, failingInput);
			gen.start();

			assertTrue(gen.await(5, TimeUnit.SECONDS), "the generator ends");
			final List<String> errors = log.errorsContaining("MixedLoadGenerator error in doWork");
			assertEquals(1, errors.size(), errors.toString());
		}
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
	@DisplayName("An operation the driver completes before put returns does not stay counted in flight")
	void aCompletionBeforePutReturnsLeavesNothingInFlight() throws Exception {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(20));
		for (int i = 0; i < 100; i++) {
			pool.addDeleteItem(new ItemImpl("to-delete-" + i));
		}
		final AtomicReference<MixedLoadGenerator> generator = new AtomicReference<>();
		final Map<OpType, Integer> countedDuringPut = new EnumMap<>(OpType.class);
		// A driver that completes each operation on the calling thread, before put returns.
		final Output<Operation> completesInsidePut = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				final MixedLoadGenerator gen = generator.get();
				countedDuringPut.merge(op.type(), gen.inFlightCount(op.type()), Math::min);
				gen.onOperationCompleted(op);
				gen.releasePermit();
				return true;
			}
		};
		final MixedLoadGenerator gen = readCreateDeleteGenerator(pool, completesInsidePut, 4);
		generator.set(gen);

		dispatch(gen, 300);

		assertEquals(300, gen.generatedOpCount(), "sanity: every operation was accepted");
		for (final OpType type : List.of(OpType.READ, OpType.CREATE, OpType.DELETE)) {
			assertEquals(1, countedDuringPut.get(type), type + " is counted in flight while the driver holds it");
			assertEquals(0, gen.inFlightCount(type), type + " completed inside put must not stay counted");
		}
	}

	@Test
	@DisplayName("Operations the driver refuses or fails to accept are not counted in flight")
	@Timeout(30) // a leaked permit blocks the next doWork() instead of failing an assertion
	void refusedAndFailedPutsLeaveNothingInFlight() throws Exception {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(20));
		for (int i = 0; i < 100; i++) {
			pool.addDeleteItem(new ItemImpl("to-delete-" + i));
		}
		final Output<Operation> refusing = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				return false;
			}
		};
		final MixedLoadGenerator refusedGen = readCreateDeleteGenerator(pool, refusing, 4);
		dispatch(refusedGen, 300);
		assertEquals(0, refusedGen.generatedOpCount());
		assertEquals(4, availablePermits(refusedGen), "every refused operation's permit is released");
		assertEquals(100, pool.deleteQueueSize(), "every refused DELETE's item is back in the queue");

		final MixedLoadGenerator failedGen = readCreateDeleteGenerator(
						pool, refusingOutput(null, new IOException("synthetic driver failure")), 4);
		int failures = 0;
		for (int i = 0; i < 300; i++) {
			try {
				failedGen.doWork();
			} catch (final IOException e) {
				failures++;
			}
		}
		assertEquals(300, failures, "precondition: every put failed");
		assertEquals(4, availablePermits(failedGen), "every failed operation's permit is released");
		for (final OpType type : List.of(OpType.READ, OpType.CREATE, OpType.DELETE)) {
			assertEquals(0, refusedGen.inFlightCount(type), "refused " + type);
			assertEquals(0, failedGen.inFlightCount(type), "failed " + type);
		}
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

	/** A READ, CREATE and DELETE generator with {@code permits} permits on {@code driver}, not started. */
	private MixedLoadGenerator readCreateDeleteGenerator(
					final PoolItemInput<Item> pool, final Output<Operation> driver, final int permits) {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 40);
		weights.put(OpType.CREATE, 30);
		weights.put(OpType.DELETE, 30);
		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		for (final OpType type : weights.keySet()) {
			builders.put(type, testBuilder(type, null));
		}
		return new MixedLoadGenerator(
						executor, new OpSchedule(weights), pool, builders, driver, permits, mockNewItemSupplier());
	}

	/** An Output that accepts everything; tests override {@code put(Operation)}. */
	private abstract static class AcceptingOutput implements Output<Operation> {

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
	}

	/** Starts a READ and CREATE generator with 4 permits on {@code driver}. */
	private MixedLoadGenerator startReadCreateGenerator(final Output<Operation> driver, final Input<Item> newItems) {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 50);
		weights.put(OpType.CREATE, 50);
		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, null));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		final MixedLoadGenerator gen = new MixedLoadGenerator(executor, new OpSchedule(weights),
						new PoolItemInput<>(makeItems(5)), builders, driver, 4, newItems);
		gen.start();
		return gen;
	}

	/**
	 * An Output that throws {@code failure} from put() for operations of {@code refused}, or for
	 * every operation when it is null, and accepts the others.
	 */
	private static Output<Operation> refusingOutput(final OpType refused, final IOException failure) {
		return new Output<>() {
			@Override
			public boolean put(final Operation op) {
				if (refused == null || op.type() == refused) {
					throwUnchecked(failure);
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
