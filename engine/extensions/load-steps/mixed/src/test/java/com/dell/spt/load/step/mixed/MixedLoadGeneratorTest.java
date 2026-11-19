package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.CompositeResults;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
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
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

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

	@Test
	void theThreadExecutorConstructorDescriptorRemainsAvailable() throws Exception {
		assertNotNull(MixedLoadGenerator.class.getConstructor(
						ThreadTaskExecutor.class,
						OpSchedule.class,
						PoolItemInput.class,
						Map.class,
						Output.class,
						int.class,
						Input.class));
	}

	@Test
	@DisplayName("A count-limited generator dispatches its count, then reports the cap drained once each has its final result")
	void aCountLimitedGeneratorDispatchesItsCount() throws Exception {
		final List<Operation> dispatched = new CopyOnWriteArrayList<>();
		final MixedLoadGenerator gen = countLimitedGenerator(capturingOutput(dispatched), 5);

		dispatch(gen, 20);

		assertEquals(5, dispatched.size(), "operations handed to the driver");
		assertEquals(5, gen.generatedOpCount());
		assertTrue(gen.isItemInputFinished(), "a capped generator has finished its input");
		assertFalse(gen.isCountLimitDrained(0), "its operations are still in flight");
		for (int i = 0; i < 5; i++) {
			gen.onOperationCompleted(dispatched.get(i));
			gen.releasePermit();
		}
		// Out of flight is not ended: a multipart upload leaves flight at its initiation.
		assertFalse(gen.isCountLimitDrained(0), "no operation has published its final result");
		assertFalse(gen.isCountLimitDrained(4), "four counted results, one final result to come");
		gen.onUncountedFinalResult();
		assertTrue(gen.isCountLimitDrained(4), "four counted results and one uncounted final result");
		assertTrue(gen.isCountLimitDrained(5), "every dispatched operation has ended");
	}

	@Test
	@DisplayName("A refused operation does not count toward the count limit")
	void aRefusedOperationDoesNotCountTowardTheLimit() throws Exception {
		final List<Operation> dispatched = new CopyOnWriteArrayList<>();
		final Output<Operation> capture = capturingOutput(dispatched);
		final AtomicInteger calls = new AtomicInteger();
		final Output<Operation> refusesEveryOther = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				return calls.getAndIncrement() % 2 == 0 && capture.put(op);
			}
		};
		final MixedLoadGenerator gen = countLimitedGenerator(refusesEveryOther, 3);

		dispatch(gen, 20);

		assertEquals(3, dispatched.size(), "operations the driver accepted");
		assertEquals(3, gen.generatedOpCount());
	}

	@Test
	@DisplayName("Without a count limit the generator dispatches on every call and never reports a drained cap")
	void anUncappedGeneratorIsNotLimited() throws Exception {
		final List<Operation> dispatched = new CopyOnWriteArrayList<>();
		final MixedLoadGenerator gen = countLimitedGenerator(capturingOutput(dispatched), 0);

		dispatch(gen, 50);

		assertEquals(50, dispatched.size());
		assertFalse(gen.isItemInputFinished());
		for (final Operation op : dispatched) {
			gen.onOperationCompleted(op);
			gen.releasePermit();
		}
		assertFalse(gen.isCountLimitDrained(dispatched.size()), "no count limit, so no cap to drain");
	}

	@Test
	@DisplayName("A step caps its dispatch only at a count limit, on a driver that declares its composite results")
	void aStepCapsDispatchOnlyOnADeclaredDriver() {
		for (final CompositeResults declared : new CompositeResults[]{CompositeResults.IN_PARTS, CompositeResults.ONE_RESULT
		}) {
			assertTrue(MixedLoadGenerator.capsDispatch(1, declared), declared + " at a count of 1");
			assertTrue(MixedLoadGenerator.capsDispatch(Long.MAX_VALUE - 1, declared), declared + " at a large count");
			assertFalse(MixedLoadGenerator.capsDispatch(0, declared), declared + " with no count limit");
			assertFalse(MixedLoadGenerator.capsDispatch(Long.MAX_VALUE, declared), declared + " at Long.MAX_VALUE");
		}
		assertFalse(MixedLoadGenerator.capsDispatch(1, CompositeResults.UNDECLARED), "an undeclared driver");
		assertFalse(MixedLoadGenerator.capsDispatch(1, null), "a driver that returns null");
	}

	@Test
	@DisplayName("Long.MAX_VALUE, like 0, is no count limit")
	void maxValueIsNoCountLimit() throws Exception {
		assertFalse(MixedLoadGenerator.isCountLimit(0));
		assertFalse(MixedLoadGenerator.isCountLimit(-1));
		assertFalse(MixedLoadGenerator.isCountLimit(Long.MAX_VALUE));
		assertTrue(MixedLoadGenerator.isCountLimit(1));
		assertTrue(MixedLoadGenerator.isCountLimit(Long.MAX_VALUE - 1));
		final List<Operation> dispatched = new CopyOnWriteArrayList<>();
		final MixedLoadGenerator gen = countLimitedGenerator(capturingOutput(dispatched), Long.MAX_VALUE);

		dispatch(gen, 50);

		assertEquals(50, dispatched.size());
		assertFalse(gen.isItemInputFinished());
		assertFalse(gen.isCountLimitDrained(Long.MAX_VALUE), "no count limit, so no cap to drain");
	}

	@Test
	@DisplayName("A capped step's result output tells final from not final, and counted from uncounted, as the step context counts")
	void resultKinds() {
		final int notFinal = UncountedFinalResultOutput.NOT_FINAL;
		final int counted = UncountedFinalResultOutput.COUNTED;
		final int uncounted = UncountedFinalResultOutput.UNCOUNTED;
		for (final boolean inParts : new boolean[]{true, false
		}) {
			assertEquals(counted, kind(operation(OpType.READ, Operation.Status.SUCC), inParts));
			assertEquals(counted, kind(operation(OpType.READ, Operation.Status.FAIL_IO), inParts));
			assertEquals(counted, kind(operation(OpType.CREATE, Operation.Status.PENDING), inParts));
			assertEquals(uncounted, kind(operation(OpType.CREATE, Operation.Status.INTERRUPTED), inParts));
			assertEquals(uncounted, kind(operation(OpType.CREATE, Operation.Status.OMIT), inParts));
			assertEquals(notFinal, kind(part(Operation.Status.SUCC), inParts));
			assertEquals(notFinal, kind(part(Operation.Status.FAIL_IO), inParts));
			// A finished composite: the upload's completion, counted like any other result.
			assertEquals(counted, kind(composite(OpType.CREATE, Operation.Status.SUCC, true), inParts));
			assertEquals(uncounted, kind(composite(OpType.CREATE, Operation.Status.INTERRUPTED, true), inParts));
			// A failed unfinished composite (its initiation, or its abort) is counted as a failure.
			assertEquals(counted, kind(composite(OpType.CREATE, Operation.Status.FAIL_IO, false), inParts));
			assertEquals(uncounted, kind(composite(OpType.CREATE, Operation.Status.INTERRUPTED, false), inParts));
			// A composite of another type has one result and no parts: only an out-of-tree builder makes one.
			assertEquals(uncounted, kind(composite(OpType.DELETE, Operation.Status.SUCC, false), inParts));
			assertEquals(uncounted, kind(composite(OpType.STAT, Operation.Status.SUCC, false), inParts));
		}
		// The successful initiation of an upload or a range read: parts follow on a cooperative driver,
		// and nothing follows on a driver that publishes one result for each composite.
		assertEquals(notFinal, kind(composite(OpType.CREATE, Operation.Status.SUCC, false), true));
		assertEquals(notFinal, kind(composite(OpType.READ, Operation.Status.SUCC, false), true));
		assertEquals(uncounted, kind(composite(OpType.CREATE, Operation.Status.SUCC, false), false));
		assertEquals(uncounted, kind(composite(OpType.READ, Operation.Status.SUCC, false), false));
	}

	@Test
	@DisplayName("A capped step's result output classifies every status and shape as the rules it was written to")
	void resultKindsMatchTheirRulesForEveryStatusAndShape() {
		final List<Operation.Status> statuses = new ArrayList<>(List.of(Operation.Status.values()));
		statuses.add(null);
		for (final boolean inParts : new boolean[]{true, false
		}) {
			for (final OpType type : OpType.values()) {
				for (final Operation.Status status : statuses) {
					final List<Operation<Item>> shapes = List.of(
									operation(type, status),
									part(type, status),
									composite(type, status, true),
									composite(type, status, false),
									partOfComposite(type, status, true),
									partOfComposite(type, status, false));
					for (final Operation<Item> op : shapes) {
						assertEquals(ruledKind(op, inParts), kind(op, inParts),
										() -> op + " " + type + " " + status
														+ " in parts " + inParts);
					}
				}
			}
		}
	}

	/** The classification rules, in the order they are stated, against which the output's faster order is tested. */
	private static int ruledKind(final Operation<?> op, final boolean compositesInParts) {
		if (op instanceof PartialOperation) {
			return UncountedFinalResultOutput.NOT_FINAL;
		}
		final Operation.Status status = op.status();
		final boolean notFailed = Operation.Status.SUCC.equals(status) || Operation.Status.PENDING.equals(status)
						|| Operation.Status.ACTIVE.equals(status) || Operation.Status.INTERRUPTED.equals(status)
						|| Operation.Status.OMIT.equals(status);
		if (op instanceof CompositeOperation<?> composite && !composite.allSubOperationsDone()) {
			if (!notFailed) {
				return UncountedFinalResultOutput.COUNTED;
			}
			if (compositesInParts && Operation.Status.SUCC.equals(status)
							&& (OpType.CREATE.equals(op.type()) || OpType.READ.equals(op.type()))) {
				return UncountedFinalResultOutput.NOT_FINAL;
			}
			return UncountedFinalResultOutput.UNCOUNTED;
		}
		if (Operation.Status.INTERRUPTED.equals(status) || Operation.Status.OMIT.equals(status)) {
			return UncountedFinalResultOutput.UNCOUNTED;
		}
		return UncountedFinalResultOutput.COUNTED;
	}

	@Test
	@Timeout(30)
	@DisplayName("Uncounted final results from many platform and virtual threads at once are all counted")
	void uncountedFinalResultsFromManyThreadsAreAllCounted() throws Exception {
		final MixedLoadGenerator gen = cappedAtOne();
		final int threadsOfEachKind = 4;
		final int resultsPerThread = 100_000;
		final CountDownLatch start = new CountDownLatch(1);
		final List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < threadsOfEachKind; i++) {
			for (final Thread.Builder builder : List.of(Thread.ofPlatform(), Thread.ofVirtual())) {
				threads.add(builder.start(() -> {
					try {
						start.await();
					} catch (final InterruptedException e) {
						throw new IllegalStateException(e);
					}
					for (int n = 0; n < resultsPerThread; n++) {
						gen.onUncountedFinalResult();
					}
				}));
			}
		}
		start.countDown();
		for (final Thread thread : threads) {
			thread.join();
		}
		assertEquals(2L * threadsOfEachKind * resultsPerThread, gen.uncountedFinalResultCount());
	}

	@Test
	@Timeout(30)
	@DisplayName("Each platform thread counts uncounted final results in a cell of its own")
	void eachPlatformThreadCountsInItsOwnCell() throws Exception {
		final MixedLoadGenerator gen = cappedAtOne();
		final MixedLoadGenerator.UncountedCell first = countOnPlatformThread(gen, 3);
		final MixedLoadGenerator.UncountedCell second = countOnPlatformThread(gen, 5);

		assertNotSame(first, second, "the second thread was given the first thread's cell");
		assertEquals(List.of(first, second), uncountedCells(gen), "each thread's cell is registered once, in order");
		assertEquals(3, first.count(), "the first thread's cell holds its own count only");
		assertEquals(5, second.count(), "the second thread's cell holds its own count only");
		assertEquals(0, uncountedOnVirtualThreads(gen).sum(), "no platform thread's result went to the adder");
		assertEquals(8, gen.uncountedFinalResultCount());
	}

	/** Counts {@code results} on a new platform thread, joined, and returns the cell that thread counted in. */
	private static MixedLoadGenerator.UncountedCell countOnPlatformThread(
					final MixedLoadGenerator gen, final int results) throws Exception {
		final AtomicReference<MixedLoadGenerator.UncountedCell> cell = new AtomicReference<>();
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final Thread thread = Thread.ofPlatform().start(() -> {
			try {
				for (int n = 0; n < results; n++) {
					gen.onUncountedFinalResult();
				}
				cell.set(((ThreadLocal<MixedLoadGenerator.UncountedCell>) generatorField(gen, "uncountedCell")).get());
			} catch (final Throwable e) {
				failure.set(e);
			}
		});
		thread.join();
		if (failure.get() != null) {
			throw new AssertionError(failure.get());
		}
		return cell.get();
	}

	@Test
	@Timeout(30)
	@DisplayName("Virtual threads count uncounted final results in the shared adder, and register no cell")
	void virtualThreadsCountInTheAdder() throws Exception {
		final MixedLoadGenerator gen = cappedAtOne();
		final List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			threads.add(Thread.ofVirtual().start(() -> {
				for (int n = 0; n < 4; n++) {
					gen.onUncountedFinalResult();
				}
			}));
		}
		for (final Thread thread : threads) {
			thread.join();
		}

		assertEquals(List.of(), uncountedCells(gen), "a virtual thread was given a cell");
		assertEquals(12, uncountedOnVirtualThreads(gen).sum());
		assertEquals(12, gen.uncountedFinalResultCount());
	}

	@Test
	@Timeout(30)
	@DisplayName("A count taken while results arrive never exceeds the results begun so far")
	void aCountTakenWhileResultsArriveNeverRunsAhead() throws Exception {
		final MixedLoadGenerator gen = cappedAtOne();
		final int resultsPerThread = 50_000;
		final AtomicInteger begun = new AtomicInteger();
		final List<Thread> threads = new ArrayList<>();
		for (final Thread.Builder builder : List.of(Thread.ofPlatform(), Thread.ofPlatform(), Thread.ofVirtual())) {
			threads.add(builder.start(() -> {
				for (int n = 0; n < resultsPerThread; n++) {
					begun.incrementAndGet();
					gen.onUncountedFinalResult();
				}
			}));
		}
		long last = 0;
		while (threads.stream().anyMatch(Thread::isAlive)) {
			// Read the count before the bound: every result in the count had begun by then.
			final long count = gen.uncountedFinalResultCount();
			final int bound = begun.get();
			assertTrue(count <= bound, () -> count + " counted, only " + bound + " begun");
			final long earlier = last;
			assertTrue(count >= earlier, () -> count + " counted after " + earlier + " earlier");
			last = count;
		}
		for (final Thread thread : threads) {
			thread.join();
		}
		assertEquals(3L * resultsPerThread, gen.uncountedFinalResultCount());
	}

	private static List<MixedLoadGenerator.UncountedCell> uncountedCells(final MixedLoadGenerator gen)
					throws ReflectiveOperationException {
		return (List<MixedLoadGenerator.UncountedCell>) generatorField(gen, "uncountedCells");
	}

	private static LongAdder uncountedOnVirtualThreads(final MixedLoadGenerator gen)
					throws ReflectiveOperationException {
		return (LongAdder) generatorField(gen, "uncountedOnVirtualThreads");
	}

	private static Object generatorField(final MixedLoadGenerator gen, final String name)
					throws ReflectiveOperationException {
		final var field = MixedLoadGenerator.class.getDeclaredField(name);
		field.setAccessible(true);
		return field.get(gen);
	}

	@Test
	@DisplayName("A final result the step context threw on still ends its operation")
	void aResultTheStepContextThrewOnIsAccounted() throws Exception {
		final MixedLoadGenerator gen = cappedAtOne();
		final Output<Operation> throwing = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				throw new IllegalStateException("thrown by the step context");
			}
		};
		final UncountedFinalResultOutput output = new UncountedFinalResultOutput(throwing, gen, true);

		assertThrows(IllegalStateException.class, () -> output.put(part(Operation.Status.SUCC)));
		assertFalse(gen.isCountLimitDrained(0), "a part's result does not end its operation");
		assertThrows(IllegalStateException.class, () -> output.put(operation(OpType.READ, Operation.Status.SUCC)));
		assertTrue(gen.isCountLimitDrained(0), "the result was not counted, and it ended its operation");
	}

	@Test
	@DisplayName("A batch of results is accounted as the same results one at a time, refused or not")
	void aBatchIsAccountedAsSingleResults() throws Exception {
		final List<Operation> taken = new ArrayList<>();
		final Output<Operation> refusing = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				taken.add(op);
				return false;
			}
		};
		final MixedLoadGenerator single = cappedAtOne();
		assertFalse(new UncountedFinalResultOutput(refusing, single, true)
						.put(operation(OpType.READ, Operation.Status.INTERRUPTED)));
		assertTrue(single.isCountLimitDrained(0), "a refused final result ended its operation");

		final MixedLoadGenerator batch = cappedAtOne();
		final List<Operation> results = List.of(
						part(Operation.Status.SUCC), operation(OpType.READ, Operation.Status.INTERRUPTED));
		assertEquals(2, new UncountedFinalResultOutput(refusing, batch, true).put(results, 0, 2));
		assertTrue(batch.isCountLimitDrained(0), "the same result in a batch ended its operation");
		assertEquals(3, taken.size(), "each result was passed on");
	}

	/** A generator that has dispatched its count limit of 1. */
	private MixedLoadGenerator cappedAtOne() throws Exception {
		final MixedLoadGenerator gen = countLimitedGenerator(capturingOutput(new CopyOnWriteArrayList<>()), 1);
		dispatch(gen, 3);
		assertTrue(gen.isItemInputFinished(), "the generator reached its count limit");
		return gen;
	}

	private static int kind(final Operation<?> op, final boolean compositesInParts) {
		return UncountedFinalResultOutput.kind(op, compositesInParts);
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> operation(final OpType type, final Operation.Status status) {
		final Operation<Item> op = mock(Operation.class);
		when(op.type()).thenReturn(type);
		when(op.status()).thenReturn(status);
		return op;
	}

	private static Operation<Item> part(final Operation.Status status) {
		return part(OpType.CREATE, status);
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> part(final OpType type, final Operation.Status status) {
		final PartialOperation<Item> op = mock(PartialOperation.class);
		when(op.type()).thenReturn(type);
		when(op.status()).thenReturn(status);
		return op;
	}

	/** An operation that is both a part and a composite, which no class in the engine is. */
	@SuppressWarnings("unchecked")
	private static Operation<Item> partOfComposite(
					final OpType type, final Operation.Status status, final boolean done) {
		final CompositeOperation<Item> op = mock(CompositeOperation.class, withSettings().extraInterfaces(PartialOperation.class));
		when(op.type()).thenReturn(type);
		when(op.status()).thenReturn(status);
		when(op.allSubOperationsDone()).thenReturn(done);
		return op;
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> composite(
					final OpType type, final Operation.Status status, final boolean done) {
		final CompositeOperation<Item> op = mock(CompositeOperation.class);
		when(op.type()).thenReturn(type);
		when(op.status()).thenReturn(status);
		when(op.allSubOperationsDone()).thenReturn(done);
		return op;
	}

	private MixedLoadGenerator countLimitedGenerator(final Output<Operation> driver, final long countLimit) {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 50);
		weights.put(OpType.CREATE, 50);
		final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
		builders.put(OpType.READ, testBuilder(OpType.READ, null));
		builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null));
		return new MixedLoadGenerator(executor, new OpSchedule(weights), new PoolItemInput<>(makeItems(10)),
						builders, driver, 10_000, mockNewItemSupplier(), countLimit);
	}

	private static Output<Operation> capturingOutput(final List<Operation> dispatched) {
		return new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				dispatched.add(op);
				return true;
			}
		};
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
	@DisplayName("Operations the driver held unsent count out once at stop; refused and sent ones are not counted out")
	@Timeout(30)
	void heldOperationsCountOutWhenUnattemptedAndRefusedOrSentOnesDoNot() throws Exception {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(20));
		for (int i = 0; i < 100; i++) {
			pool.addDeleteItem(new ItemImpl("to-delete-" + i));
		}
		final OperationLifecycleTracker lifecycle = new OperationLifecycleTracker();
		final List<Operation> held = new ArrayList<>();
		final List<Operation> refused = new ArrayList<>();
		final AtomicInteger puts = new AtomicInteger();
		// A cooperative driver's admission: every third operation is refused and the tracker never
		// holds it; the others are queued in the driver's custody.
		final Output<Operation> driver = new AcceptingOutput() {
			@Override
			public boolean put(final Operation op) {
				if (puts.incrementAndGet() % 3 == 0) {
					refused.add(op);
					return false;
				}
				assertTrue(lifecycle.driverQueued(op));
				held.add(op);
				return true;
			}
		};
		final MixedLoadGenerator gen = readCreateDeleteGenerator(pool, driver, 1_000);
		gen.operationLifecycle(lifecycle);
		dispatch(gen, 300);
		// The schedule is shuffled, so the sent operations are chosen by type: four of each.
		final List<OpType> types = List.of(OpType.READ, OpType.CREATE, OpType.DELETE);
		final List<Operation> sent = new ArrayList<>();
		for (final OpType type : types) {
			held.stream().filter(op -> op.type() == type).limit(4).forEach(sent::add);
		}
		for (final Operation op : sent) {
			assertTrue(lifecycle.dispatched(op));
		}
		for (final OpType type : types) {
			assertEquals(count(held, type), gen.inFlightCount(type), "precondition: held " + type);
		}

		// The stop: what the driver held unsent becomes unattempted, by recovery or by the driver.
		for (final Operation op : held) {
			if (sent.stream().noneMatch(sentOp -> sentOp == op)) {
				assertTrue(lifecycle.unattempted(op));
			}
		}
		// An operation the driver refused was counted out at the refusal, not again here.
		for (final Operation op : refused) {
			lifecycle.unattempted(op);
		}
		// One still dispatched at the drain deadline stays counted.
		assertEquals(sent.size(), lifecycle.expireTerminalDeadline());

		assertTrue(lifecycle.counters().unattempted() > 0, "precondition: operations were unattempted");
		assertFalse(refused.isEmpty(), "precondition: operations were refused");
		for (final OpType type : types) {
			assertTrue(count(sent, type) > 0, "precondition: a sent " + type);
			assertEquals(count(sent, type), gen.inFlightCount(type), type + " still counted");
		}
	}

	@Test
	@DisplayName("A part of an unattempted composite does not count out; the composite counts out once")
	void anUnattemptedCompositeCountsOutOnceAndItsPartsNever() throws Exception {
		try (final DataInput dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4KB"), 4, false)) {
			final OperationLifecycleTracker lifecycle = new OperationLifecycleTracker();
			final List<Operation> held = new ArrayList<>();
			final Output<Operation> driver = new AcceptingOutput() {
				@Override
				public boolean put(final Operation op) {
					assertTrue(lifecycle.driverQueued(op));
					held.add(op);
					return true;
				}
			};
			final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
			weights.put(OpType.READ, 50);
			weights.put(OpType.CREATE, 50);
			final Map<OpType, OperationsBuilder> builders = new EnumMap<>(OpType.class);
			builders.put(OpType.READ, testBuilder(OpType.READ, null));
			builders.put(OpType.CREATE, testBuilder(OpType.CREATE, null, item -> {
				final DataItemImpl dataItem = new DataItemImpl(item.name(), 0, 2048);
				dataItem.dataInput(dataInput);
				return (Operation) new CompositeDataOperationImpl<>(0, OpType.CREATE, dataItem, null, "/bucket", null, null, 0, 1024);
			}));
			final MixedLoadGenerator gen = new MixedLoadGenerator(executor, new OpSchedule(weights),
							new PoolItemInput<>(makeItems(10)), builders, driver, 1_000, mockNewItemSupplier());
			gen.operationLifecycle(lifecycle);
			dispatch(gen, 40);
			final List<Operation> composites = held.stream().filter(op -> op instanceof CompositeOperation).toList();
			assertTrue(composites.size() >= 2, "precondition: composites were dispatched");
			final int creates = composites.size();
			assertEquals(creates, gen.inFlightCount(OpType.CREATE), "precondition");

			// The first composite started: its parts were queued, then taken back unsent.
			final CompositeOperation started = (CompositeOperation) composites.get(0);
			assertTrue(lifecycle.dispatched(started));
			for (final Object part : started.subOperations()) {
				assertTrue(part instanceof PartialOperation);
				assertTrue(lifecycle.driverQueued((Operation) part));
				assertTrue(lifecycle.unattempted((Operation) part));
			}
			assertEquals(creates, gen.inFlightCount(OpType.CREATE), "unattempted parts change nothing");

			// The second never started, and counts out once however often it is settled.
			final Operation neverStarted = composites.get(1);
			assertTrue(lifecycle.unattempted(neverStarted));
			assertFalse(lifecycle.unattempted(neverStarted));
			assertEquals(creates - 1, gen.inFlightCount(OpType.CREATE), "one composite counted out");
		}
	}

	private static long count(final List<Operation> ops, final OpType type) {
		return ops.stream().filter(op -> op.type() == type).count();
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
		return testBuilder(opType, tracker, item -> new OperationImpl<>(0, opType, item, null, null, null));
	}

	/** Creates a builder that records the op type when buildOp is called, and builds with {@code factory}. */
	private static OperationsBuilder<Item, Operation<Item>> testBuilder(
					final OpType opType, final ConcurrentLinkedQueue<OpType> tracker,
					final Function<Item, Operation<Item>> factory) {
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
				return factory.apply(item);
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
