package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Reads split by the size threshold ({@code --part-size}) on the {@code s3-aws} driver, against a
 * loopback S3 that serves ranged GETs. The read sends no request of its own; each part is a ranged
 * GET at its position in the object, whatever the item's offset in the data ring, which every test
 * makes nonzero. The read is counted once, when its last part ends, with its parts' bytes and a
 * duration from its initial pass to that end. A step's count is therefore the complete sets of
 * ranges the client completed, whenever the step ends. The step tests take the ranges completed
 * from the client's own range results, in the step's operation trace: a request the stub received
 * is not one the client completed. The step's {@code operation.lifecycle.csv} ends exactly the
 * results in its trace, successes and failures, so a result the step output, such as a read it
 * counted, is never also left unresolved.
 */
@Timeout(value = 240, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsCompositeReadTest {

	private static final long MIB = 1024 * 1024;
	private static final long OBJECT_SIZE = 12 * MIB;
	private static final long PART_SIZE = 5 * MIB;
	/** The data-ring offset of the first item the lab seeded when the defect was measured. */
	private static final long OFFSET = 0x3209dd930a5f4c47L;
	private static final List<String> PART_RANGES = List.of(
					"bytes=0-5242879", "bytes=5242880-10485759", "bytes=10485760-12582911");
	/** A part's first attempt and the driver's three retries. */
	private static final int DRIVER_ATTEMPTS_PER_PART = 4;
	private static final long RESULT_TIMEOUT_SECONDS = 60;
	/** The objects of the step tests, read in {@link #STEP_RANGES} ranges of {@link #STEP_RANGE_SIZE}. */
	private static final int STEP_RANGE_SIZE = 16 * 1024;
	private static final int STEP_RANGES = 4;
	private static final int STEP_OBJECT_SIZE = STEP_RANGES * STEP_RANGE_SIZE;
	/** More objects than a step can read in {@link #STEP_TIME_LIMIT}. */
	private static final int MANY_OBJECTS = 5_000;
	private static final int FEW_OBJECTS = 16;
	private static final String STEP_TIME_LIMIT = "3s";
	/**
	 * The time limits a recycling step tries in turn, until its objects have been read more than once:
	 * a loaded host may read fewer than {@link #FEW_OBJECTS} + 1 objects in the first.
	 */
	private static final List<String> RECYCLING_TIME_LIMITS = List.of(STEP_TIME_LIMIT, "6s", "12s", "24s");
	/** How long a step may drain after its limit; a recycling step less, so the ladder ends in minutes. */
	private static final int WAIT_LIMIT_SECONDS = 60;
	private static final int RECYCLING_WAIT_LIMIT_SECONDS = 10;
	private static final int STEP_CONCURRENCY = 4;
	private static final String LONG_TIME_LIMIT = "120s";
	/** One object in this many has a range the target refuses, in the tests that refuse any. */
	private static final int REFUSED_ONE_IN = 8;
	/** An answer the CRT client does not retry itself and the engine's {@code load.op.retry} does. */
	private static final int RETRYABLE_STATUS = 501;
	private static final String OP_RETRY = ", \"retry\": true, \"retryLimit\": 1";

	private final RangeStub stub = new RangeStub();
	private final RangeResultTrace trace = new RangeResultTrace();
	private String stepId;

	@TempDir
	Path home;

	@AfterEach
	void stop() {
		trace.close();
		stub.close();
	}

	@Test
	void aReadSplitByThePartSizeSendsARangedGetPerPartAndIsCountedOnce() throws Exception {
		try (var driver = new StartedDriver(driver())) {
			final var start = System.nanoTime();
			final var read = driver.read(readOp("split"));
			final var wallMicros = (System.nanoTime() - start) / 1_000;

			assertEquals(PART_RANGES, stub.sortedRanges(), stub.gets::toString);
			final var parent = finalization(read);
			assertEquals(Operation.Status.SUCC, parent.status(), read::toString);
			assertEquals(0, ((DataOperation<?>) parent).countBytesDone(), "the parts count the bytes");
			final var parts = parts(read);
			assertEquals(3, parts.size(), read::toString);
			assertEquals(OBJECT_SIZE, parts.stream().mapToLong(PartialDataOperation::countBytesDone).sum());
			for (final var part : parts) {
				assertEquals(Operation.Status.SUCC, part.status(), part::toString);
			}
			assertInitialPassThenFinalization(read);
			final var initial = read.get(0);
			assertEquals(initial.reqTimeStart(), parent.reqTimeStart(), "the read is timed from its initial pass");
			for (final var part : parts) {
				assertTrue(parent.reqTimeStart() <= part.reqTimeStart() && part.respTimeDone() <= parent.respTimeDone(),
								() -> "parent " + times(parent) + ", part " + times(part));
			}
			assertTrue(parent.duration() > 0 && parent.duration() <= wallMicros,
							() -> "duration " + parent.duration() + " µs against " + wallMicros + " µs of wall time");
		}
	}

	/**
	 * A recycled read is the result of the finished one, handed to the driver again, as recycle
	 * mode does. It has no parts and counts them done, and is still read part by part.
	 */
	@Test
	void aRecycledReadSendsItsRangedGetsAgain() throws Exception {
		try (var driver = new StartedDriver(driver())) {
			final var first = finalization(driver.read(readOp("recycled")));
			assertTrue(((CompositeDataOperation<?>) first).allSubOperationsDone(), first::toString);
			final long firstDone = first.respTimeDone();
			stub.gets.clear();

			final var again = driver.read((Operation<DataItem>) first);

			assertEquals(PART_RANGES, stub.sortedRanges(), stub.gets::toString);
			assertInitialPassThenFinalization(again);
			final var parent = finalization(again);
			assertEquals(Operation.Status.SUCC, parent.status(), again::toString);
			assertEquals(3, parts(again).size(), again::toString);
			assertEquals(OBJECT_SIZE, parts(again).stream().mapToLong(PartialDataOperation::countBytesDone).sum());
			assertTrue(parent.reqTimeStart() > firstDone && parent.duration() > 0, () -> times(parent));
		}
	}

	/**
	 * The middle part is refused on every attempt: it is sent four times, the other parts once, and
	 * the read ends once, with the part's status.
	 */
	@Test
	void aFailedMiddlePartFailsTheReadOnceWithThePartsStatus() throws Exception {
		stub.refuse = rangeStart -> rangeStart == PART_SIZE;
		try (var driver = new StartedDriver(driver())) {
			final var read = driver.read(readOp("refused"), DRIVER_ATTEMPTS_PER_PART + 2);

			final var parent = finalization(read);
			assertEquals(Operation.Status.RESP_FAIL_AUTH, parent.status(), read::toString);
			assertEquals(1, read.stream().filter(result -> result instanceof CompositeDataOperation<?>
							&& result.status() != Operation.Status.SUCC).count(), read::toString);
			final var refused = parts(read).stream().filter(part -> part.partNumber() == 1).toList();
			assertEquals(DRIVER_ATTEMPTS_PER_PART, refused.size(), read::toString);
			for (final var part : refused) {
				assertEquals(Operation.Status.RESP_FAIL_AUTH, part.status(), part::toString);
			}
			assertEquals(DRIVER_ATTEMPTS_PER_PART + 2, stub.gets.size(), stub.gets::toString);
			assertTrue(parent.duration() > 0, () -> times(parent));
		}
	}

	/**
	 * A read that is not split, of the same object, is one GET of the whole object, as before: the
	 * CRT client asks for the object's size, then names the whole object as its range.
	 */
	@Test
	void aWholeObjectReadIsUnchanged() throws Exception {
		try (var driver = new StartedDriver(driver())) {
			final DataItem item = new DataItemImpl("whole", OFFSET, OBJECT_SIZE);
			item.dataInput(dataInput());
			final var result = driver.next(new DataOperationImpl<DataItem>(
							0, OpType.READ, item, "/bucket", null, S3AwsDriverHarness.CREDENTIAL, null, 0));

			assertEquals(Operation.Status.SUCC, result.status(), () -> result + " " + stub.gets);
			assertEquals(OBJECT_SIZE, ((DataOperation<?>) result).countBytesDone());
			assertEquals(List.of("GET /bucket/whole bytes=0-" + (OBJECT_SIZE - 1)), stub.gets);
		}
	}

	/**
	 * Through a load step, as {@code spt run read --part-size 5MiB} runs it: two items read once
	 * each count two successes, no failure, and their bytes once.
	 */
	@Test
	void aReadStepCountsEachSplitReadOnceWithItsBytes() throws Exception {
		final var outcome = readStep(2, false);

		assertEquals(List.of(2L, 0L, 2 * OBJECT_SIZE), outcome, outcome::toString);
		assertEquals(2 * PART_RANGES.size(), stub.gets.size(), stub.gets::toString);
	}

	/** In recycle mode the step reads the same two items again, each time part by part. */
	@Test
	void aRecyclingReadStepReadsEachItemPartByPartEveryTime() throws Exception {
		final var outcome = readStep(6, true);

		assertEquals(List.of(6L, 0L, 6 * OBJECT_SIZE), outcome, outcome::toString);
		assertEquals(6 * PART_RANGES.size(), stub.gets.size(), stub.gets::toString);
		final Map<String, Integer> byRange = new ConcurrentHashMap<>();
		stub.gets.forEach(get -> byRange.merge(get.substring(get.indexOf("bytes=")), 1, Integer::sum));
		assertEquals(Map.of(PART_RANGES.get(0), 6, PART_RANGES.get(1), 6, PART_RANGES.get(2), 6), byRange);
	}

	/** The input is longer than the step can read, so every read's ranges wait behind earlier ones. */
	@Test
	void aTimeLimitedStepCountsEveryObjectWhoseRangesAllRan() throws Exception {
		final var outcome = splitStep(STEP_TIME_LIMIT, 0, false, "", MANY_OBJECTS);

		assertCounted(outcome);
	}

	/**
	 * Recycled reads circulate until the limit: each complete set of an object's ranges is one read.
	 * Each step is checked, also one that completed no read; a longer one follows until the objects
	 * have been read more than once.
	 */
	@Test
	void aTimeLimitedRecyclingStepCountsEveryCompleteSetOfRanges() throws Exception {
		RangeResultTrace.Completed completed = null;
		for (final var limit : RECYCLING_TIME_LIMITS) {
			final var outcome = splitStep(limit, RECYCLING_WAIT_LIMIT_SECONDS, 0, true, "", FEW_OBJECTS);

			completed = completed();
			assertCountedAsCompleted(outcome, completed);
			if (completed.complete() > FEW_OBJECTS) {
				return;
			}
		}
		final var last = completed;
		throw new AssertionError("the objects were not read more than once in " + RECYCLING_TIME_LIMITS + ": " + last);
	}

	/**
	 * The count limit is exact: the reads it admits all finish, no other object is read, and the
	 * client completes each of their ranges once. A transient failure can add a range's retry, so the
	 * number of range requests is not checked.
	 */
	@Test
	void aCountLimitedStepReadsAndCountsExactlyItsLimit() throws Exception {
		final var limit = 100;

		final var outcome = splitStep(LONG_TIME_LIMIT, limit, false, "", MANY_OBJECTS);

		final var completed = completed();
		assertEquals(List.of((long) limit, 0L, (long) limit * STEP_OBJECT_SIZE), outcome, completed::toString);
		assertEquals(new RangeResultTrace.Completed(limit, 0, (long) limit * STEP_OBJECT_SIZE, 0), completed);
		assertEquals(limit, stub.gets.stream().map(get -> get.split(" ")[1]).distinct().count(),
						"no object beyond the limit's reads is read");
		assertEquals(0, unresolved());
		assertLifecycleEndsEveryResult();
	}

	/** A range refused once is retried by the driver: its read succeeds and is counted. */
	@Test
	void aRangeRetriedByTheDriverLeavesItsReadCounted() throws Exception {
		final Map<String, Integer> refusals = new ConcurrentHashMap<>();
		stub.refusalStatus = RETRYABLE_STATUS;
		stub.refuseRange = (path, rangeStart) -> rangeStart == STEP_RANGE_SIZE && refusedObject(path)
						&& refusals.merge(path, 1, Integer::sum) == 1;

		final var outcome = splitStep(STEP_TIME_LIMIT, 0, false, "", MANY_OBJECTS);

		assertFalse(refusals.isEmpty(), "a range was refused");
		assertCounted(outcome);
	}

	/**
	 * A range the target always refuses fails its read, which is counted as one failure when its
	 * attempts and the other ranges have all ended; every other read is counted as usual.
	 */
	@Test
	void aFailedReadIsCountedOnceAsAFailure() throws Exception {
		stub.refuseRange = (path, rangeStart) -> rangeStart == STEP_RANGE_SIZE && refusedObject(path);

		final var outcome = splitStep(STEP_TIME_LIMIT, 0, false, "", MANY_OBJECTS);

		final var completed = completed();
		assertEquals(completed.complete(), outcome.get(0), () -> outcome + " against " + completed);
		assertEquals(readsOutOfAttempts(), outcome.get(1), () -> outcome + " against " + stub.gets);
		assertEquals(completed.bytes(), outcome.get(2), () -> outcome + " against " + completed);
		assertLifecycleEndsEveryResult();
	}

	/**
	 * With {@code load.op.retry} every read not refused is counted, and the reads refused every time
	 * fail no more than once each.
	 */
	@Test
	void aTimeLimitedStepWithOperationRetryCountsTheObjectsNotRefused() throws Exception {
		stub.refusalStatus = RETRYABLE_STATUS;
		stub.refuseRange = (path, rangeStart) -> rangeStart == STEP_RANGE_SIZE && refusedObject(path);

		final var outcome = splitStep(STEP_TIME_LIMIT, 0, false, OP_RETRY, MANY_OBJECTS);

		// A refused object's attempts complete at most three ranges each, but can add up to a set: only
		// the objects never refused have complete sets. The stub's requests bound the refused.
		final var completed = trace.completed(stepId, STEP_RANGE_SIZE, STEP_RANGES, path -> !refusedObject(path));
		final var refusedObjects = stub.gets.stream().map(get -> get.split(" ")[1]).distinct()
						.filter(S3AwsCompositeReadTest::refusedObject).count();
		assertEquals(completed.complete(), outcome.get(0), () -> outcome + " against " + completed);
		assertTrue(outcome.get(1) <= refusedObjects, () -> outcome + " with " + refusedObjects + " objects refused");
		assertEquals(completed.bytes(), outcome.get(2), () -> outcome + " against " + completed);
		assertLifecycleEndsEveryResult();
	}

	/**
	 * A count limit with {@code load.op.retry}: reads the target always refuses fail after their retry,
	 * a read refused until its first attempt has failed succeeds on the retry, and each of the limit's
	 * reads ends as one or the other, with none unresolved.
	 */
	@Test
	void aCountLimitedStepWithOperationRetryEndsEveryReadOnce() throws Exception {
		final var limit = 40;
		final var alwaysRefused = limit / REFUSED_ONE_IN;
		final Map<String, Integer> refusals = new ConcurrentHashMap<>();
		stub.refusalStatus = RETRYABLE_STATUS;
		stub.refuseRange = (path, rangeStart) -> rangeStart == STEP_RANGE_SIZE && (refusedObject(path)
						|| objectIndex(path) == 3 && refusals.merge(path, 1, Integer::sum) <= DRIVER_ATTEMPTS_PER_PART);

		final var outcome = splitStep(LONG_TIME_LIMIT, limit, false, OP_RETRY, limit);

		assertEquals(List.of((long) limit - alwaysRefused, (long) alwaysRefused), outcome.subList(0, 2),
						() -> outcome + " against " + stub.gets);
		assertEquals(0, unresolved());
		assertLifecycleEndsEveryResult();
	}

	/** The step counted what the client completed, and completed some reads. */
	private void assertCounted(final List<Long> outcome) throws Exception {
		final var completed = completed();
		assertTrue(completed.complete() > 0, completed::toString);
		assertCountedAsCompleted(outcome, completed);
	}

	/** The step counted what the client completed, which may be nothing. */
	private void assertCountedAsCompleted(final List<Long> outcome, final RangeResultTrace.Completed completed)
					throws IOException, InterruptedException {
		assertEquals(List.of(completed.complete(), 0L, completed.bytes()), outcome, completed::toString);
		assertLifecycleEndsEveryResult();
	}

	/**
	 * The lifecycle accepted exactly the successful results in the step's trace and failed exactly the
	 * others: a result output and then left unresolved, or ended without output, breaks one of them.
	 * It checks neither {@code unattempted} nor {@code unresolved}: the artifact's reconciliation pins
	 * only their sum, and no test checks {@code selected}.
	 */
	private void assertLifecycleEndsEveryResult() throws IOException, InterruptedException {
		final var lifecycle = lifecycle();
		final var results = trace.results(stepId);
		assertEquals(results, List.of(lifecycle.get("accepted"), lifecycle.get("failed")),
						() -> "the lifecycle " + lifecycle + " against the trace's results " + results);
	}

	private static boolean refusedObject(final String path) {
		return objectIndex(path) % REFUSED_ONE_IN == 0;
	}

	private static int objectIndex(final String path) {
		return Integer.parseInt(path.substring(path.lastIndexOf('-') + 1));
	}

	/**
	 * The reads whose refused range, by the client's results, failed every attempt with the stub's
	 * refusal and whose other ranges all succeeded. Each object is read once.
	 */
	private long readsOutOfAttempts() throws InterruptedException {
		final var refusal = Operation.Status.RESP_FAIL_AUTH.name();
		final Map<String, Long> refusedAttempts = new HashMap<>();
		final Map<String, Long> succeededRanges = new HashMap<>();
		for (final var row : trace.rows(stepId)) {
			if (refusal.equals(row[3])) {
				refusedAttempts.merge(row[1], 1L, Long::sum);
			} else if ("SUCC".equals(row[3]) && Integer.toString(STEP_RANGE_SIZE).equals(row[8])) {
				succeededRanges.merge(row[1], 1L, Long::sum);
			}
		}
		// The read's own failure is one more row with the refusal.
		return refusedAttempts.entrySet().stream()
						.filter(entry -> refusedObject(entry.getKey()) && entry.getValue() >= DRIVER_ATTEMPTS_PER_PART
										&& succeededRanges.getOrDefault(entry.getKey(), 0L) == STEP_RANGES - 1)
						.count();
	}

	/** What the client completed in the last step, of every object. */
	private RangeResultTrace.Completed completed() throws InterruptedException {
		return trace.completed(stepId, STEP_RANGE_SIZE, STEP_RANGES, path -> true);
	}

	/** {@code unresolved} in the last step's {@code operation.lifecycle.csv}, which reconciles. */
	private long unresolved() throws IOException {
		return lifecycle().get("unresolved");
	}

	/** The counts of the last step's {@code operation.lifecycle.csv}, which reconciles, by column. */
	private Map<String, Long> lifecycle() throws IOException {
		final var lines = Files.readAllLines(
						Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)));
		assertEquals(OperationLifecycleArtifact.HEADER, lines.get(0));
		assertEquals(2, lines.size(), lines::toString);
		final var header = List.of(lines.get(0).split(",", -1));
		final var row = lines.get(1).split(",", -1);
		assertEquals("true", row[header.indexOf("terminal")], () -> "the lifecycle reconciles: " + lines);
		final Map<String, Long> counts = new LinkedHashMap<>();
		for (final var column : List.of("selected", "accepted", "failed", "unattempted", "unresolved")) {
			counts.put(column, Long.parseLong(row[header.indexOf(column)]));
		}
		return counts;
	}

	/**
	 * A step of split reads of {@code objects} objects, which the stub serves unwritten, at
	 * {@link #STEP_CONCURRENCY}; returns its successes, failures and bytes.
	 */
	private List<Long> splitStep(final String timeLimit, final int count, final boolean recycle, final String opSettings,
					final int objects) throws Exception {
		return splitStep(timeLimit, WAIT_LIMIT_SECONDS, count, recycle, opSettings, objects);
	}

	/** As above, with the step's drain after its limit, {@code waitSeconds}. */
	private List<Long> splitStep(final String timeLimit, final int waitSeconds, final int count, final boolean recycle,
					final String opSettings, final int objects) throws Exception {
		stub.objectSize = STEP_OBJECT_SIZE;
		final var items = home.resolve("split-items.csv");
		Files.writeString(items, IntStream.range(0, objects)
						.mapToObj(i -> "/bucket/split-" + i + "," + Long.toHexString(OFFSET + i) + "," + STEP_OBJECT_SIZE + ",0/")
						.collect(Collectors.joining("\n", "", "\n")));
		return step("""
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"type": "read", "limit": {"count": %d}, "recycle": {"mode": %s}, "wait": {"limit": %d}%s}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"input": {"file": "%s"}, "data": {"ranges": {"threshold": "%d"}}},
							"output": {"metrics": {"trace": {"persist": true}}}
						}).run();
						""".formatted(timeLimit, count, recycle, waitSeconds, opSettings, STEP_CONCURRENCY,
						items.toString().replace("\\", "/"), STEP_RANGE_SIZE));
	}

	/** A read step of {@code count} reads of two items; returns its successes, failures and bytes. */
	private List<Long> readStep(final int count, final boolean recycle) throws Exception {
		final var items = home.resolve("items.csv");
		Files.writeString(items, "/bucket/step-1," + Long.toHexString(OFFSET) + "," + OBJECT_SIZE + ",0/\n"
						+ "/bucket/step-2," + Long.toHexString(OFFSET + 1) + "," + OBJECT_SIZE + ",0/\n");
		return step("""
						Load.config({
							"load": {
								"step": {"limit": {"time": "120s"}},
								"op": {"type": "read", "limit": {"count": %d}, "recycle": {"mode": %s}}
							},
							"storage": {"driver": {"limit": {"concurrency": 2}}},
							"item": {"input": {"file": "%s"}, "data": {"ranges": {"threshold": "%d"}}}
						}).run();
						""".formatted(count, recycle, items.toString().replace("\\", "/"), PART_SIZE));
	}

	/** Runs {@code scenario} as a new step; returns its successes, failures and bytes. */
	private List<Long> step(final String scenario) throws Exception {
		final var currentStep = "s3-aws-composite-read-" + System.nanoTime();
		stepId = currentStep;
		final var config = S3AwsMergedConfig.create(home, stub.port(), Map.of(
						"load-step-id", currentStep,
						"load-step-idAutoGenerated", false));
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = Extension.load(classLoader);
		final Map<String, List<Long>> outcomes = new ConcurrentHashMap<>();
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR) {
			@Override
			public void unregister(final MetricsContext metricsCtx) {
				super.unregister(metricsCtx);
				if (!(metricsCtx instanceof DistributedMetricsContext) && currentStep.equals(metricsCtx.loadStepId())) {
					final var snapshot = metricsCtx.lastSnapshot();
					outcomes.put(metricsCtx.opType().name(), List.of(
									snapshot.successSnapshot().count(),
									snapshot.failsSnapshot().count(),
									snapshot.byteSnapshot().count()));
				}
			}
		}) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("s3-aws composite read", scenario, engine, 1L).run();
		}
		final var outcome = outcomes.get("READ");
		assertNotNull(outcome, outcomes::toString);
		return outcome;
	}

	private static void assertInitialPassThenFinalization(final List<Operation<?>> read) {
		final var composites = read.stream().filter(result -> result instanceof CompositeDataOperation<?>).toList();
		assertEquals(2, composites.size(), "the initial pass and the finalization: " + read);
		assertEquals(Operation.Status.SUCC, composites.get(0).status(), read::toString);
		assertTrue(!((CompositeDataOperation<?>) composites.get(0)).allSubOperationsDone(), read::toString);
	}

	/** The read's last result of its own; a part that ended with it may be output after it. */
	private static Operation<?> finalization(final List<Operation<?>> read) {
		final var composites = read.stream().filter(result -> result instanceof CompositeDataOperation<?>).toList();
		return composites.get(composites.size() - 1);
	}

	private static List<PartialDataOperation<?>> parts(final List<Operation<?>> results) {
		return results.stream().filter(result -> result instanceof PartialDataOperation<?>)
						.<PartialDataOperation<?>> map(result -> (PartialDataOperation<?>) result).toList();
	}

	private static String times(final Operation<?> result) {
		return result.getClass().getSimpleName() + " " + result.status() + " [" + result.reqTimeStart() + ", "
						+ result.reqTimeDone() + ", " + result.respTimeStart() + ", " + result.respTimeDone() + "]";
	}

	private static DataInput dataInput() throws Exception {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 1, false);
	}

	private static CompositeDataOperation<DataItem> readOp(final String name) throws Exception {
		final DataItem item = new DataItemImpl(name, OFFSET, OBJECT_SIZE);
		item.dataInput(dataInput());
		return new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, "/bucket", null, S3AwsDriverHarness.CREDENTIAL, null, 0, PART_SIZE);
	}

	@SuppressWarnings("unchecked")
	private S3AwsStorageDriver<DataItem, Operation<DataItem>> driver() throws Exception {
		final var storage = S3AwsMergedConfig.create(home, stub.port(), Map.of()).configVal("storage");
		return (S3AwsStorageDriver<DataItem, Operation<DataItem>>) new S3AwsStorageDriverFactory<DataItem, Operation<DataItem>>()
						.create("s3-aws-composite-read", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
										storage, false, 16);
	}

	/** A started driver and the results it outputs. */
	private static final class StartedDriver implements AutoCloseable {

		private final S3AwsStorageDriver<DataItem, Operation<DataItem>> driver;
		private final LinkedBlockingQueue<Operation<?>> results = new LinkedBlockingQueue<>();

		StartedDriver(final S3AwsStorageDriver<DataItem, Operation<DataItem>> driver) throws Exception {
			this.driver = driver;
			driver.operationResultOutput(new ResultOutput(results));
			driver.start();
		}

		/** Every result of a read whose parts each take one attempt. */
		List<Operation<?>> read(final Operation<?> op) throws Exception {
			return read(op, PART_RANGES.size());
		}

		/**
		 * Every result of the read, in the order output, up to the composite's last and
		 * {@code partResults} results of parts. The last part to end ends the composite, and another
		 * part that ended just before it may still be on its way to the output.
		 */
		@SuppressWarnings("unchecked")
		List<Operation<?>> read(final Operation<?> op, final int partResults) throws Exception {
			assertTrue(driver.put((Operation<DataItem>) op));
			final List<Operation<?>> read = new ArrayList<>();
			var ended = false;
			var parts = 0;
			while (!ended || parts < partResults) {
				final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
				assertNotNull(result, () -> "the read did not finish: " + read);
				read.add(result);
				parts += result instanceof PartialDataOperation<?> ? 1 : 0;
				ended |= result instanceof CompositeDataOperation<?> composite
								&& (composite.allSubOperationsDone() || result.status() != Operation.Status.SUCC);
			}
			return read;
		}

		Operation<?> next(final Operation<DataItem> op) throws Exception {
			assertTrue(driver.put(op));
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, "the read did not finish");
			return result;
		}

		@Override
		public void close() throws Exception {
			driver.close();
		}
	}

	/**
	 * A loopback S3 that serves objects of {@link #objectSize} bytes: their size to a HEAD, and their
	 * data, whole or by range, to a GET. It answers every other request with an empty 200. It refuses
	 * with {@link #refusalStatus}, by default 403, which the client does not retry itself, the ranged
	 * GETs that {@link #refuse} or {@link #refuseRange} picks. It records each GET as its request line
	 * and range.
	 */
	private static final class RangeStub implements AutoCloseable {

		private static final byte[] CHUNK = new byte[64 * 1024];

		private final HttpServer server;
		private final ExecutorService threads = Executors.newCachedThreadPool();
		final List<String> gets = new CopyOnWriteArrayList<>();
		volatile LongPredicate refuse = rangeStart -> false;
		volatile BiPredicate<String, Long> refuseRange = (path, rangeStart) -> false;
		volatile int refusalStatus = 403;
		volatile long objectSize = OBJECT_SIZE;

		RangeStub() {
			try {
				server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			} catch (final IOException e) {
				throw new IllegalStateException(e);
			}
			server.setExecutor(threads);
			server.createContext("/", this::handle);
			server.start();
		}

		int port() {
			return server.getAddress().getPort();
		}

		List<String> sortedRanges() {
			return gets.stream().map(get -> get.substring(get.indexOf(' ', 4) + 1)).sorted((a, b) -> Long.compare(start(a), start(b)))
							.toList();
		}

		private static long start(final String range) {
			return range.startsWith("bytes=") ? Long.parseLong(range.substring(6, range.indexOf('-'))) : -1;
		}

		private void handle(final HttpExchange exchange) throws IOException {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				final var headers = exchange.getResponseHeaders();
				headers.set("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)));
				final var query = exchange.getRequestURI().getRawQuery();
				final var method = exchange.getRequestMethod();
				final boolean object = exchange.getRequestURI().getPath().indexOf('/', 1) > 0;
				if ("HEAD".equals(method) && object && query == null) {
					// The CRT client asks for the size of an object it reads whole.
					headers.set("Content-Length", Long.toString(objectSize));
					headers.set("ETag", "\"stub\"");
					headers.set("Accept-Ranges", "bytes");
					exchange.sendResponseHeaders(200, -1);
					return;
				}
				if (!"GET".equals(method) || query != null) {
					headers.set("Content-Length", "0");
					exchange.sendResponseHeaders(200, -1);
					return;
				}
				final var range = exchange.getRequestHeaders().getFirst("Range");
				gets.add("GET " + exchange.getRequestURI().getPath() + " " + (range == null ? "none" : range));
				headers.set("ETag", "\"stub\"");
				headers.set("Accept-Ranges", "bytes");
				headers.set("Content-Type", "application/octet-stream");
				final long size = objectSize;
				if (range == null) {
					body(exchange, 200, size);
					return;
				}
				final long first = start(range);
				final long last = Math.min(Long.parseLong(range.substring(range.indexOf('-') + 1)), size - 1);
				final var path = exchange.getRequestURI().getPath();
				if (refuse.test(first) || refuseRange.test(path, first)) {
					final var xml = "<Error><Code>AccessDenied</Code><Message>scripted</Message></Error>"
									.getBytes(StandardCharsets.UTF_8);
					headers.set("Content-Type", "application/xml");
					exchange.sendResponseHeaders(refusalStatus, xml.length);
					exchange.getResponseBody().write(xml);
					return;
				}
				headers.set("Content-Range", "bytes " + first + "-" + last + "/" + size);
				body(exchange, 206, last - first + 1);
			}
		}

		private static void body(final HttpExchange exchange, final int status, final long length) throws IOException {
			exchange.sendResponseHeaders(status, length);
			final OutputStream out = exchange.getResponseBody();
			for (long left = length; left > 0; left -= CHUNK.length) {
				out.write(CHUNK, 0, (int) Math.min(CHUNK.length, left));
			}
		}

		@Override
		public void close() {
			server.stop(0);
			threads.shutdownNow();
		}
	}

	private static final class ResultOutput implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<?>> results;

		ResultOutput(final LinkedBlockingQueue<Operation<?>> results) {
			this.results = results;
		}

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			for (var i = from; i < to; i++) {
				results.offer(values.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}
}
