package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Load steps of reads split into ranges, on the {@code s3} driver and its NIC-set driver against the
 * loopback stub. A read is counted when its last range ends, so the step's count is the complete sets
 * of ranges the client completed, whenever the step ends. The ranges completed are the client's own
 * range results, from the step's operation trace: a request the stub received is not one the client
 * completed. The step's bytes are those of every range completed: {@code count * object size} plus
 * the ranges of the reads the stop left unfinished, which are the only reads that may be unresolved.
 * A read the stop leaves unfinished may have completed none of its ranges.
 */
@Timeout(180)
final class S3SplitReadCountStepTest {

	private static final int RANGE_SIZE = 1024;
	private static final int RANGES = 4;
	private static final int OBJECT_SIZE = RANGES * RANGE_SIZE;
	/** More objects than a step can read in {@link #TIME_LIMIT_SECONDS}. */
	private static final int MANY_OBJECTS = 5_000;
	private static final int FEW_OBJECTS = 16;
	private static final int CONCURRENCY = 4;
	private static final int TIME_LIMIT_SECONDS = 3;
	/**
	 * The time limits a recycling step tries in turn, until its objects have been read more than once:
	 * a loaded host may read fewer than {@link #FEW_OBJECTS} + 1 objects in the first.
	 */
	private static final List<Integer> RECYCLING_TIME_LIMITS_SECONDS = List.of(TIME_LIMIT_SECONDS, 6, 12, 24);
	private static final int WAIT_LIMIT_SECONDS = 60;
	/** How long each recycling step may drain, so the ladder ends in minutes at most. */
	private static final int RECYCLING_WAIT_LIMIT_SECONDS = 10;
	/**
	 * Unresolved reads beyond the unfinished ones in the trace: the stop can refuse a read's initial
	 * pass between its dispatch and its result, which leaves it unresolved with no row. Initial passes
	 * run one at a time on a driver's dispatching thread, so this is a few at most.
	 */
	private static final int UNRESOLVED_WITHOUT_A_ROW = CONCURRENCY;
	/** A range's first attempt and the driver's three retries. */
	private static final int RANGE_ATTEMPTS = 4;
	private static final String CONTEXT = "READ-" + CONCURRENCY;

	private final NicSetLoopback stub = new NicSetLoopback();
	private final Map<String, Long> bytes = new ConcurrentHashMap<>();
	private final RangeResultTrace trace = new RangeResultTrace();
	private String stepId;

	@TempDir
	Path tempDir;

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
		stub.absentObjectSize = OBJECT_SIZE;
		stepId = "split-read-count-" + System.nanoTime();
	}

	@AfterEach
	void stop() throws Exception {
		trace.close();
		stub.close();
		NicSetLoopback.awaitNoNicIoThreads();
	}

	/** The input is longer than the step can read, so every read's ranges wait behind earlier ones. */
	@Test
	void aTimeLimitedStepCountsEveryObjectWhoseRangesAllRan() throws Exception {
		final var scenario = timeLimited(false, "", "", items(MANY_OBJECTS));

		final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

		assertCounted(counts, completed(path -> true));
	}

	@Test
	void aTimeLimitedStepOnANicSetCountsEveryObjectWhoseRangesAllRan() throws Exception {
		final var scenario = timeLimited(false, "", "", items(MANY_OBJECTS));

		final var counts = S3NicSetWorkloadTest.runScenario(
						stub.listeners(1), new NicSetLoopback.Factory(), stepId, scenario, bytes).get(CONTEXT);

		assertCounted(counts, completed(path -> true));
	}

	/**
	 * Recycled reads circulate until the limit: each complete set of an object's ranges is one read.
	 * Each step is checked, also one that completed no read; a longer one follows until the objects
	 * have been read more than once.
	 */
	@Test
	void aTimeLimitedRecyclingStepCountsEveryCompleteSetOfRanges() throws Exception {
		final var items = items(FEW_OBJECTS);
		RangeResultTrace.Completed completed = null;
		for (final var limit : RECYCLING_TIME_LIMITS_SECONDS) {
			stepId = "split-read-count-recycled-" + limit + "s-" + System.nanoTime();
			bytes.clear();
			final var scenario = timeLimited(limit, RECYCLING_WAIT_LIMIT_SECONDS, true, "", "", items);

			final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

			completed = completed(path -> true);
			assertCountedAsCompleted(counts, completed);
			if (completed.complete() > FEW_OBJECTS) {
				return;
			}
		}
		final var last = completed;
		throw new AssertionError("the objects were not read more than once in " + RECYCLING_TIME_LIMITS_SECONDS
						+ " s: " + last);
	}

	/** Verified ranges of real objects: each read ends only after its last range is verified. */
	@Test
	void aVerifiedTimeLimitedStepCountsEveryCompleteSetOfRanges() throws Exception {
		final var items = tempDir.resolve("seeded.csv").toString();
		final var seed = """
						Load.config({
							"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": %d}}},
							"item": {"data": {"size": "%d"}, "output": {"file": "%s"}}
						}).run();
						""".formatted(System.nanoTime(), FEW_OBJECTS, OBJECT_SIZE, items);
		final var scenario = seed + timeLimited(true, "", ", \"verify\": true", items);

		final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

		assertCounted(counts, completed(path -> true));
	}

	/**
	 * With {@code load.op.retry}, one object in forty has a range the target always refuses. Those
	 * reads fail, after their retry if the limit allows it, and every other read is counted as usual.
	 */
	@Test
	void aTimeLimitedStepWithOperationRetryCountsTheObjectsNotRefused() throws Exception {
		final Predicate<NicSetLoopback.Request> refused = request -> isRange(request)
						&& rangeStart(request) == RANGE_SIZE && objectIndex(request) % 40 == 0;
		stub.failRequest = refused;
		final var scenario = timeLimited(false, ", \"retry\": true, \"retryLimit\": 1", "", items(MANY_OBJECTS));

		final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

		// A refused object's reads complete at most three ranges each, but its attempts can add up to a
		// set: only the objects never refused have complete sets. The stub's requests bound the refused.
		final var completed = completed(path -> objectIndex(path) % 40 != 0);
		final var refusedObjects = stub.objectRequests().stream().filter(refused)
						.map(NicSetLoopback.Request::path).distinct().count();
		assertEquals(completed.complete(), counts.get(0), () -> counts + " against " + completed);
		assertTrue(counts.get(1) <= refusedObjects, () -> counts + " with " + refusedObjects + " objects refused");
		assertEquals(completed.bytes(), bytes.get(CONTEXT), () -> "bytes of " + completed);
		assertUnresolvedWithin(completed);
	}

	/**
	 * The count limit is exact: the reads it admits all finish, no other object is read, and the
	 * client completes each of their ranges once. A transient failure can add a range's retry, so the
	 * number of range requests is not checked.
	 */
	@Test
	void aCountLimitedStepReadsAndCountsExactlyItsLimit() throws Exception {
		final var limit = 100;

		final var counts = S3NicSetWorkloadTest.runScenario(
						stub.listeners(1), stepId, countLimited(limit, "", items(MANY_OBJECTS)), bytes).get(CONTEXT);

		final var completed = completed(path -> true);
		assertEquals(List.of((long) limit, 0L), counts, completed::toString);
		assertEquals(new RangeResultTrace.Completed(limit, 0, (long) limit * OBJECT_SIZE, 0), completed);
		assertEquals(limit, stub.objectRequests().stream().filter(S3SplitReadCountStepTest::isRange)
						.map(NicSetLoopback.Request::path).distinct().count(), "no object beyond the limit's reads is read");
		assertEquals(0, unresolved());
	}

	/**
	 * A count limit with {@code load.op.retry}: reads the target always refuses fail after their retry,
	 * a read refused only until its first attempt has failed succeeds on the retry, and each of the
	 * limit's reads ends as one or the other, with none unresolved.
	 */
	@Test
	void aCountLimitedStepWithOperationRetryEndsEveryReadOnce() throws Exception {
		final var limit = 40;
		final var alwaysRefused = limit / 8;
		final var refusalsOfTheRetriedRange = new AtomicInteger();
		stub.failRequest = request -> {
			if (!isRange(request) || rangeStart(request) != RANGE_SIZE) {
				return false;
			}
			final var object = objectIndex(request);
			return object % 8 == 0
							|| object == 3 && refusalsOfTheRetriedRange.incrementAndGet() <= RANGE_ATTEMPTS;
		};
		final var scenario = countLimited(limit, ", \"retry\": true, \"retryLimit\": 1", items(limit));

		final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

		assertEquals(List.of((long) limit - alwaysRefused, (long) alwaysRefused), counts, stub.objectRequests()::toString);
		assertEquals(0, unresolved());
	}

	private static String countLimited(final int limit, final String opSettings, final String items) {
		return """
						Load.config({
							"load": {
								"step": {"limit": {"time": "120s"}},
								"op": {"type": "read", "limit": {"count": %d}, "wait": {"limit": %d}%s}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"ranges": {"threshold": "%d"}}, "input": {"file": "%s"}},
							"output": {"metrics": {"trace": {"persist": true}}}
						}).run();
						""".formatted(limit, WAIT_LIMIT_SECONDS, opSettings, CONCURRENCY, RANGE_SIZE, items);
	}

	/** The step counted what the client completed, and completed some reads. */
	private void assertCounted(final List<Long> counts, final RangeResultTrace.Completed completed) throws IOException {
		assertTrue(completed.complete() > 0, completed::toString);
		assertCountedAsCompleted(counts, completed);
	}

	/** The step counted what the client completed, which may be nothing. */
	private void assertCountedAsCompleted(final List<Long> counts, final RangeResultTrace.Completed completed)
					throws IOException {
		assertEquals(List.of(completed.complete(), 0L), counts, completed::toString);
		assertEquals(completed.bytes(), bytes.getOrDefault(CONTEXT, 0L), () -> "bytes of " + completed);
		assertUnresolvedWithin(completed);
	}

	/** Only reads the stop left unfinished are unresolved. */
	private void assertUnresolvedWithin(final RangeResultTrace.Completed completed) throws IOException {
		final var unresolved = unresolved();
		assertTrue(unresolved <= completed.unfinished() + UNRESOLVED_WITHOUT_A_ROW,
						() -> unresolved + " unresolved against " + completed);
	}

	/** What the client completed of the objects {@code counted} picks, by their paths. */
	private RangeResultTrace.Completed completed(final Predicate<String> counted) throws InterruptedException {
		return trace.completed(stepId, RANGE_SIZE, RANGES, counted);
	}

	private static boolean isRange(final NicSetLoopback.Request request) {
		return "GET".equals(request.method()) && request.range() != null;
	}

	private static long rangeStart(final NicSetLoopback.Request request) {
		final var range = request.range();
		return Long.parseLong(range.substring("bytes=".length(), range.indexOf('-')));
	}

	private static int objectIndex(final NicSetLoopback.Request request) {
		return objectIndex(request.path());
	}

	private static int objectIndex(final String path) {
		return Integer.parseInt(path.substring(path.lastIndexOf('-') + 1));
	}

	/** An items file of {@code count} objects, which the stub serves without their being written. */
	private String items(final int count) throws IOException {
		final var file = tempDir.resolve("items.csv");
		Files.writeString(file, IntStream.range(0, count)
						.mapToObj(i -> "/bucket/split-" + i + "," + Long.toHexString(0x1000L + i) + "," + OBJECT_SIZE + ",0/")
						.collect(Collectors.joining("\n", "", "\n")));
		return file.toString();
	}

	private static String timeLimited(final boolean recycle, final String opSettings, final String dataSettings, final String items) {
		return timeLimited(TIME_LIMIT_SECONDS, WAIT_LIMIT_SECONDS, recycle, opSettings, dataSettings, items);
	}

	private static String timeLimited(final int seconds, final int waitSeconds, final boolean recycle, final String opSettings,
					final String dataSettings, final String items) {
		return """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%ds"}},
								"op": {"type": "read", "limit": {"count": 0}, "recycle": {"mode": %s}, "wait": {"limit": %d}%s}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"ranges": {"threshold": "%d"}%s}, "input": {"file": "%s"}},
							"output": {"metrics": {"trace": {"persist": true}}}
						}).run();
						""".formatted(seconds, recycle, waitSeconds, opSettings, CONCURRENCY, RANGE_SIZE,
						dataSettings, items);
	}

	/** {@code unresolved} in the step's {@code operation.lifecycle.csv}, which reconciles. */
	private long unresolved() throws IOException {
		final var lines = Files.readAllLines(
						Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)));
		assertEquals(OperationLifecycleArtifact.HEADER, lines.get(0));
		assertEquals(2, lines.size(), lines::toString);
		final var header = List.of(lines.get(0).split(",", -1));
		final var row = lines.get(1).split(",", -1);
		assertEquals("true", row[header.indexOf("terminal")], () -> "the lifecycle reconciles: " + lines);
		return Long.parseLong(row[header.indexOf("unresolved")]);
	}
}
