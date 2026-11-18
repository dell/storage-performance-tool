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
import java.util.HashMap;
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
 * loopback stub, which records every range it is asked for. A read is counted when its last range
 * ends, so the step's count is the complete sets of ranges the target served, whenever the step ends.
 * Its bytes are those of every range served: {@code count * object size} plus the ranges of the reads
 * the stop left with a range unsent, which are the only reads that may be unresolved.
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
	private static final int WAIT_LIMIT_SECONDS = 60;
	/** A range's first attempt and the driver's three retries. */
	private static final int RANGE_ATTEMPTS = 4;
	private static final String CONTEXT = "READ-" + CONCURRENCY;

	private final NicSetLoopback stub = new NicSetLoopback();
	private final Map<String, Long> bytes = new ConcurrentHashMap<>();
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
		stub.close();
		NicSetLoopback.awaitNoNicIoThreads();
	}

	/** The input is longer than the step can read, so every read's ranges wait behind earlier ones. */
	@Test
	void aTimeLimitedStepCountsEveryObjectWhoseRangesAllRan() throws Exception {
		final var scenario = timeLimited(false, "", "", items(MANY_OBJECTS));

		final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

		assertCounted(counts, served(request -> true));
	}

	@Test
	void aTimeLimitedStepOnANicSetCountsEveryObjectWhoseRangesAllRan() throws Exception {
		final var scenario = timeLimited(false, "", "", items(MANY_OBJECTS));

		final var counts = S3NicSetWorkloadTest.runScenario(
						stub.listeners(1), new NicSetLoopback.Factory(), stepId, scenario, bytes).get(CONTEXT);

		assertCounted(counts, served(request -> true));
	}

	/** Recycled reads circulate until the limit: each complete set of an object's ranges is one read. */
	@Test
	void aTimeLimitedRecyclingStepCountsEveryCompleteSetOfRanges() throws Exception {
		final var scenario = timeLimited(true, "", "", items(FEW_OBJECTS));

		final var counts = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, scenario, bytes).get(CONTEXT);

		final var served = served(request -> true);
		assertCounted(counts, served);
		assertTrue(served.complete() > FEW_OBJECTS, () -> "the objects were read more than once: " + served);
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

		assertCounted(counts, served(request -> true));
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

		final var served = served(refused.negate());
		final var refusedObjects = stub.objectRequests().stream().filter(refused)
						.map(NicSetLoopback.Request::path).distinct().count();
		assertEquals(served.complete(), counts.get(0), () -> counts + " against " + served);
		assertTrue(counts.get(1) <= refusedObjects, () -> counts + " with " + refusedObjects + " objects refused");
		assertEquals(served.bytes(), bytes.get(CONTEXT), () -> "bytes of " + served);
		final var unresolved = unresolved();
		assertTrue(unresolved <= served.incomplete() + refusedObjects,
						() -> unresolved + " unresolved against " + served + " and " + refusedObjects + " objects refused");
	}

	/** The count limit is exact: the reads it admits all finish and nothing more is read. */
	@Test
	void aCountLimitedStepReadsAndCountsExactlyItsLimit() throws Exception {
		final var limit = 100;

		final var counts = S3NicSetWorkloadTest.runScenario(
						stub.listeners(1), stepId, countLimited(limit, "", items(MANY_OBJECTS)), bytes).get(CONTEXT);

		final var served = served(request -> true);
		assertEquals(List.of((long) limit, 0L), counts, served::toString);
		assertEquals(new Served(limit, 0, (long) limit * OBJECT_SIZE), served);
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
							"item": {"data": {"ranges": {"threshold": "%d"}}, "input": {"file": "%s"}}
						}).run();
						""".formatted(limit, WAIT_LIMIT_SECONDS, opSettings, CONCURRENCY, RANGE_SIZE, items);
	}

	private void assertCounted(final List<Long> counts, final Served served) throws IOException {
		assertTrue(served.complete() > 0, served::toString);
		assertEquals(List.of(served.complete(), 0L), counts, served::toString);
		assertEquals(served.bytes(), bytes.get(CONTEXT), () -> "bytes of " + served);
		final var unresolved = unresolved();
		assertTrue(unresolved <= served.incomplete(), () -> unresolved + " unresolved against " + served);
	}

	/**
	 * What the target served of the ranges {@code answered} selects. An object's complete sets are the
	 * times its least-read range was served; an object with some ranges served more often than that
	 * was being read when the step ended.
	 */
	private Served served(final Predicate<NicSetLoopback.Request> answered) {
		final Map<String, Map<Long, Long>> byObject = new HashMap<>();
		stub.objectRequests().stream().filter(request -> isRange(request) && answered.test(request))
						.forEach(request -> byObject.computeIfAbsent(request.path(), unused -> new HashMap<>())
										.merge(rangeStart(request), 1L, Long::sum));
		var complete = 0L;
		var incomplete = 0L;
		var served = 0L;
		for (final var ranges : byObject.values()) {
			final var least = ranges.size() < RANGES ? 0 : ranges.values().stream().mapToLong(Long::longValue).min().orElse(0);
			final var most = ranges.values().stream().mapToLong(Long::longValue).max().orElse(0);
			complete += least;
			incomplete += most > least ? 1 : 0;
			served += ranges.values().stream().mapToLong(Long::longValue).sum() * RANGE_SIZE;
		}
		return new Served(complete, incomplete, served);
	}

	/** The complete sets of ranges served, the objects with an incomplete set, and the bytes served. */
	private record Served(long complete, long incomplete, long bytes) {}

	private static boolean isRange(final NicSetLoopback.Request request) {
		return "GET".equals(request.method()) && request.range() != null;
	}

	private static long rangeStart(final NicSetLoopback.Request request) {
		final var range = request.range();
		return Long.parseLong(range.substring("bytes=".length(), range.indexOf('-')));
	}

	private static int objectIndex(final NicSetLoopback.Request request) {
		final var path = request.path();
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
		return """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%ds"}},
								"op": {"type": "read", "limit": {"count": 0}, "recycle": {"mode": %s}, "wait": {"limit": %d}%s}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"ranges": {"threshold": "%d"}%s}, "input": {"file": "%s"}}
						}).run();
						""".formatted(TIME_LIMIT_SECONDS, recycle, WAIT_LIMIT_SECONDS, opSettings, CONCURRENCY, RANGE_SIZE,
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
