package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Request;
import io.netty.channel.epoll.Epoll;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Steps of multipart writes and multi-range reads on the shipped {@code s3} driver and the loopback
 * stub, where the stub fails chosen requests. A multipart operation is counted once, by its own
 * outcome: a part attempt takes no count-limit slot and is no failure, so a count-limited step ends
 * when its last operation does and leaves no upload open. With {@code load-op-retry}, a failed
 * operation is retried as a whole, from a new initiation, and counted once by the retry's outcome.
 *
 * <p>At concurrency 1 a step runs one upload at a time, so its object requests come in order: the
 * initiation, then parts 1 to 5, each part's retries right after it, then the completion or abort.
 * Every initiation after a failed upload also shows that the failed upload released its permit.
 */
@Timeout(60)
final class S3MultipartStepCountsTest {

	/** A count-limited step that hangs runs to this limit, or to its 30 s stop wait after it. */
	private static final String TIME_LIMIT = "30s";
	/** Well below a hang, with room for a cold JVM on a loaded runner. */
	private static final long ENDS_WITHIN_MILLIS = TimeUnit.SECONDS.toMillis(20);
	/** The shipped {@code load-op-wait-limit}. */
	private static final int DEFAULT_WAIT_LIMIT_SECONDS = 30;
	/** A short {@code load-op-wait-limit}, for the steps whose end waits for an aborted upload's parts. */
	private static final int WAIT_LIMIT_SECONDS = 2;

	private final NicSetLoopback stub = new NicSetLoopback();
	private long stepMillis;

	@TempDir
	Path tempDir;

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		stub.close();
		NicSetLoopback.awaitNoNicIoThreads();
	}

	@Test
	void aPartThatSucceedsOnItsRetryTakesNoCountSlot() throws Exception {
		aPartThatSucceedsOnItsRetryTakesNoCountSlot(false);
	}

	@Test
	void aPartThatSucceedsOnItsRetryTakesNoCountSlotWithRetry() throws Exception {
		aPartThatSucceedsOnItsRetryTakesNoCountSlot(true);
	}

	private void aPartThatSucceedsOnItsRetryTakesNoCountSlot(final boolean retry) throws Exception {
		// The first upload's first part fails once.
		stub.failObjectRequest = n -> n == 2;

		final var outcomes = write(4, 1, retry, null);

		assertEquals(List.of(4L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 4, 0);
		assertEnded();
	}

	@Test
	void aPartThatSucceedsOnItsRetryTakesNoCountSlotAtConcurrency4() throws Exception {
		aPartThatSucceedsOnItsRetryTakesNoCountSlotAtConcurrency4(false);
	}

	@Test
	void aPartThatSucceedsOnItsRetryTakesNoCountSlotAtConcurrency4WithRetry() throws Exception {
		aPartThatSucceedsOnItsRetryTakesNoCountSlotAtConcurrency4(true);
	}

	private void aPartThatSucceedsOnItsRetryTakesNoCountSlotAtConcurrency4(final boolean retry) throws Exception {
		stub.failObjectRequest = n -> n == 2;

		final var outcomes = write(8, 4, retry, null);

		assertEquals(List.of(8L, 0L), outcomes.get("CREATE-4"), outcomes::toString);
		assertRequests(8, 8, 0);
		assertEnded();
	}

	@Test
	void onANicSetAPartThatSucceedsOnItsRetryTakesNoCountSlot() throws Exception {
		onANicSetAPartThatSucceedsOnItsRetryTakesNoCountSlot(false);
	}

	@Test
	void onANicSetAPartThatSucceedsOnItsRetryTakesNoCountSlotWithRetry() throws Exception {
		onANicSetAPartThatSucceedsOnItsRetryTakesNoCountSlot(true);
	}

	private void onANicSetAPartThatSucceedsOnItsRetryTakesNoCountSlot(final boolean retry) throws Exception {
		stub.failObjectRequest = n -> n == 2;

		final var outcomes = write(4, 1, retry, new NicSetLoopback.Factory());

		assertEquals(List.of(4L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 4, 0);
		assertEnded();
	}

	/** Every part fails, so each upload's first part runs out of retries and the upload is aborted. */
	@Test
	void uploadsWhosePartsRunOutOfRetriesAreEachOneFailure() throws Exception {
		uploadsWhosePartsRunOutOfRetriesAreEachOneFailure(false);
	}

	@Test
	void uploadsWhosePartsRunOutOfRetriesAreEachOneFailureWithRetry() throws Exception {
		uploadsWhosePartsRunOutOfRetriesAreEachOneFailure(true);
	}

	private void uploadsWhosePartsRunOutOfRetriesAreEachOneFailure(final boolean retry) throws Exception {
		stub.failParts = true;

		final var outcomes = write(4, 1, retry, null);

		assertEquals(List.of(0L, 4L), outcomes.get("CREATE-1"), outcomes::toString);
		// With retry, each upload is retried once (the retry limit) from a new initiation.
		final var attempts = retry ? 8 : 4;
		assertRequests(attempts, 0, attempts);
		assertEnded();
	}

	/**
	 * A part the target refuses for good fails its upload with the part's own status, so retry doesn't
	 * start the upload again: each upload is one initiation, its first part's attempts and one abort.
	 */
	@Test
	void uploadsWhosePartsAreRefusedAreEachOneFailure() throws Exception {
		uploadsWhosePartsAreRefusedAreEachOneFailure(false);
	}

	@Test
	void uploadsWhosePartsAreRefusedAreEachOneFailureWithRetry() throws Exception {
		uploadsWhosePartsAreRefusedAreEachOneFailure(true);
	}

	private void uploadsWhosePartsAreRefusedAreEachOneFailure(final boolean retry) throws Exception {
		stub.failParts = true;
		stub.failureStatus = 403;

		final var outcomes = write(4, 1, retry, null);

		assertEquals(List.of(0L, 4L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 0, 4);
		assertEnded();
	}

	/**
	 * At concurrency 4, two parts of one upload run out of retries while its other parts and other
	 * uploads are in flight. The step's end waits for the aborted upload's other parts up to
	 * {@code load-op-wait-limit} (multipart-abort-sibling-drain), set short here; the counts are final
	 * before it.
	 */
	@Test
	void atConcurrency4AnUploadWhoseTwoPartsRunOutOfRetriesIsOneFailure() throws Exception {
		atConcurrency4AnUploadWhoseTwoPartsRunOutOfRetriesIsOneFailure(false);
	}

	@Test
	void atConcurrency4AnUploadWhoseTwoPartsRunOutOfRetriesIsOneFailureWithRetry() throws Exception {
		atConcurrency4AnUploadWhoseTwoPartsRunOutOfRetriesIsOneFailure(true);
	}

	private void atConcurrency4AnUploadWhoseTwoPartsRunOutOfRetriesIsOneFailure(final boolean retry) throws Exception {
		stub.failRequest = partsOfOneUpload(Set.of("1", "2"));

		final var outcomes = write(8, 4, retry, null, WAIT_LIMIT_SECONDS);

		if (retry) {
			assertEquals(List.of(8L, 0L), outcomes.get("CREATE-4"), outcomes::toString);
			assertRequests(9, 8, 1);
		} else {
			assertEquals(List.of(7L, 1L), outcomes.get("CREATE-4"), outcomes::toString);
			assertRequests(8, 7, 1);
		}
		assertEnded();
	}

	@Test
	void atConcurrency4UploadsWhosePartsRunOutOfRetriesAreEachOneFailure() throws Exception {
		atConcurrency4UploadsWhosePartsRunOutOfRetriesAreEachOneFailure(false);
	}

	@Test
	void atConcurrency4UploadsWhosePartsRunOutOfRetriesAreEachOneFailureWithRetry() throws Exception {
		atConcurrency4UploadsWhosePartsRunOutOfRetriesAreEachOneFailure(true);
	}

	private void atConcurrency4UploadsWhosePartsRunOutOfRetriesAreEachOneFailure(final boolean retry) throws Exception {
		stub.failParts = true;

		final var outcomes = write(4, 4, retry, null, WAIT_LIMIT_SECONDS);

		assertEquals(List.of(0L, 4L), outcomes.get("CREATE-4"), outcomes::toString);
		final var attempts = retry ? 8 : 4;
		assertRequests(attempts, 0, attempts);
		assertEnded();
	}

	@Test
	void anUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure() throws Exception {
		anUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(false);
	}

	@Test
	void anUploadWhoseFirstPartRunsOutOfRetriesIsOneFailureWithRetry() throws Exception {
		anUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(true);
	}

	private void anUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(final boolean retry) throws Exception {
		// The first part's four attempts, then the abort.
		stub.failObjectRequest = n -> n >= 2 && n <= 5;

		assertOneUploadFailsOrIsRetried(retry, write(8, 1, retry, null));
	}

	@Test
	void onANicSetAnUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure() throws Exception {
		onANicSetAnUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(false);
	}

	@Test
	void onANicSetAnUploadWhoseFirstPartRunsOutOfRetriesIsOneFailureWithRetry() throws Exception {
		onANicSetAnUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(true);
	}

	private void onANicSetAnUploadWhoseFirstPartRunsOutOfRetriesIsOneFailure(final boolean retry) throws Exception {
		stub.failObjectRequest = n -> n >= 2 && n <= 5;

		assertOneUploadFailsOrIsRetried(retry, write(8, 1, retry, new NicSetLoopback.Factory()));
	}

	@Test
	void anUploadWhoseLastPartRunsOutOfRetriesIsOneFailure() throws Exception {
		anUploadWhoseLastPartRunsOutOfRetriesIsOneFailure(false);
	}

	@Test
	void anUploadWhoseLastPartRunsOutOfRetriesIsOneFailureWithRetry() throws Exception {
		anUploadWhoseLastPartRunsOutOfRetriesIsOneFailure(true);
	}

	private void anUploadWhoseLastPartRunsOutOfRetriesIsOneFailure(final boolean retry) throws Exception {
		// Parts 1 to 4 succeed, then the last part's four attempts fail.
		stub.failObjectRequest = n -> n >= 6 && n <= 9;

		assertOneUploadFailsOrIsRetried(retry, write(8, 1, retry, null));
	}

	private void assertOneUploadFailsOrIsRetried(final boolean retry, final Map<String, List<Long>> outcomes) {
		if (retry) {
			// The failed upload is retried from a new initiation and completes.
			assertEquals(List.of(8L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
			assertRequests(9, 8, 1);
		} else {
			assertEquals(List.of(7L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
			assertRequests(8, 7, 1);
		}
		assertEnded();
	}

	/** A failed initiation keeps its status, so retry starts a 503's upload again from a new initiation. */
	@Test
	void aFailedInitiationIsOneFailureAndTheStepEnds() throws Exception {
		aFailedInitiationIsOneFailureAndTheStepEnds(false);
	}

	@Test
	void aFailedInitiationIsOneFailureAndTheStepEndsWithRetry() throws Exception {
		aFailedInitiationIsOneFailureAndTheStepEnds(true);
	}

	private void aFailedInitiationIsOneFailureAndTheStepEnds(final boolean retry) throws Exception {
		stub.failObjectRequest = n -> n == 1;

		final var outcomes = write(4, 1, retry, null);

		if (retry) {
			assertEquals(List.of(4L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
			assertRequests(5, 4, 0);
		} else {
			assertEquals(List.of(3L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
			assertRequests(4, 3, 0);
		}
		assertEnded();
	}

	/** An initiation the target refuses for good is one failure, and retry doesn't start it again. */
	@Test
	void aRefusedInitiationIsOneFailure() throws Exception {
		aRefusedInitiationIsOneFailure(false);
	}

	@Test
	void aRefusedInitiationIsOneFailureWithRetry() throws Exception {
		aRefusedInitiationIsOneFailure(true);
	}

	private void aRefusedInitiationIsOneFailure(final boolean retry) throws Exception {
		stub.failObjectRequest = n -> n == 1;
		stub.failureStatus = 403;

		final var outcomes = write(4, 1, retry, null);

		assertEquals(List.of(3L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 3, 0);
		assertEnded();
	}

	@Test
	void aStepWhoseEveryInitiationFailsCountsEachAndEnds() throws Exception {
		aStepWhoseEveryInitiationFailsCountsEachAndEnds(false);
	}

	@Test
	void aStepWhoseEveryInitiationFailsCountsEachAndEndsWithRetry() throws Exception {
		aStepWhoseEveryInitiationFailsCountsEachAndEnds(true);
	}

	private void aStepWhoseEveryInitiationFailsCountsEachAndEnds(final boolean retry) throws Exception {
		stub.failObjectRequest = n -> true;

		final var outcomes = write(4, 1, retry, null);

		assertEquals(List.of(0L, 4L), outcomes.get("CREATE-1"), outcomes::toString);
		// With retry, each upload is retried once (the retry limit) from a new initiation.
		assertRequests(retry ? 8 : 4, 0, 0);
		assertEnded();
	}

	@Test
	void aFailedCompletionIsStillOneFailure() throws Exception {
		aFailedCompletionIsStillOneFailure(false);
	}

	@Test
	void aFailedCompletionIsStillOneFailureWithRetry() throws Exception {
		aFailedCompletionIsStillOneFailure(true);
	}

	private void aFailedCompletionIsStillOneFailure(final boolean retry) throws Exception {
		// The first upload's completion; the driver then aborts the upload.
		stub.failObjectRequest = n -> n == 7;

		final var outcomes = write(8, 1, retry, null);

		if (retry) {
			assertEquals(List.of(8L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
			assertRequests(9, 9, 1);
		} else {
			assertEquals(List.of(7L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
			assertRequests(8, 8, 1);
		}
		assertEnded();
	}

	@Test
	void aRangeThatSucceedsOnItsRetryTakesNoCountSlot() throws Exception {
		aRangeThatSucceedsOnItsRetryTakesNoCountSlot(false);
	}

	@Test
	void aRangeThatSucceedsOnItsRetryTakesNoCountSlotWithRetry() throws Exception {
		aRangeThatSucceedsOnItsRetryTakesNoCountSlot(true);
	}

	private void aRangeThatSucceedsOnItsRetryTakesNoCountSlot(final boolean retry) throws Exception {
		stub.failRequest = firstRangeOfOneObject(1);

		final var outcomes = read(retry);

		assertEquals(List.of(4L, 0L), outcomes.get("READ-1"), outcomes::toString);
		assertEnded();
	}

	@Test
	void aReadWhoseRangeRunsOutOfRetriesIsOneFailure() throws Exception {
		aReadWhoseRangeRunsOutOfRetriesIsOneFailure(false);
	}

	@Test
	void aReadWhoseRangeRunsOutOfRetriesIsOneFailureWithRetry() throws Exception {
		aReadWhoseRangeRunsOutOfRetriesIsOneFailure(true);
	}

	private void aReadWhoseRangeRunsOutOfRetriesIsOneFailure(final boolean retry) throws Exception {
		// The range's four attempts fail; a retry of the whole read finds it answered.
		stub.failRequest = firstRangeOfOneObject(4);

		final var outcomes = read(retry);

		assertEquals(retry ? List.of(4L, 0L) : List.of(3L, 1L), outcomes.get("READ-1"), outcomes::toString);
		assertEnded();
	}

	/** Without any failure, retry on or off, the counts are those of every run before. */
	@Test
	void aStepWithoutFailuresIsUnchanged() throws Exception {
		aStepWithoutFailuresIsUnchanged(false);
	}

	@Test
	void aStepWithoutFailuresIsUnchangedWithRetry() throws Exception {
		aStepWithoutFailuresIsUnchanged(true);
	}

	private void aStepWithoutFailuresIsUnchanged(final boolean retry) throws Exception {
		final var outcomes = write(4, 1, retry, null);

		assertEquals(List.of(4L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 4, 0);
		assertEnded();
	}

	/** A step of {@code count} 5 KB uploads of five 1 KB parts. */
	private Map<String, List<Long>> write(final int count, final int concurrency, final boolean retry,
					final NicSetLoopback.Factory factory) throws Exception {
		return write(count, concurrency, retry, factory, DEFAULT_WAIT_LIMIT_SECONDS);
	}

	private Map<String, List<Long>> write(final int count, final int concurrency, final boolean retry,
					final NicSetLoopback.Factory factory, final int waitLimitSeconds) throws Exception {
		return timed(factory, """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": %d}, "retry": %s, "retryLimit": 1, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
						}).run();
						""".formatted(TIME_LIMIT, count, retry, waitLimitSeconds, concurrency));
	}

	/** Writes four 5 KB objects whole, then reads each in five 1 KB ranges in a step of its own. */
	private Map<String, List<Long>> read(final boolean retry) throws Exception {
		final var items = tempDir.resolve("items.csv").toString();
		return timed(null, """
						Load.config({
							"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": 4}}},
							"item": {"data": {"size": "5KB"}, "output": {"file": "%s"}}
						}).run();
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"type": "read", "limit": {"count": 4}, "retry": %s, "retryLimit": 1}
							},
							"storage": {"driver": {"limit": {"concurrency": 1}}},
							"item": {"data": {"ranges": {"threshold": "1KB"}}, "input": {"file": "%s"}}
						}).run();
						""".formatted(System.nanoTime(), items, TIME_LIMIT, retry, items));
	}

	private Map<String, List<Long>> timed(final NicSetLoopback.Factory factory, final String scenario) throws Exception {
		final var started = System.nanoTime();
		final var outcomes = S3NicSetWorkloadTest.runScenario(stub.listeners(1), factory,
						"multipart-counts-" + System.nanoTime(), scenario);
		stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		return outcomes;
	}

	/** The given part numbers of the first upload that sends one of them, on every attempt. */
	private static Predicate<Request> partsOfOneUpload(final Set<String> partNumbers) {
		final var upload = new AtomicReference<String>();
		return request -> {
			if (!request.isPart() || !partNumbers.contains(NicSetLoopback.param(request.query(), "partNumber"))) {
				return false;
			}
			final var uploadId = NicSetLoopback.param(request.query(), "uploadId");
			return upload.compareAndSet(null, uploadId) || upload.get().equals(uploadId);
		};
	}

	/** The first range GET's object, whose first range fails {@code times} times. */
	private static Predicate<Request> firstRangeOfOneObject(final int times) {
		final var object = new AtomicReference<String>();
		final var failed = new AtomicInteger();
		return request -> "GET".equals(request.method()) && request.range() != null && request.range().startsWith("bytes=0-")
						&& (object.compareAndSet(null, request.path()) || object.get().equals(request.path()))
						&& failed.getAndIncrement() < times;
	}

	private void assertRequests(final int initiations, final int completions, final int aborts) {
		final var requests = stub.objectRequests();
		assertEquals(initiations, count(requests, request -> S3NicSetWorkloadTest.phase(request).equals("POST init")),
						() -> "initiations: " + requests);
		assertEquals(completions, count(requests, request -> S3NicSetWorkloadTest.phase(request).equals("POST complete")),
						() -> "completions: " + requests);
		assertEquals(aborts, count(stub.requests(), S3NicSetWorkloadTest::isAbort), () -> "aborts: " + stub.requests());
	}

	/** The step ended by its count, not its time limit, and left no upload open on the target. */
	private void assertEnded() {
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
		assertEquals(0, stub.openUploads(), "no upload stays open");
	}

	private static long count(final List<Request> requests, final Predicate<Request> which) {
		return requests.stream().filter(which).count();
	}
}
