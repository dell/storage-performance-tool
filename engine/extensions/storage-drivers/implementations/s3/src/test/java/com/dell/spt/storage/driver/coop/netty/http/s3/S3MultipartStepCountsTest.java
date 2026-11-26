package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Request;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.regex.MatchResult;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
@Timeout(240)
final class S3MultipartStepCountsTest {

	/**
	 * A count-limited step that hangs runs to this limit, then waits up to its {@code load-op-wait-limit}
	 * for in-flight operations, within the class's 240 s timeout.
	 */
	private static final String TIME_LIMIT = "60s";
	/**
	 * Below a hang, with room for a cold JVM on a loaded runner: the class's slowest test took 19 s with
	 * three busy threads per CPU, and CI runs the engine modules' tests in parallel.
	 */
	private static final long ENDS_WITHIN_MILLIS = TimeUnit.SECONDS.toMillis(45);
	/** A loopback upload id, as {@link NicSetLoopback} hands them out. */
	private static final Pattern UPLOAD_ID = Pattern.compile("\\bupload-\\d+\\b");
	/**
	 * The steps' {@code load-op-wait-limit}, above {@link #ENDS_WITHIN_MILLIS} in place of
	 * the shipped 30 s: a step that ends its count with an operation unresolved waits out this limit, so
	 * it fails {@link #assertEnded()} instead of ending inside the bound.
	 */
	private static final int LONG_WAIT_LIMIT_SECONDS = 60;

	private final NicSetLoopback stub = new NicSetLoopback();
	private long stepMillis;
	/** The last recycled or read step's own elapsed time, from its last metrics snapshot, by operation type and concurrency limit. */
	private final Map<String, Long> stepElapsedMillis = new ConcurrentHashMap<>();
	private String stepId;

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
	 * uploads are in flight. Once the upload is aborted, its other failed part is not retried, so the
	 * step ends when its last operation does, with nothing unresolved, instead of waiting out its
	 * {@code load-op-wait-limit}.
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

		final var outcomes = write(8, 4, retry, null);

		if (retry) {
			assertEquals(List.of(8L, 0L), outcomes.get("CREATE-4"), outcomes::toString);
			assertRequests(9, 8, 1);
		} else {
			assertEquals(List.of(7L, 1L), outcomes.get("CREATE-4"), outcomes::toString);
			assertRequests(8, 7, 1);
		}
		assertEnded();
		assertNoneUnresolved();
	}

	/**
	 * At concurrency 4, part 1 of one upload runs out of retries on 503 and the upload is aborted. Part
	 * 2's last attempt reaches the target after the abort and is answered 404, as S3 answers a part of
	 * an aborted upload, before the abort's own answer. The upload still ends with part 1's 503, the
	 * reason it was aborted, so {@code load-op-retry} retries it.
	 *
	 * <p>The stub answers the abort 500 ms after it answers part 2, but the client's handling of the two
	 * answers is not ordered: if the client takes longer than that over part 2's 404, the test passes
	 * without the order it is about. {@code CompositeFailureStatusDriverTest} checks the rule in a set
	 * order; this test checks it end to end through a step.
	 */
	@Test
	void anUploadAbortedOnAThrottledPartKeepsThatStatusWhenASiblingFailsLater() throws Exception {
		anUploadAbortedOnAThrottledPartKeepsThatStatusWhenASiblingFailsLater(false);
	}

	@Test
	void anUploadAbortedOnAThrottledPartKeepsThatStatusWhenASiblingFailsLaterWithRetry() throws Exception {
		anUploadAbortedOnAThrottledPartKeepsThatStatusWhenASiblingFailsLater(true);
	}

	private void anUploadAbortedOnAThrottledPartKeepsThatStatusWhenASiblingFailsLater(final boolean retry)
					throws Exception {
		final var parts = partsOfOneUpload(Set.of("1", "2"));
		final var part2Attempts = new AtomicInteger();
		final var abortArrived = new CountDownLatch(1);
		final var part2Answered = new CountDownLatch(1);
		stub.failRequest = request -> {
			if (S3NicSetWorkloadTest.isAbort(request)) {
				// Hold the abort's answer until 500 ms after the stub takes part 2's last attempt, for at
				// most 1.5 s in all: the client gives up on a request after 2 s
				// (storage-net-timeoutMilliSec), and then the upload would stay open on the stub.
				abortArrived.countDown();
				await(part2Answered, 1);
				sleep(500);
				return false;
			}
			if (!parts.test(request)) {
				return false;
			}
			if ("2".equals(NicSetLoopback.param(request.query(), "partNumber")) && part2Attempts.incrementAndGet() == 4) {
				// Part 2's last attempt is answered after the abort reached the target.
				await(abortArrived, 10);
				stub.failureStatus = 404;
				part2Answered.countDown();
			}
			return true;
		};

		final var outcomes = write(4, 4, retry, null);

		if (retry) {
			assertEquals(List.of(4L, 0L), outcomes.get("CREATE-4"), outcomes::toString);
			assertRequests(5, 4, 1);
		} else {
			assertEquals(List.of(3L, 1L), outcomes.get("CREATE-4"), outcomes::toString);
			assertRequests(4, 3, 1);
		}
		assertEnded();
		assertNoneUnresolved();
	}

	private static void sleep(final long millis) {
		try {
			Thread.sleep(millis);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static void await(final CountDownLatch latch, final long seconds) {
		try {
			latch.await(seconds, TimeUnit.SECONDS);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
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

		final var outcomes = write(4, 4, retry, null);

		assertEquals(List.of(0L, 4L), outcomes.get("CREATE-4"), outcomes::toString);
		final var attempts = retry ? 8 : 4;
		assertRequests(attempts, 0, attempts);
		assertEnded();
		assertNoneUnresolved();
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

	/**
	 * Every completion is answered 200 with S3's keep-alive whitespace, sent in a chunk of its own, then
	 * an XML declaration and an S3 {@code <Error>} document. Each upload is one failure and is aborted.
	 */
	@Test
	void completionsAnswered200WithAnErrorBodyAreEachOneFailure() throws Exception {
		stub.completionErrorBody = "\n   \n<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\n"
						+ "<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>";
		stub.failCompletionWithErrorBody = request -> true;

		final var outcomes = write(4, 1, false, null);

		assertEquals(List.of(0L, 4L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 4, 4);
		assertEnded();
	}

	/** A transient error code in a 200 completion is retried as a whole upload, from a new initiation. */
	@Test
	void aCompletionAnswered200WithATransientErrorIsRetriedAsAWholeUpload() throws Exception {
		stub.completionErrorBody = "        <Error><Code>SlowDown</Code></Error>";
		stub.failCompletionWithErrorBody = firstCompletion();

		final var outcomes = write(4, 1, true, null);

		assertEquals(List.of(4L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(5, 5, 1);
		assertEnded();
	}

	/** A permanent error code in a 200 completion is one failure that {@code load-op-retry} does not retry. */
	@Test
	void aCompletionAnswered200WithAPermanentErrorIsNotRetried() throws Exception {
		stub.completionErrorBody = "<Error xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Code>InvalidPart</Code></Error>";
		stub.failCompletionWithErrorBody = firstCompletion();

		final var outcomes = write(4, 1, true, null);

		assertEquals(List.of(3L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(4, 4, 1);
		assertEnded();
	}

	/**
	 * Two steps in one process, whose completions all fail with the same code: each step's driver logs
	 * it, so each step's {@code errors.log} names the code.
	 */
	@Test
	void eachStepLogsTheErrorCodeOfItsFailedCompletions() throws Exception {
		stub.completionErrorBody = "<Error><Code>InternalError</Code></Error>";
		stub.failCompletionWithErrorBody = request -> true;
		final var first = "codes-first-" + System.nanoTime();
		final var second = "codes-second-" + System.nanoTime();
		try (final var errors = new ErrorsLogCapture()) {
			timed(null, """
							Load.config({
								"load": {"step": {"id": "%s", "limit": {"time": "%s"}}, "op": {"limit": {"count": 2}}},
								"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
							}).run();
							Load.config({
								"load": {"step": {"id": "%s", "limit": {"time": "%s"}}, "op": {"limit": {"count": 2}}},
								"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
							}).run();
							""".formatted(first, TIME_LIMIT, second, TIME_LIMIT));
			final var lines = errors.completionErrorLines();
			assertEquals(1, lines.stream().filter(line -> first.equals(line.stepId())).count(), lines::toString);
			assertEquals(1, lines.stream().filter(line -> second.equals(line.stepId())).count(), lines::toString);
			assertTrue(lines.stream().allMatch(line -> line.message().contains("S3 error code InternalError;")),
							lines::toString);
		}
		assertRequests(4, 4, 4);
		assertEnded();
	}

	/**
	 * A count-1 step whose only upload's completion fails ends right after that upload's result, while
	 * the stub holds the abort's answer for 300 ms. The step's stop waits for the abort before its I/O
	 * shuts down, so the abort reaches the target, its answer is read and no upload stays open.
	 *
	 * <p>"Confirmed" here is the upload's {@code multipart.lifecycle.csv} record: the step turns on
	 * integrity metadata for it, and the record says {@code failed_aborted} only when the driver read a
	 * successful answer to the abort; an abort that was not sent, or whose answer the I/O shutdown cut
	 * off, is recorded {@code failed_orphaned}.
	 */
	@Test
	void aCountLimitedStepWhoseLastCompletionFailsReadsTheAbortBeforeItEnds() throws Exception {
		// The only upload's completion: the initiation is request 1 and its five parts 2 to 6.
		stub.failObjectRequest = n -> n == 7;
		stub.abortDelayMillis = 300;

		final var outcomes = timed(null, """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": 1}, "retry": false, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": 1}}, "integrity": {"mode": "metadata"}},
							"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
						}).run();
						""".formatted(TIME_LIMIT, LONG_WAIT_LIMIT_SECONDS));

		assertEquals(List.of(0L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(1, 1, 1);
		assertEnded();
		assertEquals(List.of("failed_aborted"), multipartStates(), () -> multipartLifecycle().toString());
	}

	/**
	 * Queue pressure: a 1-slot driver input queue, batch size 1, a 500 ms child enqueue timeout, and
	 * parts that take 1.5 s, for 4-part uploads at concurrency 5. A part's enqueue times out, so its
	 * upload is aborted: through the queue, or, when the abort's own enqueue times out too, by the
	 * driver outside the queue. A count-limited step with no time limit then ends on its count with
	 * nothing unresolved, where it used to wait for the lost upload until it was stopped, and no upload
	 * stays open. With integrity metadata on, every initiated upload has exactly one terminal {@code
	 * multipart.lifecycle.csv} record, each failed one {@code failed_aborted}.
	 */
	@Test
	@Timeout(120)
	void aCountLimitedStepWhosePartEnqueueTimesOutEndsOnItsCount() throws Exception {
		final var run = partEnqueueTimesOut(false, 500);
		assertTrue(run.failures() >= 1, () -> "the upload that lost a part is one failure: " + run);
		assertEquals(run.failures(), run.aborted(), () -> "each failed upload is aborted: " + run);
	}

	/** The same with {@code load-op-retry}: the failed uploads are retried, and none stays open. */
	@Test
	@Timeout(120)
	void aCountLimitedStepWhosePartEnqueueTimesOutEndsOnItsCountWithRetry() throws Exception {
		partEnqueueTimesOut(true, 500);
	}

	/**
	 * At a 1000 ms timeout against 1.5 s parts the queued abort usually finds room: whichever route
	 * sends it, each failed upload gets exactly one abort and is recorded {@code failed_aborted}.
	 */
	@Test
	@Timeout(120)
	void aCountLimitedStepWhosePartEnqueueTimesOutAtALongerTimeoutAbortsOnce() throws Exception {
		final var run = partEnqueueTimesOut(false, 1_000);
		assertEquals(run.failures(), run.aborted(), () -> "each failed upload is aborted: " + run);
	}

	/** What a run of {@link #partEnqueueTimesOut} counted, as it reports them. */
	private record PartEnqueueTimeoutRun(
					long partTimeouts, long abortTimeouts, long abortsSent, int openUploads, long failures,
					long aborted, long orphaned, long completed) {}

	private PartEnqueueTimeoutRun partEnqueueTimesOut(final boolean retry, final long timeoutMillis) throws Exception {
		final var property = "spt.mpu.child.enqueue.timeout.millis";
		final var previous = System.getProperty(property);
		System.setProperty(property, Long.toString(timeoutMillis));
		stub.answerDelayMillis = request -> request.isPart() ? 1_500 : 0;
		final Map<String, List<Long>> outcomes;
		final List<ErrorsLogCapture.Line> errors;
		try (final var capture = new ErrorsLogCapture()) {
			outcomes = timed(null, """
							Load.config({
								"load": {
									"batch": {"size": 1},
									"op": {"limit": {"count": 4}, "retry": %s, "wait": {"limit": %d}}
								},
								"storage": {
									"driver": {"limit": {"concurrency": 5, "queue": {"input": 1}}},
									"integrity": {"mode": "metadata"}
								},
								"item": {"data": {"size": "4KB", "ranges": {"threshold": "1KB"}}}
							}).run();
							""".formatted(retry, LONG_WAIT_LIMIT_SECONDS));
			errors = capture.drained();
		} finally {
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}

		final var counts = outcomes.get("CREATE-5");
		assertEquals(4L, counts.get(0) + counts.get(1), outcomes::toString);
		final var partTimeouts = errors.stream()
						.filter(line -> line.message().contains("timed out enqueueing MPU part operation")).count();
		assertTrue(partTimeouts >= 1, () -> "a part enqueue timed out: " + errors);
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
		assertNoneUnresolved();
		// The stop waited for the aborts sent outside the queue, so the stub has had them all.
		final var initiated = count(stub.requests(), request -> "POST".equals(request.method())
						&& request.query() != null && request.query().contains("uploads"));
		final var records = multipartLifecycle(initiated);
		assertEquals(initiated, records.size(), () -> initiated + " uploads initiated, records: " + records);
		final var uploadIds = records.stream()
						.map(record -> UPLOAD_ID.matcher(record).results().map(MatchResult::group).findFirst().orElse(record))
						.distinct().count();
		assertEquals(initiated, uploadIds, () -> "one record per upload: " + records);
		final var states = records.stream()
						.map(record -> Stream.of("failed_aborted", "failed_orphaned", "completed")
										.filter(state -> record.contains("," + state + ",")).findFirst().orElse(record))
						.toList();
		assertTrue(states.stream().allMatch(state -> !state.contains(",")), () -> "terminal records: " + records);
		final var aborts = stub.requests().stream().filter(S3NicSetWorkloadTest::isAbort).toList();
		final var run = new PartEnqueueTimeoutRun(
						partTimeouts,
						errors.stream().filter(line -> line.message().contains("timed out enqueueing MPU abort operation")).count(),
						aborts.size(),
						stub.openUploads(),
						counts.get(1),
						states.stream().filter("failed_aborted"::equals).count(),
						states.stream().filter("failed_orphaned"::equals).count(),
						states.stream().filter("completed"::equals).count());
		System.out.println("PART-ENQUEUE-TIMEOUT-RUN retry=" + retry + " timeoutMillis=" + timeoutMillis + " " + run);
		assertEquals(0, run.openUploads(), () -> "no upload stays open: " + run + ", records: " + records);
		assertEquals(0, run.orphaned(), () -> "no upload is orphaned: " + records);
		assertEquals(initiated - run.completed(), run.aborted(), () -> "each failed upload is aborted: " + records);
		assertEquals(run.aborted(), run.abortsSent(), () -> "one abort per failed upload: " + aborts);
		assertEquals(run.abortsSent(), aborts.stream().map(request -> NicSetLoopback.param(request.query(), "uploadId"))
						.distinct().count(), () -> "no upload is aborted twice: " + aborts);
		return run;
	}

	/**
	 * The step's {@code multipart.lifecycle.csv} records, once there are {@code expected}: the log
	 * writes them asynchronously.
	 */
	private List<String> multipartLifecycle(final long expected) throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (multipartLifecycle().size() < expected && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		// A little longer, so a second record for one upload would show.
		Thread.sleep(200);
		return multipartLifecycle();
	}

	private static Predicate<Request> firstCompletion() {
		final var completions = new AtomicInteger();
		return request -> completions.incrementAndGet() == 1;
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

	/**
	 * A recycled CREATE of one saved item: each circulation is a new upload, with its own initiation,
	 * every part and a completion under its own upload ID, and is counted once.
	 */
	@Test
	void aRecycledUploadIsANewUploadOnEachCirculation() throws Exception {
		final var outcomes = recycle(1, 3, TIME_LIMIT, 1, 0);

		assertEquals(List.of(3L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertRequests(3, 3, 0);
		assertEquals(3L * 5, count(stub.objectRequests(), request -> S3NicSetWorkloadTest.phase(request).equals("PUT part")),
						() -> "every part of every circulation: " + stub.objectRequests());
		assertEquals(List.of("upload-1", "upload-2", "upload-3"), completedUploads());
		assertNoNullUploadId();
		assertEnded();
	}

	/**
	 * Four saved items recycled until the time limit, one upload open at a time: the step runs to its
	 * limit without a failure, every success is a completed upload, and the recycled uploads take an
	 * upload permit as new ones do.
	 */
	@Test
	void aTimeLimitedRecycledUploadStepRunsToItsLimitWithinTheUploadLimit() throws Exception {
		final var outcomes = recycle(4, 0, "4s", 4, 1);

		final var createOutcome = outcomes.get("CREATE-4");
		assertTrue(createOutcome != null, outcomes::toString);
		assertEquals(0L, createOutcome.get(1), outcomes::toString);
		assertTrue(createOutcome.get(0) > 4, () -> "each item circulates more than once: " + outcomes);
		final var completed = completedUploads();
		assertEquals(createOutcome.get(0), completed.size(), "a completion for each success");
		assertEquals(completed.size(), completed.stream().distinct().count(), "each under its own upload ID");
		assertNoNullUploadId();
		assertEquals(1, stub.peakOpenUploads(), "uploads open at once");
		final long elapsedMillis = stepElapsedMillis.get("CREATE-4");
		assertTrue(elapsedMillis >= TimeUnit.SECONDS.toMillis(4), () -> "the step ended after " + elapsedMillis + " ms");
	}

	/** A recycled split READ of four saved items reads each, every time, in all five of its ranges. */
	@Test
	void aRecycledSplitReadReadsEveryRangeOnEachCirculation() throws Exception {
		final var outcomes = read(12, true, false);

		assertEquals(List.of(12L, 0L), outcomes.get("READ-1"), outcomes::toString);
		final var gets = stub.objectRequests().stream().filter(request -> "GET".equals(request.method())).toList();
		assertEquals(12 * 5, gets.size(), gets::toString);
		assertTrue(gets.stream().allMatch(request -> request.range() != null), gets::toString);
		final Map<String, Long> byRange = gets.stream()
						.collect(Collectors.groupingBy(request -> request.path() + " " + request.range(), Collectors.counting()));
		assertEquals(4 * 5, byRange.size(), byRange::toString);
		assertTrue(byRange.values().stream().allMatch(reads -> reads == 3), byRange::toString);
		assertEnded();
	}

	/**
	 * Saves {@code items} 5 KB objects whole, then writes them again in five 1 KB parts in a recycling
	 * CREATE step of its own, up to {@code count} results (0: no count limit) or {@code timeLimit}.
	 */
	private Map<String, List<Long>> recycle(final int items, final int count, final String timeLimit,
					final int concurrency, final int multipartObjects) throws Exception {
		final var itemsFile = tempDir.resolve("items.csv").toString();
		return timed(null, """
						Load.config({
							"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": %d}}},
							"item": {"data": {"size": "5KB"}, "output": {"file": "%s"}}
						}).run();
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": %d}, "recycle": {"mode": true}, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": %d, "multipart": {"objects": %d}}}},
							"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}, "input": {"file": "%s"}}
						}).run();
						""".formatted(System.nanoTime(), items, itemsFile, timeLimit, count, LONG_WAIT_LIMIT_SECONDS, concurrency,
						multipartObjects, itemsFile));
	}

	/** The upload ID of each completion the stub was sent, in order. */
	private List<String> completedUploads() {
		return stub.objectRequests().stream().filter(request -> S3NicSetWorkloadTest.phase(request).equals("POST complete"))
						.map(request -> NicSetLoopback.param(request.query(), "uploadId")).toList();
	}

	private void assertNoNullUploadId() {
		assertEquals(List.of(), stub.requests().stream()
						.filter(request -> request.query() != null && request.query().contains("uploadId=null")).toList());
	}

	/** A step of {@code count} 5 KB uploads of five 1 KB parts. */
	private Map<String, List<Long>> write(final int count, final int concurrency, final boolean retry,
					final NicSetLoopback.Factory factory) throws Exception {
		return timed(factory, """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": %d}, "retry": %s, "retryLimit": 1, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
						}).run();
						""".formatted(TIME_LIMIT, count, retry, LONG_WAIT_LIMIT_SECONDS, concurrency));
	}

	/** Writes four 5 KB objects whole, then reads each in five 1 KB ranges in a step of its own. */
	private Map<String, List<Long>> read(final boolean retry) throws Exception {
		return read(4, false, retry);
	}

	/**
	 * Writes four 5 KB objects whole, then reads them in five 1 KB ranges each in a step of its own,
	 * up to {@code count} reads, recycling them with {@code recycle}.
	 */
	private Map<String, List<Long>> read(final int count, final boolean recycle, final boolean retry) throws Exception {
		final var items = tempDir.resolve("items.csv").toString();
		return timed(null, """
						Load.config({
							"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": 4}}},
							"item": {"data": {"size": "5KB"}, "output": {"file": "%s"}}
						}).run();
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {
									"type": "read", "limit": {"count": %d}, "recycle": {"mode": %s},
									"retry": %s, "retryLimit": 1, "wait": {"limit": %d}
								}
							},
							"storage": {"driver": {"limit": {"concurrency": 1}}},
							"item": {"data": {"ranges": {"threshold": "1KB"}}, "input": {"file": "%s"}}
						}).run();
						""".formatted(System.nanoTime(), items, TIME_LIMIT, count, recycle, retry, LONG_WAIT_LIMIT_SECONDS, items));
	}

	private Map<String, List<Long>> timed(final NicSetLoopback.Factory factory, final String scenario) throws Exception {
		final var started = System.nanoTime();
		stepId = "multipart-counts-" + System.nanoTime();
		final var outcomes = factory == null
						? S3NicSetWorkloadTest.runScenarioTimed(stub.listeners(1), stepId, scenario, stepElapsedMillis)
						: S3NicSetWorkloadTest.runScenario(stub.listeners(1), factory, stepId, scenario);
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

	/** The step's {@code operation.lifecycle.csv} reconciles, with no operation left unresolved. */
	private void assertNoneUnresolved() throws IOException {
		final var lines = Files.readAllLines(
						Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)));
		assertEquals(OperationLifecycleArtifact.HEADER, lines.get(0));
		assertEquals(2, lines.size(), lines::toString);
		final var header = List.of(lines.get(0).split(",", -1));
		final var row = lines.get(1).split(",", -1);
		assertEquals("0", row[header.indexOf("unresolved")], () -> "unresolved operations: " + lines);
		assertEquals("true", row[header.indexOf("terminal")], () -> "the lifecycle reconciles: " + lines);
	}

	/**
	 * The states of the step's {@code multipart.lifecycle.csv} records, once there is one: the log writes
	 * them asynchronously.
	 */
	private List<String> multipartStates() throws InterruptedException {
		final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (multipartLifecycle().isEmpty() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		return multipartLifecycle().stream()
						.map(record -> Stream.of("failed_aborted", "failed_orphaned", "completed")
										.filter(state -> record.contains("," + state + ",")).findFirst().orElse(record))
						.toList();
	}

	/** The records of the step's {@code multipart.lifecycle.csv}, without its header. */
	private List<String> multipartLifecycle() {
		try {
			final var file = Path.of(FileManager.INSTANCE.logFileName(Loggers.MULTIPART_LIFECYCLE.getName(), stepId));
			return Files.exists(file)
							? Files.readAllLines(file).stream()
											.filter(line -> !line.isBlank() && !IntegrityCsvArtifacts.MULTIPART_LIFECYCLE_HEADER.equals(line))
											.toList()
							: List.of();
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static long count(final List<Request> requests, final Predicate<Request> which) {
		return requests.stream().filter(which).count();
	}
}
