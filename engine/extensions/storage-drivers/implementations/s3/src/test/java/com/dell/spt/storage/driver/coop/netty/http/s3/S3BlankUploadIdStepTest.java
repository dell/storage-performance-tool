package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Request;
import io.netty.channel.epoll.Epoll;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Multipart writes on the shipped {@code s3} driver and the loopback stub, whose initiations are
 * answered 200 with an upload ID no part URI can carry, or with no body. Each such upload fails at
 * once, never at the network timeout, and with that timeout off the step still ends.
 */
@Timeout(240)
final class S3BlankUploadIdStepTest {

	/** The shipped network timeout; at it, each stalled request took 30 s. */
	private static final int DEFAULT_TIMEOUT_MILLIS = 30_000;
	/** The count-limited steps' time limit, which ends a stalled step instead of the class timeout. */
	private static final String TIME_LIMIT = "20s";
	private static final int WAIT_LIMIT_SECONDS = 5;
	/** Well below one stalled request at the shipped timeout, and below the time limit. */
	private static final long ENDS_WITHIN_MILLIS = TimeUnit.SECONDS.toMillis(12);
	private static final String UPLOAD_ID_NOT_FOUND = "Upload id not found";

	private NicSetLoopback stub = new NicSetLoopback();
	private final Map<String, Long> stepElapsedMillis = new ConcurrentHashMap<>();
	private long stepMillis;

	@BeforeEach
	void requireEpoll() {
		assumeTrue(Epoll.isAvailable(), "the stub fixture needs the epoll transport");
	}

	@AfterEach
	void stop() throws Exception {
		stub.close();
		NicSetLoopback.awaitNoNicIoThreads();
	}

	/**
	 * Each upload whose initiation carries a blank upload ID fails at once as not-found, and {@code
	 * load-op-retry} does not start it again; no part, completion or abort is sent, and the step's
	 * {@code errors.log} names the missing upload ID once.
	 */
	@Test
	void uploadsWithABlankUploadIdFailAtOnce() throws Exception {
		for (final String blank : List.of(" ", "\t", "\n", "\r\n", " \t\r\n ", "\u2003")) {
			for (final boolean retry : List.of(false, true)) {
				freshStub().initiationUploadId = uploadId -> blank;
				assertEachFailsNotFound(retry, DEFAULT_TIMEOUT_MILLIS);
			}
		}
	}

	/** With the network timeout off, nothing else would end such an upload. */
	@Test
	void uploadsWithABlankUploadIdFailAtOnceWithTheTimeoutOff() throws Exception {
		for (final boolean retry : List.of(false, true)) {
			freshStub().initiationUploadId = uploadId -> " ";
			assertEachFailsNotFound(retry, 0);
		}
	}

	@Test
	void uploadsWhoseInitiationHasNoBodyFailAtOnce() throws Exception {
		for (final boolean retry : List.of(false, true)) {
			freshStub().initiationUploadId = uploadId -> null;
			assertEachFailsNotFound(retry, DEFAULT_TIMEOUT_MILLIS);
		}
	}

	/** A time-limited step counts its uploads failed, instead of ending with them unresolved. */
	@Test
	void aTimeLimitedStepCountsEachUploadWithABlankUploadIdFailed() throws Exception {
		aTimeLimitedStepCountsEachUploadWithABlankUploadIdFailed(DEFAULT_TIMEOUT_MILLIS);
	}

	@Test
	void aTimeLimitedStepCountsEachUploadWithABlankUploadIdFailedWithTheTimeoutOff() throws Exception {
		aTimeLimitedStepCountsEachUploadWithABlankUploadIdFailed(0);
	}

	private void aTimeLimitedStepCountsEachUploadWithABlankUploadIdFailed(final int timeoutMillis) throws Exception {
		stub.initiationUploadId = uploadId -> " ";
		final Map<String, List<Long>> outcomes;
		final List<String> warnings;
		try (final var errors = new ErrorsLogCapture()) {
			outcomes = write(0, false, timeoutMillis, "3s");
			warnings = uploadIdWarnings(errors);
		}

		final var counts = outcomes.get("CREATE-1");
		assertEquals(0L, counts.get(0), outcomes::toString);
		assertTrue(counts.get(1) > 0, outcomes::toString);
		// an initiation in flight at the stop may be left unresolved, uncounted
		final long initiations = count(isPhase("POST init"));
		assertTrue(initiations == counts.get(1) || initiations == counts.get(1) + 1,
						() -> initiations + " initiations: " + outcomes);
		assertEquals(0, count(isPhase("PUT part")) + count(S3NicSetWorkloadTest::isAbort), () -> stub.requests().toString());
		assertEquals(1, warnings.size(), warnings::toString);
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
	}

	/**
	 * An upload ID with a space around or inside it is kept as sent, so its part requests fail to
	 * build: the upload fails at once as {@code FAIL_UNKNOWN} with the network timeout off, returns its
	 * permit, and the driver's next upload, answered with an ordinary upload ID, succeeds. Retried, the
	 * failed upload starts again from a new initiation and succeeds.
	 */
	@Test
	void anUploadIdThatNoUriCanCarryFailsTheUploadAtOnce() throws Exception {
		for (final String uploadId : List.of(" abc ", "u 1")) {
			for (final boolean retry : List.of(false, true)) {
				freshStub();
				anUploadIdThatNoUriCanCarryFailsTheUploadAtOnce(uploadId, retry);
			}
		}
	}

	private void anUploadIdThatNoUriCanCarryFailsTheUploadAtOnce(final String uploadId, final boolean retry) throws Exception {
		final var first = new AtomicBoolean(true);
		stub.initiationUploadId = opened -> first.getAndSet(false) ? uploadId : opened;

		final var outcomes = write(3, retry, 0, TIME_LIMIT);

		assertEquals(retry ? List.of(3L, 0L) : List.of(2L, 1L), outcomes.get("CREATE-1"),
						() -> "'" + uploadId + "', retry " + retry + ": " + outcomes);
		assertEquals(retry ? 4 : 3, count(isPhase("POST init")), () -> stub.objectRequests().toString());
		final int completed = retry ? 3 : 2;
		assertEquals(completed, count(isPhase("POST complete")), () -> stub.objectRequests().toString());
		assertEquals(5 * completed, count(isPhase("PUT part")), () -> "only the uploads with an ordinary ID send parts: " + stub.objectRequests());
		assertEquals(0, count(S3NicSetWorkloadTest::isAbort), () -> stub.requests().toString());
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
	}

	/** Guard: with ordinary upload IDs, every upload completes. */
	@Test
	void uploadsWithOrdinaryUploadIdsComplete() throws Exception {
		final var outcomes = write(2, false, DEFAULT_TIMEOUT_MILLIS, TIME_LIMIT);

		assertEquals(List.of(2L, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(2, count(isPhase("POST complete")), () -> stub.objectRequests().toString());
		assertEquals(0, stub.openUploads());
	}

	/** A new stub in place of the last, for the next case of a test. */
	private NicSetLoopback freshStub() throws InterruptedException {
		stub.close();
		NicSetLoopback.awaitNoNicIoThreads();
		stub = new NicSetLoopback();
		return stub;
	}

	private void assertEachFailsNotFound(final boolean retry, final int timeoutMillis) throws Exception {
		final Map<String, List<Long>> outcomes;
		final List<String> warnings;
		try (final var errors = new ErrorsLogCapture()) {
			outcomes = write(2, retry, timeoutMillis, TIME_LIMIT);
			warnings = uploadIdWarnings(errors);
		}

		assertEquals(List.of(0L, 2L), outcomes.get("CREATE-1"), () -> "retry " + retry + ": " + outcomes);
		assertEquals(2, count(isPhase("POST init")), () -> "no initiation again: " + stub.objectRequests());
		assertEquals(0, count(isPhase("PUT part")) + count(isPhase("POST complete")), () -> stub.objectRequests().toString());
		assertEquals(0, count(S3NicSetWorkloadTest::isAbort), () -> stub.requests().toString());
		assertEquals(1, warnings.size(), warnings::toString);
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
	}

	/** A step of {@code count} (0: no count limit) 5 KB uploads of five 1 KB parts, at concurrency 1. */
	private Map<String, List<Long>> write(final int count, final boolean retry, final int timeoutMillis, final String timeLimit)
					throws Exception {
		final var started = System.nanoTime();
		final var outcomes = S3NicSetWorkloadTest.runScenarioTimed(stub.listeners(1), "blank-upload-id-" + System.nanoTime(), """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%s"}},
								"op": {"limit": {"count": %d}, "retry": %s, "retryLimit": 1, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": 1}}, "net": {"timeoutMilliSec": %d}},
							"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
						}).run();
						""".formatted(timeLimit, count, retry, WAIT_LIMIT_SECONDS, timeoutMillis), stepElapsedMillis);
		stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		return outcomes;
	}

	private static List<String> uploadIdWarnings(final ErrorsLogCapture errors) throws InterruptedException {
		return errors.drained().stream().map(ErrorsLogCapture.Line::message)
						.filter(message -> message.contains(UPLOAD_ID_NOT_FOUND)).toList();
	}

	private static Predicate<Request> isPhase(final String phase) {
		return request -> S3NicSetWorkloadTest.phase(request).equals(phase);
	}

	private long count(final Predicate<Request> which) {
		return stub.requests().stream().filter(Request::isObjectRequest).filter(which).count();
	}
}
