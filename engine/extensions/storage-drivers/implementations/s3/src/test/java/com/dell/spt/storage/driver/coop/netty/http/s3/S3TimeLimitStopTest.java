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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Time-limited steps of multipart writes and multi-range reads on the shipped {@code s3} driver and the
 * loopback stub. The stub holds one chosen request until it sees the stop, so that its operation is in
 * flight when the time limit stops the step, whatever the timing. The stop refuses that operation's next
 * part, next range or completion: such an operation is reported unresolved at once, so the step ends a
 * few seconds after its limit instead of after its {@code load-op-wait-limit}, and every upload left open
 * on the stub is counted unresolved.
 */
@Timeout(300)
final class S3TimeLimitStopTest {

	private static final int TIME_LIMIT_SECONDS = 4;
	/**
	 * The steps' {@code load-op-wait-limit}, far above {@link #ENDS_WITHIN_MILLIS}: a step that waits for
	 * an operation the stop refused runs this long after its time limit, so it fails the bound.
	 */
	private static final int WAIT_LIMIT_SECONDS = 90;
	/**
	 * The time limit plus room for a cold scenario engine on a loaded runner, and far below the time
	 * limit plus {@link #WAIT_LIMIT_SECONDS}.
	 */
	/** Longer than a held answer waits, so that the held request does not time out. */
	private static final long NET_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);
	private static final long ENDS_WITHIN_MILLIS = TimeUnit.SECONDS.toMillis(TIME_LIMIT_SECONDS + 30);

	private final NicSetLoopback stub = new NicSetLoopback();
	private long stepMillis;
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

	/**
	 * Sixteen threads write 5 KB objects in five 1 KB parts, four uploads at a time. The stub holds the
	 * first part's answer until it sees the stop, so that upload has parts or its completion still to send
	 * at the stop; others may too. Each is unresolved as soon as the stop refuses its next request, and
	 * stays open on the stub.
	 */
	@Test
	void aTimeLimitedMultipartWriteEndsAtItsLimitAndCountsTheOpenUploads() throws Exception {
		final var hold = new HoldUntilStopped(request -> true, NicSetLoopback.Request::isPart);
		stub.answerDelayMillis = hold;

		write(16);

		hold.assertHeld();
		assertEndedNearItsLimit();
		final var open = stub.openUploads();
		assertTrue(open > 0, "the stop left uploads in flight");
		assertEquals(open, unresolved(), "every upload left open is unresolved");
		assertEquals(open, initiations() - completions(), "each open upload has an INIT row and no COMPLETE row");
	}

	/**
	 * The stub holds the first initiation's answer until it sees the stop; the other uploads run on. The
	 * held answer then opens the upload on the stub, but none of its parts can be sent. It is unresolved,
	 * as every other upload left open; it used to count as unattempted, so nothing reported it.
	 */
	@Test
	void anUploadInitiatedAfterTheStopIsCountedUnresolved() throws Exception {
		final var hold = new HoldUntilStopped(request -> true, request -> "POST init".equals(S3NicSetWorkloadTest.phase(request)));
		stub.answerDelayMillis = hold;

		write(4);

		hold.assertHeld();
		assertTrue(stub.objectRequests().stream().noneMatch(request -> request.isPart() && hold.heldPath().equals(request.path())),
						"no part of the upload initiated after the stop was sent");
		assertEndedNearItsLimit();
		final var open = stub.openUploads();
		assertTrue(open > 0, "the stop left initiations in flight");
		assertEquals(open, unresolved(), "every upload left open is unresolved");
		assertEquals(open, initiations() - completions(), "each open upload has an INIT row and no COMPLETE row");
	}

	/**
	 * Eight threads read 5 KB objects in five 1 KB ranges. The stub holds the first range's answer until it
	 * sees the stop, so that read has ranges or its local finalization still to run at the stop; others may
	 * too. Each is unresolved as soon as the stop refuses it, and the step ends at its limit.
	 */
	@Test
	void aTimeLimitedMultiRangeReadEndsAtItsLimit() throws Exception {
		final var hold = new HoldUntilStopped(request -> request.range() != null, request -> request.range() != null);
		stub.answerDelayMillis = hold;

		read(8);

		hold.assertHeld();
		assertEndedNearItsLimit();
		assertTrue(unresolved() > 0, "the stop left reads in flight");
	}

	/**
	 * Holds the answer of the first request that {@code held} selects until the stub sees the stop: the
	 * time limit has passed since the step's first request, which {@code stepRequest} selects, and no
	 * other request has come for {@link #QUIET_MILLIS}. While admission is open the step's other
	 * operations keep sending, so that quiet comes only after the stop. Every other request is answered
	 * at once.
	 */
	private static final class HoldUntilStopped implements ToLongFunction<NicSetLoopback.Request> {
		private static final long QUIET_MILLIS = 500;
		private static final long HOLD_AT_MOST_MILLIS = TimeUnit.SECONDS.toMillis(30);

		private final Predicate<NicSetLoopback.Request> stepRequest;
		private final Predicate<NicSetLoopback.Request> held;
		private final AtomicBoolean taken = new AtomicBoolean();
		private final AtomicLong stepStartNanos = new AtomicLong();
		private volatile long lastOtherNanos;
		private volatile String heldPath;
		private volatile boolean sawStop;

		HoldUntilStopped(final Predicate<NicSetLoopback.Request> stepRequest, final Predicate<NicSetLoopback.Request> held) {
			this.stepRequest = stepRequest;
			this.held = held;
		}

		@Override
		public long applyAsLong(final NicSetLoopback.Request request) {
			final long now = System.nanoTime();
			if (stepRequest.test(request)) {
				stepStartNanos.compareAndSet(0, now);
			}
			if (stepStartNanos.get() != 0 && held.test(request) && taken.compareAndSet(false, true)) {
				heldPath = request.path();
				sawStop = awaitStop();
				return 0;
			}
			lastOtherNanos = now;
			return 0;
		}

		private boolean awaitStop() {
			final long limitNanos = TimeUnit.SECONDS.toNanos(TIME_LIMIT_SECONDS);
			final long quietNanos = TimeUnit.MILLISECONDS.toNanos(QUIET_MILLIS);
			final long giveUp = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(HOLD_AT_MOST_MILLIS);
			while (System.nanoTime() < giveUp) {
				final long now = System.nanoTime();
				if (now - stepStartNanos.get() > limitNanos && now - lastOtherNanos > quietNanos) {
					return true;
				}
				try {
					Thread.sleep(20);
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
			}
			return false;
		}

		String heldPath() {
			return heldPath;
		}

		/** A request was held, and answered only once the stub saw the stop. */
		void assertHeld() {
			assertTrue(taken.get(), "a request was held");
			assertTrue(sawStop, "the held request was answered after the stop");
		}
	}

	/** A time-limited step of 5 KB uploads of five 1 KB parts. */
	private void write(final int concurrency) throws Exception {
		timed(stepScenario("create", concurrency, "\"item\": {\"data\": {\"size\": \"5KB\", \"ranges\": {\"threshold\": \"1KB\"}}}"));
	}

	/** Writes eight 5 KB objects whole, then a time-limited step reads them over and over in five 1 KB ranges. */
	private void read(final int concurrency) throws Exception {
		final var items = tempDir.resolve("items.csv").toString();
		final var seed = """
						Load.config({
							"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": 8}}},
							"item": {"data": {"size": "5KB"}, "output": {"file": "%s"}}
						}).run();
						""".formatted(System.nanoTime(), items);
		timed(seed + stepScenario("read", concurrency, """
						"item": {"data": {"ranges": {"threshold": "1KB"}}, "input": {"file": "%s"}}""".formatted(items)));
	}

	private static String stepScenario(final String opType, final int concurrency, final String item) {
		return """
						Load.config({
							"load": {
								"step": {"limit": {"time": "%ds"}},
								"op": {"type": "%s", "limit": {"count": 0}, "recycle": {"mode": %s}, "wait": {"limit": %d}}
							},
							"storage": {"driver": {"limit": {"concurrency": %d}}, "net": {"timeoutMilliSec": %d}},
							%s
						}).run();
						""".formatted(TIME_LIMIT_SECONDS, opType, "read".equals(opType), WAIT_LIMIT_SECONDS, concurrency, NET_TIMEOUT_MILLIS, item);
	}

	private Map<String, List<Long>> timed(final String scenario) throws Exception {
		stepId = "time-limit-stop-" + System.nanoTime();
		final var started = System.nanoTime();
		final var outcomes = S3NicSetWorkloadTest.runScenario(stub.listeners(1), null, stepId, scenario);
		stepMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		return outcomes;
	}

	/** The step ended a few seconds after its time limit, not after its {@code load-op-wait-limit}. */
	private void assertEndedNearItsLimit() {
		assertTrue(stepMillis < ENDS_WITHIN_MILLIS, () -> "the step took " + stepMillis + " ms");
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

	private long initiations() throws IOException {
		return multipartRows("INIT,");
	}

	private long completions() throws IOException {
		return multipartRows("COMPLETE,");
	}

	/** The rows of the step's {@code parts.upload.csv} that start with {@code phase}. */
	private long multipartRows(final String phase) throws IOException {
		final var file = Path.of(FileManager.INSTANCE.logFileName(Loggers.MULTIPART.getName(), stepId));
		return Files.readAllLines(file).stream().filter(line -> line.startsWith(phase)).count();
	}
}
