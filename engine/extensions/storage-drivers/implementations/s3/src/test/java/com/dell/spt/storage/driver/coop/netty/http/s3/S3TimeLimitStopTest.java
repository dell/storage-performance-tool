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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Time-limited steps of multipart writes and multi-range reads on the shipped {@code s3} driver and the
 * loopback stub, which answers each object request after a delay so that operations are in flight when
 * the time limit stops the step. The stop refuses their next part, next range or completion: such an
 * operation is reported unresolved at once, so the step ends a few seconds after its limit instead of
 * after its {@code load-op-wait-limit}, and every upload left open on the stub is counted unresolved.
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
	 * Sixteen threads write 5 KB objects in five 1 KB parts, four uploads at a time, each request answered
	 * after 50 to 150 ms, by object. At the stop, the uploads in flight have parts or a completion still to
	 * send. Each is unresolved as soon as the stop refuses its next request, and stays open on the stub. A
	 * single delay kept the four uploads in step, so a stop could fall between two waves with none in flight.
	 */
	@Test
	void aTimeLimitedMultipartWriteEndsAtItsLimitAndCountsTheOpenUploads() throws Exception {
		stub.answerDelayMillis = request -> 50 + Math.floorMod(request.path().hashCode(), 101);

		write(16);

		assertEndedNearItsLimit();
		final var open = stub.openUploads();
		assertTrue(open > 0, "the stop left uploads in flight");
		assertEquals(open, unresolved(), "every upload left open is unresolved");
		assertEquals(open, initiations() - completions(), "each open upload has an INIT row and no COMPLETE row");
	}

	/**
	 * Every initiation is answered after 1.5 s and the parts at once, so the stop finds initiations in
	 * flight. Their answers come after it: the upload is open on the stub, but none of its parts can be
	 * sent. It is unresolved, as every other upload left open; it used to count as unattempted, so
	 * nothing reported it.
	 */
	@Test
	void anUploadInitiatedAfterTheStopIsCountedUnresolved() throws Exception {
		stub.answerDelayMillis = request -> "POST init".equals(S3NicSetWorkloadTest.phase(request)) ? 1_500 : 0;

		write(4);

		assertEndedNearItsLimit();
		final var open = stub.openUploads();
		assertTrue(open > 0, "the stop left initiations in flight");
		assertEquals(open, unresolved(), "every upload left open is unresolved");
		assertEquals(open, initiations() - completions(), "each open upload has an INIT row and no COMPLETE row");
	}

	/**
	 * Eight threads read 5 KB objects in five 1 KB ranges, two reads at a time, each range answered after
	 * 100 ms. At the stop, the reads in flight have ranges or their local finalization still to run: each
	 * is unresolved as soon as the stop refuses it, and the step ends at its limit.
	 */
	@Test
	void aTimeLimitedMultiRangeReadEndsAtItsLimit() throws Exception {
		read(8);

		assertEndedNearItsLimit();
		assertTrue(unresolved() > 0, "the stop left reads in flight");
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
		stub.answerDelayMillis = request -> request.range() != null ? 100 : 0;
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
							"storage": {"driver": {"limit": {"concurrency": %d}}},
							%s
						}).run();
						""".formatted(TIME_LIMIT_SECONDS, opType, "read".equals(opType), WAIT_LIMIT_SECONDS, concurrency, item);
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
