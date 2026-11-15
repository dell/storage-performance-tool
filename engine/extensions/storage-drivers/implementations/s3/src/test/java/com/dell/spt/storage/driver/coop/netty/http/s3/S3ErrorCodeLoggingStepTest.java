package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.netty.channel.epoll.Epoll;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Steps on the shipped {@code s3} driver and the loopback stub, whose refusals carry an S3 error
 * document: each refused request's {@code errors.log} has its status line and a line with the S3
 * code and message, and the step's counts are those of the refusals.
 */
@Timeout(240)
final class S3ErrorCodeLoggingStepTest {

	private static final String ENTITY_TOO_SMALL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>"
					+ "<Code>EntityTooSmall</Code><Message>Your proposed upload is smaller than the minimum allowed"
					+ " object size.</Message><UploadId>u</UploadId><RequestId>r</RequestId></Error>";
	private static final String TIME_LIMIT = "60s";

	private final NicSetLoopback stub = new NicSetLoopback();

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
	void aRefusedCompletionLogsEntityTooSmall() throws Exception {
		// The upload's object requests: its initiation, five parts, then its completion.
		stub.failObjectRequest = n -> n == 7;
		stub.failureStatus = 400;
		stub.failureBody = ENTITY_TOO_SMALL;
		try (final var errors = new ErrorsLogCapture()) {
			final var outcomes = run("""
							Load.config({
								"load": {"step": {"limit": {"time": "%s"}}, "op": {"limit": {"count": 1}, "retry": false}},
								"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}}
							}).run();
							""".formatted(TIME_LIMIT));
			assertEquals(List.of(0L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
			final var all = messages(errors.drained());
			assertTrue(all.stream().anyMatch(line -> line.endsWith(": 400 Bad Request")), all::toString);
			final var lines = messages(errors.failedResponseLines());
			assertEquals(1, lines.size(), all::toString);
			assertTrue(lines.get(0).endsWith(": S3 error code EntityTooSmall, message: Your proposed upload is smaller"
							+ " than the minimum allowed object size."), lines::toString);
		}
	}

	@Test
	void aRefusedPutLogsItsCode() throws Exception {
		stub.failObjectRequest = n -> n == 1;
		stub.failureStatus = 400;
		stub.failureBody = "<Error><Code>InvalidArgument</Code><Message>Bad header</Message></Error>";
		try (final var errors = new ErrorsLogCapture()) {
			final var outcomes = run("""
							Load.config({
								"load": {"step": {"limit": {"time": "%s"}}, "op": {"limit": {"count": 2}, "retry": false}},
								"item": {"data": {"size": "5KB"}}
							}).run();
							""".formatted(TIME_LIMIT));
			assertEquals(List.of(1L, 1L), outcomes.get("CREATE-1"), outcomes::toString);
			final var all = messages(errors.drained());
			assertTrue(all.stream().anyMatch(line -> line.endsWith(": 400 Bad Request")), all::toString);
			final var lines = messages(errors.failedResponseLines());
			assertEquals(1, lines.size(), all::toString);
			assertTrue(lines.get(0).endsWith(": S3 error code InvalidArgument, message: Bad header"), lines::toString);
		}
	}

	@Test
	void aRefusedGetLogsItsCode() throws Exception {
		final var items = tempDir.resolve("items.csv").toString();
		stub.failRequest = request -> "GET".equals(request.method());
		stub.failureStatus = 404;
		stub.failureBody = "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>";
		try (final var errors = new ErrorsLogCapture()) {
			final var outcomes = run("""
							Load.config({
								"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": 2}}},
								"item": {"data": {"size": "5KB"}, "output": {"file": "%s"}}
							}).run();
							Load.config({
								"load": {"step": {"limit": {"time": "%s"}}, "op": {"type": "read", "limit": {"count": 2}, "retry": false}},
								"item": {"input": {"file": "%s"}}
							}).run();
							""".formatted(System.nanoTime(), items, TIME_LIMIT, items));
			assertEquals(List.of(0L, 2L), outcomes.get("READ-1"), outcomes::toString);
			final var lines = messages(errors.failedResponseLines());
			assertEquals(2, lines.size(), lines::toString);
			assertTrue(lines.stream().allMatch(line -> line.startsWith("READ,")
							&& line.endsWith(": S3 error code NoSuchKey, message: The specified key does not exist.")),
							lines::toString);
		}
	}

	@Test
	void successfulStepsLogNoErrorCode() throws Exception {
		final var items = tempDir.resolve("items.csv").toString();
		try (final var errors = new ErrorsLogCapture()) {
			final var outcomes = run("""
							Load.config({
								"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": 2}}},
								"item": {"data": {"size": "5KB", "ranges": {"threshold": "1KB"}}, "output": {"file": "%s"}}
							}).run();
							Load.config({
								"load": {"step": {"limit": {"time": "%s"}}, "op": {"type": "read", "limit": {"count": 2}}},
								"item": {"input": {"file": "%s"}}
							}).run();
							""".formatted(System.nanoTime(), items, TIME_LIMIT, items));
			assertEquals(List.of(2L, 0L), outcomes.get("READ-1"), outcomes::toString);
			assertEquals(List.of(), errors.failedResponseLines());
		}
	}

	private Map<String, List<Long>> run(final String scenario) throws Exception {
		return S3NicSetWorkloadTest.runScenario(stub.listeners(1), null, "error-code-" + System.nanoTime(), scenario);
	}

	private static List<String> messages(final List<ErrorsLogCapture.Line> lines) {
		return lines.stream().map(ErrorsLogCapture.Line::message).toList();
	}
}
