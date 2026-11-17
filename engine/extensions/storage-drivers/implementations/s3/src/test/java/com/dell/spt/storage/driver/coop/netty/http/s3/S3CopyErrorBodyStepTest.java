package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.Request;
import io.netty.channel.epoll.Epoll;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Copy steps on the shipped {@code s3} driver and the loopback stub: four 2 KB objects are written to
 * {@code /src}, then copied to {@code /bucket} by a step with {@code item.input.path=/src}. A copy the
 * stub answers {@code 200 OK} with an S3 {@code <Error>} document is a failure and counts no bytes; a
 * copy answered with a {@code <CopyObjectResult>} succeeds as before.
 */
@Timeout(120)
final class S3CopyErrorBodyStepTest {

	private static final int COUNT = 4;
	private static final long SIZE = 2048;

	private final NicSetLoopback stub = new NicSetLoopback();
	private final Map<String, Long> bytes = new ConcurrentHashMap<>();

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
	void copiesAnswered200WithAnErrorBodyFailAndCountNoBytes() throws Exception {
		stub.copyErrorBody = "\n  <?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
						+ "<Error><Code>InternalError</Code><Message>We encountered an internal error.</Message></Error>";
		stub.failCopyWithErrorBody = request -> true;

		final var outcomes = copyStep();

		assertEquals(List.of(0L, (long) COUNT), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(0L, bytes.get("CREATE-1"), bytes::toString);
		assertEquals(COUNT, copies().size(), () -> "copies: " + stub.objectRequests());
		for (final var copy : copies()) {
			assertNull(stub.object(copy.path()), "the failed copy wrote nothing");
		}
	}

	@Test
	void aPermanentErrorCodeFailsTheCopyAndRetryDoesNotSendItAgain() throws Exception {
		stub.copyErrorBody = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>";
		final var answered = new AtomicInteger();
		stub.failCopyWithErrorBody = request -> answered.incrementAndGet() == 1;

		final var outcomes = copyStep(true);

		assertEquals(List.of((long) COUNT - 1, 1L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals((COUNT - 1) * SIZE, bytes.get("CREATE-1"), bytes::toString);
		assertEquals(COUNT, copies().size(), () -> "copies: " + stub.objectRequests());
	}

	@Test
	void aTransientErrorCodeFailsTheCopyAndRetrySendsItAgain() throws Exception {
		stub.copyErrorBody = "<Error><Code>InternalError</Code><Message>m</Message></Error>";
		final var answered = new AtomicInteger();
		stub.failCopyWithErrorBody = request -> answered.incrementAndGet() == 1;

		final var outcomes = copyStep(true);

		assertEquals(List.of((long) COUNT, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(COUNT * SIZE, bytes.get("CREATE-1"), bytes::toString);
		assertEquals(COUNT + 1, copies().size(), () -> "copies: " + stub.objectRequests());
		assertEquals(COUNT, copies().stream().map(Request::path).distinct().count(), () -> "copies: " + stub.objectRequests());
		for (final var copy : copies()) {
			assertNotNull(stub.object(copy.path()), () -> "the retried copy exists: " + copy);
		}
	}

	@Test
	void normalCopiesAreUnchanged() throws Exception {
		final var outcomes = copyStep();

		assertEquals(List.of((long) COUNT, 0L), outcomes.get("CREATE-1"), outcomes::toString);
		assertEquals(COUNT * SIZE, bytes.get("CREATE-1"), bytes::toString);
		assertEquals(COUNT, copies().size(), () -> "copies: " + stub.objectRequests());
		for (final var copy : copies()) {
			final var copied = stub.object(copy.path());
			assertNotNull(copied, () -> "the copy exists: " + copy);
			assertEquals(SIZE, copied.length);
		}
	}

	private Map<String, List<Long>> copyStep() throws Exception {
		return copyStep(false);
	}

	/** Writes {@link #COUNT} objects to {@code /src}, then copies each to {@code /bucket}. */
	private Map<String, List<Long>> copyStep(final boolean retry) throws Exception {
		final var stepId = "copy-error-body-" + System.nanoTime();
		final var outcomes = S3NicSetWorkloadTest.runScenario(stub.listeners(1), stepId, """
						Load.config({
							"load": {"step": {"id": "seed-%d"}, "op": {"limit": {"count": %d}}},
							"item": {"data": {"size": "2KB"}, "output": {"path": "/src"}}
						}).run();
						Load.config({
							"load": {
								"step": {"limit": {"time": "60s"}},
								"op": {"limit": {"count": %d}, "retry": %s, "retryLimit": 1}
							},
							"item": {"input": {"path": "/src"}, "output": {"path": "/bucket"}}
						}).run();
						""".formatted(System.nanoTime(), COUNT, COUNT, retry), bytes);
		assertTrue(seeded().size() == COUNT, () -> "seeded: " + stub.objectRequests());
		return outcomes;
	}

	private List<Request> seeded() {
		return stub.objectRequests().stream()
						.filter(request -> "PUT".equals(request.method()) && request.path().startsWith("/src/"))
						.toList();
	}

	private List<Request> copies() {
		return stub.objectRequests().stream()
						.filter(request -> "PUT".equals(request.method()) && request.path().startsWith("/bucket/"))
						.toList();
	}
}
