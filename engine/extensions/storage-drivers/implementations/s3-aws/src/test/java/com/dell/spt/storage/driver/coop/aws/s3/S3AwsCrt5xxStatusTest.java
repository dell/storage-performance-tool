package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.op.Operation.Status;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * The status the driver gives a single PUT and a part answered 5xx through the real CRT transfer
 * client, against a loopback S3. The client retries a 5xx itself and then reports the request as a
 * causeless client exception that names the CRT error and not the status, so a 504 cannot be told
 * from a 500; each ends as a service failure. A CRT or SDK upgrade that rewords those reports fails
 * here.
 *
 * <p>The cases run at the same time, each on its own object key: one case is the request and the
 * client's own retries with back-off, several seconds.
 */
@Timeout(120)
final class S3AwsCrt5xxStatusTest {

	private static final S3AwsClientTimeouts TIMEOUTS = new S3AwsClientTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(30));

	private record Case(boolean part, int status, String errorCode) {

		String key() {
			return (part ? "part" : "put") + "-" + status + "-" + (errorCode == null ? "no-body" : errorCode);
		}
	}

	private final Map<String, Case> cases = new ConcurrentHashMap<>();
	private final Map<String, AtomicInteger> requests = new ConcurrentHashMap<>();
	private final ExecutorService threads = Executors.newCachedThreadPool();
	private HttpServer server;
	private S3AsyncClient client;

	@BeforeEach
	void start() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.setExecutor(threads);
		server.createContext("/", this::handle);
		server.start();
		client = S3AwsRawClients.transfer(server.getAddress().getPort(), TIMEOUTS);
	}

	@AfterEach
	void stop() {
		client.close();
		server.stop(0);
		threads.shutdownNow();
	}

	@Test
	void singlePutsAndPartsAnswered5xxAreServiceFailures() {
		final List<Case> all = new ArrayList<>();
		for (final boolean part : new boolean[]{false, true
		}) {
			all.add(new Case(part, 500, "InternalError"));
			all.add(new Case(part, 500, null));
			all.add(new Case(part, 503, "SlowDown"));
			all.add(new Case(part, 503, null));
		}
		all.add(new Case(false, 504, "GatewayTimeout"));
		all.add(new Case(true, 504, null));

		final Map<Case, CompletableFuture<?>> sent = new LinkedHashMap<>();
		for (final Case c : all) {
			cases.put(c.key(), c);
			requests.put(c.key(), new AtomicInteger());
			sent.put(c, send(c));
		}
		for (final var entry : sent.entrySet()) {
			final Case c = entry.getKey();
			final Exception failure = failure(entry.getValue());
			assertEquals(Status.RESP_FAIL_SVC, S3AwsStorageDriver.classifyFailure(failure), () -> c + ": " + failure);
			assertTrue(requests.get(c.key()).get() > 0, c::toString);
		}
	}

	private CompletableFuture<?> send(final Case c) {
		final var body = AsyncRequestBody.fromBytes(new byte[1024]);
		if (c.part()) {
			return client.uploadPart(b -> b.bucket("bucket").key(c.key()).uploadId("upload-1").partNumber(1).contentLength(1024L), body);
		}
		return client.putObject(b -> b.bucket("bucket").key(c.key()), body);
	}

	/** The failure as the driver sees it, from a join. */
	private static Exception failure(final CompletableFuture<?> request) {
		try {
			request.join();
		} catch (final RuntimeException e) {
			return e;
		}
		throw new AssertionError("the request succeeded");
	}

	private void handle(final HttpExchange exchange) throws IOException {
		try (exchange) {
			exchange.getRequestBody().readAllBytes();
			exchange.getResponseHeaders().set("Date", DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)));
			final String path = exchange.getRequestURI().getPath();
			final Case c = cases.get(path.substring(path.lastIndexOf('/') + 1));
			if (c == null) {
				exchange.getResponseHeaders().set("Content-Length", "0");
				exchange.sendResponseHeaders(400, -1);
				return;
			}
			requests.get(c.key()).incrementAndGet();
			if (c.errorCode() == null) {
				exchange.getResponseHeaders().set("Content-Length", "0");
				exchange.sendResponseHeaders(c.status(), -1);
				return;
			}
			final var bytes = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>" + c.errorCode()
							+ "</Code><Message>scripted</Message><RequestId>r</RequestId></Error>").getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().set("Content-Type", "application/xml");
			exchange.sendResponseHeaders(c.status(), bytes.length);
			exchange.getResponseBody().write(bytes);
		}
	}
}
