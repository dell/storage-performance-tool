package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * A bucket whose name breaks the AWS naming rules cannot be created through the SDK, but one that
 * already exists is usable: the driver's clients send its object requests path-style, as written.
 * This pins that for both clients the driver uses, on the SDK version it ships with.
 */
class S3AwsNonDnsBucketNameTest {

	private static final byte[] CONTENT = "abcd".getBytes(StandardCharsets.UTF_8);

	@ParameterizedTest(name = "CRT transfer client: {0}")
	@ValueSource(booleans = {true, false
	})
	void objectOperationsGoOutPathStyle(final boolean transferClient) throws Exception {
		final var requests = new CopyOnWriteArrayList<String>();
		final var server = objectStub(requests);
		final var timeouts = new S3AwsClientTimeouts(Duration.ofSeconds(5), Duration.ofSeconds(10));
		final var port = server.getAddress().getPort();
		try (S3AsyncClient client = transferClient
						? S3AwsRawClients.transfer(port, timeouts)
						: S3AwsRawClients.exactVersion(port, timeouts)) {
			assertSentAsWritten(requests, "PUT", () -> client.putObject(request -> request.bucket("PS_US").key("k/o"),
							AsyncRequestBody.fromBytes(CONTENT)).get(20, TimeUnit.SECONDS));
			assertSentAsWritten(requests, "HEAD", () -> client.headObject(request -> request.bucket("PS_US").key("k/o"))
							.get(20, TimeUnit.SECONDS));
			assertSentAsWritten(requests, "GET", () -> assertArrayEquals(CONTENT, client.getObject(
							request -> request.bucket("PS_US").key("k/o"), AsyncResponseTransformer.toBytes())
							.get(20, TimeUnit.SECONDS).asByteArray()));
			assertSentAsWritten(requests, "DELETE", () -> client.deleteObject(request -> request.bucket("PS_US").key("k/o"))
							.get(20, TimeUnit.SECONDS));
		} finally {
			server.stop(0);
		}
	}

	private static void assertSentAsWritten(final List<String> requests, final String method, final Call call) throws Exception {
		requests.clear();
		call.run();
		assertTrue(requests.contains(method + " /PS_US/k/o"), method + " was not sent as written: " + requests);
		for (final var request : requests) {
			assertTrue(request.endsWith(" /PS_US/k/o"), request);
		}
	}

	/** Holds one four-byte object at any key and honors a range, as the CRT client's GET expects. */
	private static HttpServer objectStub(final List<String> requests) throws Exception {
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			final var method = exchange.getRequestMethod();
			requests.add(method + " " + exchange.getRequestURI().getRawPath());
			exchange.getRequestBody().readAllBytes();
			exchange.getResponseHeaders().add("ETag", "\"etag\"");
			if ("HEAD".equals(method)) {
				exchange.getResponseHeaders().add("Content-Length", String.valueOf(CONTENT.length));
				exchange.sendResponseHeaders(200, -1);
			} else if ("GET".equals(method)) {
				final boolean ranged = exchange.getRequestHeaders().containsKey("Range");
				if (ranged) {
					exchange.getResponseHeaders().add("Content-Range", "bytes 0-3/4");
				}
				exchange.sendResponseHeaders(ranged ? 206 : 200, CONTENT.length);
				exchange.getResponseBody().write(CONTENT);
			} else {
				exchange.sendResponseHeaders("DELETE".equals(method) ? 204 : 200, -1);
			}
			exchange.close();
		});
		server.start();
		return server;
	}

	@FunctionalInterface
	private interface Call {
		void run() throws Exception;
	}
}
