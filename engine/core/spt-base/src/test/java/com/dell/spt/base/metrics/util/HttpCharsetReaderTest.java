package com.dell.spt.base.metrics.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.integration.EntryWorkerJsonMetricsIntegrationTest;
import com.dell.spt.integration.JsonMetricsIntegrationTest;
import com.dell.spt.integration.WorkerJsonMetricsIntegrationTest;
import com.sun.net.httpserver.HttpServer;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test coverage ensuring the HTTP readers respect declared response charsets.
 * These mirror the helpers used by the integration tests but isolate them so the
 * default charset assumptions become visible in CI.
 */
final class HttpCharsetReaderTest {

	private HttpServer server;

	@BeforeEach
	void setUpServer() throws Exception {
		server = HttpServer.create(new InetSocketAddress(0), 0);
	}

	@AfterEach
	void tearDownServer() {
		if (server != null) {
			server.stop(0);
		}
	}

	@Test
	void jsonFetchRespectsUtf16Charset() throws Exception {
		registerUtf16Context("/json");
		server.start();
		try {
			final Method fetch = JsonMetricsIntegrationTest.class.getDeclaredMethod("fetchFromUrl", String.class);
			fetch.setAccessible(true);
			final var instance = new JsonMetricsIntegrationTest();
			final String response = (String) fetch.invoke(instance, url("/json"));
			assertEquals("føø\n", response);
		} finally {
			server.stop(0);
		}
	}

	@Test
	void workerFetchRespectsUtf16Charset() throws Exception {
		registerUtf16Context("/worker");
		server.start();
		try {
			final Method fetch = WorkerJsonMetricsIntegrationTest.class.getDeclaredMethod("fetch", String.class);
			fetch.setAccessible(true);
			final String response = (String) fetch.invoke(null, url("/worker"));
			assertEquals("føø\n", response);
		} finally {
			server.stop(0);
		}
	}

	@Test
	void entryFetchRespectsUtf16Charset() throws Exception {
		registerUtf16Context("/entry");
		server.start();
		try {
			final Method fetch = EntryWorkerJsonMetricsIntegrationTest.class.getDeclaredMethod("fetch", String.class);
			fetch.setAccessible(true);
			final String response = (String) fetch.invoke(null, url("/entry"));
			assertEquals("très\n", response);
		} finally {
			server.stop(0);
		}
	}

	private void registerUtf16Context(final String path) {
		server.createContext(path, exchange -> {
			final String payload = path.contains("entry") ? "très" : "føø";
			final byte[] body = payload.getBytes(StandardCharsets.UTF_16LE);
			exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-16LE");
			exchange.sendResponseHeaders(200, body.length);
			try (OutputStream os = exchange.getResponseBody()) {
				os.write(body);
			}
		});
	}

	private String url(final String path) {
		return "http://127.0.0.1:" + server.getAddress().getPort() + path;
	}
}
