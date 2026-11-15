package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.TestConfigBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.akurilov.confuse.Config;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A node's {@code GET /config} masks the {@code storage.auth} credentials and the listed request
 * headers, as the step {@code config.yaml} does, while the node keeps the real values. All
 * credentials here are made up.
 */
class ConfigServletMaskingTest {

	private static final List<String> MARKERS = List.of(
					"FAKE-UID-MARKER", "FAKE-SECRET-MARKER", "FAKE-TOKEN-MARKER", "FAKE-AUTH-MARKER", "FAKE-XAUTH-MARKER");

	private Server server;
	private Config config;
	private String base;

	@BeforeEach
	void start() throws Exception {
		final Map<String, Object> headers = new HashMap<>();
		headers.put("Authorization", "AWS FAKE-AUTH-MARKER");
		headers.put("x-auth-token", "FAKE-XAUTH-MARKER");
		headers.put("X-Custom", "custom-value");
		config = TestConfigBuilder.configWithHttpHeaders(headers);
		config.val("storage-auth-uid", "FAKE-UID-MARKER");
		config.val("storage-auth-secret", "FAKE-SECRET-MARKER");
		config.val("storage-auth-token", "FAKE-TOKEN-MARKER");
		server = new Server(0);
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		context.addServlet(new ServletHolder(new ConfigServlet(config)), "/config/*");
		server.setHandler(context);
		server.start();
		base = "http://127.0.0.1:" + ((ServerConnector) server.getConnectors()[0]).getLocalPort();
	}

	@AfterEach
	void stop() throws Exception {
		server.stop();
	}

	/** No Accept header gives YAML, as do other types; {@code application/json} and {@code text/json} give JSON. */
	@ParameterizedTest
	@ValueSource(strings = {"", "text/yaml", "application/json", "text/json"
	})
	void getConfigMasksCredentialsAndListedHeadersInEachFormat(final String accept) throws Exception {
		final var request = HttpRequest.newBuilder(URI.create(base + "/config"));
		if (!accept.isEmpty()) {
			request.header("Accept", accept);
		}
		final var response = HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());

		assertEquals(200, response.statusCode());
		final var body = response.body();
		for (final var marker : MARKERS) {
			assertFalse(body.contains(marker), marker + " in " + accept);
		}
		final var mapper = accept.contains("json") ? new ObjectMapper() : new ObjectMapper(new YAMLFactory());
		final JsonNode storage = mapper.readTree(body).path("storage");
		for (final var key : ConfigUtil.MASKED_AUTH_KEYS) {
			assertEquals(ConfigUtil.MASKED_VALUE, storage.path("auth").path(key).asText(), key);
		}
		final var headers = storage.path("net").path("http").path("headers");
		assertEquals(ConfigUtil.MASKED_VALUE, headers.path("Authorization").asText());
		assertEquals(ConfigUtil.MASKED_VALUE, headers.path("x-auth-token").asText());
		assertEquals("custom-value", headers.path("X-Custom").asText());
		// The node keeps the real values.
		assertEquals("FAKE-SECRET-MARKER", config.stringVal("storage-auth-secret"));
		assertEquals("AWS FAKE-AUTH-MARKER", config.mapVal("storage-net-http-headers").get("Authorization"));
	}
}
