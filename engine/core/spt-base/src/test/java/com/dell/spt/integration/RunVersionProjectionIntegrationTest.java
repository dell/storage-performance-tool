package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.buildinfo.EngineBuildInfoProvider;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.ApiStatus;
import com.dell.spt.base.control.run.RunServlet;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.testing.tags.IntegrationTest;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import javax.servlet.MultipartConfigElement;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("Control API run.version projection")
@IntegrationTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RunVersionProjectionIntegrationTest {

	@TempDir
	Path tempDir;

	Stream<Arguments> partialConfigurations() {
		return Stream.of(
						Arguments.of("empty", "{}\n"),
						Arguments.of("without-run", "output:\n  color: false\n"),
						Arguments.of("run-id-only", "run:\n  id: 123456789\n"),
						Arguments.of("version-override", "run:\n  version: user-value\n"));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("partialConfigurations")
	void multipartSubmissionReceivesTheImmutableVersion(final String caseName, final String incomingConfig)
					throws Exception {
		final var server = new Server(0);
		final var context = new ServletContextHandler();
		context.setContextPath("/");
		server.setHandler(context);
		final var runServlet = new RunServlet(
						Thread.currentThread().getContextClassLoader(),
						List.of(),
						mock(MetricsManager.class),
						TestConfigBuilder.config(),
						tempDir,
						mock(LoadStepManagerService.class),
						new ApiStatus());
		final var holder = new ServletHolder(runServlet);
		context.addServlet(holder, "/run");
		holder.getRegistration().setMultipartConfig(new MultipartConfigElement(""));

		final var observedVersionFile = tempDir.resolve(caseName + "-version.txt").toAbsolutePath();
		final var scenario = """
						var Files = Java.type('java.nio.file.Files');
						var Path = Java.type('java.nio.file.Path');
						var JString = Java.type('java.lang.String');
						Files.writeString(Path.of('%s'), JString.valueOf(config.stringVal('run-version')));
						""".formatted(observedVersionFile);

		try {
			server.start();
			final int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
			final var multipart = multipartBody(incomingConfig, scenario);
			final var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/run"))
							.header("Content-Type", "multipart/form-data; boundary=" + multipart.boundary())
							.timeout(Duration.ofSeconds(10))
							.POST(HttpRequest.BodyPublishers.ofByteArray(multipart.body()))
							.build();

			final var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding());

			assertEquals(202, response.statusCode());
			awaitFile(observedVersionFile, Duration.ofSeconds(5));
			assertEquals(
							EngineBuildInfoProvider.global().snapshot().version(),
							Files.readString(observedVersionFile, StandardCharsets.UTF_8));
		} finally {
			server.stop();
			server.destroy();
		}
	}

	private static MultipartBody multipartBody(final String config, final String scenario) throws Exception {
		final String boundary = "spt-" + UUID.randomUUID();
		final var output = new ByteArrayOutputStream();
		writePart(output, boundary, "defaults", "defaults.yaml", "application/yaml", config);
		writePart(output, boundary, "scenario", "scenario.js", "application/javascript", scenario);
		output.write(("--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8));
		return new MultipartBody(boundary, output.toByteArray());
	}

	private static void writePart(
					final ByteArrayOutputStream output,
					final String boundary,
					final String name,
					final String fileName,
					final String contentType,
					final String content)
					throws Exception {
		output.write(("--" + boundary + "\r\n").getBytes(StandardCharsets.UTF_8));
		output.write(("Content-Disposition: form-data; name=\"" + name + "\"; filename=\"" + fileName + "\"\r\n")
						.getBytes(StandardCharsets.UTF_8));
		output.write(("Content-Type: " + contentType + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
		output.write(content.getBytes(StandardCharsets.UTF_8));
		output.write("\r\n".getBytes(StandardCharsets.UTF_8));
	}

	private static void awaitFile(final Path path, final Duration timeout) throws Exception {
		final long deadline = System.nanoTime() + timeout.toNanos();
		while (!Files.isRegularFile(path) && System.nanoTime() < deadline) {
			Thread.sleep(25);
		}
		assertTrue(Files.isRegularFile(path), "Scenario did not record the projected run.version");
	}

	private static final class MultipartBody {

		private final String boundary;
		private final byte[] body;

		private MultipartBody(final String boundary, final byte[] body) {
			this.boundary = boundary;
			this.body = body;
		}

		private String boundary() {
			return boundary;
		}

		private byte[] body() {
			return body;
		}
	}
}
