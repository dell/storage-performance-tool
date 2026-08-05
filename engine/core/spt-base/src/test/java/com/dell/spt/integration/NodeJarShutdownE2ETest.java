package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.testing.tags.IntegrationTest;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("E2E: packaged jar /shutdown triggers graceful stop with linger")
@IntegrationTest
public class NodeJarShutdownE2ETest {

	private static Path findProjectRoot(Path start) {
		Path p = start;
		for (int i = 0; i < 12 && p != null; i++, p = p.getParent()) {
			if (Files.isRegularFile(p.resolve("gradlew"))
							&& Files.isRegularFile(p.resolve("bundle/build.gradle"))) {
				return p;
			}
			final Path engineDir = p.resolve("engine");
			if (Files.isRegularFile(engineDir.resolve("gradlew"))
							&& Files.isRegularFile(engineDir.resolve("bundle/build.gradle"))) {
				return engineDir;
			}
		}
		return start; // fallback
	}

	@Test
	void jarShutdownLifecycle() throws Exception {
		Path moduleDir = Paths.get("").toAbsolutePath();
		Path root = findProjectRoot(moduleDir);
		Path jar = root.resolve("bundle/build/dist/spt.jar");

		if (!Files.exists(jar)) {
			// Attempt to build the bundle so the jar exists
			ProcessBuilder pb = new ProcessBuilder(
							root.resolve("gradlew").toString(), ":bundle:build", "-q");
			pb.directory(root.toFile());
			Process build = pb.start();
			if (!build.waitFor(120, java.util.concurrent.TimeUnit.SECONDS) || build.exitValue() != 0) {
				// If build fails, skip to avoid false negatives in environments without docker/JDK
				org.junit.jupiter.api.Assumptions.abort("Could not build bundle for E2E test; module=" + moduleDir + ", root=" + root + ", jar=" + jar);
			}
			assertTrue(Files.exists(jar), "Bundle jar not found after build");
		}

		final int port = 20000 + ThreadLocalRandom.current().nextInt(10000);
		final var outFile = Files.createTempFile("spt-node-out", ".log");
		final var errFile = Files.createTempFile("spt-node-err", ".log");
		final var logDir = Files.createTempDirectory("spt-node-log");

		ProcessBuilder nodePb = new ProcessBuilder(
						"java",
						"-jar",
						jar.toString(),
						"--run-node",
						"--run-port=" + port,
						"--api-linger-sec=2",
						"--storage-driver-type=netty-mock");
		nodePb.environment().put("SPT_LOG_DIR", logDir.toString());
		nodePb.redirectOutput(outFile.toFile());
		nodePb.redirectError(errFile.toFile());
		Process node = nodePb.start();

		HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
		try {
			final String base = "http://127.0.0.1:" + port;

			// Wait readiness up to 30s
			boolean ready = false;
			IOException lastConnectFailure = null;
			for (int i = 0; i < 30; i++) {
				try {
					int code = client.send(HttpRequest.newBuilder(URI.create(base + "/metrics")).GET().build(),
									HttpResponse.BodyHandlers.discarding()).statusCode();
					if (code == 200) {
						ready = true;
						break;
					}
				} catch (final IOException e) {
					lastConnectFailure = e;
				}
				Thread.sleep(1000);
			}
			assertTrue(
							ready,
							"Node did not become ready in time; last error="
											+ (lastConnectFailure == null ? "<none>" : lastConnectFailure)
											+ "; stdout="
											+ readTail(outFile)
											+ ", stderr="
											+ readTail(errFile));

			// Basic /status should be 200
			var statusResp = client.send(HttpRequest.newBuilder(URI.create(base + "/status")).GET().build(), HttpResponse.BodyHandlers.ofString());
			assertEquals(200, statusResp.statusCode());

			// Start a real packaged Netty workload so shutdown exercises the extension loader.
			var runResp = client.send(
							HttpRequest.newBuilder(URI.create(base + "/run")).POST(HttpRequest.BodyPublishers.noBody()).build(),
							HttpResponse.BodyHandlers.discarding());
			assertEquals(202, runResp.statusCode());
			awaitStatus(client, base, "RUNNING", Duration.ofSeconds(5));

			// Request shutdown
			var shutResp = client.send(HttpRequest.newBuilder(URI.create(base + "/shutdown")).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
			assertEquals(202, shutResp.statusCode());

			// During linger, status may still be 200
			try {
				Thread.sleep(500);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				fail("Interrupted while waiting during linger", e);
			}
			awaitStatus(client, base, "STOPPED", Duration.ofSeconds(5));

			// Wait for process to exit within linger+grace period
			boolean exited = node.waitFor(15, java.util.concurrent.TimeUnit.SECONDS);
			if (!exited) {
				node.destroyForcibly();
				fail("Node did not exit after shutdown; stdout=" + readTail(outFile) + ", stderr=" + readTail(errFile));
			}
			final var output = readFile(outFile) + readFile(errFile);
			assertEquals(0, node.exitValue(), output);
			assertFalse(output.contains("IllegalAccessError"), output);
			assertFalse(output.contains("I/O workers did not stop"), output);
			assertFalse(output.contains("Graceful I/O workers shutdown was interrupted"), output);
			assertFalse(output.contains("Uncaught exception"), output);
		} finally {
			if (node.isAlive()) {
				node.destroy();
				if (!node.waitFor(2, java.util.concurrent.TimeUnit.SECONDS)) {
					node.destroyForcibly();
				}
			}
		}
	}

	private static HttpResponse<String> awaitStatus(
					final HttpClient client,
					final String base,
					final String expectedState,
					final Duration timeout)
					throws Exception {
		final long deadlineNanos = System.nanoTime() + timeout.toNanos();
		HttpResponse<String> response = null;
		do {
			response = client.send(
							HttpRequest.newBuilder(URI.create(base + "/status")).GET().build(),
							HttpResponse.BodyHandlers.ofString());
			if (response.statusCode() == 200
							&& response.body().contains("\"" + expectedState + "\"")) {
				return response;
			}
			Thread.sleep(100);
		} while (System.nanoTime() < deadlineNanos);
		fail("Status did not become " + expectedState + "; last response=" + response);
		return response;
	}

	private static String readFile(Path file) {
		try {
			return Files.readString(file, StandardCharsets.UTF_8);
		} catch (final IOException e) {
			return "<unavailable>";
		}
	}

	private static String readTail(Path file) {
		try {
			final var lines = Files.readAllLines(file, StandardCharsets.UTF_8);
			return String.join("\n", lines.subList(Math.max(0, lines.size() - 50), lines.size()));
		} catch (IOException e) {
			return "<unavailable>";
		}
	}
}
