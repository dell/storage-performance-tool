package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.testing.tags.IntegrationTest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
		for (int i = 0; i < 4 && p != null; i++, p = p.getParent()) {
			if (Files.exists(p.resolve("gradlew")) && Files.isRegularFile(p.resolve("gradlew"))) {
				return p;
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
				org.junit.jupiter.api.Assumptions.abort("Could not build bundle for E2E test; skipping.");
			}
			assertTrue(Files.exists(jar), "Bundle jar not found after build");
		}

		final int port = 20000 + ThreadLocalRandom.current().nextInt(10000);
		final var outFile = Files.createTempFile("spt-node-out", ".log");
		final var errFile = Files.createTempFile("spt-node-err", ".log");

		ProcessBuilder nodePb = new ProcessBuilder(
						"java", "-jar", jar.toString(), "--run-node", "--run-port=" + port, "--api-linger-sec=2");
		nodePb.redirectOutput(outFile.toFile());
		nodePb.redirectError(errFile.toFile());
		Process node = nodePb.start();

		HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
		final String base = "http://127.0.0.1:" + port;

		// Wait readiness up to 30s
		boolean ready = false;
		for (int i = 0; i < 30; i++) {
			try {
				int code = client.send(HttpRequest.newBuilder(URI.create(base + "/metrics")).GET().build(),
								HttpResponse.BodyHandlers.discarding()).statusCode();
				if (code == 200) {
					ready = true;
					break;
				}
			} catch (IOException ignored) {}
			Thread.sleep(1000);
		}
		assertTrue(ready, "Node did not become ready in time; stdout=" + readTail(outFile) + ", stderr=" + readTail(errFile));

		// Basic /status should be 200
		var statusResp = client.send(HttpRequest.newBuilder(URI.create(base + "/status")).GET().build(), HttpResponse.BodyHandlers.ofString());
		assertEquals(200, statusResp.statusCode());

		// Request shutdown
		var shutResp = client.send(HttpRequest.newBuilder(URI.create(base + "/shutdown")).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
		assertEquals(202, shutResp.statusCode());

		// During linger, status may still be 200
		try {
			Thread.sleep(500);
		} catch (InterruptedException ignored) {}
		statusResp = client.send(HttpRequest.newBuilder(URI.create(base + "/status")).GET().build(), HttpResponse.BodyHandlers.ofString());
		assertEquals(200, statusResp.statusCode());

		// Wait for process to exit within linger+grace period
		boolean exited = node.waitFor(8, java.util.concurrent.TimeUnit.SECONDS);
		if (!exited) {
			node.destroyForcibly();
			fail("Node did not exit after shutdown; stdout=" + readTail(outFile) + ", stderr=" + readTail(errFile));
		}
	}

	private static String readTail(Path file) {
		try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(file)))) {
			StringBuilder sb = new StringBuilder();
			String line;
			int count = 0;
			while ((line = br.readLine()) != null && count < 50) {
				sb.append(line).append('\n');
				count++;
			}
			return sb.toString();
		} catch (IOException e) {
			return "<unavailable>";
		}
	}
}
