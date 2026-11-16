package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.testing.tags.IntegrationTest;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Two packaged engines on the loopback interface, an Entry Node and a worker, run a distributed
 * dummy-mock step that is stopped mid-step by {@code POST /shutdown} or {@code DELETE /run}. The
 * Entry Node must still publish the step's totals and close the step in the run: the merged
 * operation lifecycle and the worker's node files are fetched, and the worker's step service is
 * closed before the Entry Node exits.
 */
@DisplayName("E2E: an interrupted distributed run keeps its totals and closes its step")
@IntegrationTest
class NodeJarInterruptedDistributedRunE2ETest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	/** Below the Entry Node's linger, so a close left to its JVM exit misses it. */
	private static final Duration CLOSE_IN_RUN_TIMEOUT = Duration.ofSeconds(8);
	private static final int LINGER_SECONDS = 15;
	private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(40);

	private final List<Process> nodes = new ArrayList<>();
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

	@AfterEach
	void stopNodes() throws InterruptedException {
		for (final Process node : nodes) {
			if (node.isAlive()) {
				node.destroy();
				if (!node.waitFor(5, TimeUnit.SECONDS)) {
					node.destroyForcibly();
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"shutdown", "delete"
	})
	void interruptedDistributedRunPublishesItsTotalsAndClosesItsStep(final String stop, @TempDir final Path dir)
					throws Exception {
		final Path jar = packagedJar();
		final int entryApi = freePort();
		final int entryRmi = freePort();
		final int workerApi = freePort();
		final int workerRmi = freePort();
		final Path workerDir = Files.createDirectories(dir.resolve("worker"));
		final Path entryDir = Files.createDirectories(dir.resolve("entry"));
		final Process worker = startNode(jar, workerDir, workerApi, workerRmi, null);
		final Process entry = startNode(jar, entryDir, entryApi, entryRmi, "127.0.0.1:" + workerRmi);
		final String entryBase = "http://127.0.0.1:" + entryApi;
		awaitReady(entryBase, entryDir);
		awaitReady("http://127.0.0.1:" + workerApi, workerDir);

		final String stepId = "interrupted-distributed-" + stop + "-" + System.nanoTime();
		final HttpResponse<String> run = postScenario(entryBase, scenario(stepId));
		assertEquals(202, run.statusCode(), run.body());
		final String runId = run.headers().firstValue("ETag").orElseThrow();
		final Path stepDir = entryDir.resolve("log").resolve(stepId);
		// Mid-step: the worker slice has reported periodic metrics, and the 60 s limit is far off.
		awaitFile(stepDir.resolve("metrics.csv"), Duration.ofSeconds(30), entryDir);
		Thread.sleep(3_000);

		final var stopRequest = "shutdown".equals(stop)
						? HttpRequest.newBuilder(URI.create(entryBase + "/shutdown")).POST(HttpRequest.BodyPublishers.noBody())
						: HttpRequest.newBuilder(URI.create(entryBase + "/run")).header("If-Match", runId).DELETE();
		final int stopStatus = http.send(stopRequest.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
		assertEquals("shutdown".equals(stop) ? 202 : 200, stopStatus);
		// The Entry Node writes its own lifecycle file during the step; only the step's close
		// fetches the worker's. It must do so in the run, while the Entry Node still lingers, and
		// not at JVM exit, when a worker shut down meanwhile no longer answers.
		awaitFile(stepDir.resolve("operation.lifecycle.node-001.csv"), CLOSE_IN_RUN_TIMEOUT, entryDir);
		assertTrue(entry.isAlive(), "the step must close before the Entry Node exits");
		// Let the close finish publishing the merged file after the node source.
		Thread.sleep(2_000);
		if ("shutdown".equals(stop) && !entry.waitFor(EXIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
			fail("the Entry Node did not exit after /shutdown: " + tail(entryDir.resolve("stdout.log")));
		}

		for (final String name : List.of(
						"metrics.total.csv",
						"metrics.steady.total.csv",
						"result.xml",
						"operation.lifecycle.csv",
						"operation.lifecycle.node-000.csv",
						"operation.lifecycle.node-001.csv",
						"event.loops.node-001.json")) {
			assertTrue(Files.isRegularFile(stepDir.resolve(name)), name + " is missing; step files: " + listing(stepDir));
		}
		final String entryOutput = read(entryDir.resolve("stdout.log"));
		assertFalse(entryOutput.contains("failed to close the step service"), entryOutput);
		assertFalse(entryOutput.contains("no event loops file from node"), entryOutput);
		assertTrue(worker.isAlive(), "the worker must outlive the Entry Node's stop");
	}

	private Process startNode(
					final Path jar, final Path dir, final int apiPort, final int rmiPort, final String workerAddr)
					throws IOException {
		final List<String> command = new ArrayList<>(List.of(
						Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
						"-Xmx1g",
						"-Djava.rmi.server.hostname=127.0.0.1",
						"-Dsun.rmi.transport.tcp.responseTimeout=10000",
						"-jar",
						jar.toString()));
		if (workerAddr != null) {
			command.add("--load-step-node-addrs=" + workerAddr);
		}
		command.addAll(List.of(
						"--run-node=true",
						"--run-port=" + apiPort,
						"--load-step-node-port=" + rmiPort,
						"--api-linger-sec=" + LINGER_SECONDS));
		final ProcessBuilder pb = new ProcessBuilder(command).directory(dir.toFile());
		pb.environment().put("SPT_LOG_DIR", dir.toString());
		pb.environment().put("SPT_HOME", dir.resolve("home").toString());
		pb.redirectErrorStream(true);
		pb.redirectOutput(dir.resolve("stdout.log").toFile());
		final Process node = pb.start();
		nodes.add(node);
		return node;
	}

	private static String scenario(final String stepId) {
		return "Load.config({\n"
						+ "  \"storage\": {\"driver\": {\"type\": \"dummy-mock\", \"limit\": {\"concurrency\": 2}}},\n"
						+ "  \"item\": {\"data\": {\"size\": \"1KB\"}},\n"
						+ "  \"load\": {\"op\": {\"type\": \"create\"}, \"step\": {\"limit\": {\"time\": \"60s\"}, \"id\": \""
						+ stepId + "\"}}\n"
						+ "}).run();\n";
	}

	private HttpResponse<String> postScenario(final String base, final String scenario) throws Exception {
		final String boundary = "----spt-e2e-" + System.nanoTime();
		final String body = "--" + boundary + "\r\n"
						+ "Content-Disposition: form-data; name=\"scenario\"; filename=\"scenario.js\"\r\n"
						+ "Content-Type: application/javascript\r\n\r\n"
						+ scenario + "\r\n"
						+ "--" + boundary + "--\r\n";
		return http.send(
						HttpRequest.newBuilder(URI.create(base + "/run"))
										.header("Content-Type", "multipart/form-data; boundary=" + boundary)
										.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
										.build(),
						HttpResponse.BodyHandlers.ofString());
	}

	private void awaitReady(final String base, final Path dir) throws Exception {
		final long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (System.nanoTime() < deadline) {
			try {
				if (http.send(HttpRequest.newBuilder(URI.create(base + "/ready")).GET().build(),
								HttpResponse.BodyHandlers.discarding()).statusCode() == 200) {
					return;
				}
			} catch (final IOException notYet) {
				// The node is still starting.
			}
			Thread.sleep(250);
		}
		fail("node " + base + " was not ready: " + tail(dir.resolve("stdout.log")));
	}

	private static void awaitFile(final Path file, final Duration timeout, final Path nodeDir) throws Exception {
		final long deadline = System.nanoTime() + timeout.toNanos();
		while (!Files.isRegularFile(file)) {
			if (System.nanoTime() > deadline) {
				fail(file + " did not appear; step files: " + listing(file.getParent()) + "; "
								+ tail(nodeDir.resolve("stdout.log")));
			}
			Thread.sleep(200);
		}
	}

	private static Path packagedJar() {
		Path p = Paths.get("").toAbsolutePath();
		for (int i = 0; i < 12 && p != null; i++, p = p.getParent()) {
			for (final Path root : List.of(p, p.resolve("engine"))) {
				final Path jar = root.resolve("bundle/build/dist/spt.jar");
				if (Files.isRegularFile(root.resolve("bundle/build.gradle")) && Files.isRegularFile(jar)) {
					return jar;
				}
			}
		}
		Assumptions.abort("packaged spt.jar not found; run :bundle:assembleDist first");
		return null;
	}

	private static int freePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static String listing(final Path dir) {
		if (!Files.isDirectory(dir)) {
			return "<no directory " + dir + ">";
		}
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(f -> f.getFileName().toString()).sorted().toList().toString();
		} catch (final IOException e) {
			return "<unreadable: " + e + ">";
		}
	}

	private static String read(final Path file) {
		try {
			return Files.readString(file, StandardCharsets.UTF_8);
		} catch (final IOException e) {
			return "<unavailable>";
		}
	}

	private static String tail(final Path file) {
		final String text = read(file);
		return text.substring(Math.max(0, text.length() - 4_000));
	}
}
