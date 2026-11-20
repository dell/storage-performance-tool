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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Two packaged engines on the loopback interface: a {@code --run-node} worker, and an Entry Node
 * running a distributed dummy-mock step from the command line. The worker gets SIGTERM or SIGKILL
 * mid-step. The Entry Node must stop the step at once and exit 1 with the lost-slice failure, for a
 * step with no limit (which used to run forever) and for one with a 60 s time limit (which used to
 * run to its limit and fail on the worker's missing files).
 */
@DisplayName("E2E: a distributed step whose worker is signalled mid-step fails at once")
@IntegrationTest
class NodeJarLostWorkerE2ETest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	/** The ticket's bound from the signal to the Entry Node's exit. */
	private static final Duration EXIT_AFTER_SIGNAL = Duration.ofSeconds(10);
	private static final String LOST_FAILURE = "engine 2 of 2: the entry lost this node's step slice";

	private final List<Process> nodes = new ArrayList<>();
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

	@AfterEach
	void stopNodes() throws InterruptedException {
		for (final Process node : nodes) {
			if (node.isAlive()) {
				node.destroyForcibly();
				node.waitFor(10, TimeUnit.SECONDS);
			}
		}
	}

	@ParameterizedTest(name = "{0}, time limit {1}")
	@CsvSource({"TERM, 0", "KILL, 0", "TERM, 60s", "KILL, 60s"
	})
	void theEntryFailsTheStepAtOnceNamingTheLostEngine(
					final String signal, final String timeLimit, @TempDir final Path dir) throws Exception {
		final Path jar = packagedJar();
		final int workerApi = freePort();
		final int workerRmi = freePort();
		final Path workerDir = Files.createDirectories(dir.resolve("worker"));
		final Path entryDir = Files.createDirectories(dir.resolve("entry"));
		final Process worker = start(jar, workerDir, List.of(
						"--run-node=true",
						"--run-port=" + workerApi,
						"--load-step-node-port=" + workerRmi));
		awaitReady("http://127.0.0.1:" + workerApi, workerDir);

		final String stepId = "lost-worker-" + signal.toLowerCase(Locale.ROOT) + "-" + System.nanoTime();
		final Process entry = start(jar, entryDir, List.of(
						"--load-step-node-addrs=127.0.0.1:" + workerRmi,
						"--load-step-id=" + stepId,
						"--load-step-limit-time=" + timeLimit,
						"--load-op-type=create",
						"--load-op-limit-rate=20",
						"--item-data-size=1KB",
						"--storage-driver-type=dummy-mock",
						"--output-metrics-average-period=1s"));
		// Mid-step: the step has reported its metrics, so both slices have started.
		awaitFile(entryDir.resolve("log").resolve(stepId).resolve("metrics.csv"), Duration.ofSeconds(60), entryDir);
		Thread.sleep(3_000);
		assertTrue(entry.isAlive(), "the step is still running: " + tail(entryDir.resolve("stdout.log")));

		final long signalled = System.nanoTime();
		if ("KILL".equals(signal)) {
			worker.destroyForcibly();
		} else {
			worker.destroy();
		}
		if (!entry.waitFor(EXIT_AFTER_SIGNAL.toMillis(), TimeUnit.MILLISECONDS)) {
			fail("the Entry Node did not exit within " + EXIT_AFTER_SIGNAL + " of the worker's SIG" + signal + ": "
							+ tail(entryDir.resolve("stdout.log")));
		}
		final long tookMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - signalled);

		final String output = read(entryDir.resolve("stdout.log"));
		assertEquals(1, entry.exitValue(), output);
		assertTrue(output.contains(LOST_FAILURE), "the lost engine is the step's failure: " + tail(entryDir.resolve("stdout.log")));
		assertFalse(output.lines().anyMatch(line -> line.contains("failed to aggregate") && !line.contains("CAUSE")
						&& !line.contains("Suppressed")),
						"the close's failure is not the step's: " + tail(entryDir.resolve("stdout.log")));
		System.out.println("SIG" + signal + ", time limit " + timeLimit + ": the Entry Node exited 1 after "
						+ tookMillis + " ms");
	}

	private Process start(final Path jar, final Path dir, final List<String> args) throws IOException {
		final List<String> command = new ArrayList<>(List.of(
						Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
						"-Xmx1g",
						"-Djava.rmi.server.hostname=127.0.0.1",
						"-Dsun.rmi.transport.tcp.responseTimeout=10000",
						"-jar",
						jar.toString()));
		command.addAll(args);
		final ProcessBuilder pb = new ProcessBuilder(command).directory(dir.toFile());
		pb.environment().put("SPT_LOG_DIR", dir.toString());
		pb.environment().put("SPT_HOME", dir.resolve("home").toString());
		pb.redirectErrorStream(true);
		pb.redirectOutput(dir.resolve("stdout.log").toFile());
		final Process node = pb.start();
		nodes.add(node);
		return node;
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
				fail(file + " did not appear: " + tail(nodeDir.resolve("stdout.log")));
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
