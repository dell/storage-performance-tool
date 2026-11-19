package com.dell.spt.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Packaged {@code --run-node} engines on the loopback interface, {@code fs} driver: what a worker's
 * {@code /run} and {@code /shutdown} do about the step slices it holds, after the slice's step ended
 * and after its entry was killed.
 */
@DisplayName("E2E: a worker's /run and /shutdown follow its step slices")
@IntegrationTest
class WorkerSliceLifecycleE2ETest {

	private static final Duration WAIT = Duration.ofSeconds(60);
	private static final int LINGER_SECONDS = 1;

	private final List<Process> nodes = new ArrayList<>();
	private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();

	@TempDir
	Path dir;

	private record Node(String name, Process process, String api, int rmi, Path home) {}

	@AfterEach
	void stopNodes() throws InterruptedException {
		for (final Process node : nodes) {
			if (node.isAlive()) {
				node.destroyForcibly();
				node.waitFor(10, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void shutdownEndsAnOrphanThatALaterStepRanBeside() throws Exception {
		final Node worker = startNode("worker");
		final Node deadEntry = startNode("dead-entry");
		final Node entry = startNode("entry");
		final Path orphanData = dir.resolve("orphan-data");
		final long orphanRun = orphanSliceOn(worker, deadEntry, orphanData);

		final Path laterData = dir.resolve("later-data");
		assertEquals(202, post(entry, 2002L, "later-step", worker, laterData, "3s"));
		awaitStatus(entry, "COMPLETED");
		assertTrue(fileCount(laterData) > 0, "the later step ran on the worker");
		final int before = fileCount(orphanData);
		await(() -> fileCount(orphanData) > before, "the orphan keeps writing beside the later step", worker);
		assertEquals(String.valueOf(orphanRun), etag(worker), "the finished later step is not busy, the orphan is");
		assertEquals(404, delete(worker, orphanRun + 1));

		assertEquals(202, send(request(worker, "/shutdown").POST(HttpRequest.BodyPublishers.noBody())));

		awaitExit(worker);
		final int atExit = fileCount(orphanData);
		Thread.sleep(1_000);
		assertEquals(atExit, fileCount(orphanData), "the orphan stopped writing");
	}

	@Test
	void deleteStopsAnOrphanAndTheWorkerTakesARunAgain() throws Exception {
		final Node worker = startNode("worker");
		final Node deadEntry = startNode("dead-entry");
		final Path orphanData = dir.resolve("orphan-data");
		final long orphanRun = orphanSliceOn(worker, deadEntry, orphanData);
		assertEquals(200, get(worker, orphanRun));
		assertEquals(204, get(worker, orphanRun + 1));
		assertEquals(409, post(worker, 3003L, "refused-step", null, dir.resolve("refused-data"), "1s"));
		assertTrue(read(worker, "/status").contains("\"IDLE\""), "a refused run leaves /status alone");

		assertEquals(404, delete(worker, orphanRun + 1));
		assertEquals(200, delete(worker, orphanRun));

		await(() -> headStatus(worker) == 204, "HEAD /run answers 204 after the stop", worker);
		final int stopped = fileCount(orphanData);
		Thread.sleep(1_500);
		assertEquals(stopped, fileCount(orphanData), "the orphan stopped writing");
		assertEquals(200, delete(worker, orphanRun), "the stopped slice is still open and still matches");
		assertEquals(1, countLines(worker, "stopped this node's share of run " + orphanRun), "one warning per stop");
		assertTrue(worker.process().isAlive());

		assertEquals(202, post(worker, 3004L, "after-stop", null, dir.resolve("after-stop-data"), "1s"));
		awaitStatus(worker, "COMPLETED");
		assertEquals(202, send(request(worker, "/shutdown").POST(HttpRequest.BodyPublishers.noBody())));
		awaitExit(worker);
	}

	@Test
	void aFormerWorkerIsIdleAndServesAsAnEntry() throws Exception {
		final Node nodeA = startNode("a");
		final Node nodeB = startNode("b");
		assertEquals(202, post(nodeB, 4001L, "first-step", nodeA, dir.resolve("first-data"), "3s"));
		await(() -> headStatus(nodeA) == 200, "the worker reports its slice", nodeA);
		assertEquals("4001", etag(nodeA));
		assertEquals(409, post(nodeA, 4999L, "refused-step", null, dir.resolve("refused-data"), "1s"));
		awaitStatus(nodeB, "COMPLETED");

		await(() -> headStatus(nodeA) == 204, "HEAD /run answers 204 after the step", nodeA);
		assertEquals(204, get(nodeA, 4001L));
		assertTrue(read(nodeA, "/status").contains("\"IDLE\""), "a worker's /status stays IDLE");

		assertEquals(202, post(nodeA, 4002L, "standalone-step", null, dir.resolve("standalone-data"), "1s"));
		awaitStatus(nodeA, "COMPLETED");
		assertTrue(read(nodeA, "/status").contains("4002"));
		assertEntryArtifacts(nodeA, "standalone-step");

		assertEquals(202, post(nodeA, 4003L, "swapped-step", nodeB, dir.resolve("swapped-data"), "2s"));
		await(() -> read(nodeA, "/status").contains("4003") && read(nodeA, "/status").contains("COMPLETED"),
						"the swapped run completes", nodeA);
		assertEntryArtifacts(nodeA, "swapped-step");
		await(() -> headStatus(nodeB) == 204, "the former entry is idle after serving a slice", nodeB);
	}

	private static void assertEntryArtifacts(final Node entry, final String stepId) throws IOException {
		final Path total = entry.home().resolve("log").resolve(stepId).resolve("metrics.total.csv");
		assertTrue(Files.isRegularFile(total) && Files.size(total) > 0, "no " + total);
	}

	/** Starts a long step whose slice is on the worker, kills its entry and returns the run id. */
	private long orphanSliceOn(final Node worker, final Node entry, final Path data) throws Exception {
		final long runId = 1001L;
		assertEquals(202, post(entry, runId, "orphan-step", worker, data, "300s"));
		await(() -> headStatus(worker) == 200, "the slice runs on the worker", worker);
		await(() -> fileCount(data) > 5, "the step writes", worker);
		entry.process().destroyForcibly();
		assertTrue(entry.process().waitFor(10, TimeUnit.SECONDS));
		final int atKill = fileCount(data);
		await(() -> fileCount(data) > atKill + 5, "the orphan keeps writing", worker);
		assertEquals(String.valueOf(runId), etag(worker));
		return runId;
	}

	private Node startNode(final String name) throws Exception {
		final Path home = Files.createDirectories(dir.resolve(name));
		final int[] ports = FreePorts.freePorts(2);
		final List<String> command = List.of(
						Paths.get(System.getProperty("java.home"), "bin", "java").toString(),
						"-Xmx512m",
						"-Djava.rmi.server.hostname=127.0.0.1",
						"-jar",
						packagedJar().toString(),
						"--run-node=true",
						"--run-port=" + ports[0],
						"--load-step-node-port=" + ports[1],
						"--api-linger-sec=" + LINGER_SECONDS);
		final ProcessBuilder builder = new ProcessBuilder(command).directory(home.toFile());
		builder.environment().put("SPT_LOG_DIR", home.toString());
		builder.environment().put("SPT_HOME", home.resolve("home").toString());
		builder.redirectErrorStream(true);
		builder.redirectOutput(home.resolve("stdout.log").toFile());
		final Process process = builder.start();
		nodes.add(process);
		final Node node = new Node(name, process, "http://127.0.0.1:" + ports[0], ports[1], home);
		await(() -> statusOf(request(node, "/ready").GET()) == 200, "the node is ready", node);
		return node;
	}

	private int post(
					final Node node, final long runId, final String stepId, final Node worker, final Path data, final String time)
					throws Exception {
		Files.createDirectories(data);
		final String addrs = worker == null ? "" : "\n    node:\n      addrs: [\"127.0.0.1:" + worker.rmi() + "\"]";
		final String defaults = "run:\n  id: " + runId + "\n"
						+ "load:\n  op:\n    limit:\n      rate: 50\n  step:\n    id: " + stepId + addrs
						+ "\n    limit:\n      time: " + time + "\n"
						+ "storage:\n  driver:\n    type: fs\n"
						+ "item:\n  data:\n    size: 1KB\n  output:\n    path: " + data + "\n";
		final String boundary = "----spt-e2e-" + System.nanoTime();
		final String body = part(boundary, "defaults", "application/yaml", defaults)
						+ part(boundary, "scenario", "application/javascript", "Load.run();\n")
						+ "--" + boundary + "--\r\n";
		return send(request(node, "/run")
						.header("Content-Type", "multipart/form-data; boundary=" + boundary)
						.POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)));
	}

	private static String part(final String boundary, final String name, final String type, final String content) {
		return "--" + boundary + "\r\nContent-Disposition: form-data; name=\"" + name + "\"; filename=\"" + name
						+ "\"\r\nContent-Type: " + type + "\r\n\r\n" + content + "\r\n";
	}

	private static HttpRequest.Builder request(final Node node, final String path) {
		return HttpRequest.newBuilder(URI.create(node.api() + path)).timeout(Duration.ofSeconds(30));
	}

	private int send(final HttpRequest.Builder request) throws Exception {
		return http.send(request.build(), HttpResponse.BodyHandlers.discarding()).statusCode();
	}

	/** The status, or -1 while the node does not answer yet. */
	private int statusOf(final HttpRequest.Builder request) {
		try {
			return send(request);
		} catch (final IOException notYet) {
			return -1;
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private int headStatus(final Node node) {
		return statusOf(request(node, "/run").method("HEAD", HttpRequest.BodyPublishers.noBody()));
	}

	private String etag(final Node node) throws Exception {
		return http.send(
						request(node, "/run").method("HEAD", HttpRequest.BodyPublishers.noBody()).build(),
						HttpResponse.BodyHandlers.discarding())
						.headers().firstValue("ETag").orElse("");
	}

	private int get(final Node node, final long runId) throws Exception {
		return send(request(node, "/run").header("If-Match", String.valueOf(runId)).GET());
	}

	private int delete(final Node node, final long runId) throws Exception {
		return send(request(node, "/run").header("If-Match", String.valueOf(runId)).DELETE());
	}

	private String read(final Node node, final String path) {
		try {
			return http.send(request(node, path).GET().build(), HttpResponse.BodyHandlers.ofString()).body();
		} catch (final IOException e) {
			return "";
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	private void awaitStatus(final Node node, final String state) throws Exception {
		await(() -> read(node, "/status").contains('"' + state + '"'), "/status " + state, node);
	}

	private void awaitExit(final Node node) throws Exception {
		if (!node.process().waitFor(WAIT.toSeconds(), TimeUnit.SECONDS)) {
			fail("the node did not exit after /shutdown: " + tail(node));
		}
	}

	private static void await(final BooleanSupplier condition, final String what, final Node node) throws Exception {
		final long deadline = System.nanoTime() + WAIT.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				fail("timed out waiting: " + what + "; " + tail(node));
			}
			Thread.sleep(100);
		}
	}

	private static int fileCount(final Path data) {
		if (!Files.isDirectory(data)) {
			return 0;
		}
		try (Stream<Path> files = Files.list(data)) {
			return (int) files.count();
		} catch (final IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static long countLines(final Node node, final String text) {
		return log(node).lines().filter(line -> line.contains(text)).count();
	}

	private static String log(final Node node) {
		try {
			return Files.readString(node.home().resolve("stdout.log"), StandardCharsets.UTF_8);
		} catch (final IOException e) {
			return "";
		}
	}

	private static String tail(final Node node) {
		final String text = log(node);
		return node.name() + " log: " + text.substring(Math.max(0, text.length() - 4_000));
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
}
