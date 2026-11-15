package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice.Kind;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Entry Node copies the other nodes' bucket versioning notices into its own messages and errors, which the CLI
 * fetches as {@code messages.log} and {@code errors.log}: a two-engine step shows the notice of the engine that
 * enabled versioning, not only the Entry Node's.
 */
@Timeout(60)
class BucketVersioningRelayTest {

	private static final String LOGGER = Loggers.BUCKET_VERSIONING.getName();

	@TempDir
	Path dir;

	/** What the relay logs: a WARN goes to the errors, anything else to the messages. */
	private final List<String> relayed = new CopyOnWriteArrayList<>();

	private BucketVersioningRelay relay(final String step, final List<FileManager> managers, final List<String> nodes,
					final long boundMillis) {
		return new BucketVersioningRelay(step, managers, NicPathsCollector.nodeNames(nodes),
						(level, message) -> relayed.add(level + " " + message), boundMillis);
	}

	/** A node whose file manager reads {@code file} for the step's notices. */
	private static FileManager node(final Path file, final String step) throws Exception {
		final var node = mock(FileManager.class);
		when(node.logFileName(LOGGER, step)).thenReturn(file.toString());
		when(node.fileSize(file.toString())).thenAnswer(inv -> FileManager.INSTANCE.fileSize(file.toString()));
		when(node.readFromFile(eq(file.toString()), anyLong()))
						.thenAnswer(inv -> FileManager.INSTANCE.readFromFile(file.toString(), inv.getArgument(1)));
		return node;
	}

	@Test
	void eachOtherNodesNoticesReachTheEntryNodesMessagesAndErrors() throws Exception {
		final var step = "relay-" + System.nanoTime();
		final var entry = mock(FileManager.class);
		final var file1 = dir.resolve("1").resolve(BucketVersioningNotice.FILE_NAME);
		final var file2 = dir.resolve("2").resolve(BucketVersioningNotice.FILE_NAME);
		final var broken = mock(FileManager.class);
		when(broken.logFileName(LOGGER, step)).thenReturn("/node-3/" + BucketVersioningNotice.FILE_NAME);
		when(broken.fileSize("/node-3/" + BucketVersioningNotice.FILE_NAME)).thenThrow(new IOException("connection reset"));
		final var managers = List.of(entry, node(file1, step), node(file2, step), broken);
		final var relay = relay(step, managers, List.of("node-b:1099", "node-c:1099", "node-d:1099"), 10_000);
		Files.createDirectories(file1.getParent());
		Files.writeString(file1, "CHECK bucket b1\n"
						+ "INFO bucket b1: versioning enabled by this run (the bucket had never been versioned)\n"
						+ "DONE bucket b1\n"
						+ "CHECK bucket b2\n"
						+ "WARN bucket b2: versioning is suspended; SPT left it alone, and the run goes on un-versioned\n"
						+ "DONE bucket b2\n");

		relay.close();

		// Each node is read on a thread of its own, so only each node's own records keep their order.
		assertEquals(List.of(
						"INFO " + step + ": node 001 (node-b:1099): bucket b1: versioning enabled by this run"
										+ " (the bucket had never been versioned)",
						"WARN " + step + ": node 001 (node-b:1099): bucket b2: versioning is suspended; SPT left it"
										+ " alone, and the run goes on un-versioned"),
						relayed.stream().filter(record -> record.contains("node 001")).toList());
		assertEquals(List.of(
						"WARN " + step + ": could not relay the bucket versioning notices of node 003 (node-d:1099):"
										+ " connection reset"),
						relayed.stream().filter(record -> !record.contains("node 001")).toList(),
						"node 002 has no notices and is quiet; the Entry Node's own are not read");
		verifyNoInteractions(entry);
	}

	@Test
	void aSecondPassRelaysOnlyWhatIsNew() throws Exception {
		final var step = "relay-twice-" + System.nanoTime();
		final var file = dir.resolve(BucketVersioningNotice.FILE_NAME);
		final var relay = relay(step, List.of(mock(FileManager.class), node(file, step)), List.of("node-b:1099"), 10_000);
		Files.writeString(file, "INFO bucket b3: versioning is already enabled\nINFO bucket b4: partly wri");

		relay.relayAtStop();
		Files.writeString(file, Files.readString(file) + "tten\n");
		relay.close();

		assertEquals(List.of("INFO " + step + ": node 001 (node-b:1099): bucket b3: versioning is already enabled",
						"INFO " + step + ": node 001 (node-b:1099): bucket b4: partly written"), relayed);
	}

	@Test
	void theStopWaitsForACheckStillRunning() throws Exception {
		final var step = "relay-pending-" + System.nanoTime();
		final var file = Path.of(FileManager.INSTANCE.logFileName(LOGGER, step));
		final var node = node(file, step);
		final var reads = new AtomicInteger();
		final var polled = new CountDownLatch(1);
		when(node.fileSize(file.toString())).thenAnswer(inv -> {
			// The init reads once and the stop's first pass once: a third read is the stop polling the running check.
			if (reads.incrementAndGet() >= 3) {
				polled.countDown();
			}
			return FileManager.INSTANCE.fileSize(file.toString());
		});
		final var relay = relay(step, List.of(mock(FileManager.class), node), List.of("node-b:1099"), 30_000);
		final var check = BucketVersioningNotice.check(step, "bucket b5");
		final var ended = new AtomicBoolean();
		final var worker = Thread.ofPlatform().start(() -> {
			try {
				if (!polled.await(20, TimeUnit.SECONDS)) {
					return;
				}
			} catch (final InterruptedException e) {
				return;
			}
			BucketVersioningNotice.info(step, "bucket b5: versioning enabled by this run");
			check.close();
			ended.set(true);
		});

		relay.relayAtStop();
		final boolean endedBeforeTheStopReturned = ended.get();
		worker.join();

		assertTrue(endedBeforeTheStopReturned, "the stop was waiting when the check ended, and returned after it");
		assertEquals(List.of("INFO " + step + ": node 001 (node-b:1099): bucket b5: versioning enabled by this run"), relayed,
						"the check's notice, and no WARN that it had not ended");
	}

	@Test
	void pastTheCapTheStopDoesNotWaitForChecksItCannotFollow() throws Exception {
		final var step = "relay-capped-" + System.nanoTime();
		final var file = dir.resolve(BucketVersioningNotice.FILE_NAME);
		final var relay = relay(step, List.of(mock(FileManager.class), node(file, step)), List.of("node-b:1099"), 30_000);
		final var line = "INFO bucket b: " + "x".repeat(1000) + "\n";
		// A check opens, and its DONE lies past the cap.
		Files.writeString(file, "CHECK bucket bc\n" + line.repeat(BucketVersioningRelay.MAX_BYTES / line.length() + 1)
						+ "DONE bucket bc\n");

		final long start = System.nanoTime();
		relay.relayAtStop();
		final long millis = (System.nanoTime() - start) / 1_000_000;

		assertTrue(millis < 20_000, "the stop did not wait out the bound: " + millis + " ms");
		assertEquals(List.of("WARN " + step + ": node 001 (node-b:1099): its bucket versioning notices pass the 1 MiB the"
						+ " relay reads, so its running checks can't be followed; the rest are only in that node's"
						+ " bucket.versioning.log"), relayed.stream().filter(record -> record.startsWith("WARN")).toList(),
						"the cap warning, and no false WARN that the check had not ended");
	}

	@Test
	void aNodeThatStallsIsLeftOutWithinTheBound() throws Exception {
		final var step = "relay-stall-" + System.nanoTime();
		final var file = dir.resolve(BucketVersioningNotice.FILE_NAME);
		final var stalled = node(file, step);
		final var quiet = node(dir.resolve("q").resolve(BucketVersioningNotice.FILE_NAME), step);
		final var release = new CountDownLatch(1);
		final var reads = new AtomicInteger();
		when(stalled.fileSize(file.toString())).thenAnswer(inv -> {
			if (reads.incrementAndGet() > 1) {
				release.await(30, TimeUnit.SECONDS);
				// Read after the relay gave up on the node: it must be dropped.
				Files.writeString(file, "WARN bucket late: read after the bound\n");
			}
			return FileManager.INSTANCE.fileSize(file.toString());
		});
		final var relay = relay(step, List.of(mock(FileManager.class), stalled, quiet), List.of("node-b:1099", "node-c:1099"),
						500);
		try {
			final long start = System.nanoTime();
			relay.relayAtStop();
			final long stopMillis = (System.nanoTime() - start) / 1_000_000;
			relay.close();

			assertTrue(stopMillis < 5_000, "the stop returned at its bound: " + stopMillis + " ms");
			assertEquals(List.of("WARN " + step + ": node 001 (node-b:1099) did not answer within 500 ms when the step"
							+ " stopped, so the relay left it out; its bucket versioning notices are only in that node's"
							+ " bucket.versioning.log"), relayed, "one WARN, and the close leaves the node out");
		} finally {
			release.countDown();
		}
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().contains(step))
						&& System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertFalse(Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().contains(step)),
						"the abandoned thread ended once released");
		assertEquals(1, relayed.size(), "its late read was dropped: " + relayed);
	}

	@Test
	void aCheckThatOutlivesTheWaitIsNamedOnce() throws Exception {
		final var step = "relay-stuck-" + System.nanoTime();
		final var file = Path.of(FileManager.INSTANCE.logFileName(LOGGER, step));
		final var relay = relay(step, List.of(mock(FileManager.class), node(file, step)), List.of("node-b:1099"), 200);
		BucketVersioningNotice.check(step, "bucket b6");

		relay.relayAtStop();
		relay.close();

		assertEquals(List.of("WARN " + step + ": node 001 (node-b:1099): the versioning check of bucket b6 had not ended"
						+ " when the step stopped; its notice may be only in that node's bucket.versioning.log"), relayed);
	}

	@Test
	void aNoticeIsInTheFileWhenItReturns() throws Exception {
		final var step = "notice-sync-" + System.nanoTime();
		final var file = Path.of(FileManager.INSTANCE.logFileName(LOGGER, step));

		BucketVersioningNotice.warn(step, "bucket b7: could not enable versioning");

		assertEquals(List.of("WARN bucket b7: could not enable versioning"), Files.readAllLines(file),
						"written before the driver goes on, not by the async logger");
	}

	@Test
	void aNoticeWithALineBreakIsOneLine() throws Exception {
		final var step = "notice-forge-" + System.nanoTime();
		final var file = Path.of(FileManager.INSTANCE.logFileName(LOGGER, step));

		BucketVersioningNotice.warn(step, "bucket b8: the versioning status \"X\nINFO bucket b9: versioning is already"
						+ " enabled\" is neither Enabled nor Suspended");

		assertEquals(List.of("WARN bucket b8: the versioning status \"X INFO bucket b9: versioning is already enabled\" is"
						+ " neither Enabled nor Suspended"), Files.readAllLines(file));
	}

	@Test
	void theLoggerNamesTheStepsBucketVersioningLog() throws Exception {
		final var name = FileManager.INSTANCE.logFileName(LOGGER, "some-step");
		assertTrue(name.endsWith("some-step" + java.io.File.separator + BucketVersioningNotice.FILE_NAME), name);
	}

	@Test
	void aLineIsReadBackAsItsKindAndText() {
		assertEquals(new BucketVersioningNotice.Line(Kind.WARN, "bucket b: x y"),
						BucketVersioningNotice.Line.parse("WARN bucket b: x y\r"));
		assertEquals(new BucketVersioningNotice.Line(Kind.INFO, "bucket b: x"),
						BucketVersioningNotice.Line.parse("INFO bucket b: x"));
		assertEquals(new BucketVersioningNotice.Line(Kind.CHECK, "bucket b"),
						BucketVersioningNotice.Line.parse("CHECK bucket b"));
		assertEquals(org.apache.logging.log4j.Level.WARN, BucketVersioningNotice.Line.parse("WARN b").level());
		assertEquals(org.apache.logging.log4j.Level.INFO, BucketVersioningNotice.Line.parse("DONE b").level());
		assertNull(BucketVersioningNotice.Line.parse("ERROR bucket b: x"), "an unknown kind is skipped");
		assertNull(BucketVersioningNotice.Line.parse(""));
		assertNull(BucketVersioningNotice.Line.parse("WARN"));
		assertNull(BucketVersioningNotice.Line.parse("WARN "));
	}

	@Test
	void theRelayRunsOnlyWhenTheStepUsesVersioning() {
		assertFalse(BucketVersioningRelay.versioningRequested(versioning(false), null));
		assertTrue(BucketVersioningRelay.versioningRequested(versioning(true), null));
		assertTrue(BucketVersioningRelay.versioningRequested(versioning(false), List.of(versioning(false), versioning(true))));
		assertFalse(BucketVersioningRelay.versioningRequested(versioning(false), List.of(versioning(false))));
		assertFalse(BucketVersioningRelay.versioningRequested(new BasicConfig("-", Map.of()), null),
						"a step without the S3 schema has no versioning");
	}

	@Test
	void aDistributedStepWithVersioningRelaysAndOthersDoNot() throws Exception {
		assertTrue(relayAfterInit(stepConfig(true), 2), "two engines, versioning on");
		assertFalse(relayAfterInit(stepConfig(false), 2), "two engines, versioning off: nothing changes");
		assertFalse(relayAfterInit(stepConfig(true), 1), "one engine: its driver logs its notices itself");
	}

	/** Whether the client holds a relay after its init with {@code engines} file managers. */
	private static boolean relayAfterInit(final Config config, final int engines) throws Exception {
		final var client = new TestLoadStepClient(config, Collections.emptyList(), Collections.emptyList(),
						mock(MetricsManager.class));
		final var fileMgrs = LoadStepClientBase.class.getDeclaredField("fileMgrs");
		fileMgrs.setAccessible(true);
		@SuppressWarnings("unchecked")
		final var managers = (List<FileManager>) fileMgrs.get(client);
		for (int i = 0; i < engines; i++) {
			managers.add(mock(FileManager.class));
		}
		final var init = LoadStepClientBase.class.getDeclaredMethod("initBucketVersioningRelay", List.class);
		init.setAccessible(true);
		init.invoke(client, Collections.nCopies(engines - 1, "node-b:1099"));
		final var relay = LoadStepClientBase.class.getDeclaredField("bucketVersioningRelay");
		relay.setAccessible(true);
		return relay.get(client) != null;
	}

	private static Config stepConfig(final boolean versioning) {
		final var base = TestConfigBuilder.config();
		final Map<String, Object> keys = Map.of("storage", Map.of("object", Map.of("versioning", Boolean.class)));
		final var schema = TreeUtil.reduceForest(List.of(TreeUtil.copyTree(base.schema()), keys));
		final Config config = new BasicConfig(base.pathSep(), schema, Config.deepToMap(base));
		config.val(HttpVersioningSynonym.TARGET_PATH, versioning);
		return config;
	}

	private static Config versioning(final boolean on) {
		final Config config = new BasicConfig("-", Map.of("storage", Map.of("object", Map.of("versioning", Boolean.class))));
		config.val(HttpVersioningSynonym.TARGET_PATH, on);
		return config;
	}
}
