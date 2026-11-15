package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The Entry Node copies the other nodes' bucket versioning notices into its own messages and errors, which the CLI
 * fetches as {@code messages.log} and {@code errors.log}: a two-engine step shows the notice of the engine that
 * enabled versioning, not only the Entry Node's.
 */
class BucketVersioningRelayTest {

	private static final String LOGGER = Loggers.BUCKET_VERSIONING.getName();

	@TempDir
	Path dir;

	/** What the relay logs: a WARN goes to the errors, anything else to the messages. */
	private final List<String> relayed = new CopyOnWriteArrayList<>();

	private BucketVersioningRelay relay(final String step, final List<FileManager> managers, final List<String> nodes) {
		return new BucketVersioningRelay(step, managers, NicPathsCollector.nodeNames(nodes),
						(level, message) -> relayed.add(level + " " + message));
	}

	@Test
	void eachOtherNodesNoticesReachTheEntryNodesMessagesAndErrors() throws Exception {
		final var step = "relay-" + System.nanoTime();
		final var managers = new ArrayList<FileManager>();
		for (int i = 0; i < 4; i++) {
			final var manager = mock(FileManager.class);
			when(manager.logFileName(LOGGER, step)).thenReturn("/node-" + i + "/bucket.versioning.log");
			managers.add(manager);
		}
		final byte[] node1 = ("INFO bucket b1: versioning enabled by this run (the bucket had never been versioned)\n"
						+ "WARN bucket b2: versioning is suspended; SPT left it alone, and the run goes on un-versioned\n")
						.getBytes(StandardCharsets.UTF_8);
		when(managers.get(1).readFromFile("/node-1/bucket.versioning.log", 0)).thenReturn(node1);
		when(managers.get(1).readFromFile("/node-1/bucket.versioning.log", node1.length)).thenThrow(new EOFException());
		when(managers.get(2).readFromFile("/node-2/bucket.versioning.log", 0))
						.thenThrow(new NoSuchFileException("/node-2/bucket.versioning.log"));
		when(managers.get(3).readFromFile("/node-3/bucket.versioning.log", 0)).thenThrow(new IOException("connection reset"));

		relay(step, managers, List.of("node-b:1099", "node-c:1099", "node-d:1099")).close();

		assertEquals(List.of(
						"INFO " + step + ": node 001 (node-b:1099): bucket b1: versioning enabled by this run"
										+ " (the bucket had never been versioned)",
						"WARN " + step + ": node 001 (node-b:1099): bucket b2: versioning is suspended; SPT left it"
										+ " alone, and the run goes on un-versioned",
						"WARN " + step + ": could not read the bucket versioning notices of node 003 (node-d:1099):"
										+ " connection reset"),
						relayed, "node 002 has no notices and is quiet; the Entry Node's own are not read");
		verify(managers.get(0), never()).readFromFile("/node-0/bucket.versioning.log", 0);
	}

	@Test
	void aNodeFileWrittenThroughTheLoggerIsRelayed() throws Exception {
		final var step = "relay-file-" + System.nanoTime();
		final var file = dir.resolve("bucket.versioning.log");
		// What the driver's notice writes, through the appender's "%p %m%n" layout.
		Files.writeString(file, "INFO bucket b3: versioning is already enabled\n");
		final var entry = mock(FileManager.class);
		final var node = mock(FileManager.class);
		when(node.logFileName(LOGGER, step)).thenReturn(file.toString());
		when(node.readFromFile(file.toString(), 0)).thenAnswer(inv -> FileManager.INSTANCE.readFromFile(file.toString(), 0));
		when(node.readFromFile(file.toString(), Files.size(file)))
						.thenAnswer(inv -> FileManager.INSTANCE.readFromFile(file.toString(), Files.size(file)));

		relay(step, List.of(entry, node), List.of("node-b:1099")).close();

		assertEquals(List.of("INFO " + step + ": node 001 (node-b:1099): bucket b3: versioning is already enabled"), relayed);
	}

	@Test
	void theLoggerWritesTheStepsBucketVersioningLog() throws Exception {
		final var name = FileManager.INSTANCE.logFileName(LOGGER, "some-step");
		assertTrue(name.endsWith("some-step" + java.io.File.separator + "bucket.versioning.log"), name);
	}

	@Test
	void aLineIsReadBackAsItsLevelAndText() {
		assertEquals(new BucketVersioningNotice.Line(Level.WARN, "bucket b: x y"),
						BucketVersioningNotice.Line.parse("WARN bucket b: x y\r"));
		assertEquals(new BucketVersioningNotice.Line(Level.INFO, "bucket b: x"),
						BucketVersioningNotice.Line.parse("INFO bucket b: x"));
		assertEquals(Level.INFO, BucketVersioningNotice.Line.parse("ERROR bucket b: x").level());
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
