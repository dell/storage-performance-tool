package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Whether the other nodes' bucket versioning notices reach the Entry Node's messages and errors in time, through the
 * real notice writer, the real file reads and the step client's own stop and close: the notices a step run wrote, all
 * of them, before the step publishes the total metrics that let the CLI fetch the logs.
 */
@Timeout(60)
class BucketVersioningRelayDeliveryTest {

	private static final String LOGGER = Loggers.BUCKET_VERSIONING.getName();
	private static final String NOTICE_FILE = "bucket.versioning.log";

	@TempDir
	Path dir;

	private final List<String> relayed = new CopyOnWriteArrayList<>();

	private BucketVersioningRelay relay(final String step, final FileManager node) {
		return new BucketVersioningRelay(step, List.of(mock(FileManager.class), node), List.of("entry", "worker"),
						(level, message) -> relayed.add(level + " " + message));
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

	private static Path noticeFile(final String step) throws Exception {
		return Path.of(FileManager.INSTANCE.logFileName(LOGGER, step));
	}

	/** Holds the engine's async log consumer, as a slow console or disk does, until the latch is released. */
	@Test
	void aNoticeIsRelayedWhileTheAsyncLoggerIsBehind() throws Exception {
		final var step = "relay-async-" + System.nanoTime();
		final var file = noticeFile(step);
		final var entered = new CountDownLatch(1);
		final var release = new CountDownLatch(1);
		final var blocker = new AbstractAppender("relay-block-" + step, null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				if (event.getMessage().getFormattedMessage().equals("block " + step)) {
					entered.countDown();
					try {
						release.await(10, TimeUnit.SECONDS);
					} catch (final InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}
		};
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.TEST.getName());
		blocker.start();
		logger.addAppender(blocker);
		try (final var ctx = CloseableThreadContext.put(Constants.KEY_STEP_ID, step)) {
			final var relay = relay(step, node(file, step));
			Loggers.TEST.info("block " + step);
			assertTrue(entered.await(5, TimeUnit.SECONDS));

			BucketVersioningNotice.info(step, "bucket b: versioning enabled by this run");
			relay.close();
		} finally {
			release.countDown();
			logger.removeAppender(blocker);
			blocker.stop();
		}

		assertEquals(List.of("INFO " + step + ": node 001 (worker): bucket b: versioning enabled by this run"), relayed);
	}

	@Test
	void aReusedStepIdRelaysOnlyThisRunsNotices() throws Exception {
		final var step = "relay-reused-" + System.nanoTime();
		final var file = noticeFile(step);
		try (final var ctx = CloseableThreadContext.put(Constants.KEY_STEP_ID, step)) {
			BucketVersioningNotice.info(step, "bucket old: versioning enabled by this run");
			awaitText(file, "bucket old");

			final var secondRun = relay(step, node(file, step));
			BucketVersioningNotice.info(step, "bucket current: versioning is already enabled");
			awaitText(file, "bucket current");
			secondRun.close();
		}

		assertEquals(List.of("INFO " + step + ": node 001 (worker): bucket current: versioning is already enabled"), relayed,
						"the first run's notice is not this run's");
	}

	@Test
	void theCapRelaysWholeLinesAndSaysItStoppedShort() throws Exception {
		final var step = "relay-cap-" + System.nanoTime();
		final var file = dir.resolve(NOTICE_FILE);
		final var relay = relay(step, node(file, step));
		final var row = "INFO bucket repeated: versioning is already enabled\n";
		Files.writeString(file, row.repeat(BucketVersioningRelay.MAX_BYTES / row.length() + 10)
						+ "WARN bucket last: could not enable versioning\n");

		relay.close();

		final var warnings = relayed.stream().filter(line -> line.startsWith("WARN")).toList();
		assertEquals(List.of("WARN " + step + ": node 001 (worker): its bucket versioning notices pass the 1 MiB the relay"
						+ " reads; the rest are only in that node's bucket.versioning.log"), warnings);
		final var expected = "INFO " + step + ": node 001 (worker): bucket repeated: versioning is already enabled";
		final var notices = relayed.stream().filter(line -> line.startsWith("INFO")).toList();
		assertEquals(BucketVersioningRelay.MAX_BYTES / row.length(), notices.size());
		assertTrue(notices.stream().allMatch(expected::equals), "only whole lines");
	}

	@Test
	void anOlderEngineWithoutTheNoticeFileIsNamedNotRelayed() throws Exception {
		final var step = "relay-older-" + System.nanoTime();
		// An engine without the logger names its root appender's file.
		final var file = dir.resolve("3rdparty.log");
		Files.writeString(file, "INFO a third-party library line\n");
		final var node = node(file, step);

		relay(step, node).close();

		assertEquals(List.of("WARN " + step + ": could not relay the bucket versioning notices of node 001 (worker): its"
						+ " engine writes no bucket.versioning.log"), relayed);
	}

	/**
	 * The worker writes its notice before its slice's stop returns, as a check that ends at the step's end does. The
	 * Entry Node's messages have it when the step client publishes its total metrics; a notice written later reaches
	 * its errors at close.
	 */
	@Test
	void theStepsStopRelaysBeforeTheTotalMetricsAndItsCloseRelaysTheRest() throws Exception {
		final var metricsMgr = mock(MetricsManager.class);
		final var client = new TestLoadStepClient(stepConfig(), Collections.emptyList(), Collections.emptyList(),
						metricsMgr);
		final var step = client.loadStepId();
		final var file = dir.resolve(NOTICE_FILE);
		managers(client).add(mock(FileManager.class));
		managers(client).add(node(file, step));
		final var init = LoadStepClientBase.class.getDeclaredMethod("initBucketVersioningRelay", List.class);
		init.setAccessible(true);
		init.invoke(client, List.of("worker:1099"));

		final var slice = mock(LoadStep.class);
		doAnswer(inv -> {
			append(file, "INFO bucket w1: versioning enabled by this run (the bucket had never been versioned)\n");
			return null;
		}).when(slice).stop();
		slices(client).add(slice);
		final var context = mock(MetricsContext.class);
		metricsContexts(client).add(context);

		final var events = new CopyOnWriteArrayList<String>();
		final var capture = new AbstractAppender("relay-capture-" + step, null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				events.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
			}
		};
		final var loggerContext = LoggerContext.getContext(false);
		final var msg = loggerContext.getLogger(Loggers.MSG.getName());
		final var err = loggerContext.getLogger(Loggers.ERR.getName());
		capture.start();
		msg.addAppender(capture);
		err.addAppender(capture);
		final var w1 = "INFO " + step + ": node 001 (worker:1099): bucket w1: versioning enabled by this run";
		final var w2 = "WARN " + step + ": node 001 (worker:1099): bucket w2: could not enable versioning";
		final var relayedAtTotals = new AtomicBoolean();
		doAnswer(inv -> {
			// What is logged before the totals reaches the messages; wait for the async logger to deliver it.
			relayedAtTotals.set(awaitEvent(events, w1, 3));
			return null;
		}).when(metricsMgr).unregister(any());
		try {
			client.doStop();
			assertTrue(relayedAtTotals.get(), "the relay ran before the total metrics: " + events);

			append(file, "WARN bucket w2: could not enable versioning (PUT ?versioning timed out after 400 ms); the run goes"
							+ " on un-versioned\n");
			client.doClose();
			assertTrue(awaitEvent(events, w2, 5), "the close relays what was written later: " + events);
			assertEquals(1, events.stream().filter(event -> event.startsWith(w1)).count(), "relayed once: " + events);
		} finally {
			msg.removeAppender(capture);
			err.removeAppender(capture);
			capture.stop();
		}
	}

	private static boolean awaitEvent(final List<String> events, final String prefix, final int seconds)
					throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
		while (System.nanoTime() < deadline) {
			if (events.stream().anyMatch(event -> event.startsWith(prefix))) {
				return true;
			}
			Thread.sleep(10);
		}
		return false;
	}

	private static void append(final Path file, final String text) throws Exception {
		Files.writeString(file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	}

	private static void awaitText(final Path file, final String text) throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (System.nanoTime() < deadline) {
			if (Files.exists(file) && Files.readString(file).contains(text)) {
				return;
			}
			Thread.sleep(10);
		}
		fail("the file did not get " + text);
	}

	@SuppressWarnings("unchecked")
	private static List<FileManager> managers(final LoadStepClientBase<?> client) throws Exception {
		final var field = LoadStepClientBase.class.getDeclaredField("fileMgrs");
		field.setAccessible(true);
		return (List<FileManager>) field.get(client);
	}

	@SuppressWarnings("unchecked")
	private static List<LoadStep> slices(final LoadStepClientBase<?> client) throws Exception {
		final var field = LoadStepClientBase.class.getDeclaredField("stepSlices");
		field.setAccessible(true);
		return (List<LoadStep>) field.get(client);
	}

	@SuppressWarnings("unchecked")
	private static List<MetricsContext<?>> metricsContexts(final LoadStepClientBase<?> client) throws Exception {
		final var field = LoadStepBase.class.getDeclaredField("metricsContexts");
		field.setAccessible(true);
		return (List<MetricsContext<?>>) field.get(client);
	}

	private static Config stepConfig() {
		final var base = TestConfigBuilder.config();
		final Map<String, Object> keys = Map.of("storage", Map.of("object", Map.of("versioning", Boolean.class)));
		final var schema = TreeUtil.reduceForest(List.of(TreeUtil.copyTree(base.schema()), keys));
		final Config config = new BasicConfig(base.pathSep(), schema, Config.deepToMap(base));
		config.val("load-step-id", "relay-stop-" + System.nanoTime());
		config.val(HttpVersioningSynonym.TARGET_PATH, true);
		return config;
	}
}
