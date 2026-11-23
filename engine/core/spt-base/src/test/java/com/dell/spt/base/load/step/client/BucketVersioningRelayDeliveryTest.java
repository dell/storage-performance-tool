package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
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
import com.dell.spt.base.load.step.service.file.FileManagerService;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.rmi.MarshalledObject;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
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
		// The Test logger has no entry of its own: the window adds one, which takes the appender, and its close
		// removes that entry again once the appender is off it, so that the logger follows the root after this test.
		try (final var levels = LoggerLevels.set(Map.of(Loggers.TEST.getName(), Level.INFO))) {
			logger.addAppender(blocker);
			try (final var ctx = CloseableThreadContext.put(Constants.KEY_STEP_ID, step)) {
				final var relay = relay(step, node(file, step));
				Loggers.TEST.info("block " + step);
				assertTrue(entered.await(5, TimeUnit.SECONDS));

				BucketVersioningNotice.info(step, "bucket b: versioning is already enabled");
				relay.close();
			} finally {
				release.countDown();
				logger.removeAppender(blocker);
				blocker.stop();
			}
		}

		assertEquals(List.of("INFO " + step + ": node 001 (worker): bucket b: versioning is already enabled"), relayed);
	}

	@Test
	void aReusedStepIdRelaysOnlyThisRunsNotices() throws Exception {
		final var step = "relay-reused-" + System.nanoTime();
		final var file = noticeFile(step);
		try (final var ctx = CloseableThreadContext.put(Constants.KEY_STEP_ID, step)) {
			BucketVersioningNotice.info(step, "bucket old: versioning is already enabled");
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
						+ " reads, so its running checks can't be followed; the rest are only in that node's"
						+ " bucket.versioning.log"), warnings);
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
			append(file, "INFO bucket w1: versioning is already enabled\n");
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
		// Earlier tests in this JVM may leave the errors at ERROR, which would drop the relayed WARN.
		final var levels = LoggerLevels.atMost(Level.INFO, Loggers.MSG.getName(), Loggers.ERR.getName());
		capture.start();
		msg.addAppender(capture);
		err.addAppender(capture);
		final var w1 = "INFO " + step + ": node 001 (worker:1099): bucket w1: versioning is already enabled";
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
			levels.close();
		}
	}

	/** The most a stalled node may hold a step thread past the relay's bound: generous for a loaded 2-core runner. */
	private static final long MARGIN_MILLIS = 5_000;

	/**
	 * A worker's real remote file service, over RMI on an ephemeral 127.0.0.1 port, whose {@code fileSize} stalls from
	 * its {@code stallAt}-th call until released; an RMI read can't be interrupted. Once released it reports a notice,
	 * which the relay, having given up on the node, must drop.
	 */
	private static final class StalledNode implements AutoCloseable {

		final FileManagerService impl = mock(FileManagerService.class);
		final FileManager remote;
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		private final AtomicInteger calls = new AtomicInteger();

		StalledNode(final Path file, final int stallAt) throws Exception {
			final var late = "INFO bucket late: read after the relay gave up\n".getBytes(StandardCharsets.UTF_8);
			when(impl.logFileName(eq(LOGGER), anyString())).thenReturn(file.toString());
			when(impl.fileSize(file.toString())).thenAnswer(inv -> {
				if (calls.incrementAndGet() < stallAt) {
					return 0L;
				}
				entered.countDown();
				release.await(30, TimeUnit.SECONDS);
				return (long) late.length;
			});
			when(impl.readFromFile(eq(file.toString()), anyLong())).thenReturn(late);
			final RMIClientSocketFactory clients = (RMIClientSocketFactory & Serializable) (host, port) -> new Socket(InetAddress.getLoopbackAddress(), port);
			final RMIServerSocketFactory servers = port -> new ServerSocket(port, 50, InetAddress.getLoopbackAddress());
			remote = (FileManager) UnicastRemoteObject.exportObject(impl, 0, clients, servers);
		}

		@Override
		public void close() throws Exception {
			release.countDown();
			UnicastRemoteObject.unexportObject(impl, true);
		}
	}

	/** A client with the stalled node's file manager as its worker's, and the relay as the step's start makes it. */
	private TestLoadStepClient client(final MetricsManager metricsMgr, final FileManager worker) throws Exception {
		final var client = new TestLoadStepClient(stepConfig(), Collections.emptyList(), Collections.emptyList(),
						metricsMgr);
		managers(client).add(mock(FileManager.class));
		managers(client).add(worker);
		metricsContexts(client).add(mock(MetricsContext.class));
		return client;
	}

	private static void initRelay(final LoadStepClientBase<?> client) throws Exception {
		final var init = LoadStepClientBase.class.getDeclaredMethod("initBucketVersioningRelay", List.class);
		init.setAccessible(true);
		init.invoke(client, List.of("worker:1099"));
	}

	private static long millisOf(final ThrowingRunnable action) throws Exception {
		final long start = System.nanoTime();
		action.run();
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}

	/** Captures the step's messages and errors, letting them through whatever level earlier tests left. */
	private static final class Events extends AbstractAppender implements AutoCloseable {

		final List<String> lines = new CopyOnWriteArrayList<>();
		private final List<org.apache.logging.log4j.core.Logger> loggers;
		private final LoggerLevels levels = LoggerLevels.atMost(Level.INFO, Loggers.MSG.getName(), Loggers.ERR.getName());

		Events() {
			super("relay-events-" + System.nanoTime(), null, null, true, Property.EMPTY_ARRAY);
			final var context = LoggerContext.getContext(false);
			loggers = List.of(context.getLogger(Loggers.MSG.getName()), context.getLogger(Loggers.ERR.getName()));
			start();
			loggers.forEach(logger -> logger.addAppender(this));
		}

		@Override
		public void append(final LogEvent event) {
			lines.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
		}

		@Override
		public void close() {
			loggers.forEach(logger -> logger.removeAppender(this));
			stop();
			levels.close();
		}
	}

	/** Waits until no relay thread of the step is left. */
	private static void awaitRelayThreadsEnded(final String step) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
		while (relayThreads(step) && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertFalse(relayThreads(step), "every relay thread of the step ended");
	}

	private static boolean relayThreads(final String step) {
		return Thread.getAllStackTraces().keySet().stream().anyMatch(thread -> thread.getName().contains(step));
	}

	/** Sol's review probe, inverted: before, the stop stayed blocked past the bound, and past an interrupt. */
	@Test
	void aStalledWorkerFileServiceHoldsTheStopOnlyToTheBound() throws Exception {
		final var metricsMgr = mock(MetricsManager.class);
		final var totalsPublished = new AtomicBoolean();
		doAnswer(inv -> {
			totalsPublished.set(true);
			return null;
		}).when(metricsMgr).unregister(any());
		final var file = dir.resolve(NOTICE_FILE);
		try (final var events = new Events(); final var node = new StalledNode(file, 2)) {
			final var client = client(metricsMgr, node.remote);
			final var stepId = client.loadStepId();
			initRelay(client);

			final long millis = millisOf(client::doStop);

			assertTrue(node.entered.await(0, TimeUnit.SECONDS), "the stop's read stalled");
			assertTrue(millis < BucketVersioningRelay.BOUND_MILLIS + MARGIN_MILLIS, "the stop returned in " + millis + " ms");
			assertTrue(totalsPublished.get(), "the stop went on to publish the total metrics");
			final var warn = "WARN " + stepId + ": node 001 (worker:1099) did not answer within 10000 ms when the step"
							+ " stopped, so the relay left it out";
			assertTrue(awaitEvent(events.lines, warn, 5), events.lines::toString);

			node.release.countDown();
			client.doClose();
			awaitRelayThreadsEnded(stepId);
			Thread.sleep(200);
			assertEquals(1, events.lines.stream().filter(line -> line.startsWith(warn)).count(), events.lines::toString);
			assertFalse(events.lines.stream().anyMatch(line -> line.contains("bucket late")), "the late read was dropped: "
							+ events.lines);
		}
	}

	@Test
	void aStalledWorkerFileServiceHoldsTheCloseOnlyToTheBound() throws Exception {
		final var file = dir.resolve(NOTICE_FILE);
		try (final var events = new Events(); final var node = new StalledNode(file, 3)) {
			final var client = client(mock(MetricsManager.class), node.remote);
			final var stepId = client.loadStepId();
			initRelay(client);
			client.doStop();

			final long millis = millisOf(client::doClose);

			assertTrue(node.entered.await(0, TimeUnit.SECONDS), "the close's read stalled");
			assertTrue(millis < BucketVersioningRelay.BOUND_MILLIS + MARGIN_MILLIS, "the close returned in " + millis + " ms");
			final var warn = "WARN " + stepId + ": node 001 (worker:1099) did not answer within 10000 ms when the step"
							+ " closed, so the relay left it out";
			assertTrue(awaitEvent(events.lines, warn, 5), events.lines::toString);
			node.release.countDown();
			awaitRelayThreadsEnded(stepId);
			Thread.sleep(200);
			assertFalse(events.lines.stream().anyMatch(line -> line.contains("bucket late")), events.lines::toString);
		}
	}

	@Test
	void aStalledWorkerFileServiceHoldsTheStartOnlyToTheBoundAndIsLeftOut() throws Exception {
		final var file = dir.resolve(NOTICE_FILE);
		try (final var events = new Events(); final var node = new StalledNode(file, 1)) {
			final var client = client(mock(MetricsManager.class), node.remote);
			final var stepId = client.loadStepId();

			final long millis = millisOf(() -> initRelay(client));

			assertTrue(node.entered.await(0, TimeUnit.SECONDS), "the start's read stalled");
			assertTrue(millis < BucketVersioningRelay.BOUND_MILLIS + MARGIN_MILLIS, "the start returned in " + millis + " ms");
			final var warn = "WARN " + stepId + ": node 001 (worker:1099) did not answer within 10000 ms when the step"
							+ " started, so the relay left it out";
			assertTrue(awaitEvent(events.lines, warn, 5), events.lines::toString);
			node.release.countDown();
			awaitRelayThreadsEnded(stepId);

			final long stopAndClose = millisOf(() -> {
				client.doStop();
				client.doClose();
			});

			assertTrue(stopAndClose < MARGIN_MILLIS, "the node left out is not read again: " + stopAndClose + " ms");
			Thread.sleep(200);
			assertEquals(1, events.lines.stream().filter(line -> line.contains(stepId + ": node 001")).count(),
							events.lines::toString);
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

	/** Another stub of the same remote file service, as a later step's lookup of the worker gets. */
	private static FileManager anotherStub(final FileManager stub) throws Exception {
		return new MarshalledObject<>(stub).get();
	}

	private static long liveRelayThreads(final String prefix) {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(thread -> thread.isAlive() && thread.getName().contains(prefix))
						.count();
	}

	/**
	 * Sol's lifetime probe: with no RMI response timeout a stalled read never returns, so each step's relay used to
	 * leave one more blocked thread. Later steps now skip the worker at once while its earlier read is outstanding.
	 */
	@Test
	void aStalledWorkerHoldsOneRelayThreadAcrossSteps() throws Exception {
		final var prefix = "relay-lifetime-" + System.nanoTime();
		final var file = dir.resolve(NOTICE_FILE);
		try (final var node = new StalledNode(file, 2)) {
			final var first = new BucketVersioningRelay(prefix + "-0", List.of(mock(FileManager.class), anotherStub(node.remote)),
							List.of("entry", "worker:1099"), (level, message) -> relayed.add(level + " " + message), 300);
			first.relayAtStop();
			first.close();
			assertTrue(node.entered.await(0, TimeUnit.SECONDS), "the first step's read stalled");
			assertEquals(1, liveRelayThreads(prefix), "the abandoned read's thread");

			for (int i = 1; i <= 3; i++) {
				final var step = prefix + "-" + i;
				final var stub = anotherStub(node.remote);
				final long millis = millisOf(() -> {
					final var relay = new BucketVersioningRelay(step, List.of(mock(FileManager.class), stub),
									List.of("entry", "worker:1099"), (level, message) -> relayed.add(level + " " + message), 10_000);
					relay.relayAtStop();
					relay.close();
				});
				assertEquals(1, liveRelayThreads(prefix), "step " + i + " added no thread for the stalled worker");
				assertTrue(millis < 2_000, "step " + i + " skipped the worker at once: " + millis + " ms");
			}
			final var skipped = relayed.stream()
							.filter(line -> line.contains(": node 001 (worker:1099): an earlier read of its bucket.versioning.log has"
											+ " not returned, so the relay left it out when the step started"))
							.count();
			assertEquals(3, skipped, relayed::toString);

			node.release.countDown();
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
			while (liveRelayThreads(prefix) > 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(0, liveRelayThreads(prefix), "the released read ended its thread");
		}
	}

	/**
	 * Fable's probe B: a run stopped through the engine API interrupts the step thread in {@code await}, whose
	 * {@code finally} stops the step with the interrupt still set. The healthy worker's notice still reaches the
	 * entry's logs before the totals.
	 */
	@Test
	void anInterruptedRunStillRelaysAtItsStop() throws Exception {
		final var metricsMgr = mock(MetricsManager.class);
		final var client = new TestLoadStepClient(stepConfig(), Collections.emptyList(), Collections.emptyList(),
						metricsMgr);
		final var step = client.loadStepId();
		final var file = dir.resolve(NOTICE_FILE);
		Files.writeString(file, "");
		final var worker = node(file, step);
		when(worker.fileSize(file.toString())).thenAnswer(inv -> {
			// Healthy, but not instant: the stop is still waiting when it sees its interrupt.
			Thread.sleep(200);
			return FileManager.INSTANCE.fileSize(file.toString());
		});
		managers(client).add(mock(FileManager.class));
		managers(client).add(worker);
		metricsContexts(client).add(mock(MetricsContext.class));
		initRelay(client);
		append(file, "WARN bucket w3: versioning is suspended; SPT left it alone, and the run goes on un-versioned\n");
		final var slice = mock(LoadStep.class);
		final var awaiting = new CountDownLatch(1);
		when(slice.await(anyLong(), any())).thenAnswer(inv -> {
			awaiting.countDown();
			return false;
		});
		slices(client).add(slice);
		final var notice = "WARN " + step + ": node 001 (worker:1099): bucket w3: versioning is suspended";
		try (final var events = new Events()) {
			final var noticeAtTotals = new AtomicBoolean();
			final var interruptedAtTotals = new AtomicBoolean();
			doAnswer(inv -> {
				// The stop runs with the run's interrupt still set: wait for the async logger without consuming it.
				interruptedAtTotals.set(Thread.interrupted());
				try {
					noticeAtTotals.set(awaitEvent(events.lines, notice, 3));
				} finally {
					if (interruptedAtTotals.get()) {
						Thread.currentThread().interrupt();
					}
				}
				return null;
			}).when(metricsMgr).unregister(any());
			final var runner = Thread.ofPlatform().start(() -> {
				try {
					client.await(60, TimeUnit.SECONDS);
				} catch (final Throwable t) {
					// The interrupt ends the await.
				}
			});
			assertTrue(awaiting.await(5, TimeUnit.SECONDS));
			runner.interrupt();
			runner.join(30_000);

			assertFalse(runner.isAlive(), "the interrupted run stopped");
			assertTrue(interruptedAtTotals.get(), "the stop kept the run's interrupt");
			assertTrue(noticeAtTotals.get(), "the notice reached the logs before the totals: " + events.lines);
			assertFalse(events.lines.stream().anyMatch(line -> line.contains("did not answer")), events.lines::toString);
		}
	}

	/**
	 * Sol's duration probe: a duration DELETE whose admission closure fails on one slice publishes its failed
	 * outcome on an early route, and its close fails. A healthy worker's completed notice is still relayed before the
	 * totals, and the close's failure still propagates.
	 */
	@Test
	void aFailedDurationDeleteStillRelaysAtItsStop() throws Exception {
		final var base = TestConfigBuilder.config();
		final Map<String, Object> keys = Map.of("storage", Map.of("object", Map.of("versioning", Boolean.class)));
		final var schema = TreeUtil.reduceForest(List.of(TreeUtil.copyTree(base.schema()), keys));
		final Config config = new BasicConfig(base.pathSep(), schema, Config.deepToMap(base));
		config.val("load-step-id", "relay-duration-" + System.nanoTime());
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val(HttpVersioningSynonym.TARGET_PATH, true);
		final var metricsMgr = mock(MetricsManager.class);
		final var client = new TestLoadStepClient(config, List.of(), List.of(), metricsMgr);
		final var step = client.loadStepId();
		final var file = dir.resolve(NOTICE_FILE);
		Files.writeString(file, "");
		final var relay = relay(step, node(file, step));
		final var field = LoadStepClientBase.class.getDeclaredField("bucketVersioningRelay");
		field.setAccessible(true);
		field.set(client, relay);
		metricsContexts(client).add(mock(MetricsContext.class));
		final var failedSlice = mock(LoadStep.class);
		doThrow(new RemoteException("admission unavailable")).when(failedSlice).closeOperationAdmissionForStepStop();
		slices(client).add(failedSlice);
		final var atTotals = new CopyOnWriteArrayList<String>();
		doAnswer(inv -> {
			atTotals.addAll(relayed);
			return null;
		}).when(metricsMgr).unregister(any());
		Files.writeString(file, "CHECK bucket b\nWARN bucket b: versioning is suspended; SPT left it alone\nDONE bucket b\n");

		client.stop();

		final var notice = "WARN " + step + ": node 001 (worker): bucket b: versioning is suspended; SPT left it alone";
		assertEquals(List.of(notice), atTotals, "relayed at the stop, before the totals");
		final var failure = assertThrows(IntegrityTerminalException.class, client::close);
		assertTrue(failure.getMessage().contains("failed to close operation admission"), failure.getMessage());
		assertEquals(List.of(notice), relayed, "relayed once");
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
