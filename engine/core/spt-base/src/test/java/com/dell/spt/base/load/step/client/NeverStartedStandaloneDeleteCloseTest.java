package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A standalone DELETE step client that started no standalone DELETE work (an engine prototype built
 * from a base configuration with {@code load-op-delete-standalone}, or a step refused, stopped or
 * failed before its first slice started) closes without a failure-budget verdict: no budget line, no
 * outcome published, no close failure. A client that started a slice, or holds slices, is judged as
 * before and fails closed.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NeverStartedStandaloneDeleteCloseTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final String BUDGET_LINE = "Standalone DELETE";

	private final String stepId = "never-started-delete-" + UUID.randomUUID();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	private final List<String> messages = Collections.synchronizedList(new ArrayList<>());
	private final AbstractAppender appender = new AbstractAppender(
					"neverStartedDeleteCapture", null, null, true, Property.EMPTY_ARRAY) {
		@Override
		public void append(final LogEvent event) {
			messages.add(event.getMessage().getFormattedMessage());
		}
	};
	private String originalRmiHostname;
	private boolean rmiHostnameSet;
	private LoadStepManagerServiceImpl node;

	@BeforeEach
	void captureMessages() {
		appender.start();
		messageLogger().addAppender(appender);
	}

	@AfterEach
	void cleanUp() throws Exception {
		messageLogger().removeAppender(appender);
		appender.stop();
		JvmExitTestSupport.reset();
		// A start's interrupt must not leak into the next test.
		Thread.interrupted();
		try {
			if (node != null && !node.isClosed()) {
				node.close();
			}
		} finally {
			if (node != null) {
				ServiceUtil.shutdown();
			}
			if (rmiHostnameSet) {
				if (originalRmiHostname == null) {
					System.clearProperty(RMI_HOSTNAME);
				} else {
					System.setProperty(RMI_HOSTNAME, originalRmiHostname);
				}
			}
		}
	}

	@Test
	void aNeverStartedCountPrototypeClosesWithoutAVerdict() {
		final TestLoadStepClient prototype = new TestLoadStepClient(countConfig(), List.of(), null, metricsMgr);

		assertDoesNotThrow(prototype::close);

		assertTrue(prototype.isClosed());
		assertNoVerdict();
	}

	@Test
	void aNeverStartedPrototypeOfAnotherOpTypeClosesWithoutAVerdict() {
		final Config config = countConfig();
		config.val("load-op-type", "create");
		final TestLoadStepClient prototype = new TestLoadStepClient(config, List.of(), null, metricsMgr);

		assertDoesNotThrow(prototype::close);

		assertTrue(prototype.isClosed());
		assertNoVerdict();
	}

	@Test
	void aNeverStartedDurationPrototypeClosesWithoutAVerdict() {
		final TestLoadStepClient prototype = new TestLoadStepClient(durationConfig(), List.of(), null, metricsMgr);

		assertDoesNotThrow(prototype::close);

		assertTrue(prototype.isClosed());
		assertNoVerdict();
	}

	@Test
	void aNeverStartedDurationPrototypeClosedDirectlyDoesNotThrow() {
		final TestLoadStepClient prototype = new TestLoadStepClient(durationConfig(), List.of(), null, metricsMgr);

		assertDoesNotThrow(prototype::doClose);

		assertNoVerdict();
	}

	@Test
	void aNeverStartedDurationPrototypeStoppedDirectlyPublishesNoOutcome() throws Exception {
		try (var prototype = new TestLoadStepClient(durationConfig(), List.of(), null, metricsMgr)) {
			assertDoesNotThrow(prototype::doStop);

			assertNoVerdict();
		}
		assertNoVerdict();
	}

	@Test
	void aRunRefusedAtJvmExitEndsClosedWithoutAVerdict() {
		final TestLoadStepClient client = new TestLoadStepClient(countConfig(), List.of(), null, metricsMgr);
		JvmExit.markBegun();

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::run));
		Thread.interrupted();

		assertTrue(client.isClosed(), "the refused run's close failed");
		assertNoVerdict();
	}

	@Test
	void aCountStepStoppedBeforeItsFirstSliceStartedClosesWithoutAVerdict() throws Exception {
		assertStoppedAtFirstSliceClosesCleanly(countConfig());
	}

	@Test
	void aDurationStepStoppedBeforeItsFirstSliceStartedClosesWithoutAVerdict() throws Exception {
		assertStoppedAtFirstSliceClosesCleanly(durationConfig());
	}

	@Test
	void aStartFailingBeforeAnySliceKeepsOnlyItsOwnError() {
		// No selection manifest: the start fails before it creates a slice.
		final TestLoadStepClient client = new TestLoadStepClient(countConfig(), List.of(), null, metricsMgr) {
			@Override
			protected boolean supportsStandaloneDeleteArtifactAggregation() {
				return true;
			}
		};

		final Throwable failure = assertThrows(Throwable.class, client::start);

		assertTrue(failure.getMessage().contains("requires a frozen selection manifest"), failure.getMessage());
		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertNoVerdict();
	}

	@Test
	void aStartFailingInItsFirstSliceKeepsOnlyItsOwnError() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		doAnswer(call -> {
			throw new IllegalStateException("the slice did not start");
		}).when(entry).start();
		final TestLoadStepClient client = client(countConfig(), entry, null);

		assertThrows(Throwable.class, client::start);

		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertNoVerdict();
	}

	@Test
	void aCountStepStoppedAfterItsFirstSliceStartedFailsClosed() throws Exception {
		assertStoppedAfterFirstSliceFailsClosed(countConfig());
	}

	@Test
	void aDurationStepStoppedAfterItsFirstSliceStartedFailsClosed() throws Exception {
		assertStoppedAfterFirstSliceFailsClosed(durationConfig());
	}

	@Test
	void aStartedStepWithNoAcceptedDeletesFailsClosedAtEveryClose() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(entry.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(entry.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(0, 0, 0, 0, 0, 0, 0, 0, true));
		final TestLoadStepClient client = client(countConfig(), entry, null);
		client.start();

		assertInstanceOf(IntegrityTerminalException.class, assertThrows(Throwable.class, client::close));
		assertInstanceOf(IntegrityTerminalException.class, assertThrows(Throwable.class, client::close));

		assertFalse(client.isClosed());
		assertFailedVerdictPublished();
	}

	@Test
	void slicesHeldWithoutAStartAreJudgedAtEveryClose() throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(countConfig(), List.of(), null, metricsMgr);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		addRawStepSlice(client, slice);

		final Throwable first = assertThrows(IntegrityTerminalException.class, client::close);
		// The first close cleared the slices; the failure stays.
		final Throwable second = assertThrows(IntegrityTerminalException.class, client::close);

		assertTrue(first.getMessage().contains("counters are missing"), first.getMessage());
		assertEquals(first.getMessage(), second.getMessage());
		assertFalse(client.isClosed());
	}

	@Test
	void aNeverStartedLocalStepClosesAsBefore() throws Exception {
		for (final Config config : List.of(countConfig(), durationConfig())) {
			final var local = new LinearLoadStepLocal(config, List.of(), List.of(), metricsMgr);

			assertDoesNotThrow(local::close);

			assertTrue(local.isClosed());
		}
		assertNoVerdict();
	}

	private void assertStoppedAtFirstSliceClosesCleanly(final Config config) throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		// As LoadStepLocalBase does at JVM exit: the slice is refused before it runs.
		doAnswer(call -> {
			throw new InterruptedException(stepId + ": not started, the engine is exiting");
		}).when(entry).start();
		final TestLoadStepClient client = client(config, entry, null);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();

		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertNoVerdict();
	}

	private void assertStoppedAfterFirstSliceFailsClosed(final Config config) throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(entry.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		final TestLoadStepClient client = twoSliceClient(config, entry);

		// The second slice interrupts the start, as a stop does, once the first slice started.
		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();

		assertThrows(IntegrityTerminalException.class, client::close);
		assertFalse(client.isClosed());
		assertFailedVerdictPublished();
	}

	private void assertNoVerdict() {
		final List<String> budgetLines;
		synchronized (messages) {
			budgetLines = messages.stream().filter(message -> message.contains(BUDGET_LINE)).toList();
		}
		assertEquals(List.of(), budgetLines);
		verify(metricsMgr, never()).updateTerminalDeleteFailureOutcome(anyString(), anyString());
	}

	/** The failure-budget verdict was given and published as failed, whichever failure path gave it. */
	private void assertFailedVerdictPublished() {
		verify(metricsMgr, atLeastOnce())
						.updateTerminalDeleteFailureOutcome(stepId, MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED);
	}

	/** A step whose entry slice is {@code entry} and whose other-node slice, if any, is {@code worker}. */
	@SuppressWarnings("unchecked")
	private TestLoadStepClient client(final Config config, final LoadStep entry, final LoadStep worker) {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		final var created = new AtomicInteger();
		when(factory.createLocal(any(), anyList(), anyList(), any()))
						.thenAnswer(call -> created.getAndIncrement() == 0 ? entry : worker);
		return new TestLoadStepClient(config, List.of(factory), null, metricsMgr);
	}

	/** A step on this node and one other; the other node's slice interrupts the start, then fails. */
	private TestLoadStepClient twoSliceClient(final Config config, final LoadStep entry) throws Exception {
		final Thread starter = Thread.currentThread();
		final LoadStep worker = mock(LoadStep.class);
		doAnswer(call -> {
			starter.interrupt();
			throw new IllegalStateException("the node is going away");
		}).when(worker).start();
		when(worker.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(worker.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		final String nodeAddr = "127.0.0.1:" + port;
		config.val("load-step-node-addrs", List.of(nodeAddr));
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		rmiHostnameSet = true;
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		final TestLoadStepClient client = client(config, entry, worker);
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> nodeFactory = mock(LoadStepFactory.class);
		when(nodeFactory.id()).thenReturn("test-load-step");
		when(nodeFactory.createLocal(any(), anyList(), anyList(), any())).thenReturn(worker);
		node = new LoadStepManagerServiceImpl(port, List.of(nodeFactory), metricsMgr);
		node.start();
		return client.withNodeFileManagers(Map.of(nodeAddr, workerFiles()));
	}

	private static FileManager workerFiles() throws Exception {
		final FileManager files = mock(FileManager.class);
		when(files.logFileName(anyString(), anyString())).thenAnswer(call -> "/w/" + call.getArgument(0));
		when(files.fileSize(anyString())).thenReturn(0L);
		when(files.readFromFile(anyString(), anyLong())).thenAnswer(call -> {
			throw new NoSuchFileException(call.getArgument(0));
		});
		return files;
	}

	@SuppressWarnings("unchecked")
	private static void addRawStepSlice(final LoadStepClientBase<?> client, final LoadStep slice) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("stepSlices");
			field.setAccessible(true);
			((List<LoadStep>) field.get(client)).add(slice);
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	private Config countConfig() {
		final Config config = baseConfig();
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		return config;
	}

	private Config durationConfig() {
		final Config config = baseConfig();
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		return config;
	}

	private Config baseConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-step-node-addrs", List.of());
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		return config;
	}

	private static Logger messageLogger() {
		return LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
	}
}
