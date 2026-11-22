package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A start that fails before any slice's start returned closes like a start stopped before its first
 * slice: no terminal failure count capture, nothing published, no missing-artifact or retained DELETE
 * aggregator failure. The start's own error is what the run reports, and the exit hook's close finds
 * the step closed. A start that fails after a slice started, and a started step whose counts are
 * missing, still fail closed.
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class FailedStartBeforeAnySliceCloseTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final long WAIT_SECONDS = 30;

	private final String stepId = "failed-start-close-" + UUID.randomUUID();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	@TempDir
	Path tempDir;
	private Path stepDir;
	private String originalRmiHostname;
	private boolean rmiHostnameSet;

	@BeforeEach
	void locateStepDir() throws Exception {
		stepDir = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId))
						.toAbsolutePath()
						.getParent();
	}

	@AfterEach
	void cleanUp() throws Exception {
		Thread.interrupted();
		if (rmiHostnameSet) {
			if (originalRmiHostname == null) {
				System.clearProperty(RMI_HOSTNAME);
			} else {
				System.setProperty(RMI_HOSTNAME, originalRmiHostname);
			}
		}
		if (stepDir != null) {
			StepIdReuseAtStartTest.removeFolder(stepDir);
		}
	}

	@ParameterizedTest(name = "metadata={0}")
	@ValueSource(booleans = {true, false})
	void aStandaloneDeleteWithoutASelectionManifestClosesCleanly(final boolean metadata) throws Exception {
		final FailedStartClient client = deleteClient(deleteConfig(metadata, false));

		assertStartFails(client, "requires a frozen selection manifest");

		assertClosesCleanlyTwice(client);
		assertNothingPublished();
	}

	@ParameterizedTest(name = "metadata={0}")
	@ValueSource(booleans = {true, false})
	void aDurationStandaloneDeleteWithoutASelectionManifestRunsToItsOwnErrorOnly(final boolean metadata)
					throws Exception {
		final FailedStartClient client = deleteClient(deleteConfig(metadata, true));

		assertRunFailsWithItsOwnErrorOnly(client, "requires a frozen selection manifest");

		assertNothingPublished();
	}

	@Test
	void aMetadataStandaloneDeleteWhoseSelectionManifestIsMissingRetainsNoDeleteAggregatorFailure()
					throws Exception {
		final Config config = deleteConfig(true, false);
		config.val("item-input-file", tempDir.resolve("missing.csv").toString());
		config.val("storage-integrity-input-provenance", "engine_step");
		final FailedStartClient client = deleteClient(config);

		assertStartFails(client, "integrity input is not committed by the expected producer");

		// A retained DELETE aggregator failure would fail the second close, as closeAll's does at exit.
		assertClosesCleanlyTwice(client);
		assertNothingPublished();
	}

	@Test
	void aMetadataStandaloneDeleteRunWhoseSelectionManifestIsMissingRunsToItsOwnErrorOnly() throws Exception {
		final Config config = deleteConfig(true, false);
		config.val("item-input-file", tempDir.resolve("missing.csv").toString());
		config.val("storage-integrity-input-provenance", "engine_step");

		assertRunFailsWithItsOwnErrorOnly(deleteClient(config), "integrity input is not committed by the expected producer");

		assertNothingPublished();
	}

	@Test
	void aMetadataReadWithoutAnItemsFileClosesCleanly() throws Exception {
		final TestLoadStepClient client = metadataClient(readConfig(), entryFactory(mock(LoadStep.class)), OpType.READ);

		assertStartFails(client, "metadata-mode READ requires a finite item input file");

		assertClosesCleanlyTwice(client);
		assertNothingPublished();
	}

	@Test
	void aMetadataReadWhoseItemsFileIsMissingClosesCleanly() throws Exception {
		final Config config = readConfig();
		config.val("item-input-file", tempDir.resolve("missing.csv").toString());

		final TestLoadStepClient client = metadataClient(config, entryFactory(mock(LoadStep.class)), OpType.READ);

		assertStartFails(client, "integrity input is not committed by the expected producer");
		assertClosesCleanlyTwice(client);
		assertNothingPublished();
	}

	@Test
	void aMetadataReadWhoseItemsFileIsNotCanonicalClosesCleanly() throws Exception {
		final Path items = Files.writeString(tempDir.resolve("items.csv"), "not,a,canonical,manifest\n");
		final Config config = readConfig();
		config.val("item-input-file", items.toString());

		final TestLoadStepClient client = metadataClient(config, entryFactory(mock(LoadStep.class)), OpType.READ);

		assertStartFails(client, "integrity input is not committed by the expected producer");
		assertClosesCleanlyTwice(client);
		assertNothingPublished();
	}

	@Test
	void aMetadataReadRunWithoutAnItemsFileRunsToItsOwnErrorOnly() throws Exception {
		final TestLoadStepClient client = metadataClient(readConfig(), entryFactory(mock(LoadStep.class)), OpType.READ);

		assertRunFailsWithItsOwnErrorOnly(client, "metadata-mode READ requires a finite item input file");

		assertNothingPublished();
	}

	@ParameterizedTest(name = "metadata={0}")
	@ValueSource(booleans = {true, false})
	void aStartWhoseEntrySliceFailsToStartClosesCleanly(final boolean metadata) throws Exception {
		// As a READ whose items file is missing fails in its entry slice's start.
		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(7L);
		doAnswer(call -> {
			throw new IllegalStateException("the entry slice did not start");
		}).when(entry).start();
		final TestLoadStepClient client = metadata
						? metadataClient(createConfig(), entryFactory(entry), OpType.CREATE)
						: new TestLoadStepClient(createConfig(), List.of(entryFactory(entry)), null, metricsMgr);

		assertThrows(Throwable.class, client::start);
		Thread.interrupted();

		verify(entry).start();
		assertClosesCleanlyTwice(client);
		verify(entry).close();
		assertNothingPublished();
	}

	@Test
	void theExitHooksCloseLogsNoFailureAfterAStartThatFailedBeforeAnySlice() throws Exception {
		final FailedStartClient client = deleteClient(deleteConfig(true, false));
		assertStartFails(client, "requires a frozen selection manifest");

		final List<String> failures = closeAllFailures();

		assertEquals(List.of(), failures);
		assertTrue(client.isClosed());
	}

	@Test
	void aMetadataStartThatFailsAfterItsFirstSliceStartedStillFailsClosed() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(7L);
		when(entry.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		// No node serves this address, so the second slice does not resolve.
		final String nodeAddr = "127.0.0.1:" + port;
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		rmiHostnameSet = true;
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		final Config config = createConfig();
		config.val("load-step-node-addrs", new ArrayList<>(List.of(nodeAddr)));
		final TestLoadStepClient client = metadataClient(config, entryFactory(entry), OpType.CREATE)
						.withNodeFileManagers(Map.of(nodeAddr, workerFiles()));

		assertThrows(Throwable.class, client::start);
		Thread.interrupted();

		verify(entry).start();
		final var failure = assertThrows(IntegrityTerminalException.class, client::close);
		assertTrue(failure.getMessage().contains("failed to capture terminal metadata-mode operation failure counts"),
						failure.getMessage());
		assertFalse(client.isClosed());
	}

	@Test
	void aStartedMetadataStepWithoutFinalCountsStillFailsClosed() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(7L);
		when(entry.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		final TestLoadStepClient client = metadataClient(createConfig(), entryFactory(entry), OpType.CREATE);

		client.start();

		final var failure = assertThrows(IntegrityTerminalException.class, client::close);
		assertTrue(failure.getMessage().contains("failed to capture terminal metadata-mode operation failure counts"),
						failure.getMessage());
		assertFalse(client.isClosed());
	}

	private void assertStartFails(final TestLoadStepClient client, final String message) {
		final Throwable failure = assertThrows(Throwable.class, client::start);
		Thread.interrupted();
		assertFalse(failure instanceof InterruptedException, failure::toString);
		assertTrue(String.valueOf(failure.getMessage()).contains(message), failure::toString);
	}

	private static void assertClosesCleanlyTwice(final TestLoadStepClient client) {
		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed(), "the step whose start failed before any slice is not closed");
		assertDoesNotThrow(client::close);
	}

	private void assertRunFailsWithItsOwnErrorOnly(final TestLoadStepClient client, final String message) {
		final Throwable failure = assertThrows(Throwable.class, client::run);
		Thread.interrupted();
		assertTrue(String.valueOf(failure.getMessage()).contains(message), failure::toString);
		assertArrayEquals(new Throwable[0], failure.getSuppressed(), () -> "close failures under " + failure);
		assertTrue(client.isClosed(), "the run's close failed");
	}

	/** No slice file was collected or published, and no DELETE verdict was given. */
	private void assertNothingPublished() throws Exception {
		verify(metricsMgr, never()).updateTerminalDeleteFailureOutcome(anyString(), anyString());
		if (!Files.isDirectory(stepDir)) {
			return;
		}
		try (Stream<Path> files = Files.list(stepDir)) {
			assertEquals(List.of(), files.map(file -> file.getFileName().toString())
							.filter(name -> name.contains(".node-") || name.startsWith("delete."))
							.toList());
		}
	}

	private FailedStartClient deleteClient(final Config config) {
		final FailedStartClient client = new FailedStartClient(config, extensions(), metricsMgr);
		client.addMetricsContextForTest(metricsWithoutFinalSnapshot(OpType.DELETE));
		return client;
	}

	private TestLoadStepClient metadataClient(
					final Config config, final LoadStepFactory<LoadStep, ?> entryFactory, final OpType opType) {
		config.val("run-id", 7L);
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "metadata");
		final List<Extension> extensions = new ArrayList<>(extensions());
		extensions.add(0, entryFactory);
		final TestLoadStepClient client = new TestLoadStepClient(config, extensions, null, metricsMgr);
		client.addMetricsContextForTest(metricsWithoutFinalSnapshot(opType));
		return client;
	}

	/** A mock s3 driver factory: a metadata-mode step needs one at its start. */
	@SuppressWarnings("unchecked")
	private static List<Extension> extensions() {
		final StorageDriverFactory<?, ?, ?> s3 = mock(StorageDriverFactory.class);
		when(s3.id()).thenReturn("s3");
		try {
			when(s3.create(anyString(), any(), any(), anyBoolean(), anyInt())).thenAnswer(call -> mock(StorageDriver.class));
		} catch (final Exception e) {
			throw new AssertionError(e);
		}
		return List.of(s3);
	}

	/** As a step's own metrics context before its aggregator started: it has no final snapshot. */
	@SuppressWarnings("unchecked")
	private static MetricsContext<AllMetricsSnapshot> metricsWithoutFinalSnapshot(final OpType opType) {
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		when(metrics.opType()).thenReturn(opType);
		return metrics;
	}

	@SuppressWarnings("unchecked")
	private static LoadStepFactory<LoadStep, ?> entryFactory(final LoadStep entry) {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(entry);
		return factory;
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

	private Config deleteConfig(final boolean metadata, final boolean durationMode) {
		final Config config = baseConfig();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		if (durationMode) {
			config.val("load-op-delete-duration", true);
			config.val("load-step-limit-time", "60s");
		} else {
			config.val("load-op-delete-duration", false);
			config.val("load-op-limit-count", 10L);
			config.val("load-step-limit-time", "0s");
		}
		if (metadata) {
			config.val("run-id", 7L);
			config.val("storage-driver-type", "s3");
			config.val("storage-integrity-mode", "metadata");
			config.val("storage-integrity-input-provenance", "external");
		}
		return config;
	}

	private Config readConfig() {
		final Config config = baseConfig();
		config.val("load-op-type", "read");
		config.val("storage-integrity-input-provenance", "engine_step");
		return config;
	}

	private Config createConfig() {
		final Config config = baseConfig();
		config.val("load-op-type", "create");
		return config;
	}

	private Config baseConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-node-addrs", List.of());
		config.val("output-metrics-timing-persist", false);
		config.val("output-metrics-trace-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		return config;
	}

	/** Runs closeAll and returns the close failures it logged for this class's steps. */
	private static List<String> closeAllFailures() throws InterruptedException {
		final var sentinel = new Throwable("closeAll test sentinel", null, false, false) {};
		final var appender = new CloseFailureAppender(sentinel);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		// An earlier test in this JVM may have left the logger above WARN.
		final var level = logger.getLevel();
		Configurator.setLevel(Loggers.ERR.getName(), Level.WARN);
		logger.addAppender(appender);
		try {
			DaemonBase.closeAll();
			// The engine's loggers are asynchronous but keep their order: a record closeAll logged arrives first.
			Loggers.ERR.warn("closeAll test sentinel", sentinel);
			assertTrue(appender.awaitSentinel(WAIT_SECONDS, TimeUnit.SECONDS), "the sentinel record never arrived");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
			Configurator.setLevel(Loggers.ERR.getName(), level);
		}
		return appender.failures();
	}

	private static final class CloseFailureAppender extends AbstractAppender {
		private final List<String> failures = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch sentinelSeen = new CountDownLatch(1);
		private final Throwable sentinel;

		CloseFailureAppender(final Throwable sentinel) {
			super("testFailedStartCloseFailures", null, null, true, Property.EMPTY_ARRAY);
			this.sentinel = sentinel;
		}

		@Override
		public void append(final LogEvent event) {
			if (event.getThrown() == sentinel) {
				sentinelSeen.countDown();
				return;
			}
			final var message = event.getMessage().getFormattedMessage();
			// Another test class may leave its own daemons registered for closeAll.
			if (Level.WARN.equals(event.getLevel()) && message.startsWith("Failed to close the daemon instance")
							&& message.contains(FailedStartClient.class.getName())) {
				failures.add(message + ": " + event.getThrown());
			}
		}

		boolean awaitSentinel(final long timeout, final TimeUnit unit) throws InterruptedException {
			return sentinelSeen.await(timeout, unit);
		}

		List<String> failures() {
			return List.copyOf(failures);
		}
	}

	/** A standalone DELETE client that aggregates DELETE artifacts, as the engine's linear client does. */
	static final class FailedStartClient extends TestLoadStepClient {
		FailedStartClient(final Config config, final List<Extension> extensions, final MetricsManager metricsMgr) {
			super(config, extensions, null, metricsMgr);
		}

		@Override
		protected boolean supportsStandaloneDeleteArtifactAggregation() {
			return true;
		}
	}
}
