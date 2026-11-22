package com.dell.spt.base.load.step.client;

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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.nic.EventLoopsArtifact;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A stop that reaches the entry while one of its slice calls is in progress, and that the call does
 * not end (an RMI call returns when the node answers, whatever the interrupt), ends the start once
 * that call returns: no further slice is resolved or started, and the start's own work after its
 * slices does not run. The stop is a signal (the exit hook marks the JVM exit and interrupts the
 * starting thread) or a node API stop (an interrupt only). The close then handles the slices that
 * never started: it reads no file of theirs and publishes only the started slices' output.
 *
 * <p>The slices are mocks. A remote slice runs on a real step manager service of this JVM, so its
 * calls go through RMI as on a worker.
 */
@Timeout(120)
class StopBetweenSliceStartsTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private final String stepId = "stop-between-slice-starts-" + UUID.randomUUID();
	private final String token = UUID.randomUUID().toString();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	private final List<LoadStepManagerServiceImpl> nodes = new ArrayList<>();
	private final Map<String, FileManager> nodeFiles = new LinkedHashMap<>();
	private final List<LoadStepFactory<LoadStep, ?>> nodeFactories = new ArrayList<>();
	private final List<LoadStep> remoteSlices = new ArrayList<>();
	private final Thread starter = Thread.currentThread();
	private String originalRmiHostname;
	private Path stepDir;
	private Path tempDir;

	@AfterEach
	void cleanUp() throws Exception {
		// The start's interrupt must not leak into the next test.
		Thread.interrupted();
		JvmExitTestSupport.reset();
		try {
			for (final var node : nodes) {
				if (!node.isClosed()) {
					node.close();
				}
			}
		} finally {
			if (!nodes.isEmpty()) {
				ServiceUtil.shutdown();
				if (originalRmiHostname == null) {
					System.clearProperty(RMI_HOSTNAME);
				} else {
					System.setProperty(RMI_HOSTNAME, originalRmiHostname);
				}
			}
			if (stepDir != null) {
				StepIdReuseAtStartTest.removeFolder(stepDir);
			}
			if (tempDir != null) {
				StepIdReuseAtStartTest.removeFolder(tempDir);
			}
			// The timing aggregator collects each started node's timings beside the entry's own file.
			final Path timings = Path.of(System.getProperty("java.io.tmpdir"), "spt");
			if (Files.isDirectory(timings)) {
				try (Stream<Path> files = Files.list(timings)) {
					for (final Path file : files.filter(f -> f.getFileName().toString().startsWith("timingMetrics_" + stepId)).toList()) {
						Files.deleteIfExists(file);
					}
				}
			}
		}
	}

	@Test
	void aSignalDuringTheFirstNodesStartResolvesNoFurtherSlice() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep second = mock(LoadStep.class);
		// The exit hook marks the exit while the node's start is in progress; the call then returns.
		doAnswer(call -> {
			JvmExit.markBegun();
			return null;
		}).when(first).start();
		final TestLoadStepClient client = client(config(), entry, first, second);

		assertStartStopped(client);
		Thread.interrupted();

		assertNotResolved(1);
		assertDoesNotThrow(client::close);
		verify(entry).close();
		verify(first).close();
		verify(second, never()).start();
	}

	@Test
	void aNodeApiStopDuringTheFirstNodesStartResolvesNoFurtherSlice() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep second = mock(LoadStep.class);
		// As a node API stop does: it interrupts the starting thread, and the JVM does not exit.
		interruptsTheStarterAndReturns(first);
		final TestLoadStepClient client = client(config(), entry, first, second);

		assertStartStopped(client);
		Thread.interrupted();

		assertFalse(JvmExit.begun());
		assertNotResolved(1);
		assertDoesNotThrow(client::close);
		verify(first).close();
	}

	@Test
	void aStopDuringTheFirstNodesResolveNeverStartsItsSliceAndClosesIt() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep second = mock(LoadStep.class);
		final TestLoadStepClient client = client(config(), entry, first, second);
		// The stop lands while the node creates its slice; the call then returns the slice.
		when(nodeFactories.get(0).createLocal(any(), anyList(), anyList(), any())).thenAnswer(call -> {
			starter.interrupt();
			return first;
		});

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry).start();
		verify(first, never()).start();
		assertNotResolved(1);
		assertDoesNotThrow(client::close);
		// The slice resolved but never started is not left open on its node.
		verify(first).close();
	}

	@Test
	void aStopDuringTheEntrySlicesStartResolvesNoRemoteSlice() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep second = mock(LoadStep.class);
		interruptsTheStarterAndReturns(entry);
		final TestLoadStepClient client = client(config(), entry, first, second);

		assertStartStopped(client);
		Thread.interrupted();

		assertNotResolved(0);
		assertNotResolved(1);
		assertDoesNotThrow(client::close);
		verify(entry).close();
	}

	@Test
	void aStopDuringTheLastSlicesStartEndsTheStartAsAStop() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep last = mock(LoadStep.class);
		interruptsTheStarterAndReturns(last);
		final TestLoadStepClient client = client(config(), entry, last);

		assertStartStopped(client);
		Thread.interrupted();

		// The start's work after its slices did not run: the step never counts as started.
		assertFalse(client.isStarted(), String.valueOf(client.state()));
		assertDoesNotThrow(client::close);
		verify(entry).close();
		verify(last).close();
	}

	@Test
	void aStoppedStartWithItemOutputTracesAndTimingsReadsNoFileOfTheSliceThatNeverStarted() throws Exception {
		tempDir = Files.createTempDirectory("stop-between-slice-starts");
		final Config config = config();
		final Path itemOutput = tempDir.resolve("items.csv");
		config.val("item-output-file", itemOutput.toString());
		config.val("output-metrics-trace-persist", true);
		config.val("output-metrics-timing-persist", true);
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep second = mock(LoadStep.class);
		interruptsTheStarterAndReturns(first);
		final TestLoadStepClient client = client(config, entry, first, second);
		final List<FileManager> files = List.copyOf(nodeFiles.values());

		assertStartStopped(client);
		Thread.interrupted();
		assertNotResolved(1);
		clearInvocations(files.get(0), files.get(1));

		final List<String> warnings = warningsWhile(() -> assertDoesNotThrow(client::close));

		assertEquals(List.of(), warnings);
		// The started node's item output, traces and timings are collected; nothing of the other.
		verify(files.get(0), atLeastOnce()).readFromFile(remoteFile(0, "items"), 0);
		verify(files.get(0), atLeastOnce()).readFromFile(remoteFile(0, Loggers.OP_TRACES.getName()), 0);
		verify(files.get(1), never()).readFromFile(anyString(), anyLong());
		verify(files.get(1), never()).deleteFile(anyString());
		assertTrue(Files.isRegularFile(itemOutput));
	}

	@Test
	void aStartStoppedBeforeAnySliceStartedHandsNoItemOutputToTheStep() throws Exception {
		tempDir = Files.createTempDirectory("stop-between-slice-starts");
		final Config config = config();
		config.val("item-output-file", tempDir.resolve("items.csv").toString());
		final LoadStep entry = mock(LoadStep.class);
		final List<String> collected = new CopyOnWriteArrayList<>();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, List.of(entryFactoryInterruptingTheStarter(entry)), null, metricsMgr) {
			@Override
			protected void itemOutputCollected(final String file, final Map<OpType, Long> successCounts) {
				collected.add(file);
			}
		};

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry, never()).start();
		assertDoesNotThrow(client::close);
		assertEquals(List.of(), collected);
	}

	@Test
	void aMetadataStepStoppedBeforeAnySliceStartedClosesWithNothingPublished() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(7L);
		final TestLoadStepClient client = metadataClient(entryFactoryInterruptingTheStarter(entry));

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry, never()).start();
		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertFalse(Files.exists(lifecycleFile()), "the step that started no slice published a lifecycle file");
	}

	@Test
	void aMetadataStepStoppedAfterASliceStartedStillFailsClosed() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenReturn(7L);
		interruptsTheStarterAndReturns(entry);
		final TestLoadStepClient client = metadataClient(entryFactory(entry));

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry).start();
		final var failure = assertThrows(IntegrityTerminalException.class, client::close);
		assertTrue(failure.getMessage().contains("failed to capture terminal metadata-mode operation failure counts"),
						failure.getMessage());
	}

	@Test
	void aSignalWhileTheEntryCreatesItsSliceLeavesNoFileOfThatSlice() throws Exception {
		final Config config = config();
		config.val("load-op-type", "create");
		final List<LoadStep> created = new CopyOnWriteArrayList<>();
		final LoadStepFactory<LoadStep, ?> entryFactory = realSliceFactory(created, () -> {
			JvmExit.markBegun();
			starter.interrupt();
		});
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(entryFactory), null, metricsMgr);
		stepDir = lifecycleFile().getParent();

		assertStartStopped(client);
		Thread.interrupted();

		assertEquals(1, created.size());
		assertDoesNotThrow(client::close);
		assertNoSliceFiles();
	}

	@Test
	void aMetadataStepStoppedWhileTheEntryCreatesItsSliceClosesWithNothingPublished() throws Exception {
		final List<LoadStep> created = new CopyOnWriteArrayList<>();
		final TestLoadStepClient client = metadataClient(realSliceFactory(created, starter::interrupt));

		assertStartStopped(client);
		Thread.interrupted();

		assertEquals(1, created.size());
		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertNoSliceFiles();
	}

	@Test
	void aStopWhileAWorkerCreatesItsSliceLeavesNoFileOfThatSliceOnTheWorker() throws Exception {
		// The entry's slice is a mock that writes nothing; the worker's is real and would write both files.
		final Config config = config();
		final LoadStep entry = mock(LoadStep.class);
		final Config workerConfig = config();
		workerConfig.val("load-op-type", "create");
		final LinearLoadStepLocal worker = new LinearLoadStepLocal(workerConfig, List.of(), List.of(), metricsMgr);
		final TestLoadStepClient client = client(config, entry, worker);
		// The stop lands while the worker's step manager creates the slice it then exports.
		when(nodeFactories.get(0).createLocal(any(), anyList(), anyList(), any())).thenAnswer(call -> {
			starter.interrupt();
			return worker;
		});

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry).start();
		assertDoesNotThrow(client::close);
		assertTrue(worker.isClosed(), "the slice resolved but never started was left open on its worker");
		// Neither file is left on the worker to refuse a later step with this id there.
		assertNoSliceFiles();
	}

	@Test
	void aStopJustBeforeTheSlicesCreatesNoSlice() throws Exception {
		final LoadStepFactory<LoadStep, ?> entryFactory = entryFactory(mock(LoadStep.class));
		final TestLoadStepClient client = new TestLoadStepClient(config(), List.of(entryFactory), null, metricsMgr) {
			@Override
			public String getTypeName() throws java.rmi.RemoteException {
				// Read once, by the start, just before it creates the first slice.
				starter.interrupt();
				return super.getTypeName();
			}
		};

		assertStartStopped(client);
		Thread.interrupted();

		verify(entryFactory, never()).createLocal(any(), anyList(), anyList(), any());
		assertDoesNotThrow(client::close);
	}

	@Test
	void aStopDuringTheRunIdCheckNeverStartsThatSlice() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		// The stop lands while the start asks the slice for its run id; the call then returns it.
		when(entry.runId()).thenAnswer(call -> {
			starter.interrupt();
			return 7L;
		});
		final TestLoadStepClient client = metadataClient(entryFactory(entry));

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry).runId();
		verify(entry, never()).start();
		assertDoesNotThrow(client::close);
		verify(entry).close();
	}

	@Test
	void aStopDuringTheArmOfANicSetStepNeverStartsThatSlice() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		// The stop lands while the start arms the slice's admission; the call then returns.
		doAnswer(call -> {
			starter.interrupt();
			return null;
		}).when(entry).startPreparation(anyLong(), anyLong());
		final Config config = config();
		config.val("storage-net-interfaces", List.of("eth9"));
		final TestLoadStepClient client = client(config, entry);

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry).startPreparation(anyLong(), anyLong());
		verify(entry, never()).start();
		verify(entry).fencePreparation(anyLong());
		assertDoesNotThrow(client::close);
		verify(entry).close();
	}

	@Test
	void aStopDuringTheRunIdCheckOfANicSetStepNeverArmsThatSlice() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		when(entry.runId()).thenAnswer(call -> {
			starter.interrupt();
			return 7L;
		});
		final TestLoadStepClient client = metadataClient(entryFactory(entry), true);

		assertStartStopped(client);
		Thread.interrupted();

		verify(entry).runId();
		verify(entry, never()).startPreparation(anyLong(), anyLong());
		verify(entry, never()).start();
		assertDoesNotThrow(client::close);
		verify(entry).close();
	}

	@Test
	void aStartWhoseArmFailsBeforeTheSliceStartedClosesCleanly() throws Exception {
		// A failure, not a stop: the slice never started, so it wrote no lifecycle file to collect.
		final Config config = config();
		config.val("load-op-type", "create");
		config.val("storage-net-interfaces", List.of("eth9"));
		final LoadStepFactory<LoadStep, ?> factory = realSliceFactory(new CopyOnWriteArrayList<>(), () -> {},
						"startPreparation", () -> {
							throw new java.rmi.RemoteException("the arm call failed");
						});
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(factory), null, metricsMgr);
		stepDir = lifecycleFile().getParent();

		final Throwable failure = assertThrows(Throwable.class, client::start);
		assertFalse(failure instanceof InterruptedException, failure::toString);
		Thread.interrupted();

		assertDoesNotThrow(client::close);
		assertNoSliceFiles();
	}

	@Test
	void aMetadataStartWhoseRunIdCheckFailsFailsClosedWithoutALifecycleFailure() throws Exception {
		final LoadStepFactory<LoadStep, ?> factory = realSliceFactory(new CopyOnWriteArrayList<>(), () -> {},
						"runId", () -> 8L);
		final TestLoadStepClient client = metadataClient(factory);

		final Throwable failure = assertThrows(IntegrityTerminalException.class, client::start);
		assertTrue(failure.getMessage().contains("run.id mismatch"), failure.getMessage());
		Thread.interrupted();

		final var closeFailure = assertThrows(IntegrityTerminalException.class, client::close);
		assertTrue(closeFailure.getMessage().contains("failed to capture terminal metadata-mode operation failure counts"),
						closeFailure.getMessage());
		for (final Throwable suppressed : closeFailure.getSuppressed()) {
			assertFalse(String.valueOf(suppressed.getMessage()).contains(OperationLifecycleArtifact.FILE_NAME),
							suppressed::toString);
		}
	}

	@Test
	void aStartWhoseLastSliceFailsToStartStillCollectsEverySlicesLifecycleFile() throws Exception {
		// The start reached the failing slice, so that slice may have written its file: it is collected too.
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep worker = mock(LoadStep.class);
		doAnswer(call -> {
			throw new IllegalStateException("the start failed");
		}).when(worker).start();
		final TestLoadStepClient client = lifecycleClient(false, entry, worker);
		servesItsLifecycleFile(0);

		final Throwable failure = assertThrows(Throwable.class, client::start);
		assertFalse(failure instanceof InterruptedException, failure::toString);
		Thread.interrupted();

		verify(worker).start();
		assertDoesNotThrow(client::close);
		assertTrue(Files.exists(lifecycleFile()), "no lifecycle file was published");
		assertTrue(Files.exists(nodeLifecycleFile(0)), "the entry slice's lifecycle file was not collected");
		assertTrue(Files.exists(nodeLifecycleFile(1)), "the failed slice's lifecycle file was not collected");
	}

	@Test
	void aStartWhoseSecondSliceFailsItsArmStillCollectsTheStartedSlicesLifecycleFile() throws Exception {
		// A failure, not a stop: the entry slice started and wrote its file; worker 1's never started.
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep worker = mock(LoadStep.class);
		doAnswer(call -> {
			throw new IllegalStateException("the arm failed");
		}).when(worker).startPreparation(anyLong(), anyLong());
		final TestLoadStepClient client = lifecycleClient(true, entry, worker);

		final Throwable failure = assertThrows(Throwable.class, client::start);
		assertFalse(failure instanceof InterruptedException, failure::toString);
		Thread.interrupted();

		verify(entry).start();
		verify(worker, never()).start();
		assertDoesNotThrow(client::close);
		assertTrue(Files.exists(lifecycleFile()), "no lifecycle file was published");
		assertTrue(Files.exists(nodeLifecycleFile(0)), "the started entry slice's lifecycle file was not collected");
		assertFalse(Files.exists(nodeLifecycleFile(1)), "the slice that never started has a lifecycle file");
	}

	/**
	 * A factory that creates a real slice of the configuration it is given, after {@code stop}, as a
	 * stop that lands while the slice is created.
	 */
	@SuppressWarnings("unchecked")
	private static LoadStepFactory<LoadStep, ?> realSliceFactory(final List<LoadStep> created, final Runnable stop)
					throws Exception {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenAnswer(call -> {
			stop.run();
			final LoadStep slice = new LinearLoadStepLocal(
							call.getArgument(0), call.getArgument(1), call.getArgument(2), call.getArgument(3));
			created.add(slice);
			return slice;
		});
		return factory;
	}

	/**
	 * As {@link #realSliceFactory(List, Runnable)}, but each slice's {@code method} calls {@code answer}
	 * in its place.
	 */
	@SuppressWarnings("unchecked")
	private static LoadStepFactory<LoadStep, ?> realSliceFactory(
					final List<LoadStep> created, final Runnable stop, final String method, final Callable<Object> answer)
					throws Exception {
		final LoadStepFactory<LoadStep, ?> real = realSliceFactory(created, stop);
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenAnswer(call -> {
			final LoadStep slice = real.createLocal(
							call.getArgument(0), call.getArgument(1), call.getArgument(2), call.getArgument(3));
			final Class<?>[] types = {LoadStep.class
			};
			return (LoadStep) Proxy.newProxyInstance(LoadStep.class.getClassLoader(), types, (proxy, called, args) -> {
				if (method.equals(called.getName())) {
					return answer.call();
				}
				try {
					return called.invoke(slice, args);
				} catch (final InvocationTargetException e) {
					throw e.getCause();
				}
			});
		});
		return factory;
	}

	/**
	 * A CREATE step, so with an operation lifecycle file, of {@code entry} and {@code workers}, with a
	 * NIC set when {@code nicSet}. The entry slice's start writes its lifecycle file, with no row.
	 */
	private TestLoadStepClient lifecycleClient(final boolean nicSet, final LoadStep entry, final LoadStep... workers)
					throws Exception {
		final Config config = config();
		config.val("load-op-type", "create");
		if (nicSet) {
			config.val("storage-net-interfaces", List.of("eth9"));
		}
		doAnswer(call -> {
			Files.createDirectories(lifecycleFile().getParent());
			Files.writeString(lifecycleFile(), OperationLifecycleArtifact.HEADER + "\n");
			return null;
		}).when(entry).start();
		return client(config, entry, workers);
	}

	/**
	 * Remote node {@code node} (from 0) holds its slice's lifecycle file, with no row, once the start
	 * reached that slice, even when the start then failed.
	 */
	private void servesItsLifecycleFile(final int node) throws IOException {
		final FileManagerService files = (FileManagerService) nodeFiles.values().stream().skip(node).findFirst().orElseThrow();
		final String name = remoteFile(node, Loggers.OPERATION_LIFECYCLE.getName());
		final byte[] header = (OperationLifecycleArtifact.HEADER + "\n").getBytes(StandardCharsets.UTF_8);
		doAnswer(call -> {
			if (!started(node)) {
				throw new NoSuchFileException(name + " (" + token + ")");
			}
			final long offset = call.getArgument(1);
			return offset == 0 ? header : new byte[0];
		}).when(files).readFromFile(eq(name), anyLong());
	}

	/** The lifecycle file of slice {@code slice} (the entry's is 0) as the close collected it. */
	private Path nodeLifecycleFile(final int slice) throws IOException {
		return CsvLoggerArtifactAggregator.nodeSourcePath(lifecycleFile(), slice);
	}

	/** A slice that never started wrote neither its lifecycle file nor its event loops file. */
	private void assertNoSliceFiles() throws IOException {
		final Path lifecycle = lifecycleFile();
		assertFalse(Files.exists(lifecycle), "a slice that never started wrote " + lifecycle);
		final Path eventLoops = lifecycle.resolveSibling(EventLoopsArtifact.FILE_NAME);
		assertFalse(Files.exists(eventLoops), "a slice that never started wrote " + eventLoops);
	}

	/** The client's start ends as a stop, not as a failure. */
	private static void assertStartStopped(final TestLoadStepClient client) {
		final Throwable thrown = assertThrows(Throwable.class, client::start);
		assertInstanceOf(InterruptedException.class, thrown, () -> {
			final var causes = new StringBuilder();
			for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
				causes.append("\n\t").append(cause);
			}
			return causes.toString();
		});
	}

	private void interruptsTheStarterAndReturns(final LoadStep slice) throws Exception {
		doAnswer(call -> {
			starter.interrupt();
			return null;
		}).when(slice).start();
	}

	/** The slice of remote node {@code index} (from 0) was never created on its node. */
	private void assertNotResolved(final int index) {
		if (index >= nodeFactories.size()) {
			return;
		}
		try {
			verify(nodeFactories.get(index), never()).createLocal(any(), anyList(), anyList(), any());
		} catch (final Exception e) {
			throw new AssertionError(e);
		}
	}

	/**
	 * A step on this node and one other node per remote slice. Each node has its own step manager
	 * service, which creates that node's slice, and its own file manager.
	 */
	@SuppressWarnings("unchecked")
	private TestLoadStepClient client(final Config config, final LoadStep entry, final LoadStep... remotes)
					throws Exception {
		stepDir = lifecycleFile().getParent();
		final List<String> addrs = new ArrayList<>();
		if (remotes.length > 0) {
			originalRmiHostname = System.getProperty(RMI_HOSTNAME);
			System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		}
		for (var i = 0; i < remotes.length; i++) {
			final int port;
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				port = socket.getLocalPort();
			}
			final String addr = "127.0.0.1:" + port;
			addrs.add(addr);
			final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
			when(factory.id()).thenReturn("test-load-step");
			final LoadStep remote = remotes[i];
			remoteSlices.add(remote);
			when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(remote);
			nodeFactories.add(factory);
			final var node = new LoadStepManagerServiceImpl(port, List.of(factory), metricsMgr);
			node.start();
			nodes.add(node);
			nodeFiles.put(addr, nodeFiles(i));
		}
		config.val("load-step-node-addrs", addrs);
		return new TestLoadStepClient(config, List.of(entryFactory(entry)), null, metricsMgr)
						.withNodeFileManagers(nodeFiles);
	}

	private TestLoadStepClient metadataClient(final LoadStepFactory<LoadStep, ?> entryFactory) throws Exception {
		return metadataClient(entryFactory, false);
	}

	/** As {@link #metadataClient(LoadStepFactory)}, with a NIC set when {@code nicSet}. */
	@SuppressWarnings("unchecked")
	private TestLoadStepClient metadataClient(final LoadStepFactory<LoadStep, ?> entryFactory, final boolean nicSet)
					throws Exception {
		stepDir = lifecycleFile().getParent();
		final Config config = config();
		config.val("run-id", 7L);
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "metadata");
		config.val("load-op-type", "create");
		if (nicSet) {
			config.val("storage-net-interfaces", List.of("eth9"));
		}
		// A metadata-mode step needs an s3 driver at its start; this one is a mock that does nothing.
		final StorageDriverFactory<?, ?, ?> s3 = nicSet
						? mock(StorageDriverFactory.class, withSettings().extraInterfaces(NicSetDriverFactory.class))
						: mock(StorageDriverFactory.class);
		when(s3.id()).thenReturn("s3");
		when(s3.create(anyString(), any(), any(), anyBoolean(), anyInt())).thenAnswer(call -> mock(StorageDriver.class));
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(entryFactory, s3), null, metricsMgr);
		// As a step's own metrics context: it has no final snapshot, as the metrics aggregator never started.
		final MetricsContext<AllMetricsSnapshot> metrics = mock(MetricsContext.class);
		when(metrics.opType()).thenReturn(OpType.CREATE);
		client.addMetricsContextForTest(metrics);
		return client;
	}

	@SuppressWarnings("unchecked")
	private static LoadStepFactory<LoadStep, ?> entryFactory(final LoadStep entry) throws Exception {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(entry);
		return factory;
	}

	/** As {@link #entryFactory}, but the stop lands while the entry's slice is created. */
	private LoadStepFactory<LoadStep, ?> entryFactoryInterruptingTheStarter(final LoadStep entry) throws Exception {
		final LoadStepFactory<LoadStep, ?> factory = entryFactory(entry);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenAnswer(call -> {
			starter.interrupt();
			return entry;
		});
		return factory;
	}

	private String remoteFile(final int node, final String name) {
		return "/" + token + "/node-" + node + "/" + name;
	}

	/**
	 * A node's files. Each of its file names holds {@link #token}. Its item output, traces and
	 * timings are there, empty, once its slice started; before, as on a worker, they are not.
	 */
	private FileManagerService nodeFiles(final int node) throws IOException {
		final FileManagerService files = mock(FileManagerService.class);
		when(files.logFileName(anyString(), anyString()))
						.thenAnswer(call -> remoteFile(node, call.getArgument(0)));
		when(files.newTmpFileName()).thenReturn(remoteFile(node, "items"));
		when(files.fileSize(anyString())).thenReturn(0L);
		when(files.readFromFile(anyString(), anyLong())).thenAnswer(call -> {
			final String name = call.getArgument(0);
			final boolean output = name.equals(remoteFile(node, "items"))
							|| name.equals(remoteFile(node, Loggers.OP_TRACES.getName()))
							|| name.contains("timingMetrics_");
			if (!output || !started(node)) {
				throw new NoSuchFileException(name + " (" + token + ")");
			}
			throw new EOFException();
		});
		return files;
	}

	private boolean started(final int node) {
		return node < remoteSlices.size() && mockingDetails(remoteSlices.get(node)).getInvocations().stream()
						.anyMatch(call -> "start".equals(call.getMethod().getName()));
	}

	private Path lifecycleFile() throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)).toAbsolutePath();
	}

	private Config config() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		// No operation lifecycle artifact: the slices are mocks and write none.
		config.val("load-op-type", "noop");
		config.val("load-step-node-addrs", List.of());
		config.val("output-metrics-timing-persist", false);
		config.val("output-metrics-trace-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		return config;
	}

	/**
	 * The WARN and ERROR messages that name {@link #token} or this step (in the message or its
	 * cause) that the message and error loggers received while {@code action} ran.
	 */
	private List<String> warningsWhile(final Runnable action) throws InterruptedException {
		try (var levels = LoggerLevels.atMost(Level.WARN, Loggers.MSG.getName(), Loggers.ERR.getName())) {
			final var context = LoggerContext.getContext(false);
			final Logger messages = context.getLogger(Loggers.MSG.getName());
			final Logger errors = context.getLogger(Loggers.ERR.getName());
			final String endOfLog = token + ": end of log";
			final var capture = new LogCapture(List.of(token, stepId));
			capture.start();
			messages.addAppender(capture);
			errors.addAppender(capture);
			try {
				action.run();
				// The loggers may be asynchronous: this line comes after everything the action logged.
				Loggers.ERR.error(endOfLog);
				final String endLine = Level.ERROR + " " + endOfLog;
				final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
				while (!capture.lines.contains(endLine)) {
					assertTrue(System.nanoTime() < deadline, "the log did not reach its end: " + capture.lines);
					Thread.sleep(10);
				}
				return capture.lines.stream().filter(line -> !endLine.equals(line)).toList();
			} finally {
				messages.removeAppender(capture);
				errors.removeAppender(capture);
				capture.stop();
			}
		}
	}

	private static final class LogCapture extends AbstractAppender {

		private final List<String> tokens;
		private final List<String> lines = new CopyOnWriteArrayList<>();

		private LogCapture(final List<String> tokens) {
			super("stopBetweenSliceStartsCapture", null, null, true, Property.EMPTY_ARRAY);
			this.tokens = tokens;
		}

		@Override
		public void append(final LogEvent event) {
			if (!event.getLevel().isMoreSpecificThan(Level.WARN)) {
				return;
			}
			final String message = event.getMessage().getFormattedMessage();
			final String thrown = String.valueOf(event.getThrown());
			if (tokens.stream().anyMatch(token -> message.contains(token) || thrown.contains(token))) {
				lines.add(event.getLevel() + " " + message);
			}
		}
	}
}
