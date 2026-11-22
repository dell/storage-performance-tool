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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.concurrent.JvmExitTestSupport;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
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
import java.rmi.NoSuchObjectException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * A start that ends as a stop publishes the slice files of the slices whose start returned, and
 * nothing when none did: a slice refused at JVM exit, never reached, or cut partway through its
 * start is left out. A start that fails after a slice started still needs every started slice's
 * files; one that fails before any slice's start returned publishes nothing, as a stopped one.
 */
class StoppedStartSliceArtifactsTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final String WORKER_FILE = "/w/operation.lifecycle.csv";
	private final String stepId = "stopped-start-artifacts-" + UUID.randomUUID();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	private Path stepDir;
	private Path canonical;
	private String originalRmiHostname;
	private LoadStepManagerServiceImpl node;

	@AfterEach
	void cleanUp() throws Exception {
		// The start's interrupt must not leak into the next test.
		Thread.interrupted();
		try {
			if (node != null && !node.isClosed()) {
				node.close();
			}
		} finally {
			if (node != null) {
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
		}
	}

	@Test
	void aSliceRefusedAtJvmExitAfterTheAggregatorsWereBuiltClosesWithNothingPublished() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		// As LoadStepLocalBase does when the JVM exit began: the slice never ran and writes nothing.
		doAnswer(call -> {
			throw new InterruptedException(stepId + ": not started, the engine is exiting");
		}).when(entry).start();
		final TestLoadStepClient client = client(config(), entry, null);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();

		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertFalse(Files.exists(canonical), "the step that never started published a lifecycle file");
		assertEquals(List.of(), nodeSources());
	}

	@Test
	void aRunWhoseOnlySliceIsRefusedEndsAsAStopWithoutACloseFailure() throws Exception {
		final LoadStep entry = mock(LoadStep.class);
		doAnswer(call -> {
			throw new InterruptedException(stepId + ": not started, the engine is exiting");
		}).when(entry).start();
		final TestLoadStepClient client = client(config(), entry, null);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::run));
		Thread.interrupted();

		assertInstanceOf(InterruptedException.class, client.runFailure());
		assertTrue(client.isClosed());
		// The close at the end of the run raised no failure: a failed close would leave its files.
		assertFalse(Files.exists(canonical));
		assertEquals(List.of(), nodeSources());
	}

	@Test
	void aStopThatCutsTheSecondSlicesStartPublishesTheFirstSliceOnly() throws Exception {
		final LoadStep entry = sliceWritingItsLifecycleRowAtClose("entry-worker");
		final AtomicBoolean workerWrote = new AtomicBoolean();
		final TestLoadStepClient client = twoSliceClient(entry, workerWrote, false);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();

		assertDoesNotThrow(client::close);
		assertEquals(List.of("entry-worker"), workerIds(canonical));
		assertEquals(List.of(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 0)), nodeSources());
	}

	@Test
	void aStoppedStartWithAnUnstartedRemoteSliceFailingTeardownStillPublishesTheEntrySlice() throws Exception {
		final LoadStep entry = sliceWritingItsLifecycleRowAtClose("entry-worker");
		final AtomicBoolean workerWrote = new AtomicBoolean();
		final TestLoadStepClient client = twoSliceClient(entry, workerWrote, false);

		try (MockedStatic<LoadStepSliceUtil> util = mockStatic(LoadStepSliceUtil.class, CALLS_REAL_METHODS)) {
			util.when(() -> LoadStepSliceUtil.resolveRemote(any(), anyList(), anyString(), anyString()))
							.thenAnswer(invocation -> {
								final LoadStepService stub = (LoadStepService) invocation.callRealMethod();
								return (LoadStepService) Proxy.newProxyInstance(
												LoadStepService.class.getClassLoader(),
												new Class<?>[]{LoadStepService.class
								},
												(proxy, method, args) -> {
													if (method.getDeclaringClass() == Object.class) {
														return switch (method.getName()) {
														case "equals" -> proxy == args[0];
														case "hashCode" -> System.identityHashCode(proxy);
														default -> "unstarted slice";
														};
													}
													if ("stop".equals(method.getName()) || "close".equals(method.getName())) {
														throw new NoSuchObjectException("injected: unstarted slice released");
													}
													try {
														return method.invoke(stub, args);
													} catch (final InvocationTargetException e) {
														throw e.getCause();
													}
												});
							});
			assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		}
		Thread.interrupted();

		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertEquals(List.of("entry-worker"), workerIds(canonical));
		assertEquals(List.of(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 0)), nodeSources());
	}

	@Test
	void aSliceCutPartwayThroughItsStartIsNotMergedEvenWhenItWroteItsFile() throws Exception {
		final LoadStep entry = sliceWritingItsLifecycleRowAtClose("entry-worker");
		final AtomicBoolean workerWrote = new AtomicBoolean();
		final TestLoadStepClient client = twoSliceClient(entry, workerWrote, true);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();
		assertTrue(workerWrote.get(), "the cut slice must have written its file before the stop");

		assertDoesNotThrow(client::close);
		assertEquals(List.of("entry-worker"), workerIds(canonical));
		assertEquals(List.of(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 0)), nodeSources(),
						"the cut slice's file was fetched");
	}

	@Test
	void anEntrySliceCutPartwayThroughItsStartLeavesItsOwnFileUnmerged() throws Exception {
		final LoadStep entry = sliceWritingItsLifecycleRowAtClose("entry-worker");
		// The stop interrupts the starting thread, and the slice's start gives up partway.
		doAnswer(call -> {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("cut short");
		}).when(entry).start();
		final TestLoadStepClient client = client(config(), entry, null);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
		Thread.interrupted();

		assertDoesNotThrow(client::close);
		// The slice wrote its file in place at its close, and the client merged nothing of it.
		assertEquals(List.of("entry-worker"), workerIds(canonical));
		assertEquals(List.of(), nodeSources());
	}

	@Test
	void aRestartRefusedAtJvmExitKeepsTheSlicesOfTheStartBeforeIt() throws Exception {
		final LoadStep entry = sliceWritingItsLifecycleRowAtClose("entry-worker");
		final AtomicBoolean workerWrote = new AtomicBoolean();
		final TestLoadStepClient client = twoSliceClientStartingNormally(entry, workerWrote);
		try {
			client.start();
			// The other node's slice ran and wrote its file, as a started slice does.
			workerWrote.set(true);
			client.stop();
			JvmExit.markBegun();
			// A script that starts the stopped step again while the JVM exits: the base class refuses
			// it as a stop, and the close must still publish the slices that did start.
			assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, client::start));
			Thread.interrupted();

			assertDoesNotThrow(client::close);
			assertEquals(List.of("entry-worker", "cut-worker"), workerIds(canonical));
		} finally {
			JvmExitTestSupport.reset();
		}
	}

	@Test
	void aStartThatFailsInItsFirstSlicePublishesNothing() throws Exception {
		// A slice whose own start threw did not start: the step closes as one stopped before its first slice.
		final LoadStep entry = mock(LoadStep.class);
		doAnswer(call -> {
			throw new IllegalStateException("the slice failed to start");
		}).when(entry).start();
		final TestLoadStepClient client = client(config(), entry, null);

		final Throwable startFailure = assertThrows(Throwable.class, client::start);
		assertFalse(startFailure instanceof InterruptedException, startFailure.toString());
		assertFalse(Thread.currentThread().isInterrupted());

		assertDoesNotThrow(client::close);
		assertTrue(client.isClosed());
		assertFalse(Files.exists(canonical), "a lifecycle file was published for a step that started no slice");
		assertEquals(List.of(), nodeSources());
	}

	/**
	 * A step on this node and one other, both slices mocks. The other node's slice interrupts the
	 * starting thread, as a stop does, and its start then fails; with {@code workerWrites} it wrote
	 * its lifecycle file on its node first.
	 */
	@SuppressWarnings("unchecked")
	private TestLoadStepClient twoSliceClient(
					final LoadStep entry, final AtomicBoolean workerWrote, final boolean workerWrites) throws Exception {
		final Thread starter = Thread.currentThread();
		final LoadStep worker = mock(LoadStep.class);
		doAnswer(call -> {
			workerWrote.set(workerWrites);
			starter.interrupt();
			throw new IllegalStateException("the node is going away");
		}).when(worker).start();
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		final String nodeAddr = "127.0.0.1:" + port;
		final Config config = config();
		config.val("load-step-node-addrs", List.of(nodeAddr));
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		final TestLoadStepClient client = client(config, entry, worker);
		node = new LoadStepManagerServiceImpl(port, List.of(factory), metricsMgr);
		node.start();
		return client.withNodeFileManagers(Map.of(nodeAddr, workerFiles(workerWrote)));
	}

	/** As {@link #twoSliceClient}, but the other node's slice starts normally; its file is there once {@code workerWrote}. */
	private TestLoadStepClient twoSliceClientStartingNormally(final LoadStep entry, final AtomicBoolean workerWrote)
					throws Exception {
		final LoadStep worker = mock(LoadStep.class);
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		final String nodeAddr = "127.0.0.1:" + port;
		final Config config = config();
		config.val("load-step-node-addrs", List.of(nodeAddr));
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		final TestLoadStepClient client = client(config, entry, worker);
		node = new LoadStepManagerServiceImpl(port, List.of(factory), metricsMgr);
		node.start();
		return client.withNodeFileManagers(Map.of(nodeAddr, workerFiles(workerWrote)));
	}

	private LoadStepFactory<LoadStep, ?> factory;

	@SuppressWarnings("unchecked")
	private TestLoadStepClient client(final Config config, final LoadStep entry, final LoadStep worker)
					throws IOException {
		canonical = Path.of(FileManager.INSTANCE.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId))
						.toAbsolutePath();
		stepDir = canonical.getParent();
		factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		final var created = new java.util.concurrent.atomic.AtomicInteger();
		when(factory.createLocal(any(), anyList(), anyList(), any()))
						.thenAnswer(call -> created.getAndIncrement() == 0 ? entry : worker);
		return new TestLoadStepClient(config, List.of(factory), null, metricsMgr);
	}

	/** The other node's files: its lifecycle file is there once its slice wrote it. */
	private FileManager workerFiles(final AtomicBoolean written) throws IOException {
		final FileManager files = mock(FileManager.class);
		when(files.logFileName(anyString(), eq(stepId))).thenAnswer(call -> "/w/" + call.getArgument(0));
		when(files.logFileName(Loggers.OPERATION_LIFECYCLE.getName(), stepId)).thenReturn(WORKER_FILE);
		when(files.fileSize(anyString())).thenReturn(0L);
		final byte[] body = lifecycleFile("cut-worker").getBytes(StandardCharsets.UTF_8);
		when(files.readFromFile(anyString(), anyLong())).thenAnswer(call -> {
			final String name = call.getArgument(0);
			final long offset = call.getArgument(1);
			if (!WORKER_FILE.equals(name) || !written.get()) {
				throw new NoSuchFileException(name);
			}
			if (offset >= body.length) {
				throw new EOFException();
			}
			return body;
		});
		return files;
	}

	/** A started slice that, as a local slice does, writes its lifecycle file in place when it closes. */
	private LoadStep sliceWritingItsLifecycleRowAtClose(final String workerId) throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		doAnswer(call -> {
			Files.createDirectories(stepDir);
			Files.writeString(canonical, lifecycleFile(workerId));
			return null;
		}).when(slice).close();
		return slice;
	}

	private String lifecycleFile(final String workerId) {
		final var counters = new OperationLifecycleCounters(true, 0, 0, 0, 0, 0, 0, 0, 0, 0);
		return OperationLifecycleArtifact.HEADER + "\n"
						+ OperationLifecycleArtifact.row(1, stepId, workerId, List.of(counters), true) + "\n";
	}

	private static List<String> workerIds(final Path file) throws IOException {
		try (final var parser = CSVFormat.RFC4180.builder().setHeader().get().parse(Files.newBufferedReader(file))) {
			return parser.getRecords().stream().map(row -> row.get("worker_id")).toList();
		}
	}

	private List<Path> nodeSources() throws IOException {
		if (!Files.isDirectory(stepDir)) {
			return List.of();
		}
		try (Stream<Path> files = Files.list(stepDir)) {
			return files.filter(file -> file.getFileName().toString().startsWith("operation.lifecycle.node-"))
							.sorted()
							.toList();
		}
	}

	private Config config() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-op-type", "create");
		config.val("load-step-node-addrs", List.of());
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		return config;
	}
}
