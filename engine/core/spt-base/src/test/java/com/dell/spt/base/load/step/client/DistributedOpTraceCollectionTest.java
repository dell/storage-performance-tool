package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Each engine writes one operation trace file per step id, whatever the number of the step's
 * contexts, so a traced distributed step collects each engine's file of each id once: a second
 * collector for the same file found it gone and logged a false I/O error.
 *
 * <p>The client step and the other node's step manager (RMI over the loopback interface) are the
 * real ones; both slices are mocks, and the other node's files are a fake file manager service.
 */
class DistributedOpTraceCollectionTest {

	private static final String RMI_HOSTNAME = "java.rmi.server.hostname";
	private static final byte[] TRACE_ROW = "1,trace-row\n".getBytes(StandardCharsets.UTF_8);
	private final String stepId = "op-trace-collection-" + UUID.randomUUID();
	private final MetricsManager metricsMgr = mock(MetricsManager.class);
	private String originalRmiHostname;
	private LoadStepManagerServiceImpl node;

	@AfterEach
	void cleanUp() throws Exception {
		try {
			if (node != null && !node.isClosed()) {
				node.close();
			}
		} finally {
			ServiceUtil.shutdown();
			if (originalRmiHostname == null) {
				System.clearProperty(RMI_HOSTNAME);
			} else {
				System.setProperty(RMI_HOSTNAME, originalRmiHostname);
			}
		}
	}

	/** A linear, mixed or pipeline step writes every context's traces under the step's one id. */
	@Test
	void twoTracedContextsCollectTheWorkersOneTraceFileOnce() throws Exception {
		final FileManagerService worker = workerFiles(Set.of(stepId));
		run(List.of(tracedContext(), tracedContext()), null, worker);

		verify(worker, times(1)).logFileName(Loggers.OP_TRACES.getName(), stepId);
		// One read of the row and one that reaches the end, by one collector.
		verify(worker, times(2)).readFromFile(eq(traceFile(stepId)), anyLong());
		verify(worker, times(1)).deleteFile(traceFile(stepId));
	}

	/** A step whose contexts run under their own ids, as the weighted step's do, collects each id once. */
	@Test
	void contextsWithTheirOwnIdsCollectOneTraceFilePerId() throws Exception {
		final String idA = stepId + "-wA";
		final FileManagerService worker = workerFiles(Set.of(idA, stepId));
		// The step's own id is the last context's, as the weighted step leaves it.
		run(List.of(tracedContext(), tracedContext()), List.of(idA, stepId), worker);

		for (final String id : List.of(idA, stepId)) {
			verify(worker, times(1)).logFileName(Loggers.OP_TRACES.getName(), id);
			verify(worker, times(2)).readFromFile(eq(traceFile(id)), anyLong());
			verify(worker, times(1)).deleteFile(traceFile(id));
		}
	}

	/** Each engine writes one timing metrics file per step id too, so it is fetched once, into one local copy. */
	@Test
	void twoTimedContextsCollectTheWorkersOneTimingFileOnce() throws Exception {
		final FileManagerService worker = workerFiles(Set.of(stepId));
		final Config config = config();
		config.val("output-metrics-trace-persist", false);
		config.val("output-metrics-timing-persist", true);
		final List<Config> contexts = List.of(baseConfig(), baseConfig());
		contexts.forEach(context -> context.val("output-metrics-timing-persist", true));
		try {
			run(config, contexts, null, worker);

			verify(worker, times(2)).readFromFile(eq(timingFile(stepId)), anyLong());
			verify(worker, times(1)).deleteFile(timingFile(stepId));
			assertEquals(1, localTimingCopies().size(), localTimingCopies().toString());
		} finally {
			for (final Path copy : localTimingCopies()) {
				Files.deleteIfExists(copy);
			}
		}
	}

	/** A step whose contexts run under their own ids fetches each id's timing metrics file once. */
	@Test
	void contextsWithTheirOwnIdsCollectOneTimingFilePerId() throws Exception {
		final String idA = stepId + "-wA";
		final FileManagerService worker = workerFiles(Set.of(idA, stepId));
		final Config config = config();
		config.val("output-metrics-trace-persist", false);
		config.val("output-metrics-timing-persist", true);
		final List<Config> contexts = List.of(baseConfig(), baseConfig());
		contexts.forEach(context -> context.val("output-metrics-timing-persist", true));
		try {
			run(config, contexts, List.of(idA, stepId), worker);

			for (final String id : List.of(idA, stepId)) {
				verify(worker, times(2)).readFromFile(eq(timingFile(id)), anyLong());
				verify(worker, times(1)).deleteFile(timingFile(id));
				assertEquals(1, localTimingCopies(id).size(), localTimingCopies(id).toString());
			}
		} finally {
			for (final String id : List.of(idA, stepId)) {
				for (final Path copy : localTimingCopies(id)) {
					Files.deleteIfExists(copy);
				}
			}
		}
	}

	/** A worker that traced nothing has no file: nothing to delete, and nothing to warn about. */
	@Test
	void aWorkerThatTracedNothingIsNotAskedToDeleteItsTraceFile() throws Exception {
		final FileManagerService worker = workerFiles(Set.of());
		run(List.of(), null, worker);

		verify(worker, times(1)).logFileName(Loggers.OP_TRACES.getName(), stepId);
		verify(worker, never()).deleteFile(anyString());
	}

	/**
	 * Runs a traced step of {@code contexts} on this node and one other, the other node's files
	 * served by {@code worker}. With {@code contextIds}, context {@code i} writes under its id.
	 */
	private void run(final List<Config> contexts, final List<String> contextIds, final FileManagerService worker)
					throws Exception {
		run(config(), contexts, contextIds, worker);
	}

	@SuppressWarnings("unchecked")
	private void run(
					final Config config, final List<Config> contexts, final List<String> contextIds,
					final FileManagerService worker) throws Exception {
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		final String nodeAddr = "127.0.0.1:" + port;
		originalRmiHostname = System.getProperty(RMI_HOSTNAME);
		System.setProperty(RMI_HOSTNAME, "127.0.0.1");
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		final LoadStep entry = mock(LoadStep.class);
		final LoadStep remote = mock(LoadStep.class);
		final var created = new AtomicInteger();
		when(factory.createLocal(any(), anyList(), anyList(), any()))
						.thenAnswer(call -> created.getAndIncrement() == 0 ? entry : remote);
		node = new LoadStepManagerServiceImpl(port, List.of(factory), metricsMgr);
		node.start();
		config.val("load-step-node-addrs", List.of(nodeAddr));
		final TestLoadStepClient client = new TestLoadStepClient(
						config, List.of(factory), contexts.isEmpty() ? null : contexts, metricsMgr) {
			@Override
			protected String contextOutputStepId(final int ctxIndex) {
				return contextIds == null ? super.contextOutputStepId(ctxIndex) : contextIds.get(ctxIndex);
			}
		};
		client.withNodeFileManagers(Map.of(nodeAddr, worker));
		client.start();
		assertDoesNotThrow(client::close);
	}

	/**
	 * The other node's files: one row in the trace file and in the timing metrics file of each of
	 * {@code tracedIds}, and no other file.
	 */
	private static FileManagerService workerFiles(final Set<String> tracedIds) throws Exception {
		final FileManagerService files = mock(FileManagerService.class);
		when(files.logFileName(anyString(), anyString()))
						.thenAnswer(call -> "/w/" + call.getArgument(1) + "/" + call.getArgument(0));
		final Map<String, Boolean> deleted = new ConcurrentHashMap<>();
		when(files.readFromFile(anyString(), anyLong())).thenAnswer(call -> {
			final String name = call.getArgument(0);
			final long offset = call.getArgument(1);
			if (!written(tracedIds, name) || deleted.containsKey(name)) {
				throw new NoSuchFileException(name);
			}
			if (offset >= TRACE_ROW.length) {
				throw new EOFException();
			}
			return TRACE_ROW;
		});
		doAnswer(call -> {
			final String name = call.getArgument(0);
			if (!written(tracedIds, name) || deleted.put(name, true) != null) {
				throw new NoSuchFileException(name);
			}
			return null;
		}).when(files).deleteFile(anyString());
		return files;
	}

	private static boolean written(final Set<String> ids, final String name) {
		return ids.stream().anyMatch(id -> traceFile(id).equals(name) || timingFile(id).equals(name));
	}

	/** Where the engines of one host write the timing metrics of step {@code id}. */
	private static String timingFile(final String id) {
		return Path.of(System.getProperty("java.io.tmpdir"), "spt", "timingMetrics_" + id).toString();
	}

	/** The local copies the entry fetched of the worker's timing metrics file of this step. */
	private List<Path> localTimingCopies() throws IOException {
		return localTimingCopies(stepId);
	}

	/** The local copies the entry fetched of the worker's timing metrics file of step {@code id}. */
	private static List<Path> localTimingCopies(final String id) throws IOException {
		final Path dir = Path.of(System.getProperty("java.io.tmpdir"), "spt");
		if (!Files.isDirectory(dir)) {
			return List.of();
		}
		try (Stream<Path> files = Files.list(dir)) {
			return files.filter(file -> file.getFileName().toString().startsWith("timingMetrics_" + id + "_")).toList();
		}
	}

	private static String traceFile(final String id) {
		return "/w/" + id + "/" + Loggers.OP_TRACES.getName();
	}

	private static Config tracedContext() {
		final Config context = baseConfig();
		context.val("output-metrics-trace-persist", true);
		return context;
	}

	private Config config() {
		final Config config = baseConfig();
		config.val("load-step-id", stepId);
		config.val("output-metrics-trace-persist", true);
		return config;
	}

	private static Config baseConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-idAutoGenerated", false);
		// An operation type with no lifecycle artifact for the step to merge when it closes.
		config.val("load-op-type", "noop");
		config.val("load-step-node-addrs", List.of());
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-output-file", "");
		return config;
	}
}
