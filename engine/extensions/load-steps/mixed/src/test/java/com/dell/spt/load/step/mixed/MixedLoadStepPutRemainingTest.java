package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.client.FileManagerClient;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/** The list of created items that a mixed step leaves for its cleanup, {@code put-remaining.csv}. */
class MixedLoadStepPutRemainingTest {

	private static final String STEP_ID = "mixed-put-remaining";

	@TempDir
	Path home;

	@BeforeEach
	void setHome() {
		ThreadContext.put("home_dir", home.toString());
	}

	@AfterEach
	void clearHome() {
		ThreadContext.remove("home_dir");
	}

	@Test
	void aDistributedStepListsEverySlicesCreatedItemsInTheEntryFile() throws Exception {
		final TwoNodeRun run = runOnTwoNodes(null);

		run.assertEveryCreatedItemListedOnce();
	}

	@Test
	void aDistributedAppendedStepWithAGeneratedIdListsEverySlicesCreatedItemsInItsOwnFolder() throws Exception {
		final TwoNodeRun run = runOnTwoNodes(null, Map.of("load", Map.of("op", Map.of("weights", Map.of("put", 3)))));

		run.assertEveryCreatedItemListedOnce();
		try (var folders = Files.list(home.resolve("log"))) {
			assertEquals(List.of(run.stepId()), folders.map(folder -> folder.getFileName().toString()).toList());
		}
	}

	@Test
	void aWorkerFileReadThatFailsOnceIsRetriedAndTheFileListsEveryCreatedItem() throws Exception {
		final List<String> failedReads = Collections.synchronizedList(new ArrayList<>());
		final TwoNodeRun run = runOnTwoNodes(files -> {
			final FileManagerService flaky = spy(files);
			doAnswer(read -> {
				if (failedReads.isEmpty()) {
					failedReads.add(read.getArgument(0));
					throw new IOException("injected read failure");
				}
				return read.callRealMethod();
			}).when(flaky).readFromFile(anyString(), anyLong());
			return flaky;
		});

		run.assertEveryCreatedItemListedOnce();
		assertEquals(1, failedReads.size());
		assertFalse(Files.exists(Path.of(failedReads.get(0))), "a worker file is deleted once all of it arrived");
	}

	/** What the client reaches a node's files through, given the node's file service. */
	private interface RemoteFiles {
		FileManagerService apply(FileManagerService files) throws IOException;
	}

	/** The final CREATE counts of a two-node step and the items its entry file lists. */
	private record TwoNodeRun(String stepId, long stepCreated, long nodeCreated, List<String> listed) {

		void assertEveryCreatedItemListedOnce() {
			assertTrue(0 < nodeCreated && nodeCreated < stepCreated, nodeCreated + " of " + stepCreated);
			assertEquals(stepCreated, listed.size(), "every created item, from both slices, is listed once");
			assertEquals(listed.size(), new HashSet<>(listed).size());
		}
	}

	/**
	 * Runs the step on this engine and one node engine over loopback RMI. With {@code remoteFiles},
	 * the client reaches the node's files through what it returns for the node's file service.
	 */
	private TwoNodeRun runOnTwoNodes(final RemoteFiles remoteFiles) throws Exception {
		return runOnTwoNodes(remoteFiles, null);
	}

	/**
	 * With {@code appended}, the step is the one {@code append(appended)} makes of a step whose id is
	 * generated, as a scenario's {@code MixedLoad.config(...).append(...)} gives.
	 */
	private TwoNodeRun runOnTwoNodes(final RemoteFiles remoteFiles, final Map<String, Object> appended)
					throws Exception {
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		final String nodeAddr = "127.0.0.1:" + port;
		final List<Extension> extensions = List.of(new MixedLoadStepExtension(), new DummyStorageDriverMockExtension<>());
		final MetricsManagerImpl entryMetrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final MetricsManagerImpl nodeMetrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final FileManagerServiceImpl fileService = new FileManagerServiceImpl(port);
		final LoadStepManagerServiceImpl stepService = new LoadStepManagerServiceImpl(port, extensions, nodeMetrics);
		try {
			// The node's service threads inherit its own home directory, as a separate engine has one.
			ThreadContext.put("home_dir", home.resolve("node").toString());
			fileService.start();
			stepService.start();
			ThreadContext.put("home_dir", home.toString());
			final Config config = stepConfig();
			config.val("load-step-node-addrs", List.of(nodeAddr));
			// Both slices share this JVM's log files, which the operation lifecycle collection of a
			// CREATE or READ step cannot tell apart. A mixed step takes its operations from its weights.
			config.val("load-op-type", "noop");
			// A metrics manager keeps the final counts of a step's last operation type only.
			config.val("load-op-weights-delete", 0);
			config.val("load-op-weights-stat", 0);
			if (appended != null) {
				config.val("load-step-idAutoGenerated", true);
			}
			MixedLoadStepClient step = new MixedLoadStepClient(config, extensions, null, entryMetrics);
			if (appended != null) {
				step = step.append(appended);
			}

			if (remoteFiles == null) {
				step.run();
			} else {
				final FileManagerService nodeFiles = remoteFiles.apply(fileService);
				try (MockedStatic<FileManagerClient> resolver = mockStatic(FileManagerClient.class)) {
					resolver.when(() -> FileManagerClient.resolve(nodeAddr)).thenReturn(nodeFiles);
					step.run();
				}
			}

			final String stepId = configOf(step).stringVal("load-step-id");
			return new TwoNodeRun(
							stepId,
							createdCount(entryMetrics, stepId, true),
							createdCount(nodeMetrics, stepId, false),
							Files.readAllLines(putRemainingFile(stepId), StandardCharsets.ISO_8859_1));
		} finally {
			if (!stepService.isClosed()) {
				stepService.close();
			}
			if (!fileService.isClosed()) {
				fileService.close();
			}
			ServiceUtil.shutdown();
			if (originalHost == null) {
				System.clearProperty("java.rmi.server.hostname");
			} else {
				System.setProperty("java.rmi.server.hostname", originalHost);
			}
		}
	}

	@Test
	void aStepThatCreatesItemsListsThemInItsLogDirectoryByDefault() throws Exception {
		final Config config = stepConfig();

		assertEquals(putRemainingFile().toString(), initializedConfig(config).stringVal("item-output-file"));
	}

	@Test
	void aStepKeepsTheItemOutputFileItIsGiven() throws Exception {
		final Config config = stepConfig();
		final String given = home.resolve("created.csv").toString();
		config.val("item-output-file", given);

		assertEquals(given, initializedConfig(config).stringVal("item-output-file"));
	}

	@Test
	void aStepThatCreatesNoItemsHasNoItemOutputFile() throws Exception {
		final Config config = stepConfig();
		config.val("load-op-weights-put", 0);

		assertNull(initializedConfig(config).stringVal("item-output-file"));
	}

	@Test
	void aCopyOfAnInitializedStepChoosesItsOwnItemOutputFile() throws Exception {
		final var client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class));
		client.init();

		final MixedLoadStepClient configCopy = client.config(Map.of());
		final MixedLoadStepClient appendCopy = client.append(Map.of());

		assertEquals(putRemainingFile().toString(), configOf(client).stringVal("item-output-file"));
		assertNull(configOf(configCopy).stringVal("item-output-file"));
		assertNull(configOf(appendCopy).stringVal("item-output-file"));
		assertNull(ctxConfigsOf(appendCopy).get(0).stringVal("item-output-file"));
	}

	@Test
	void aCopyKeepsAnItemOutputFileItIsGiven() throws Exception {
		final var client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class));
		client.init();
		final String given = home.resolve("copy.csv").toString();

		assertEquals(given, configOf(client.config(Map.of("item-output-file", given))).stringVal("item-output-file"));
		assertEquals(given, ctxConfigsOf(client.append(Map.of("item-output-file", given))).get(0)
						.stringVal("item-output-file"));
	}

	@Test
	void aCopyOfAStepGivenAnItemOutputFileKeepsIt() throws Exception {
		final Config config = stepConfig();
		final String given = home.resolve("created.csv").toString();
		config.val("item-output-file", given);
		final var client = new MixedLoadStepClient(config, List.of(), null, mock(MetricsManager.class));
		client.init();

		assertEquals(given, configOf(client.config(Map.of())).stringVal("item-output-file"));
	}

	@Test
	void aStepTakesTheItemOutputFileOfItsLastContextIntoItsBase() throws Exception {
		final String given = home.resolve("appended.csv").toString();
		final MixedLoadStepClient client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class))
						.append(Map.of("item", Map.of("output", Map.of("file", given))));

		client.init();

		assertEquals(given, configOf(client).stringVal("item-output-file"));
		assertNull(ctxConfigsOf(client).get(0).stringVal("item-output-file"));
		assertNull(defaultItemOutputFileOf(client), "a given file is not the step's default");
	}

	@Test
	void theLastContextsItemOutputFileWinsOverTheBases() throws Exception {
		final Config config = stepConfig();
		config.val("item-output-file", home.resolve("base.csv").toString());
		final String appended = home.resolve("appended.csv").toString();
		final MixedLoadStepClient client = new MixedLoadStepClient(config, List.of(), null, mock(MetricsManager.class))
						.append(Map.of("item", Map.of("output", Map.of("file", appended))));

		client.init();

		assertEquals(appended, configOf(client).stringVal("item-output-file"));
		assertNull(ctxConfigsOf(client).get(0).stringVal("item-output-file"));
	}

	@Test
	void anAppendedStepGivenNoItemOutputFileChoosesTheDefaultUnderItsId() throws Exception {
		final MixedLoadStepClient client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class))
						.append(Map.of());

		client.init();

		assertEquals(putRemainingFile().toString(), configOf(client).stringVal("item-output-file"));
		assertEquals(putRemainingFile().toString(), defaultItemOutputFileOf(client));
		assertNull(ctxConfigsOf(client).get(0).stringVal("item-output-file"));
	}

	@Test
	void anAppendedPutWeightGivesTheStepAnItemOutputFile() throws Exception {
		final Config config = stepConfig();
		config.val("load-op-weights-put", 0);
		final MixedLoadStepClient client = new MixedLoadStepClient(config, List.of(), null, mock(MetricsManager.class))
						.append(Map.of("load", Map.of("op", Map.of("weights", Map.of("put", 50)))));

		client.init();

		assertEquals(putRemainingFile().toString(), configOf(client).stringVal("item-output-file"));
	}

	@Test
	void theMetricsRowsNameTheOperationsTheMergedWeightsRun() throws Exception {
		final Config config = stepConfig();
		config.val("load-op-weights-get", 40);
		config.val("load-op-weights-put", 30);
		config.val("load-op-weights-delete", 30);
		config.val("load-op-weights-stat", 0);
		final MixedLoadStepClient client = new MixedLoadStepClient(config, List.of(), null, mock(MetricsManager.class))
						.append(Map.of("load", Map.of("op", Map.of("weights", Map.of("get", 0, "put", 50, "delete", 50)))));

		client.init();

		assertEquals(List.of(OpType.CREATE, OpType.DELETE), metricsOpTypesOf(client));
	}

	@Test
	void theMetricsRowsTakeTheConcurrencyAndItemSizeTheSliceRuns() throws Exception {
		final MixedLoadStepClient client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class))
						.append(Map.of(
										"storage", Map.of("driver", Map.of("limit", Map.of("concurrency", 7))),
										"item", Map.of("data", Map.of("size", "4KB"))));

		client.init();

		for (final MetricsContext<?> context : metricsContextsOf(client)) {
			assertEquals(7, context.concurrencyLimit());
			assertEquals(4096L, context.itemDataSize().get());
		}
	}

	@Test
	void anAppendedPutWeightHasTheListChecked() throws Exception {
		final Path listed = home.resolve("listed.csv");
		Files.writeString(listed, "a\n");
		final Config config = stepConfig();
		config.val("load-op-weights-put", 0);
		final MixedLoadStepClient client = new MixedLoadStepClient(config, List.of(), null, mock(MetricsManager.class))
						.append(Map.of("load", Map.of("op", Map.of("weights", Map.of("put", 50)))));

		final List<String> errors = capture(Loggers.ERR.getName(),
						() -> client.itemOutputCollected(listed.toString(), Map.of(OpType.CREATE, 3L)));

		assertTrue(errors.stream().anyMatch(message -> message.contains("lists 1 of the 3 items the step created")),
						errors.toString());
	}

	@Test
	void aListMissingCreatedItemsIsReportedAsAnError() throws Exception {
		final Path listed = home.resolve("listed.csv");
		Files.writeString(listed, "a\nb\n");
		final var client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class));

		final List<String> errors = capture(Loggers.ERR.getName(),
						() -> client.itemOutputCollected(listed.toString(), Map.of(OpType.CREATE, 5L)));
		final List<String> info = capture(Loggers.MSG.getName(),
						() -> client.itemOutputCollected(listed.toString(), Map.of(OpType.CREATE, 2L)));

		assertTrue(errors.stream().anyMatch(message -> message.contains("lists 2 of the 5 items the step created")),
						errors.toString());
		assertTrue(info.stream().anyMatch(message -> message.contains("lists all 2 items the step created")),
						info.toString());
	}

	@Test
	void listedItemsAreCountedByLineAndAMissingListIsEmpty() throws Exception {
		final Path listed = home.resolve("listed.csv");
		Files.write(listed, new byte[]{'a', '\n', (byte) 0xff, '\n', 'c'
		});

		assertEquals(3, MixedLoadStepClient.listedItemCount(listed));
		assertEquals(0, MixedLoadStepClient.listedItemCount(home.resolve("absent.csv")));
	}

	/** The configuration a client holds after its initialization; a step copies the one it is given. */
	private static Config initializedConfig(final Config config) throws ReflectiveOperationException {
		final var client = new MixedLoadStepClient(config, List.of(), null, mock(MetricsManager.class));
		client.init();
		return configOf(client);
	}

	private static String defaultItemOutputFileOf(final MixedLoadStepClient client) throws ReflectiveOperationException {
		final Field field = MixedLoadStepClient.class.getDeclaredField("defaultItemOutputFile");
		field.setAccessible(true);
		return (String) field.get(client);
	}

	private static List<OpType> metricsOpTypesOf(final MixedLoadStepClient client) throws ReflectiveOperationException {
		return metricsContextsOf(client).stream().map(MetricsContext::opType).toList();
	}

	@SuppressWarnings("unchecked")
	private static List<MetricsContext<?>> metricsContextsOf(final MixedLoadStepClient client)
					throws ReflectiveOperationException {
		final Field field = LoadStepBase.class.getDeclaredField("metricsContexts");
		field.setAccessible(true);
		return (List<MetricsContext<?>>) field.get(client);
	}

	private static Config configOf(final MixedLoadStepClient client) throws ReflectiveOperationException {
		final Field field = LoadStepBase.class.getDeclaredField("config");
		field.setAccessible(true);
		return (Config) field.get(client);
	}

	@SuppressWarnings("unchecked")
	private static List<Config> ctxConfigsOf(final MixedLoadStepClient client) throws ReflectiveOperationException {
		final Field field = LoadStepBase.class.getDeclaredField("ctxConfigs");
		field.setAccessible(true);
		return (List<Config>) field.get(client);
	}

	private Path putRemainingFile() {
		return putRemainingFile(STEP_ID);
	}

	private Path putRemainingFile(final String stepId) {
		return home.resolve("log").resolve(stepId).resolve("put-remaining.csv");
	}

	private Config stepConfig() throws Exception {
		final Config config = defaultConfig();
		config.val("load-step-id", STEP_ID);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", "30s");
		config.val("load-op-limit-count", 4_000L);
		config.val("load-op-weights-get", 1);
		config.val("load-op-weights-put", 2);
		config.val("load-op-weights-delete", 1);
		config.val("load-op-weights-stat", 1);
		config.val("storage-driver-limit-concurrency", 4);
		config.val("storage-targetBucket-provision", false);
		config.val("item-data-size", "1KB");
		config.val("item-output-path", "/bucket");
		final Path seeds = home.resolve("seeds.csv");
		final List<String> seedItems = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			seedItems.add(new DataItemImpl("/bucket/seed-" + i, 0, 1024).toString());
		}
		Files.write(seeds, seedItems);
		config.val("item-input-file", seeds.toString());
		return config;
	}

	/** The shipped defaults of the engine and of this extension, as the engine merges them. */
	private static Config defaultConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		return ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedLoadStepPutRemainingTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	private static long createdCount(final MetricsManagerImpl metrics, final String stepId, final boolean distributed) {
		return metrics.getTerminalSteps().stream()
						.filter(entry -> stepId.equals(entry.stepId) && entry.opType == OpType.CREATE)
						.filter(entry -> entry.distributed == distributed)
						.mapToLong(entry -> entry.successCount)
						.sum();
	}

	private interface Check {
		void run() throws Exception;
	}

	private static List<String> capture(final String loggerName, final Check check) throws Exception {
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		final AbstractAppender appender = new AbstractAppender(
						"mixedPutRemainingCapture", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				messages.add(event.getMessage().getFormattedMessage());
			}
		};
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(loggerName);
		logger.addAppender(appender);
		try {
			check.run();
			for (int i = 0; i < 100 && messages.isEmpty(); i++) {
				Thread.sleep(10);
			}
			synchronized (messages) {
				return List.copyOf(messages);
			}
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
	}
}
