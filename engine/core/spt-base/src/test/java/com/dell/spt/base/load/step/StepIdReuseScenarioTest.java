package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.Constants;
import com.dell.spt.base.buildinfo.EngineBuildInfoPublisher;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.client.StepIdReuseAtStartTest;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.script.ScriptEngine;
import org.apache.logging.log4j.CloseableThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Scenarios that give two steps one id, on the real linear step and the dummy mock driver: the second
 * CREATE is refused at its start, before any load, and leaves the first one's files as they were; an
 * UPDATE that reuses an id still runs; the shipped endurance examples give each step its own id; and
 * a copy of a step that has run gets its own id unless the user set the one it copies.
 */
@Timeout(180)
class StepIdReuseScenarioTest {

	private static final long TERMINAL_RETENTION_MILLIS = TimeUnit.MINUTES.toMillis(10);
	private static final String GENERATED_ID_PREFIX = "linear_";

	private final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
	private final List<Path> stepDirs = new ArrayList<>();

	@TempDir
	Path tempDir;

	@AfterEach
	void removeTheStepFolders() throws IOException {
		for (final String stepId : ranStepIds()) {
			if (stepId.startsWith(GENERATED_ID_PREFIX)) {
				stepDirs.add(stepDir(stepId));
			}
		}
		for (final Path dir : stepDirs) {
			StepIdReuseAtStartTest.removeFolder(dir);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"create", "noop"
	})
	void aConfigCopyOfAStepThatHasRunGetsItsOwnGeneratedId(final String opType) throws Exception {
		final Config config = generatedIdConfig();
		config.val("load-op-type", opType);
		config.val("load-op-limit-count", 20);

		assertDoesNotThrow(() -> run("Load.run();\nLoad.config({load:{op:{limit:{count:30}}}}).run();\n", config));

		final Map<Long, Set<String>> idsByCount = generatedIdsByCount();
		assertEquals(Set.of(20L, 30L), idsByCount.keySet(), idsByCount.toString());
		assertEquals(1, idsByCount.get(20L).size(), idsByCount.toString());
		assertEquals(1, idsByCount.get(30L).size(), idsByCount.toString());
		assertEquals(2, ranStepIds().size(), "each step has its own id: " + idsByCount);
		if ("create".equals(opType)) {
			for (final String stepId : ranStepIds()) {
				assertTrue(Files.exists(stepDir(stepId).resolve(OperationLifecycleArtifact.FILE_NAME)), stepId);
			}
		}
	}

	@Test
	void anAppendCopyOfAStepThatHasRunGetsItsOwnGeneratedId() throws Exception {
		final Config config = generatedIdConfig();
		config.val("load-op-limit-count", 20);

		assertDoesNotThrow(() -> run("var step = Load.config({});\nstep.run();\nstep.append({}).run();\n", config));

		assertEquals(2, ranStepIds().size(), "each step has its own id: " + ranStepIds());
		assertTrue(ranStepIds().stream().allMatch(stepId -> stepId.startsWith(GENERATED_ID_PREFIX)), ranStepIds().toString());
	}

	@Test
	void aCopyOfAStepThatHasRunMayNameItsOwnId() throws Exception {
		final String stepId = uniqueStepId("own-id");
		final Config config = generatedIdConfig();
		config.val("load-op-limit-count", 20);

		assertDoesNotThrow(() -> run(
						"Load.run();\nLoad.config({load:{step:{id:\"" + stepId + "\"},op:{limit:{count:30}}}}).run();\n",
						config));

		assertEquals(30, terminalSteps(stepId).get(0).successCount);
		assertEquals(2, ranStepIds().size(), ranStepIds().toString());
	}

	@Test
	void aCopyThatKeepsAUserSetIdIsStillRefused() throws Exception {
		final String stepId = uniqueStepId("user-set");
		final String ownId = uniqueStepId("user-set-own");
		final String scenario = "var step = Load.config({load:{step:{id:\"" + stepId + "\"},op:{limit:{count:20}}}});\n"
						+ "step.run();\n"
						+ "step.config({load:{step:{id:\"" + ownId + "\"},op:{limit:{count:30}}}}).run();\n"
						+ "step.config({load:{op:{limit:{count:40}}}}).run();\n";

		final var thrown = assertThrows(RunTerminalException.class, () -> run(scenario, generatedIdConfig()));

		assertRefused(thrown, stepId);
		assertEquals(20, terminalSteps(stepId).get(0).successCount, "the refused copy ran operations");
		assertEquals(30, terminalSteps(ownId).get(0).successCount);
	}

	@Test
	void copiesOfAnUnusedPrototypeStillGenerateTheirOwnIds() throws Exception {
		final Config config = generatedIdConfig();

		assertDoesNotThrow(() -> run(
						"Load.config({load:{op:{limit:{count:20}}}}).run();\nLoad.config({load:{op:{limit:{count:30}}}}).run();\n",
						config));

		assertEquals(Set.of(20L, 30L), generatedIdsByCount().keySet());
		assertEquals(2, ranStepIds().size(), ranStepIds().toString());
	}

	@Test
	void theSecondOfTwoStepsWithOneIdIsRefusedAtItsStart() throws Exception {
		final String stepId = uniqueStepId("same-id");
		final String first = "Load.config({load:{step:{id:\"" + stepId + "\"},op:{limit:{count:50}}}}).run();\n";
		// Another count, so that its metrics would show had it run.
		final String second = first.replace("count:50", "count:30");

		final var thrown = assertThrows(RunTerminalException.class, () -> run(first + second));

		assertRefused(thrown, stepId);
		final List<TerminalStepEntry> steps = terminalSteps(stepId);
		assertEquals(2, steps.size(), "the first step's node and fleet entries: " + steps);
		for (final TerminalStepEntry step : steps) {
			assertEquals(50, step.successCount, "the refused step ran operations");
			assertEquals(50, step.countLimit);
		}
	}

	/**
	 * As two engine runs into one log directory: the second one's step changes none of the files the
	 * refusal protects, the first one's build record among them.
	 */
	@Test
	void aRerunWithTheSameStepIdIsRefusedAndLeavesTheEarlierFilesAlone() throws Exception {
		final String stepId = uniqueStepId("rerun");
		final String scenario = "Load.config({load:{step:{id:\"" + stepId + "\"},op:{limit:{count:50}}}}).run();\n";
		// The build record goes to the log home of the thread context, which the loggers' folder is under here.
		final String logHome = stepDir(stepId).getParent().getParent().toString();
		try (var home = CloseableThreadContext.put(Constants.KEY_HOME_DIR, logHome)) {
			run(scenario);
			final Map<Path, String> before = protectedFiles(stepId);
			assertTrue(before.keySet().stream().anyMatch(file -> file.endsWith(OperationLifecycleArtifact.FILE_NAME)), before.toString());
			assertTrue(before.keySet().stream().anyMatch(file -> file.endsWith(EngineBuildInfoPublisher.FILE_NAME)), before.toString());
			final List<TerminalStepEntry> metricsBefore = terminalSteps(stepId);

			final var thrown = assertThrows(RunTerminalException.class, () -> run(scenario));

			assertRefused(thrown, stepId);
			assertEquals(before, protectedFiles(stepId));
			final List<TerminalStepEntry> metricsAfter = terminalSteps(stepId);
			assertEquals(metricsBefore.size(), metricsAfter.size());
			for (int i = 0; i < metricsBefore.size(); i++) {
				assertTrue(metricsBefore.get(i) == metricsAfter.get(i), "the refused step recorded metrics");
			}
		}
	}

	@Test
	void anUpdateThatReusesTheIdOfAnEarlierStepStillRuns() throws Exception {
		final String stepId = uniqueStepId("update");
		final String items = tempDir.resolve("items.csv").toString().replace("\\", "/");
		final String update = "Load.config({item:{input:{file:\"" + items + "\"}},load:{step:{id:\"" + stepId
						+ "\"},op:{type:\"update\",limit:{count:10}}}}).run();\n";

		assertDoesNotThrow(() -> run(
						"Load.config({item:{output:{file:\"" + items + "\"}},load:{step:{id:\"" + stepId
										+ "\"},op:{limit:{count:10}}}}).run();\n" + update + update));

		for (final TerminalStepEntry step : terminalSteps(stepId)) {
			assertEquals(com.dell.spt.base.item.op.OpType.UPDATE, step.opType, "the last step's entries");
			assertEquals(10, step.successCount);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"infinite_loop_test.js", "parallel_pipeline_and_infinite_loop_test.js"
	})
	void anEnduranceExampleRunsThreeLoopsWithoutReusingAStepId(final String example) throws Exception {
		final String script;
		try (InputStream in = getClass().getResourceAsStream("/example/scenario/js/endurance/" + example)) {
			assertNotNull(in, example);
			script = new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(Thread.currentThread().getContextClassLoader());
		final var stepIds = new ArrayList<String>();
		final var load = new BoundedLoad(new LinearLoadStepClient(config(), extensions(), null, metrics), stepIds);
		engine.put("Load", load);
		// The pipeline runs once, beside the loop; this test is about the loop.
		engine.put("PipelineLoad", new IgnoredLoad());

		final var thrown = assertThrows(Exception.class, () -> engine.eval(script));

		assertTrue(BoundedLoad.stopped(thrown), "the loop stopped before its third step ended: " + thrown);
		assertEquals(BoundedLoad.LOOPS, stepIds.size(), stepIds.toString());
		assertEquals(BoundedLoad.LOOPS, new HashSet<>(stepIds).size(), "a step id was reused: " + stepIds);
		for (final String stepId : stepIds) {
			stepDirs.add(stepDir(stepId));
			assertTrue(stepId.startsWith("endurance_1_"), stepId);
			assertEquals(20, terminalSteps(stepId).get(0).successCount, stepId);
		}
	}

	private static void assertRefused(final RunTerminalException thrown, final String stepId) {
		final var refused = assertInstanceOf(StepIdInUseException.class, RunTerminalException.find(thrown), thrown.toString());
		assertEquals(stepId, refused.stepId());
		assertTrue(refused.getMessage().startsWith("step id \"" + stepId + "\" is already used in this log directory ("),
						refused.getMessage());
	}

	private void run(final String scenario) {
		run(scenario, config());
	}

	private void run(final String scenario, final Config config) {
		final ScriptEngine engine = ScenarioUtil.scriptEngineByDefault(Thread.currentThread().getContextClassLoader());
		ScenarioUtil.registerStepTypes(engine, extensions(), config, metrics);
		new RunImpl("step id reuse", scenario, engine, 1).run();
	}

	/** The configuration of a run without a configured id, as {@code Main.checkAndSetStepId} leaves it. */
	private Config generatedIdConfig() {
		final Config config = config();
		config.val("load-step-id", "none-" + UUID.randomUUID());
		config.val("load-step-idAutoGenerated", true);
		return config;
	}

	private Set<String> ranStepIds() {
		return metrics.getTerminalSteps().stream().map(entry -> entry.stepId).collect(Collectors.toCollection(TreeSet::new));
	}

	private Map<Long, Set<String>> generatedIdsByCount() {
		final Map<Long, Set<String>> idsByCount = new TreeMap<>();
		for (final TerminalStepEntry step : metrics.getTerminalSteps()) {
			assertTrue(step.stepId.startsWith(GENERATED_ID_PREFIX), step.stepId);
			idsByCount.computeIfAbsent(step.successCount, count -> new TreeSet<>()).add(step.stepId);
		}
		return idsByCount;
	}

	private String uniqueStepId(final String name) throws IOException {
		final String stepId = "step-id-reuse-" + name + "-" + UUID.randomUUID();
		stepDirs.add(stepDir(stepId));
		return stepId;
	}

	private static Path stepDir(final String stepId) throws IOException {
		return Path.of(com.dell.spt.base.load.step.file.FileManager.INSTANCE.logFileName(
						com.dell.spt.base.logging.Loggers.OPERATION_LIFECYCLE.getName(), stepId)).toAbsolutePath().getParent();
	}

	/**
	 * The step's files that a refused step must leave alone, by path, with their contents and identity: its
	 * operation lifecycle evidence, its event-loop reports and its build record. Its metrics files are
	 * written asynchronously after a step ends, and its messages are a log the refusal adds to.
	 */
	private static Map<Path, String> protectedFiles(final String stepId) throws IOException {
		final var files = new LinkedHashMap<Path, String>();
		try (Stream<Path> list = Files.list(stepDir(stepId))) {
			for (final Path file : list.sorted().toList()) {
				final String name = file.getFileName().toString();
				if (name.startsWith("operation.lifecycle") || name.startsWith("event.loops")
								|| name.equals(EngineBuildInfoPublisher.FILE_NAME)) {
					// A file replaced by an identical one is replaced all the same: it has a new file key.
					final var attributes = Files.readAttributes(file, java.nio.file.attribute.BasicFileAttributes.class);
					files.put(file, Files.readString(file) + " | " + attributes.fileKey() + " | " + attributes.lastModifiedTime());
				}
			}
		}
		return files;
	}

	private List<TerminalStepEntry> terminalSteps(final String stepId) {
		return metrics.getTerminalSteps().stream().filter(entry -> stepId.equals(entry.stepId)).toList();
	}

	private Config config() {
		metrics.setTerminalRetentionMillis(TERMINAL_RETENTION_MILLIS);
		final Config config = TestConfigBuilder.config();
		config.val("load-step-limit-time", 0);
		config.val("storage-driver-type", "dummy-mock");
		config.val("output-metrics-timing-persist", false);
		config.val("output-metrics-trace-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("item-input-file", null);
		config.val("item-input-path", null);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", 1);
		config.val("load-step-node-addrs", List.of());
		return config;
	}

	private static List<Extension> extensions() {
		return List.of(new LinearLoadStepExtension<>(), new DummyStorageDriverMockExtension<>());
	}

	/** What an endurance example sees as {@code Load}: the real step, bounded by a count, for three loops. */
	public static final class BoundedLoad {

		static final int LOOPS = 3;
		private static final Map<String, Object> BOUND = Map.of("load", Map.of("op", Map.of("limit", Map.of("count", 20))));

		private final LinearLoadStepClient client;
		private final List<String> stepIds;

		BoundedLoad(final LinearLoadStepClient client, final List<String> stepIds) {
			this.client = client;
			this.stepIds = stepIds;
		}

		public BoundedLoad config(final Map<String, Object> config) {
			return new BoundedLoad(client.config(config).config(BOUND), stepIds);
		}

		public void run() {
			stepIds.add(client.loadStepId());
			client.run();
			if (stepIds.size() == LOOPS) {
				throw new LoopsDone();
			}
		}

		static boolean stopped(final Throwable thrown) {
			for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
				if (cause instanceof LoopsDone || String.valueOf(cause.getMessage()).contains(LoopsDone.MESSAGE)) {
					return true;
				}
			}
			return false;
		}

		static final class LoopsDone extends RuntimeException {
			private static final long serialVersionUID = 1L;
			static final String MESSAGE = "endurance loops done";

			LoopsDone() {
				super(MESSAGE);
			}
		}
	}

	/** Accepts the configuration calls of a step this test does not run. */
	public static final class IgnoredLoad {

		public IgnoredLoad config(final Map<String, Object> ignored) {
			return this;
		}

		public IgnoredLoad append(final Map<String, Object> ignored) {
			return this;
		}

		public IgnoredLoad start() {
			return this;
		}
	}
}
