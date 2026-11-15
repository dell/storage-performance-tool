package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.load.step.client.LoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.github.akurilov.confuse.Config;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.script.ScriptContext;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

/**
 * The shipped example scenarios, evaluated by the scenarios' script engine: each step's
 * configuration goes through the real {@code Load} client, which records it instead of running it.
 */
public class ScenarioExampleConfigTest {

	@Test
	void theBundledExampleSetsItsEndpointsFromAnArray() throws Exception {
		final var steps = steps(Files.readString(Path.of("../../../../bundle/example-scenario.js")));

		assertEquals(List.of("create", "read", "delete"), steps.stream().map(step -> step.stringVal("load-op-type")).toList());
		assertEquals("30s", steps.get(1).stringVal("load-step-limit-time"));
		for (final var step : steps) {
			assertEquals(List.of("your-s3-endpoint:9000"), step.listVal("storage-net-node-addrs"));
		}
	}

	@Test
	void theListBenchmarkExampleSetsItsEndpointsFromAnArray() throws Exception {
		final String script;
		try (InputStream in = getClass().getResourceAsStream("/example/scenario/js/s3_list_benchmark.js")) {
			script = new String(in.readAllBytes(), StandardCharsets.UTF_8);
		}
		final var steps = steps(script);

		assertEquals(1, steps.size());
		assertEquals(List.of("s3.amazonaws.com"), steps.get(0).listVal("storage-net-node-addrs"));
		assertEquals(443, steps.get(0).intVal("storage-net-node-port"));
		assertEquals("us-east-1", steps.get(0).stringVal("storage-region"));
		assertEquals(128, steps.get(0).intVal("storage-driver-limit-concurrency"));
	}

	/** The shipped JavaScript example scenarios, as the image ships them under /opt/spt/scenarios/js. */
	private static final Path EXAMPLES = Path.of("../../../../core/spt-base/src/main/resources/example/scenario/js");

	/** Steps a script may run before its evaluation is cut, so that a {@code while(true)} loop ends. */
	private static final int MAX_STEPS = 60;

	/** A name the script uses but nothing defines: in the engine, an environment variable. */
	private static final Pattern UNDEFINED = Pattern.compile("ReferenceError: \"([^\"]+)\" is not defined");

	/**
	 * The shape of an environment variable's name. Only such a name is stubbed, so a misspelt
	 * script variable still fails the check, as it fails in the engine.
	 */
	private static final Pattern ENVIRONMENT_NAME = Pattern.compile("[A-Z][A-Z0-9_]*");

	/** How long one example's evaluation may take, so that a loop that never ends fails the check. */
	private static final Duration EVALUATION_BUDGET = Duration.ofMinutes(2);

	/**
	 * Every shipped JavaScript example is evaluated with the step types the engine binds, each
	 * step's configuration going through the real client: an invalid configuration path, a value
	 * of the wrong type or a syntax error fails it. This proves syntax and configuration paths only,
	 * not that an example runs. Shell commands are replaced by a stub that runs nothing, and an
	 * environment variable the script reads is set to a harmless file name. The stub replaces the
	 * text {@code java.lang.ProcessBuilder} only: a script that reaches a process or the file system
	 * another way would really run it, and no shipped example does.
	 */
	@TestFactory
	Stream<DynamicTest> everyShippedJavaScriptExampleSetsOnlyValidConfiguration() throws Exception {
		final List<Path> files;
		try (var walk = Files.walk(EXAMPLES)) {
			files = walk.filter(file -> file.toString().endsWith(".js")).sorted().toList();
		}
		assertTrue(files.size() >= 33, "found only " + files.size() + " examples under " + EXAMPLES.toAbsolutePath());
		return files.stream()
						.map(file -> DynamicTest.dynamicTest(EXAMPLES.relativize(file).toString(), () -> assertTimeoutPreemptively(EVALUATION_BUDGET, () -> evaluate(file))));
	}

	@Test
	void theExampleCheckBindsEveryStepTypeTheEngineBinds() throws Exception {
		final var names = new TreeSet<String>();
		evaluate("", names);
		assertTrue(
						names.containsAll(
										Set.of(
														"Load",
														"MixedLoad",
														"PipelineLoad",
														"WeightedLoad",
														"PreconditionLoad",
														"NoopLoad",
														"CreateLoad",
														"ReadLoad",
														"UpdateLoad",
														"DeleteLoad",
														"ListLoad",
														"StatLoad",
														"ReadVerifyLoad",
														"ReadRandomRangeLoad",
														"ReadVerifyRandomRangeLoad",
														"UpdateRandomRangeLoad")),
						names.toString());
	}

	@Test
	void theExampleCheckFailsOnAnInvalidConfigurationPath() {
		final var e = assertThrowsFrom(
						() -> evaluate("PreconditionLoad.config({\"load\": {\"step\": {\"limit\": {\"count\": 1}}}}).run();", null));
		assertTrue(String.valueOf(e).contains("load-step-limit-count"), String.valueOf(e));
	}

	@Test
	void theExampleCheckFailsOnAMisspeltScriptVariable() {
		final var e = assertThrowsFrom(() -> evaluate("var limitCount = 5; Load.config({\"load\": {\"op\": {\"limit\": {\"count\": limitCuont}}}}).run();", null));
		assertTrue(String.valueOf(e).contains("limitCuont"), String.valueOf(e));
	}

	@Test
	void theExampleCheckFailsOnASyntaxError() {
		assertThrowsFrom(() -> evaluate("Load.config({\"load\": {\"op\": {\"type\": \"read\"} \"step\": {}}}).run();", null));
	}

	@Test
	void theExampleCheckRunsNoShellCommand() throws Exception {
		final var marker = Files.createTempFile("spt-example-check", ".marker");
		try {
			evaluate(
							"new java.lang.ProcessBuilder(\"/bin/sh\", \"-c\", \"rm -f " + marker + "\").inheritIO().start().waitFor();",
							null);
			assertTrue(Files.exists(marker));
		} finally {
			Files.deleteIfExists(marker);
		}
	}

	private static Throwable assertThrowsFrom(final ThrowingRunnable action) {
		try {
			action.run();
		} catch (final Throwable e) {
			return e;
		}
		return fail("the script was accepted");
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}

	private static void evaluate(final Path file) throws Exception {
		final var script = Files.readString(file);
		assertFalse(
						script.contains("Runtime.getRuntime"),
						"the check stubs only java.lang.ProcessBuilder; another way to run a command would really run");
		evaluate(script, null);
	}

	/**
	 * Evaluates the script as the engine would, with each bound step type replaced by a recorder
	 * around the engine's own client. A name shaped like an environment variable that the script
	 * reads but nothing defines is set to "1", and the script is evaluated again.
	 */
	private static void evaluate(final String script, final Set<String> boundNames) throws Exception {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		final var config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		final var extensions = Extension.load(Thread.currentThread().getContextClassLoader());
		final var stubbed = new ArrayList<String>();
		final var text = script.replace("java.lang.ProcessBuilder", "SptExampleProcessBuilderStub");
		while (true) {
			final var engine = new ScriptEngineManager().getEngineByName("js");
			ScenarioUtil.configure(engine, extensions, config, null);
			final var bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
			final int[] steps = {0
			};
			for (final var name : List.copyOf(bindings.keySet())) {
				if (bindings.get(name) instanceof LoadStepClient<?> client) {
					bindings.put(name, new StepRecorder(client, steps));
					if (boundNames != null) {
						boundNames.add(name);
					}
				}
			}
			engine.eval(PROCESS_BUILDER_STUB);
			for (final var name : stubbed) {
				bindings.put(name, "1");
			}
			try {
				engine.eval(text);
				return;
			} catch (final Exception e) {
				if (cutByStepLimit(e)) {
					return;
				}
				final var undefined = UNDEFINED.matcher(String.valueOf(e.getMessage()));
				if (undefined.find()
								&& ENVIRONMENT_NAME.matcher(undefined.group(1)).matches()
								&& !stubbed.contains(undefined.group(1))
								&& stubbed.size() < 40) {
					stubbed.add(undefined.group(1));
					continue;
				}
				throw e;
			}
		}
	}

	private static boolean cutByStepLimit(final Throwable e) {
		for (var cause = e; cause != null; cause = cause.getCause()) {
			if (cause instanceof StepLimitReached) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Stands in for {@code java.lang.ProcessBuilder}: the examples remove files and sleep through
	 * shell commands while they are evaluated.
	 */
	private static final String PROCESS_BUILDER_STUB = """
					var SptExampleProcessBuilderStub = function() {
						var emptyStream = function() { return new java.io.ByteArrayInputStream(Java.to([], "byte[]")); };
						var process = {
							waitFor: function() { return 0; },
							exitValue: function() { return 0; },
							getInputStream: emptyStream,
							getErrorStream: emptyStream
						};
						this.command = function() { return this; };
						this.inheritIO = function() { return this; };
						this.start = function() { return process; };
					};
					""";

	/** Thrown by a recorder's {@code run()} once a script has run {@link #MAX_STEPS} steps. */
	static final class StepLimitReached extends RuntimeException {

		private static final long serialVersionUID = 1L;

		StepLimitReached() {
			super("evaluation cut after " + MAX_STEPS + " steps", null, false, false);
		}
	}

	/** A step type as the script sees it: the engine's client configures, nothing runs. */
	public static final class StepRecorder {

		private final LoadStepClient<?> client;
		private final int[] steps;

		StepRecorder(final LoadStepClient<?> client, final int[] steps) {
			this.client = client;
			this.steps = steps;
		}

		public StepRecorder config(final Map<String, Object> config) {
			return new StepRecorder(client.config(config), steps);
		}

		public StepRecorder append(final Map<String, Object> context) {
			return new StepRecorder(client.append(context), steps);
		}

		public void run() {
			if (++steps[0] > MAX_STEPS) {
				throw new StepLimitReached();
			}
		}

		public StepRecorder start() {
			run();
			return this;
		}

		public void await() {}

		public void stop() {}

		public void close() {}
	}

	/** Each step the script runs, by its configuration. */
	private static List<Config> steps(final String script) throws Exception {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		final var config = ConfigUtil.merge("-", List.of(new BundledDefaultsProvider().config("-", root.schema()), root));
		final var extensions = Extension.load(Thread.currentThread().getContextClassLoader());
		final var steps = new ArrayList<Config>();
		final var engine = new ScriptEngineManager().getEngineByName("js");
		engine.put("Load", new RecordingLoad(new Client(config, extensions, null), steps));
		engine.eval(script);
		return steps;
	}

	/** The {@code Load} client, which exposes its configuration. */
	static final class Client extends LinearLoadStepClient {

		Client(final Config config, final List<Extension> extensions, final List<Config> ctxConfigs) {
			super(config, extensions, ctxConfigs, null);
		}

		@Override
		protected Client copyInstance(final Config config, final List<Config> ctxConfigs) {
			return new Client(config, extensions, ctxConfigs);
		}

		Config effective() {
			return config;
		}
	}

	/** What the script sees as {@code Load}: the real client's configuration calls, then a record. */
	public static final class RecordingLoad {

		private final Client client;
		private final List<Config> steps;

		RecordingLoad(final Client client, final List<Config> steps) {
			this.client = client;
			this.steps = steps;
		}

		public RecordingLoad config(final Map<String, Object> config) {
			return new RecordingLoad((Client) client.config(config), steps);
		}

		public RecordingLoad append(final Map<String, Object> context) {
			return new RecordingLoad((Client) client.append(context), steps);
		}

		public void run() {
			steps.add(client.effective());
		}

		public RecordingLoad start() {
			run();
			return this;
		}

		public void await() {}

		public void close() {}
	}
}
