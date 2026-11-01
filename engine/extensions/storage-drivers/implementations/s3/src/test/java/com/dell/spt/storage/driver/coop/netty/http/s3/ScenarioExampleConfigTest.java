package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.config.BundledDefaultsProvider;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.github.akurilov.confuse.Config;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.Test;

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
