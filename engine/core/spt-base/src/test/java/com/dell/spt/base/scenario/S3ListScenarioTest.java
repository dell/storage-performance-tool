package com.dell.spt.base.scenario;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.load.step.ScenarioUtil;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class S3ListScenarioTest {

	@Test
	void s3ListBenchmarkScenarioLoads() throws Exception {
		final var resource = S3ListScenarioTest.class.getClassLoader().getResource("example/scenario/js/s3_list_benchmark.js");
		assertNotNull(resource, "Scenario resource must be on the classpath");
		final Path scenarioPath = Path.of(resource.toURI());

		final ScriptEngine engine = ScenarioUtil.scriptEngineByFilePath(scenarioPath, Thread.currentThread().getContextClassLoader());
		final StubLoad stubLoad = new StubLoad();
		engine.put("Load", stubLoad);
		assertEquals("object", engine.eval("typeof Load"));

		final String source = new String(Files.readAllBytes(scenarioPath), StandardCharsets.UTF_8);
		final String processed = resolveEnvDefaults(source);
		try (Reader reader = new StringReader(processed)) {
			engine.eval(reader);
		}

		final Map<String, Object> capturedConfig = stubLoad.lastConfig.get();
		assertNotNull(capturedConfig, "Scenario should call Load.config");
		final Map<String, Object> load = (Map<String, Object>) capturedConfig.get("load");
		assertNotNull(load, "Load section must be present");
		final Map<String, Object> op = (Map<String, Object>) load.get("op");
		assertEquals("list", op.get("type"));
		final Map<String, Object> list = (Map<String, Object>) op.get("list");
		assertEquals(Boolean.FALSE, list.get("fetch_metadata"));
		assertEquals(Boolean.FALSE, list.get("include_versions"));
		assertEquals(1000, ((Number) list.get("max_keys")).intValue());
	}

	public static final class StubLoad {
		final AtomicReference<Map<String, Object>> lastConfig = new AtomicReference<>();

		public StubLoadStep config(final Object cfg) {
			lastConfig.set(convert(cfg));
			return new StubLoadStep();
		}

		private Map<String, Object> convert(final Object cfg) {
			if (cfg instanceof Map) {
				return (Map<String, Object>) cfg;
			}
			try {
				final Class<?> somClass = Class.forName("jdk.nashorn.api.scripting.ScriptObjectMirror");
				if (somClass.isInstance(cfg)) {
					return (Map<String, Object>) somClass.getMethod("to", Class.class).invoke(cfg, Map.class);
				}
			} catch (final Exception ignore) {}
			throw new IllegalArgumentException("Unsupported config object: " + cfg);
		}
	}

	public static final class StubLoadStep {
		public StubScenario start() {
			return new StubScenario();
		}
	}

	public static final class StubScenario {
		public void await() {}

		public void close() {}
	}

	private static final Pattern ENV_PATTERN = Pattern.compile("%\\{env:[^:=}]+:=([^}]*)}");

	private static String resolveEnvDefaults(final String script) {
		final Matcher matcher = ENV_PATTERN.matcher(script);
		final StringBuffer buff = new StringBuffer();
		while (matcher.find()) {
			final String defaultValue = matcher.group(1);
			final String replacement = defaultValue.isEmpty() ? "0" : defaultValue;
			matcher.appendReplacement(buff, Matcher.quoteReplacement(replacement));
		}
		matcher.appendTail(buff);
		return buff.toString();
	}
}
