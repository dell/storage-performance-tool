package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A Groovy or Python scenario with no engine installed fails with a message that names the missing
 * engine; any other file still falls back to JavaScript. The test classpath has no Groovy or Jython
 * engine, as the image does not.
 */
class ScenarioUtilScriptEngineTest {

	@TempDir
	Path dir;

	private Path scenario(final String fileName) throws Exception {
		final var path = dir.resolve(fileName);
		// valid JavaScript as well, like the shipped py/default.py
		Files.writeString(path, "Load.run();\n");
		return path;
	}

	private ScriptEngine engineFor(final String fileName) throws Exception {
		return ScenarioUtil.scriptEngineByFilePath(
						scenario(fileName), Thread.currentThread().getContextClassLoader());
	}

	@ParameterizedTest
	@CsvSource({"default.groovy, Groovy, groovy", "default.py, Python, py", "DEFAULT.PY, Python, PY"
	})
	void aGroovyOrPythonScenarioWithoutItsEngineFailsNamingTheEngine(
					final String fileName, final String lang, final String ext) {
		final var e = assertThrows(ScriptEngineNotInstalledException.class, () -> engineFor(fileName));
		assertEquals(
						"The " + lang + " script engine is not installed, so the \"." + ext
										+ "\" scenario cannot run: put a JSR-223 " + lang
										+ " engine jar in the ext/ directory next to spt.jar"
										+ " (doc/usage/input/scenarios/README.md, section 3.2)."
										+ " JavaScript scenarios need no engine jar.",
						e.getMessage());
		assertFalse(e.getMessage().contains(dir.toString()), "the message must not carry the scenario path");
	}

	@ParameterizedTest
	@ValueSource(strings = {"default.js", "scenario", "scenario.txt", ".groovy"
	})
	void anyOtherScenarioStillRunsAsJavaScript(final String fileName) throws Exception {
		final var se = engineFor(fileName);
		assertNotNull(se);
		assertTrue(
						se.getFactory().getExtensions().contains("js"),
						"expected the JavaScript engine, got " + se.getFactory().getEngineName());
	}

	/**
	 * A class loader whose services list {@link FakeScriptEngineFactory}, as an engine jar in
	 * {@code ext/} would.
	 */
	private ClassLoader fakeEngineClassLoader() throws Exception {
		final var services = dir.resolve("fake-engine/META-INF/services/javax.script.ScriptEngineFactory");
		Files.createDirectories(services.getParent());
		Files.writeString(services, FakeScriptEngineFactory.class.getName() + "\n");
		return new URLClassLoader(
						new URL[]{dir.resolve("fake-engine").toUri().toURL()
						},
						Thread.currentThread().getContextClassLoader());
	}

	@ParameterizedTest
	@ValueSource(strings = {"t.groovy", "t.py", "T.GROOVY", "T.PY", "t.Groovy"
	})
	void anInstalledGroovyOrPythonEngineIsUsed(final String fileName) throws Exception {
		final var se = ScenarioUtil.scriptEngineByFilePath(scenario(fileName), fakeEngineClassLoader());
		assertNotNull(se);
		assertEquals(FakeScriptEngineFactory.ENGINE_NAME, se.getFactory().getEngineName());
	}

	@Test
	void theDefaultEngineIsJavaScript() {
		final var se = ScenarioUtil.scriptEngineByDefault(Thread.currentThread().getContextClassLoader());
		assertTrue(se.getFactory().getExtensions().contains("js"));
	}
}
