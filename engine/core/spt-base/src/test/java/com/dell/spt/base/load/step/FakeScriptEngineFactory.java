package com.dell.spt.base.load.step;

import java.io.Reader;
import java.util.List;
import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.SimpleBindings;

/**
 * A script engine factory that claims the {@code groovy} and {@code py} extensions and evaluates
 * nothing. It is not registered on the test classpath: a test exposes it through its own class
 * loader, so other tests still see no Groovy or Python engine.
 */
public final class FakeScriptEngineFactory implements ScriptEngineFactory {

	public static final String ENGINE_NAME = "Fake Groovy and Python";

	@Override
	public String getEngineName() {
		return ENGINE_NAME;
	}

	@Override
	public String getEngineVersion() {
		return "0";
	}

	@Override
	public List<String> getExtensions() {
		return List.of("groovy", "py");
	}

	@Override
	public List<String> getMimeTypes() {
		return List.of();
	}

	@Override
	public List<String> getNames() {
		return List.of("fake");
	}

	@Override
	public String getLanguageName() {
		return "fake";
	}

	@Override
	public String getLanguageVersion() {
		return "0";
	}

	@Override
	public Object getParameter(final String key) {
		return null;
	}

	@Override
	public String getMethodCallSyntax(final String obj, final String m, final String... args) {
		return "";
	}

	@Override
	public String getOutputStatement(final String toDisplay) {
		return "";
	}

	@Override
	public String getProgram(final String... statements) {
		return "";
	}

	@Override
	public ScriptEngine getScriptEngine() {
		final var factory = this;
		return new AbstractScriptEngine() {

			@Override
			public Object eval(final String script, final ScriptContext context) {
				return null;
			}

			@Override
			public Object eval(final Reader reader, final ScriptContext context) {
				return null;
			}

			@Override
			public Bindings createBindings() {
				return new SimpleBindings();
			}

			@Override
			public ScriptEngineFactory getFactory() {
				return factory;
			}
		};
	}
}
