package com.dell.spt.base.control.run;

import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import org.apache.logging.log4j.Level;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

public final class RunImpl implements Run {

	private final String comment;
	private final String scenario;
	private final ScriptEngine scriptEngine;
	private final long runId;

	public RunImpl(final String comment, final String scenario, final ScriptEngine scriptEngine, final long runId) {
		this.comment = comment;
		this.scenario = scenario;
		this.scriptEngine = scriptEngine;
		this.runId = runId;
	}

	@Override
	public final long runId() {
		return runId;
	}

	@Override
	public final String comment() {
		return comment;
	}

	@Override
	public final void run() {
		Loggers.SCENARIO.log(Level.INFO, scenario);
		try {
			scriptEngine.eval(scenario);
		} catch (final RuntimeException e) {
			final var cause = e.getCause();
			if (cause instanceof InterruptedException) {
				throwUnchecked(cause);
			}
			rethrowIntegrityTerminal(e);
		} catch (final ScriptException e) {
			final var terminalCause = IntegrityTerminalException.find(e);
			LogUtil.trace(
							Loggers.ERR,
							Level.ERROR,
							e,
							"\nScenario failed, line #{}, column #{}:\n{}",
							e.getLineNumber(),
							e.getColumnNumber(),
							e.getMessage());
			if (terminalCause != null) {
				throw terminalCause;
			}
		}
	}

	private static void rethrowIntegrityTerminal(final Throwable outer) {
		final var terminalCause = IntegrityTerminalException.find(outer);
		if (terminalCause != null) {
			LogUtil.exception(Level.ERROR, terminalCause, "Metadata-integrity scenario failure");
			throw terminalCause;
		}
	}
}
