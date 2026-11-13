package com.dell.spt.base.control.run;

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

	/**
	 * Runs the scenario. A scenario failure is logged and ends the run; a node's run reports it
	 * through the step status, so it is not thrown, except a typed terminal failure.
	 */
	@Override
	public final void run() {
		evaluate(false);
	}

	/**
	 * Runs the scenario as {@link #run()} does, for an engine started with a scenario, and returns the
	 * scenario's failure so that the engine exits with a failure status.
	 *
	 * @return the logged failure of the scenario, or {@code null} when it ran to its end
	 * @throws RunTerminalException for a typed terminal failure, as {@link #run()} does
	 */
	public Throwable runAndReturnFailure() {
		return evaluate(true);
	}

	private Throwable evaluate(final boolean logEveryFailure) {
		Loggers.SCENARIO.log(Level.INFO, scenario);
		try (final var scope = RunScope.open()) {
			scriptEngine.eval(scenario);
		} catch (final RuntimeException e) {
			final var cause = e.getCause();
			if (cause instanceof InterruptedException) {
				throwUnchecked(cause);
			}
			rethrowRunTerminal(e);
			if (logEveryFailure) {
				LogUtil.exception(Level.ERROR, e, "Scenario failed");
			}
			return e;
		} catch (final ScriptException e) {
			final var terminalCause = RunTerminalException.find(e);
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
			return e;
		}
		return null;
	}

	private static void rethrowRunTerminal(final Throwable outer) {
		final var terminalCause = RunTerminalException.find(outer);
		if (terminalCause != null) {
			LogUtil.exception(Level.ERROR, terminalCause, "Scenario terminal failure");
			throw terminalCause;
		}
	}
}
