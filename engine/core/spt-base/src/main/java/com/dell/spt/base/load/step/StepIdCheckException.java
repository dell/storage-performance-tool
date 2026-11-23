package com.dell.spt.base.load.step;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.logging.LogUtil;
import org.apache.logging.log4j.Level;

/**
 * Refuses a step at its start, before any load, when one of the files {@link StepIdInUseException}
 * tests could not be tested, or, on a worker engine, could not be moved aside: a node that cannot tell whether an earlier step with the same id left
 * a file there must not run a slice that may publish over it. Like a node that cannot be reached at
 * the step's start, it is an execution failure, not a sign that the id is in use. The message is
 * public, so it counts the engine and leaves its address, the file and the cause to the log.
 */
public final class StepIdCheckException extends RunTerminalException {

	private static final long serialVersionUID = 1L;

	private StepIdCheckException(final String stepId, final String message, final Throwable cause) {
		super(Category.EXECUTION, stepId, message, cause);
	}

	/** The step's file {@code file} in this engine's log directory could not be tested. */
	public static StepIdCheckException here(final String stepId, final String file, final Throwable cause) {
		LogUtil.exception(Level.ERROR, cause, "{}: could not test whether an earlier step left {}", stepId, file);
		return new StepIdCheckException(
						stepId, "step id \"" + stepId + "\" could not be checked in this log directory; see the engine log", cause);
	}

	/**
	 * A worker engine found {@code file}, which an earlier run left under the step's id, and could not
	 * move it aside: the slice must not run with the earlier run's file in place.
	 */
	public static StepIdCheckException notMovedAside(final String stepId, final String file, final Throwable cause) {
		LogUtil.exception(Level.ERROR, cause, "{}: could not move aside {}, which an earlier run left", stepId, file);
		return new StepIdCheckException(
						stepId,
						"step id \"" + stepId + "\" was used by an earlier run in this worker's log directory, and its files "
										+ "could not be moved aside; see the engine log",
						cause);
	}

	/**
	 * The step's file {@code file} on the node {@code node}, the engine {@code slice + 1} of {@code
	 * sliceCount}, could not be tested.
	 */
	public static StepIdCheckException onNode(
					final String stepId,
					final int slice,
					final int sliceCount,
					final String node,
					final String file,
					final Throwable cause) {
		LogUtil.exception(
						Level.ERROR, cause, "{}: could not test whether an earlier step left {} on node {}", stepId, file, node);
		return new StepIdCheckException(
						stepId,
						"engine " + (slice + 1) + " of " + sliceCount + ": step id \"" + stepId
										+ "\" could not be checked in that node's log directory; see the entry's log",
						cause);
	}
}
