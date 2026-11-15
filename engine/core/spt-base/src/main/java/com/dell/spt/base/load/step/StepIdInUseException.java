package com.dell.spt.base.load.step;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Refuses a step at its start, before any load, when an earlier step with the same id left a
 * result file in this log directory that the step would refuse to replace when it publishes its
 * own: a step's evidence is never mixed with, or replaced by, another step's. The checks at
 * publication stay as a second line of defence.
 */
public final class StepIdInUseException extends RunTerminalException {

	private static final long serialVersionUID = 1L;

	public StepIdInUseException(final String stepId, final Path existing) {
		super(Category.CONFIGURATION, stepId, message(stepId, existing), null);
	}

	/** The step's file {@code existing} exists on the remote node {@code node}. */
	public StepIdInUseException(final String stepId, final String node, final String existing) {
		super(Category.CONFIGURATION, stepId, "step id \"" + stepId + "\" is already used in the log directory of node "
						+ node + " (" + existing + " exists); give each step its own load.step.id", null);
	}

	static String message(final String stepId, final Path existing) {
		return "step id \"" + stepId + "\" is already used in this log directory (" + existing
						+ " exists); give each step its own load.step.id";
	}

	/**
	 * Refuses step {@code stepId} when an earlier step left any of {@code paths}.
	 *
	 * @param paths the files the step would refuse to replace
	 * @throws StepIdInUseException for the first of {@code paths} that exists
	 * @throws StepIdCheckException for the first of {@code paths} that cannot be tested
	 */
	public static void refuseExisting(final String stepId, final Iterable<Path> paths) {
		for (final Path path : paths) {
			final boolean exists;
			try {
				exists = StepIdFiles.exists(path);
			} catch (final IOException unknown) {
				throw StepIdCheckException.here(stepId, path.toString(), unknown);
			}
			if (exists) {
				throw new StepIdInUseException(stepId, path);
			}
		}
	}
}
