package com.dell.spt.base.load.step;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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

	private StepIdInUseException(final String stepId, final String message) {
		super(Category.CONFIGURATION, stepId, message, null);
	}

	/**
	 * The step's file {@code existing} exists on the node {@code node}, the engine {@code slice + 1}
	 * of {@code sliceCount}. The message is public, so it counts the engine and leaves its address
	 * and the file to the log.
	 */
	public static StepIdInUseException onNode(
					final String stepId, final int slice, final int sliceCount, final String node, final String existing) {
		Loggers.ERR.error("{}: an earlier step with this id left {} on node {}", stepId, existing, node);
		return new StepIdInUseException(stepId, "engine " + (slice + 1) + " of " + sliceCount + ": step id \"" + stepId
						+ "\" is already used in that node's log directory; see the entry's log");
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

	/**
	 * Those of {@code paths}, the files step {@code stepId} would refuse to replace, that an earlier
	 * step left, in order.
	 *
	 * @throws StepIdCheckException for the first of {@code paths} that cannot be tested
	 */
	public static List<Path> existing(final String stepId, final Iterable<Path> paths) {
		final var existing = new ArrayList<Path>();
		for (final Path path : paths) {
			try {
				if (StepIdFiles.exists(path)) {
					existing.add(path);
				}
			} catch (final IOException unknown) {
				throw StepIdCheckException.here(stepId, path.toString(), unknown);
			}
		}
		return existing;
	}
}
