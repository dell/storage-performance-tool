package com.dell.spt.base.storage.driver.bucket;

import com.dell.spt.base.logging.Loggers;
import org.apache.logging.log4j.Level;

/**
 * A notice of what a driver's first-use bucket versioning check found or changed.
 *
 * <p>The driver logs it on its own engine, an INFO to the messages and a WARN to the errors, and
 * writes it as one line to the step's {@code bucket.versioning.log} ({@link Loggers#BUCKET_VERSIONING}).
 * On a distributed step the Entry Node copies each other node's lines into its own messages and
 * errors (see {@code BucketVersioningRelay}), so the result directory the CLI fetches has every
 * node's notices, not only the Entry Node's.
 *
 * <p>A driver logs one notice per bucket it checks, so a notice is never on the per-operation path.
 */
public final class BucketVersioningNotice {

	private BucketVersioningNotice() {}

	/** Logs an INFO notice: {@code text} names the bucket and what was found or changed. */
	public static void info(final String stepId, final String text) {
		log(Level.INFO, stepId, text);
	}

	/** Logs a WARN notice: {@code text} names the bucket, what went wrong, and what the run does. */
	public static void warn(final String stepId, final String text) {
		log(Level.WARN, stepId, text);
	}

	private static void log(final Level level, final String stepId, final String text) {
		(Level.WARN.equals(level) ? Loggers.ERR : Loggers.MSG).log(level, "{}: {}", stepId, text);
		Loggers.BUCKET_VERSIONING.log(level, text);
	}

	/**
	 * A line of {@code bucket.versioning.log}: its level and text.
	 *
	 * @param level INFO or WARN; any other level a line names is read as INFO
	 */
	public record Line(Level level, String text) {

		/** Reads a line {@code "<LEVEL> <text>"}; null for a blank or malformed line. */
		public static Line parse(final String line) {
			final var trimmed = line.strip();
			final var space = trimmed.indexOf(' ');
			if (space <= 0 || space == trimmed.length() - 1) {
				return null;
			}
			final var level = Level.WARN.name().equals(trimmed.substring(0, space)) ? Level.WARN : Level.INFO;
			return new Line(level, trimmed.substring(space + 1));
		}
	}
}
