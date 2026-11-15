package com.dell.spt.base.storage.driver.bucket;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Level;

/**
 * A notice of what a driver's first-use bucket versioning check found or changed.
 *
 * <p>The driver logs it on its own engine, an INFO to the messages and a WARN to the errors. It
 * also appends it, synchronously, as one line to the step's {@code bucket.versioning.log}, the
 * file that {@link Loggers#BUCKET_VERSIONING}'s appender names. That file is an internal transport,
 * not a result artifact: on a distributed step the Entry Node copies each other node's lines into
 * its own messages and errors (see {@code BucketVersioningRelay}), so the result directory the CLI
 * fetches has every node's notices, not only the Entry Node's. The line is written before the
 * driver goes on, so it is in the file before the step's slices report they have stopped.
 *
 * <p>Each check is bracketed by a {@link Kind#CHECK} and a {@link Kind#DONE} line (see {@link
 * #check}), so the Entry Node can tell a check still running from one that left no notice.
 *
 * <p>A driver logs one notice per bucket it checks, so a notice is never on the per-operation path.
 */
public final class BucketVersioningNotice {

	/** The name of the file each engine writes for a step. */
	public static final String FILE_NAME = "bucket.versioning.log";

	/** Line breaks and other control characters, which would split a notice into two lines. */
	private static final Pattern CONTROL = Pattern.compile("[\\p{Cntrl}\\u0085\\u2028\\u2029]+");

	private static final Object WRITE_LOCK = new Object();

	private BucketVersioningNotice() {}

	/** What a line of the file is. */
	public enum Kind {
		/** A notice for the messages. */
		INFO,
		/** A notice for the errors. */
		WARN,
		/** A bucket's check started; its text is the bucket. */
		CHECK,
		/** A bucket's check ended; its text is the bucket. */
		DONE
	}

	/** Logs an INFO notice: {@code text} names the bucket and what was found, with nothing changed. */
	public static void info(final String stepId, final String text) {
		log(Kind.INFO, stepId, text);
	}

	/**
	 * Logs a WARN notice: {@code text} names the bucket, what went wrong or what the run changed on it, and what
	 * the run does.
	 */
	public static void warn(final String stepId, final String text) {
		log(Kind.WARN, stepId, text);
	}

	/**
	 * Marks the start of a bucket's check; closing the result marks its end, whatever the check
	 * logged.
	 *
	 * @param bucket the bucket, as its notices name it, for example {@code "bucket b1"}
	 */
	public static Check check(final String stepId, final String bucket) {
		final var label = oneLine(bucket);
		write(stepId, Kind.CHECK, label);
		return () -> write(stepId, Kind.DONE, label);
	}

	/** A check started by {@link #check}; {@link #close} never throws. */
	@FunctionalInterface
	public interface Check extends AutoCloseable {
		@Override
		void close();
	}

	private static void log(final Kind kind, final String stepId, final String text) {
		final var line = oneLine(text);
		if (kind == Kind.WARN) {
			Loggers.ERR.warn("{}: {}", stepId, line);
		} else {
			Loggers.MSG.info("{}: {}", stepId, line);
		}
		write(stepId, kind, line);
	}

	/** {@code text} on one line: each run of control characters, line breaks included, becomes a space. */
	public static String oneLine(final String text) {
		return CONTROL.matcher(text).replaceAll(" ").strip();
	}

	private static void write(final String stepId, final Kind kind, final String text) {
		final byte[] bytes = (kind.name() + " " + text + "\n").getBytes(StandardCharsets.UTF_8);
		try {
			final var fileName = FileManager.INSTANCE.logFileName(Loggers.BUCKET_VERSIONING.getName(), stepId);
			synchronized (WRITE_LOCK) {
				final var parent = Path.of(fileName).getParent();
				if (parent != null) {
					Files.createDirectories(parent);
				}
				FileManager.INSTANCE.writeToFile(fileName, bytes);
			}
		} catch (final IOException | RuntimeException e) {
			Loggers.ERR.warn("{}: could not record a bucket versioning notice for the Entry Node to relay: {}", stepId,
							e.toString());
		}
	}

	/**
	 * A line of {@code bucket.versioning.log}: its kind and text.
	 */
	public record Line(Kind kind, String text) {

		/** The level a notice is relayed at: WARN for {@link Kind#WARN}, INFO otherwise. */
		public Level level() {
			return kind == Kind.WARN ? Level.WARN : Level.INFO;
		}

		/** Reads a line {@code "<KIND> <text>"}; null for a blank, malformed or unknown line. */
		public static Line parse(final String line) {
			final var trimmed = line.strip();
			final var space = trimmed.indexOf(' ');
			if (space <= 0 || space == trimmed.length() - 1) {
				return null;
			}
			final Kind kind;
			try {
				kind = Kind.valueOf(trimmed.substring(0, space));
			} catch (final IllegalArgumentException unknown) {
				return null;
			}
			return new Line(kind, trimmed.substring(space + 1));
		}
	}
}
