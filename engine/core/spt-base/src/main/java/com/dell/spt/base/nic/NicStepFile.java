package com.dell.spt.base.nic;

import com.dell.spt.base.load.step.file.FileManager;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Logger;

/** Publishes a step's NIC-set evidence file once: staged beside it, then moved into place. */
final class NicStepFile {

	private NicStepFile() {}

	/**
	 * Writes {@code content} to {@code logger}'s file of the step, synchronously after stop: async
	 * logging plus flush does not drain its queue.
	 *
	 * @param what the evidence's name in errors, such as {@code NIC paths}
	 * @throws IllegalStateException when the file already exists or cannot be written
	 */
	static void publish(final Logger logger, final String stepId, final String what, final String content) {
		publish(logger, stepId, what, content, false);
	}

	/**
	 * As {@link #publish(Logger, String, String, String)}, but with {@code replace} an existing file,
	 * left by an earlier step with the same id, is replaced instead of refused.
	 */
	static void publish(
					final Logger logger, final String stepId, final String what, final String content, final boolean replace) {
		Path staging = null;
		try {
			final var path = Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
			Files.createDirectories(path.toAbsolutePath().getParent());
			if (!replace && Files.exists(path)) {
				throw new IOException("stale " + what + " artifact exists");
			}
			staging = Files.createTempFile(path.toAbsolutePath().getParent(), ".nic-", ".tmp");
			Files.writeString(staging, content);
			// An atomic rename replaces an existing target.
			Files.move(staging, path, StandardCopyOption.ATOMIC_MOVE);
		} catch (final IOException e) {
			throw new IllegalStateException("failed to publish the terminal " + what + " evidence", e);
		} finally {
			if (staging != null) {
				try {
					Files.deleteIfExists(staging);
				} catch (final IOException ignored) { /* Primary publication error is retained. */ }
			}
		}
	}

	/**
	 * Removes {@code logger}'s file of the step, and the Entry Node's {@code <name>.node-NNN.<ext>}
	 * copies of it beside it, if an earlier step with the same id left them. A logging configuration
	 * without the logger's file has nothing to remove.
	 *
	 * @throws IllegalStateException when one cannot be removed
	 */
	static void discard(final Logger logger, final String stepId, final String what) {
		final Path path;
		try {
			path = Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId)).toAbsolutePath();
		} catch (final IOException | RuntimeException noFile) {
			return; // Its publication warns at stop.
		}
		final var name = path.getFileName().toString();
		final var dot = name.lastIndexOf('.');
		final var copies = Pattern.compile(
						Pattern.quote(name.substring(0, dot)) + "\\.node-[0-9]{3}" + Pattern.quote(name.substring(dot)));
		try {
			Files.deleteIfExists(path);
			final var dir = path.getParent();
			if (dir != null && Files.isDirectory(dir)) {
				try (final var files = Files.list(dir)) {
					for (final var file : files.filter(file -> copies.matcher(file.getFileName().toString()).matches()).toList()) {
						Files.deleteIfExists(file);
					}
				}
			}
		} catch (final IOException e) {
			throw new IllegalStateException("failed to remove an earlier " + what + " file", e);
		}
	}
}
