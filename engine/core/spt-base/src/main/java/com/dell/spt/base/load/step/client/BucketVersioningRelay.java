package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import org.apache.logging.log4j.Level;

/**
 * Copies the bucket versioning notices of every node but the Entry Node into the Entry Node's
 * messages and errors when a distributed step ends, so the result directory the CLI fetches shows
 * what each node's first-use check found or changed (see {@link BucketVersioningNotice}). The
 * Entry Node's own driver logs its notices there directly.
 *
 * <p>A node without notices has no file, which is normal. A file that can't be read is a WARN, not
 * a step failure. The relay reads at most {@link #MAX_BYTES} of a node's file.
 */
final class BucketVersioningRelay implements AutoCloseable {

	/** The most of a node's file the relay reads: a notice line per bucket per driver is far less. */
	static final int MAX_BYTES = 1 << 20;

	private final String loadStepId;
	private final List<FileManager> fileManagers;
	private final List<String> nodeNames;
	private final BiConsumer<Level, String> output;

	/**
	 * @param fileManagers one per slice, the Entry Node's first
	 * @param nodeNames one per slice, for the messages
	 */
	BucketVersioningRelay(final String loadStepId, final List<FileManager> fileManagers, final List<String> nodeNames) {
		this(loadStepId, fileManagers, nodeNames, BucketVersioningRelay::log);
	}

	/** @param output takes each relayed record: a WARN for the errors, anything else for the messages */
	BucketVersioningRelay(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<String> nodeNames,
					final BiConsumer<Level, String> output) {
		this.output = output;
		if (fileManagers.size() != nodeNames.size()) {
			throw new IllegalArgumentException("one file manager and name per slice");
		}
		this.loadStepId = loadStepId;
		this.fileManagers = List.copyOf(fileManagers);
		this.nodeNames = List.copyOf(nodeNames);
	}

	/**
	 * Whether the step or any of its contexts turns on {@code storage.object.versioning}; the
	 * Mongoose synonym is already resolved into it on the step configuration.
	 */
	static boolean versioningRequested(final Config config, final List<Config> ctxConfigs) {
		if (versioning(config)) {
			return true;
		}
		if (ctxConfigs != null) {
			for (final var ctxConfig : ctxConfigs) {
				if (versioning(ctxConfig)) {
					return true;
				}
			}
		}
		return false;
	}

	private static boolean versioning(final Config config) {
		if (config == null) {
			return false;
		}
		try {
			return Boolean.parseBoolean(String.valueOf(config.val(HttpVersioningSynonym.TARGET_PATH)));
		} catch (final NoSuchElementException | InvalidValuePathException e) {
			return false;
		}
	}

	@Override
	public void close() {
		for (int i = 1; i < fileManagers.size(); i++) {
			final var node = String.format("%03d", i);
			try {
				relay(i, node);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				output.accept(Level.WARN, loadStepId + ": could not read the bucket versioning notices of node " + node + " ("
								+ nodeNames.get(i) + "): " + e.getMessage());
			}
		}
	}

	private void relay(final int index, final String node) throws IOException {
		final var fileManager = fileManagers.get(index);
		final var fileName = fileManager.logFileName(Loggers.BUCKET_VERSIONING.getName(), loadStepId);
		final var content = read(fileManager, fileName);
		if (content == null) {
			return;
		}
		for (final var raw : content.split("\n", -1)) {
			final var line = BucketVersioningNotice.Line.parse(raw);
			if (line == null) {
				continue;
			}
			output.accept(line.level(), loadStepId + ": node " + node + " (" + nodeNames.get(index) + "): " + line.text());
		}
	}

	private static void log(final Level level, final String message) {
		(Level.WARN.equals(level) ? Loggers.ERR : Loggers.MSG).log(level, "{}", message);
	}

	/** The file's text, or null when the node has none. */
	private static String read(final FileManager fileManager, final String fileName) throws IOException {
		final var out = new ByteArrayOutputStream();
		try {
			while (out.size() < MAX_BYTES) {
				final byte[] bytes = fileManager.readFromFile(fileName, out.size());
				if (bytes.length == 0) {
					break;
				}
				out.write(bytes, 0, Math.min(bytes.length, MAX_BYTES - out.size()));
			}
		} catch (final EOFException expected) {
			// FileManager signals normal EOF with this exception.
		} catch (final NoSuchFileException absent) {
			return null;
		}
		if (out.size() == 0) {
			return null;
		}
		// A cut at MAX_BYTES leaves a partial last line, which is relayed as it is.
		return out.toString(StandardCharsets.UTF_8);
	}
}
