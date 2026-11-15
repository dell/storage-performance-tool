package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.config.HttpVersioningSynonym;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.bucket.BucketVersioningNotice;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.logging.log4j.Level;

/**
 * Copies the bucket versioning notices of every node but the Entry Node into the Entry Node's
 * messages and errors, so the result directory the CLI fetches shows what each node's first-use
 * check found or changed (see {@link BucketVersioningNotice}). The Entry Node's own driver logs its
 * notices there directly.
 *
 * <p>The step client creates the relay when the step starts, and the relay notes how long each
 * node's file is then: it relays only what this step run wrote, even when an earlier run used the
 * same step ID. It relays when the step's slices have stopped and before the step's total metrics
 * are published ({@link #relayAtStop}), since the CLI may fetch the logs once it sees those
 * metrics. A check still running then is waited for, up to a bound, and named in a WARN if it
 * hasn't ended. {@link #close} relays anything written later.
 *
 * <p>A node without notices has an empty or no file, which is normal. A file that can't be read is
 * a WARN, not a step failure. The relay reads at most {@link #MAX_BYTES} of a node's file per step
 * run, relays only whole lines, and says so when it stops short.
 */
final class BucketVersioningRelay implements AutoCloseable {

	/** The most of a node's file the relay reads per step run: a notice line per bucket per driver is far less. */
	static final int MAX_BYTES = 1 << 20;

	/** How long the stop pass waits for checks still running. */
	static final long PENDING_WAIT_MILLIS = 10_000;

	private static final long POLL_MILLIS = 50;

	private final String loadStepId;
	private final BiConsumer<Level, String> output;
	private final long pendingWaitMillis;
	private final List<Node> nodes = new ArrayList<>();

	/**
	 * @param fileManagers one per slice, the Entry Node's first
	 * @param nodeNames one per slice, for the messages
	 */
	BucketVersioningRelay(final String loadStepId, final List<FileManager> fileManagers, final List<String> nodeNames) {
		this(loadStepId, fileManagers, nodeNames, BucketVersioningRelay::log, PENDING_WAIT_MILLIS);
	}

	/** @param output takes each relayed record: a WARN for the errors, anything else for the messages */
	BucketVersioningRelay(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<String> nodeNames,
					final BiConsumer<Level, String> output) {
		this(loadStepId, fileManagers, nodeNames, output, PENDING_WAIT_MILLIS);
	}

	BucketVersioningRelay(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<String> nodeNames,
					final BiConsumer<Level, String> output,
					final long pendingWaitMillis) {
		if (fileManagers.size() != nodeNames.size()) {
			throw new IllegalArgumentException("one file manager and name per slice");
		}
		this.loadStepId = loadStepId;
		this.output = output;
		this.pendingWaitMillis = pendingWaitMillis;
		for (int i = 1; i < fileManagers.size(); i++) {
			nodes.add(new Node(fileManagers.get(i), "node " + String.format("%03d", i) + " (" + nodeNames.get(i) + ")"));
		}
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

	/**
	 * Relays what the nodes wrote, once the slices have stopped: waits up to the bound for any check
	 * still running, then names each one that hasn't ended in a WARN.
	 */
	synchronized void relayAtStop() {
		relayAll();
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(pendingWaitMillis);
		while (anyPending() && System.nanoTime() < deadline) {
			try {
				Thread.sleep(POLL_MILLIS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
			relayAll();
		}
		warnPending();
	}

	/** Relays anything written since the last pass. */
	@Override
	public synchronized void close() {
		relayAll();
		warnPending();
	}

	private void relayAll() {
		for (final var node : nodes) {
			node.relay();
		}
	}

	private boolean anyPending() {
		return nodes.stream().anyMatch(node -> node.problem == null && !node.pending.isEmpty());
	}

	private void warnPending() {
		for (final var node : nodes) {
			if (node.problem == null && !node.pending.isEmpty() && !node.pendingWarned) {
				node.pendingWarned = true;
				output.accept(Level.WARN, loadStepId + ": " + node.label + ": the versioning check of "
								+ String.join(", ", node.pending.keySet()) + " had not ended when the step stopped; its notice"
								+ " may be only in that node's " + BucketVersioningNotice.FILE_NAME);
			}
		}
	}

	private static void log(final Level level, final String message) {
		if (Level.WARN.equals(level)) {
			Loggers.ERR.warn("{}", message);
		} else {
			Loggers.MSG.info("{}", message);
		}
	}

	/** One node's file and how far the relay has read it. */
	private final class Node {

		private final FileManager fileManager;
		private final String label;
		private final Map<String, Integer> pending = new LinkedHashMap<>();
		private String fileName;
		/** Why this node's notices can't be relayed; null while they can. */
		private String problem;
		private boolean problemWarned;
		private boolean pendingWarned;
		private boolean truncationWarned;
		private long offset;
		private long read;

		Node(final FileManager fileManager, final String label) {
			this.fileManager = fileManager;
			this.label = label;
			try {
				fileName = fileManager.logFileName(Loggers.BUCKET_VERSIONING.getName(), loadStepId);
				final var name = fileName == null ? null : Path.of(fileName).getFileName();
				if (name == null || !BucketVersioningNotice.FILE_NAME.equals(name.toString())) {
					// An engine without the logger names its root appender's file instead.
					problem = "its engine writes no " + BucketVersioningNotice.FILE_NAME;
					return;
				}
				// What an earlier run with the same step ID wrote is not this run's.
				offset = fileManager.fileSize(fileName);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				problem = String.valueOf(e.getMessage());
			}
		}

		void relay() {
			if (problem != null) {
				if (!problemWarned) {
					problemWarned = true;
					output.accept(Level.WARN, loadStepId + ": could not relay the bucket versioning notices of " + label
									+ ": " + problem);
				}
				return;
			}
			try {
				final long size = fileManager.fileSize(fileName);
				boolean capped = false;
				while (offset < size) {
					if (read >= MAX_BYTES) {
						capped = true;
						break;
					}
					final byte[] bytes;
					try {
						bytes = fileManager.readFromFile(fileName, offset);
					} catch (final EOFException end) {
						break;
					} catch (final IndexOutOfBoundsException partOfALine) {
						// FileManagerImpl's answer when all that is left is part of a line; the next pass reads it.
						break;
					}
					final int limit = (int) Math.min(bytes.length, MAX_BYTES - read);
					final int whole = wholeLines(bytes, limit);
					if (whole > 0) {
						relayLines(new String(bytes, 0, whole, StandardCharsets.UTF_8));
						offset += whole;
						read += whole;
					}
					if (limit < bytes.length) {
						capped = true;
						break;
					}
					if (whole == 0) {
						// Only part of a line so far; the next pass reads it whole.
						break;
					}
				}
				if (capped && !truncationWarned) {
					truncationWarned = true;
					output.accept(Level.WARN, loadStepId + ": " + label + ": its bucket versioning notices pass the "
									+ (MAX_BYTES >> 20) + " MiB the relay reads; the rest are only in that node's "
									+ BucketVersioningNotice.FILE_NAME);
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				problem = String.valueOf(e.getMessage());
				problemWarned = true;
				output.accept(Level.WARN, loadStepId + ": could not read the bucket versioning notices of " + label + ": "
								+ problem);
			}
		}

		private void relayLines(final String text) {
			for (final var raw : text.split("\n", -1)) {
				final var line = BucketVersioningNotice.Line.parse(raw);
				if (line == null) {
					continue;
				}
				switch (line.kind()) {
				case CHECK -> pending.merge(line.text(), 1, Integer::sum);
				case DONE -> pending.computeIfPresent(line.text(), (bucket, count) -> count > 1 ? count - 1 : null);
				default -> output.accept(line.level(), loadStepId + ": " + label + ": " + line.text());
				}
			}
		}
	}

	/** The length of the whole lines in the first {@code limit} bytes. */
	private static int wholeLines(final byte[] bytes, final int limit) {
		for (int i = limit - 1; i >= 0; i--) {
			if (bytes[i] == '\n') {
				return i + 1;
			}
		}
		return 0;
	}
}
