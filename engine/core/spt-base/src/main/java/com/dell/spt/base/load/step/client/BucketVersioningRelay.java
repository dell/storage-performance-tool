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
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

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
 * metrics. A check still running then is waited for, and named in a WARN if it hasn't ended.
 * {@link #close} relays anything written later.
 *
 * <p>The relay is best-effort evidence and never holds the step up past its bound. Each pass (at
 * start, at stop and at close) reads every node on a daemon thread of its own, and the step thread
 * waits for them at most {@link #BOUND_MILLIS} from the start of the pass, remote reads and the
 * wait for running checks included: a remote call over RMI can't be interrupted. A node whose
 * thread hasn't finished by then is named in one WARN and left out of the relay for good, and
 * anything its abandoned thread reads later is dropped.
 *
 * <p>A node without notices has an empty or no file, which is normal. A file that can't be read is
 * a WARN, not a step failure. The relay reads at most {@link #MAX_BYTES} of a node's file per step
 * run, relays only whole lines, and says so when it stops short.
 */
final class BucketVersioningRelay implements AutoCloseable {

	/** The most of a node's file the relay reads per step run: a notice line per bucket per driver is far less. */
	static final int MAX_BYTES = 1 << 20;

	/** The most one pass of the relay may hold the step thread, from the start of the pass. */
	static final long BOUND_MILLIS = 10_000;

	/** The end of a pass left for each node's last read once the wait for running checks ends. */
	private static final long GRACE_MILLIS = 500;

	private static final long POLL_MILLIS = 50;

	private final String loadStepId;
	private final BiConsumer<Level, String> output;
	private final long boundMillis;
	private final List<Node> nodes = new ArrayList<>();
	/** Taken to relay a node's record and to abandon a node, so no record of an abandoned node is written. */
	private final Object emitLock = new Object();

	/**
	 * @param fileManagers one per slice, the Entry Node's first
	 * @param nodeNames one per slice, for the messages
	 */
	BucketVersioningRelay(final String loadStepId, final List<FileManager> fileManagers, final List<String> nodeNames) {
		this(loadStepId, fileManagers, nodeNames, BucketVersioningRelay::log, BOUND_MILLIS);
	}

	/** @param output takes each relayed record: a WARN for the errors, anything else for the messages */
	BucketVersioningRelay(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<String> nodeNames,
					final BiConsumer<Level, String> output) {
		this(loadStepId, fileManagers, nodeNames, output, BOUND_MILLIS);
	}

	/** @param boundMillis the most each pass may hold the calling thread */
	BucketVersioningRelay(
					final String loadStepId,
					final List<FileManager> fileManagers,
					final List<String> nodeNames,
					final BiConsumer<Level, String> output,
					final long boundMillis) {
		if (fileManagers.size() != nodeNames.size()) {
			throw new IllegalArgumentException("one file manager and name per slice");
		}
		this.loadStepId = loadStepId;
		this.output = output;
		this.boundMillis = boundMillis;
		final var candidates = new ArrayList<Node>();
		for (int i = 1; i < fileManagers.size(); i++) {
			candidates.add(new Node(fileManagers.get(i), "node " + String.format("%03d", i) + " (" + nodeNames.get(i) + ")"));
		}
		final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(boundMillis);
		final var stalled = runPass(candidates, Node::start, end);
		for (final var node : candidates) {
			if (!stalled.contains(node)) {
				nodes.add(node);
			}
		}
		warnStalled(stalled, "when the step started");
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
	 * Relays what the nodes wrote, once the slices have stopped: waits for any check still running
	 * while the bound allows, then names each one that hasn't ended in a WARN.
	 */
	synchronized void relayAtStop() {
		final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(boundMillis);
		final long waitEnd = end - TimeUnit.MILLISECONDS.toNanos(Math.min(GRACE_MILLIS, boundMillis / 4));
		final var stalled = runPass(nodes, node -> {
			node.relay();
			while (node.waiting() && System.nanoTime() < waitEnd && !node.abandoned) {
				try {
					Thread.sleep(POLL_MILLIS);
				} catch (final InterruptedException e) {
					return;
				}
				node.relay();
			}
		}, end);
		warnPending();
		warnStalled(stalled, "when the step stopped");
	}

	/** Relays anything written since the last pass. */
	@Override
	public synchronized void close() {
		final long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(boundMillis);
		final var stalled = runPass(nodes, Node::relay, end);
		warnPending();
		warnStalled(stalled, "when the step closed");
	}

	/**
	 * Runs the work for each node not yet abandoned on a daemon thread of its own, and waits for them
	 * until {@code end}. Abandons and returns the nodes whose thread is still running then.
	 */
	private List<Node> runPass(final List<Node> passNodes, final Consumer<Node> work, final long end) {
		final var threads = new LinkedHashMap<Node, Thread>();
		// The records go to the step's logs by the caller's log context.
		final Map<String, String> logContext = ThreadContext.getImmutableContext();
		for (final var node : passNodes) {
			if (!node.abandoned) {
				threads.put(node, Thread.ofPlatform().daemon().name("bucket-versioning-relay-" + loadStepId + "-" + node.label)
								.start(() -> {
									try (var ctx = CloseableThreadContext.putAll(logContext)) {
										work.accept(node);
									} catch (final Throwable t) {
										// Already a WARN through the node, or the node was abandoned.
									}
								}));
			}
		}
		boolean interrupted = false;
		final var stalled = new ArrayList<Node>();
		for (final var entry : threads.entrySet()) {
			final long left = end - System.nanoTime();
			if (!interrupted && left > 0) {
				try {
					entry.getValue().join(Duration.ofNanos(left));
				} catch (final InterruptedException e) {
					interrupted = true;
				}
			}
			if (entry.getValue().isAlive()) {
				synchronized (emitLock) {
					entry.getKey().abandoned = true;
				}
				stalled.add(entry.getKey());
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
		return stalled;
	}

	private void warnStalled(final List<Node> stalled, final String when) {
		if (stalled.isEmpty()) {
			return;
		}
		final boolean one = stalled.size() == 1;
		output.accept(Level.WARN, loadStepId + ": " + String.join(", ", stalled.stream().map(node -> node.label).toList())
						+ " did not answer within " + boundMillis + " ms " + when + ", so the relay left " + (one ? "it" : "them")
						+ " out; " + (one ? "its" : "their") + " bucket versioning notices are only in "
						+ (one ? "that node's " : "each of those nodes' ") + BucketVersioningNotice.FILE_NAME);
	}

	private void warnPending() {
		for (final var node : nodes) {
			if (!node.abandoned && node.waiting() && !node.pendingWarned) {
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

	/**
	 * One node's file and how far the relay has read it. Only one thread at a time uses it: a pass's
	 * thread for the node, then the step thread once that thread has ended.
	 */
	private final class Node {

		private final FileManager fileManager;
		private final String label;
		private final Map<String, Integer> pending = new LinkedHashMap<>();
		private String fileName;
		/** Why this node's notices can't be relayed; null while they can. */
		private String problem;
		private boolean problemWarned;
		private boolean pendingWarned;
		/** Whether the cap stopped the relay short, so its checks can't be followed. */
		private boolean capped;
		private boolean truncationWarned;
		/** Set under the emit lock when a pass gave up on the node; it is then out of the relay. */
		private volatile boolean abandoned;
		private long offset;
		private long read;

		Node(final FileManager fileManager, final String label) {
			this.fileManager = fileManager;
			this.label = label;
		}

		/** Resolves the node's file and notes how long it is. */
		void start() {
			try {
				final var name = fileManager.logFileName(Loggers.BUCKET_VERSIONING.getName(), loadStepId);
				final var file = name == null ? null : Path.of(name).getFileName();
				if (file == null || !BucketVersioningNotice.FILE_NAME.equals(file.toString())) {
					// An engine without the logger names its root appender's file instead.
					problem = "its engine writes no " + BucketVersioningNotice.FILE_NAME;
					return;
				}
				// What an earlier run with the same step ID wrote is not this run's.
				offset = fileManager.fileSize(name);
				fileName = name;
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				problem = String.valueOf(e.getMessage());
			}
		}

		/** Whether a check the node started is still open and can still be followed. */
		boolean waiting() {
			return problem == null && !capped && !pending.isEmpty();
		}

		private void emit(final Level level, final String message) {
			synchronized (emitLock) {
				if (!abandoned) {
					output.accept(level, message);
				}
			}
		}

		void relay() {
			if (problem != null) {
				if (!problemWarned) {
					problemWarned = true;
					emit(Level.WARN, loadStepId + ": could not relay the bucket versioning notices of " + label + ": " + problem);
				}
				return;
			}
			try {
				final long size = fileManager.fileSize(fileName);
				while (offset < size && !abandoned) {
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
					emit(Level.WARN, loadStepId + ": " + label + ": its bucket versioning notices pass the "
									+ (MAX_BYTES >> 20) + " MiB the relay reads, so its running checks can't be followed;"
									+ " the rest are only in that node's " + BucketVersioningNotice.FILE_NAME);
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				problem = String.valueOf(e.getMessage());
				problemWarned = true;
				emit(Level.WARN, loadStepId + ": could not read the bucket versioning notices of " + label + ": " + problem);
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
				default -> emit(line.level(), loadStepId + ": " + label + ": " + line.text());
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
