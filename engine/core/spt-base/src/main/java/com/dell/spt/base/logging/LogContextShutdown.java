package com.dell.spt.base.logging;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadFactory;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusLogger;

/**
 * Stops the engine's Log4j contexts at exit, so the events still queued for the asynchronous logger
 * thread reach the console and the files before the JVM halts.
 *
 * <p>{@link LogManager#shutdown()} alone is not enough: it stops the context of its caller's class
 * loader, which Log4j finds only through a multi-release class that a jar without
 * {@code Multi-Release: true} does not load. In the packaged engine it finds none, so it stops nothing,
 * and Log4j's own shutdown hook is disabled ({@link ShutdownCallbackRegistryImpl}). The engine's loggers
 * are bound to a default context that the selector does not list, so that context is found through
 * one of the loggers and stopped first, then each listed one.
 */
final class LogContextShutdown {

	/** The wait when the configuration sets no {@code shutdownTimeout}; {@code log4j2.yaml} sets 10 s too. */
	static final long DEFAULT_TIMEOUT_MILLIS = 10_000;

	private LogContextShutdown() {}

	/** The context of the engine's loggers, then each context the selector lists, each once. */
	static List<LoggerContext> engineContexts() {
		final Set<LoggerContext> contexts = Collections.newSetFromMap(new IdentityHashMap<>());
		final List<LoggerContext> ordered = new ArrayList<>();
		if (Loggers.MSG instanceof org.apache.logging.log4j.core.Logger logger
						&& contexts.add(logger.getContext())) {
			ordered.add(logger.getContext());
		}
		if (LogManager.getFactory() instanceof Log4jContextFactory factory) {
			for (final var context : factory.getSelector().getLoggerContexts()) {
				if (contexts.add(context)) {
					ordered.add(context);
				}
			}
		}
		return ordered;
	}

	/** The first context's configured {@code shutdownTimeout}, or {@link #DEFAULT_TIMEOUT_MILLIS}. */
	static long timeoutMillis(final List<LoggerContext> contexts) {
		if (!contexts.isEmpty()) {
			final var config = contexts.get(0).getConfiguration();
			if (config != null && config.getShutdownTimeoutMillis() > 0) {
				return config.getShutdownTimeoutMillis();
			}
		}
		return DEFAULT_TIMEOUT_MILLIS;
	}

	/**
	 * Raises the status logger's level to ERROR, so stopping the contexts prints no INFO status lines on
	 * stderr ({@code Stopping configuration ...}), and an event logged while a context stops prints no
	 * {@code Ignoring log event after log4j was shut down} warning. Call it after the last context is
	 * created: each configuration's {@code status} sets the level again when it starts.
	 */
	static void quietStatusLogger() {
		final var statusLogger = StatusLogger.getLogger();
		statusLogger.setLevel(Level.ERROR);
		for (final var listener : statusLogger.getListeners()) {
			if (listener instanceof StatusConsoleListener consoleListener) {
				consoleListener.setLevel(Level.ERROR);
			}
		}
	}

	/**
	 * Stops the contexts in turn, each with the time left, then calls {@link LogManager#shutdown()}, all
	 * within one deadline. A context's stop waits for its queue to drain; Log4j's own drain wait ignores
	 * the timeout it is given, so the stops run in a daemon thread of their own and the caller waits for
	 * that thread only until the deadline. An empty queue returns at once. The caller writes nothing, so
	 * a full stdout or stderr pipe, which blocks the stops, cannot keep it past the deadline.
	 *
	 * @param first run in the stopper thread before the stops, for a write that may block
	 * @return true when every stop finished before the deadline; false when the stopper was still running
	 *         at the deadline, or the caller was interrupted, and the events still queued may be lost, as
	 *         the JVM halts once the shutdown hooks return
	 */
	static boolean stopAll(final List<LoggerContext> contexts, final long timeoutMillis, final Runnable first) {
		return stopAll(contexts, timeoutMillis, first, LogManager::shutdown, Thread::new);
	}

	/**
	 * {@link #stopAll(List, long, Runnable)}, running {@code last} instead of {@link LogManager#shutdown()}
	 * and making the stopper thread with {@code threads}.
	 */
	static boolean stopAll(
					final List<LoggerContext> contexts,
					final long timeoutMillis,
					final Runnable first,
					final Runnable last,
					final ThreadFactory threads) {
		final long deadline = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
		final var stopper = threads.newThread(() -> {
			first.run();
			for (final var context : contexts) {
				final long remaining = deadline - System.nanoTime();
				if (remaining <= 0) {
					return;
				}
				try {
					context.stop(remaining, NANOSECONDS);
				} catch (final RuntimeException e) {
					e.printStackTrace(System.err);
				}
			}
			last.run();
		});
		stopper.setName("logCtxStopper");
		stopper.setDaemon(true);
		stopper.start();
		try {
			for (long remaining = deadline - System.nanoTime(); remaining > 0 && stopper.isAlive(); remaining = deadline - System.nanoTime()) {
				NANOSECONDS.timedJoin(stopper, remaining);
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return !stopper.isAlive();
	}
}
