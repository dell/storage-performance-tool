package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.logging.Loggers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/** The WARN lines of the errors logger while it is open; the engine's loggers are asynchronous. */
final class CollectionWarnings extends AbstractAppender implements AutoCloseable {
	private final List<String> messages = new CopyOnWriteArrayList<>();
	private final org.apache.logging.log4j.core.Logger errors = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
	private final Level level = errors.getLevel();

	CollectionWarnings() {
		super("missingSliceWarnings", null, null, true, Property.EMPTY_ARRAY);
		start();
		errors.addAppender(this);
		errors.setLevel(Level.WARN);
	}

	@Override
	public void append(final LogEvent event) {
		if (Level.WARN.equals(event.getLevel())) {
			messages.add(event.getMessage().getFormattedMessage());
		}
	}

	void await(final String... parts) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		for (final String part : parts) {
			while (messages.stream().noneMatch(message -> message.contains(part))) {
				assertTrue(System.nanoTime() < deadline, "no warning with " + part + " in " + messages);
				Thread.sleep(5);
			}
		}
	}

	/**
	 * Logs a marker line of {@code stepId} and waits for it, so every warning logged before it on
	 * this thread has arrived too; the marker is not one of {@link #lines}.
	 */
	void flush(final String stepId) throws InterruptedException {
		final String marker = stepId + ": end of the collection's warnings";
		Loggers.ERR.warn(marker);
		await(marker);
		messages.remove(marker);
	}

	/** Flushes the warnings of {@code stepId} logged before this, and forgets every line so far. */
	void clear(final String stepId) throws InterruptedException {
		flush(stepId);
		messages.clear();
	}

	List<String> lines(final String stepId) {
		final List<String> lines = new ArrayList<>();
		for (final String message : messages) {
			if (message.startsWith(stepId + ": ")) {
				lines.add(message);
			}
		}
		return lines;
	}

	@Override
	public void close() {
		errors.removeAppender(this);
		errors.setLevel(level);
		stop();
	}
}
