package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertFalse;

import com.dell.spt.base.logging.Loggers;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * Collects the records of the engine's error logger while open. The logger is asynchronous, so a
 * read first sends a marker through it and waits for the marker to arrive.
 */
final class ErrorLogCapture implements AutoCloseable {

	private static final String MARKER = "mixed-error-log-capture-marker-";
	private static final AtomicInteger MARKERS = new AtomicInteger();
	private static final long MARKER_WAIT_NANOS = TimeUnit.SECONDS.toNanos(10);

	private final List<LogEvent> events = new CopyOnWriteArrayList<>();
	private final AbstractAppender appender;

	ErrorLogCapture() {
		appender = new AbstractAppender("mixedErrorLogCapture", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				events.add(event.toImmutable());
			}
		};
		appender.start();
		LoggerContext.getContext(false).getLogger(Loggers.ERR.getName()).addAppender(appender);
	}

	/** The ERROR and FATAL messages logged so far that contain {@code text}. */
	List<String> errorsContaining(final String text) throws InterruptedException {
		final String marker = MARKER + MARKERS.incrementAndGet();
		Loggers.ERR.warn(marker);
		final long deadline = System.nanoTime() + MARKER_WAIT_NANOS;
		while (events.stream().noneMatch(event -> marker.equals(event.getMessage().getFormattedMessage()))) {
			assertFalse(System.nanoTime() > deadline, "the log marker did not arrive");
			Thread.sleep(5);
		}
		final List<String> errors = new ArrayList<>();
		for (final LogEvent event : events) {
			final String message = event.getMessage().getFormattedMessage();
			if (event.getLevel().isMoreSpecificThan(Level.ERROR) && message.contains(text)) {
				errors.add(message);
			}
		}
		return errors;
	}

	@Override
	public void close() {
		LoggerContext.getContext(false).getLogger(Loggers.ERR.getName()).removeAppender(appender);
		appender.stop();
	}
}
