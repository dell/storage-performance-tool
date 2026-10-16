package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * Captures what the driver logs, down to DEBUG, on the thread that made the capture: only what
 * that thread logs belongs to its test.
 */
final class DriverLogCapture implements AutoCloseable {

	record Event(Level level, String message) {}

	private final List<Event> captured = Collections.synchronizedList(new ArrayList<>());
	private final String thread = Thread.currentThread().getName();
	private final CapturingAppender appender = new CapturingAppender();
	private final org.apache.logging.log4j.core.Logger driverLogger;
	private final Level previousLevel;
	private volatile String flushMarker = "";
	private volatile CountDownLatch flushed = new CountDownLatch(0);

	DriverLogCapture() {
		appender.start();
		driverLogger = LoggerContext.getContext(false).getLogger(S3AwsStorageDriver.class.getName());
		previousLevel = driverLogger.getLevel();
		driverLogger.setLevel(Level.DEBUG);
		driverLogger.addAppender(appender);
	}

	/**
	 * Returns what the driver has logged. The engine's loggers are asynchronous, so a marker is
	 * logged and awaited: the events before it have been delivered when it arrives.
	 */
	List<Event> events() throws InterruptedException {
		final String marker = "flush-" + System.nanoTime();
		final var latch = new CountDownLatch(1);
		flushMarker = marker;
		flushed = latch;
		driverLogger.info(marker);
		assertTrue(latch.await(10, TimeUnit.SECONDS), "the logged events were not delivered");
		synchronized (captured) {
			return List.copyOf(captured);
		}
	}

	static long count(final List<Event> events, final Level level) {
		return events.stream().filter(event -> event.level() == level).count();
	}

	@Override
	public void close() {
		driverLogger.removeAppender(appender);
		driverLogger.setLevel(previousLevel);
		appender.stop();
	}

	private final class CapturingAppender extends AbstractAppender {

		private CapturingAppender() {
			super("s3-aws-driver-capture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (!thread.equals(event.getThreadName())) {
				return;
			}
			if (message.equals(flushMarker)) {
				flushed.countDown();
			} else if (event.getLevel() != Level.DEBUG || !message.endsWith("stack trace")) {
				// The stack trace logged at DEBUG next to every failure is not a message of its own.
				captured.add(new Event(event.getLevel(), message));
			}
		}
	}
}
