package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.Constants;
import com.dell.spt.base.logging.Loggers;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/** The WARN and higher lines of the Errors logger, the {@code errors.log} of each step, while attached. */
final class ErrorsLogCapture extends AbstractAppender implements AutoCloseable {

	/** One captured line: the step it was logged for ({@code step_id}, or null) and its message. */
	record Line(String stepId, String message) {}

	private static final String DRAINED = "errors-log-capture-drained";

	private final List<Line> lines = new CopyOnWriteArrayList<>();
	private final org.apache.logging.log4j.core.Logger logger;
	private volatile CountDownLatch drain = new CountDownLatch(1);

	/** Captures from now on: lines logged earlier, which the asynchronous logger may still deliver, are dropped. */
	ErrorsLogCapture() throws InterruptedException {
		super("errors-log-capture", null, null, true, Property.EMPTY_ARRAY);
		logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.ERR.getName());
		start();
		logger.addAppender(this);
		drained();
		lines.clear();
	}

	@Override
	public void append(final LogEvent event) {
		final var message = event.getMessage().getFormattedMessage();
		if (DRAINED.equals(message)) {
			drain.countDown();
		} else if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
			lines.add(new Line(event.getContextData().getValue(Constants.KEY_STEP_ID), message));
		}
	}

	/** Every line so far; a marker logged after them on the same, asynchronous, logger has arrived. */
	List<Line> drained() throws InterruptedException {
		final var marker = new CountDownLatch(1);
		drain = marker;
		Loggers.ERR.error(DRAINED);
		assertTrue(marker.await(30, TimeUnit.SECONDS), "the Errors logger drained");
		return List.copyOf(lines);
	}

	/** The captured lines that report a 2xx completion's error code. */
	List<Line> completionErrorLines() throws InterruptedException {
		return drained().stream().filter(line -> line.message().contains("multipart completion answered 2xx")).toList();
	}

	/** The captured lines that report the S3 error code and message of a failed response. */
	List<Line> failedResponseLines() throws InterruptedException {
		return drained().stream().filter(line -> line.message().contains(": S3 error code ")
						&& line.message().contains(", message: ")).toList();
	}

	@Override
	public void close() {
		logger.removeAppender(this);
		stop();
	}
}
