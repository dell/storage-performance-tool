package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.logging.Loggers;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * The client's own results of a split-read step, from its operation trace
 * ({@code output.metrics.trace.persist}): one row for each result the step accepted, ranges and
 * reads alike. A range's successful row carries its range's bytes and a read's row none, so the
 * successful rows of {@code rangeSize} bytes are the ranges the client completed, whatever the
 * target received. A read attempt has two rows with the same start: its initial pass, successful
 * and unfinished, and its end; an attempt with only the first had not ended when the step did.
 */
final class RangeResultTrace implements AutoCloseable {

	private static final int PATH = 1;
	private static final int STATUS = 3;
	private static final int START = 4;
	private static final int SIZE = 8;

	private final Logger traces = (Logger) Loggers.OP_TRACES;
	private final LinkedBlockingQueue<String[]> events = new LinkedBlockingQueue<>();
	private final Map<String, List<String[]>> rowsByStep = new HashMap<>();
	private final AbstractAppender appender;
	private final Level previousLevel;

	RangeResultTrace() {
		appender = new AbstractAppender("split-read-range-trace-" + System.nanoTime(), null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				final String stepId = event.getContextData().getValue(KEY_STEP_ID);
				events.add(new String[]{stepId == null ? "" : stepId, event.getMessage().getFormattedMessage()
				});
			}
		};
		appender.start();
		previousLevel = traces.getLevel();
		traces.addAppender(appender);
		traces.setLevel(Level.INFO);
	}

	/**
	 * Every trace row of step {@code stepId} so far: a fence logged after them reaches the appender
	 * after them, on the logger's thread.
	 */
	List<String[]> rows(final String stepId) throws InterruptedException {
		final String fence = "split-read-range-trace-fence-" + System.nanoTime();
		traces.info(fence);
		while (true) {
			final String[] event = events.poll(30, TimeUnit.SECONDS);
			assertNotNull(event, "the fence must arrive");
			if (fence.equals(event[1])) {
				return List.copyOf(rowsByStep.getOrDefault(stepId, List.of()));
			}
			final var rows = rowsByStep.computeIfAbsent(event[0], unused -> new ArrayList<>());
			event[1].lines().forEach(line -> rows.add(line.split(",", -1)));
		}
	}

	/**
	 * What the client completed in step {@code stepId}, in ranges of {@code rangeSize} bytes,
	 * {@code ranges} to an object. The objects {@code counted} picks give the complete sets: an
	 * object's successful ranges divided by its ranges, since each read of it completes each of its
	 * ranges once at most; one with a remainder was being read when the step ended. The bytes are
	 * those of every successful range. The unfinished reads, of every object, are the read attempts
	 * whose initial pass the step received and whose end it did not, whatever their ranges did:
	 * those a stop may leave unresolved.
	 */
	Completed completed(final String stepId, final long rangeSize, final int ranges, final Predicate<String> counted)
					throws InterruptedException {
		final Map<String, Long> byObject = new HashMap<>();
		// A read attempt's rows by its object and start, first row first; range rows' keys are their own.
		final Map<String, List<String>> readRows = new LinkedHashMap<>();
		var bytes = 0L;
		final var rangeBytes = Long.toString(rangeSize);
		for (final var row : rows(stepId)) {
			if (rangeBytes.equals(row[SIZE])) {
				if ("SUCC".equals(row[STATUS])) {
					bytes += rangeSize;
					byObject.merge(row[PATH], 1L, Long::sum);
				}
			} else {
				readRows.computeIfAbsent(row[PATH] + " " + row[START], unused -> new ArrayList<>()).add(row[STATUS]);
			}
		}
		var complete = 0L;
		var incomplete = 0L;
		for (final var entry : byObject.entrySet()) {
			if (counted.test(entry.getKey())) {
				complete += entry.getValue() / ranges;
				incomplete += entry.getValue() % ranges == 0 ? 0 : 1;
			}
		}
		final var unfinished = readRows.values().stream()
						.filter(statuses -> statuses.size() == 1 && "SUCC".equals(statuses.get(0)))
						.count();
		return new Completed(complete, incomplete, bytes, unfinished);
	}

	/**
	 * The complete sets of ranges the client completed, the objects with an incomplete set, the bytes,
	 * and the reads begun and not ended.
	 */
	record Completed(long complete, long incomplete, long bytes, long unfinished) {}

	@Override
	public void close() {
		traces.removeAppender(appender);
		traces.setLevel(previousLevel);
		appender.stop();
	}
}
