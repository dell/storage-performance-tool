package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.logging.Loggers;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;

/**
 * Captures the bucket versioning notices a driver logs to the messages (INFO) and the errors (WARN). Engine loggers
 * are asynchronous, so a check waits for the notice.
 */
final class BucketVersioningNotices extends AbstractAppender implements AutoCloseable {

	record Notice(Level level, String text) {}

	private final List<Notice> notices = new CopyOnWriteArrayList<>();
	private final List<org.apache.logging.log4j.core.Logger> loggers;

	private BucketVersioningNotices() {
		super("testBucketVersioningNotices" + System.nanoTime(), null, null, true, Property.EMPTY_ARRAY);
		final var context = LoggerContext.getContext(false);
		loggers = List.of(context.getLogger(Loggers.MSG.getName()), context.getLogger(Loggers.ERR.getName()));
	}

	static BucketVersioningNotices attach() {
		final var notices = new BucketVersioningNotices();
		notices.start();
		notices.loggers.forEach(logger -> logger.addAppender(notices));
		return notices;
	}

	@Override
	public void append(final LogEvent event) {
		notices.add(new Notice(event.getLevel(), event.getMessage().getFormattedMessage()));
	}

	/** Waits for the bucket's notice that contains {@code part}, then checks it is the bucket's only one. */
	Notice awaitOnly(final String bucket, final Level level, final String part) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (System.nanoTime() < deadline) {
			final var found = forBucket(bucket);
			if (found.stream().anyMatch(notice -> notice.text().contains(part))) {
				Thread.sleep(100);
				final var settled = forBucket(bucket);
				assertEquals(1, settled.size(), "one notice per bucket: " + settled);
				assertEquals(level, settled.get(0).level(), settled::toString);
				return settled.get(0);
			}
			Thread.sleep(10);
		}
		fail("no notice for " + bucket + " containing \"" + part + "\": " + notices);
		return null;
	}

	/** The notices that name the bucket, as {@code "<step>: bucket <name>: ..."}. */
	List<Notice> forBucket(final String bucket) {
		final var prefix = ": bucket " + bucket + ":";
		return notices.stream().filter(notice -> notice.text().contains(prefix)).toList();
	}

	@Override
	public void close() {
		loggers.forEach(logger -> logger.removeAppender(this));
		stop();
	}
}
