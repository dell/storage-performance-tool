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
 * Captures the lines a driver writes to {@code bucket.versioning.log}, the ones the Entry Node relays. Engine
 * loggers are asynchronous, so a check waits for the line.
 */
final class BucketVersioningNotices extends AbstractAppender implements AutoCloseable {

	record Notice(Level level, String text) {}

	private final List<Notice> notices = new CopyOnWriteArrayList<>();
	private final org.apache.logging.log4j.core.Logger logger;

	private BucketVersioningNotices() {
		super("testBucketVersioningNotices" + System.nanoTime(), null, null, true, Property.EMPTY_ARRAY);
		logger = LoggerContext.getContext(false).getLogger(Loggers.BUCKET_VERSIONING.getName());
	}

	static BucketVersioningNotices attach() {
		final var notices = new BucketVersioningNotices();
		notices.start();
		notices.logger.addAppender(notices);
		return notices;
	}

	@Override
	public void append(final LogEvent event) {
		notices.add(new Notice(event.getLevel(), event.getMessage().getFormattedMessage()));
	}

	/** Waits for the bucket's notice that contains {@code part}, then checks it is the bucket's only one. */
	void awaitOnly(final String bucket, final Level level, final String part) throws InterruptedException {
		final var prefix = "bucket " + bucket + ":";
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (System.nanoTime() < deadline) {
			final var found = forBucket(prefix);
			if (found.stream().anyMatch(notice -> notice.text().contains(part))) {
				Thread.sleep(100);
				final var settled = forBucket(prefix);
				assertEquals(1, settled.size(), "one notice per bucket: " + settled);
				assertEquals(level, settled.get(0).level(), settled::toString);
				return;
			}
			Thread.sleep(10);
		}
		fail("no notice for " + bucket + " containing \"" + part + "\": " + notices);
	}

	private List<Notice> forBucket(final String prefix) {
		return notices.stream().filter(notice -> notice.text().startsWith(prefix)).toList();
	}

	@Override
	public void close() {
		logger.removeAppender(this);
		stop();
	}
}
