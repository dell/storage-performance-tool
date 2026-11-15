package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Closing the driver shuts down its upload executor. An uninterrupted close waits up to 30 s for
 * the tasks still running, then interrupts them. A close on an interrupted thread does not wait:
 * it interrupts them at once, so none outlives the driver, and it keeps the interrupt set.
 */
// An upload task the close fails to interrupt runs without end.
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsCloseTest {

	/** How long a close, and the end of the tasks it interrupts, may take. */
	private static final Duration CLOSE_BUDGET = Duration.ofSeconds(2);

	@Test
	void aCloseOnAnInterruptedThreadInterruptsTheUploadTasksStillRunning(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final S3AwsStorageDriver<Item, Operation<Item>> driver = S3AwsDriverHarness.driver(home, server, Map.of());
			driver.start();
			final ExecutorService uploadExecutor = uploadExecutor(driver);
			final var running = new CountDownLatch(1);
			final var interrupted = new CountDownLatch(1);
			// A body pump that never ends by itself.
			uploadExecutor.submit(() -> {
				running.countDown();
				try {
					Thread.sleep(Long.MAX_VALUE);
				} catch (final InterruptedException e) {
					interrupted.countDown();
				}
			});
			try {
				assertTrue(running.await(CLOSE_BUDGET.toMillis(), TimeUnit.MILLISECONDS), "the upload task never started");

				final long start = System.nanoTime();
				final boolean flagKept;
				Thread.currentThread().interrupt();
				try {
					assertDoesNotThrow(driver::close);
				} finally {
					// Never leave the interrupt to later tests on this thread.
					flagKept = Thread.interrupted();
				}
				final long deadline = start + CLOSE_BUDGET.toNanos();

				assertTrue(flagKept, "the close must keep the interrupt set");
				assertTrue(driver.isClosed());
				assertTrue(interrupted.await(deadline - System.nanoTime(), TimeUnit.NANOSECONDS),
								"the upload task was not interrupted within " + CLOSE_BUDGET);
				assertTrue(uploadExecutor.awaitTermination(deadline - System.nanoTime(), TimeUnit.NANOSECONDS),
								"the upload executor did not terminate within " + CLOSE_BUDGET);
			} finally {
				uploadExecutor.shutdownNow();
			}
		}
	}

	@Test
	void anUninterruptedCloseWithNoUploadTasksReturnsAtOnce(@TempDir final Path home) throws Exception {
		try (final var server = new ScriptedS3Server()) {
			final S3AwsStorageDriver<Item, Operation<Item>> driver = S3AwsDriverHarness.driver(home, server, Map.of());
			driver.start();
			final ExecutorService uploadExecutor = uploadExecutor(driver);

			final long start = System.nanoTime();
			assertDoesNotThrow(driver::close);
			final Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

			assertTrue(elapsed.compareTo(CLOSE_BUDGET) < 0, "the close took " + elapsed);
			assertTrue(driver.isClosed());
			assertTrue(uploadExecutor.isTerminated());
			assertFalse(Thread.currentThread().isInterrupted(), "an uninterrupted close must not set the interrupt");
		}
	}

	private static ExecutorService uploadExecutor(final S3AwsStorageDriver<?, ?> driver) throws Exception {
		final Field field = S3AwsStorageDriver.class.getDeclaredField("uploadExecutor");
		field.setAccessible(true);
		return (ExecutorService) field.get(driver);
	}
}
