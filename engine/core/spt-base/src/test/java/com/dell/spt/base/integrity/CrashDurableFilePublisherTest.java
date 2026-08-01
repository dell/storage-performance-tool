package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CrashDurableFilePublisherTest {

	@TempDir
	Path tempDir;

	@Test
	void realFilesystemCanaryPublishesSyncedAtomicFile() throws Exception {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		Files.writeString(staging, "durable-content");

		CrashDurableFilePublisher.publish(staging, target);

		assertFalse(Files.exists(staging));
		assertEquals("durable-content", Files.readString(target));
	}

	@Test
	void ordersFileSyncLinkDeleteAndDirectorySync() throws Exception {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		final RecordingOperations operations = new RecordingOperations(null);

		CrashDurableFilePublisher.publish(staging, target, operations);

		assertEquals(
						List.of(
										"sync-file:" + staging.toAbsolutePath(),
										"link:" + staging.toAbsolutePath() + "->" + target.toAbsolutePath(),
										"delete:" + staging.toAbsolutePath(),
										"sync-directory:" + tempDir.toAbsolutePath()),
						operations.events);
	}

	@Test
	void refusesToReplaceExistingTarget() throws Exception {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		Files.writeString(staging, "new-content");
		Files.writeString(target, "existing-content");

		assertThrows(
						FileAlreadyExistsException.class,
						() -> CrashDurableFilePublisher.publish(staging, target));

		assertEquals("new-content", Files.readString(staging));
		assertEquals("existing-content", Files.readString(target));
	}

	@Test
	void concurrentPublishersHaveExactlyOneWinner() throws Exception {
		final Path first = tempDir.resolve(".first.staging");
		final Path second = tempDir.resolve(".second.staging");
		final Path target = tempDir.resolve("manifest.csv");
		Files.writeString(first, "first");
		Files.writeString(second, "second");
		final CountDownLatch ready = new CountDownLatch(2);
		final CountDownLatch start = new CountDownLatch(1);
		final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
		final AtomicReference<Throwable> secondFailure = new AtomicReference<>();
		final Thread firstPublisher = publisherThread(first, target, ready, start, firstFailure);
		final Thread secondPublisher = publisherThread(second, target, ready, start, secondFailure);

		firstPublisher.start();
		secondPublisher.start();
		ready.await();
		start.countDown();
		firstPublisher.join();
		secondPublisher.join();

		final boolean firstWon = firstFailure.get() == null;
		final boolean secondWon = secondFailure.get() == null;
		assertTrue(firstWon ^ secondWon);
		assertTrue(firstWon
						? secondFailure.get() instanceof FileAlreadyExistsException
						: firstFailure.get() instanceof FileAlreadyExistsException);
		assertEquals(firstWon ? "first" : "second", Files.readString(target));
	}

	@Test
	void syncFailurePreventsPublication() {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		final RecordingOperations operations = new RecordingOperations("sync-file");

		assertThrows(
						IOException.class,
						() -> CrashDurableFilePublisher.publish(staging, target, operations));

		assertEquals(List.of("sync-file:" + staging.toAbsolutePath()), operations.events);
	}

	@Test
	void directorySyncFailureReportsIndeterminatePublishedState() {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		final RecordingOperations operations = new RecordingOperations("sync-directory");

		final IOException failure = assertThrows(
						IOException.class,
						() -> CrashDurableFilePublisher.publish(staging, target, operations));

		assertTrue(failure.getMessage().contains("durable state is indeterminate"));
		assertEquals(4, operations.events.size());
	}

	@Test
	void deleteFailureReportsIndeterminatePublishedState() {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		final RecordingOperations operations = new RecordingOperations("delete");

		final IOException failure = assertThrows(
						IOException.class,
						() -> CrashDurableFilePublisher.publish(staging, target, operations));

		assertTrue(failure.getMessage().contains("durable state is indeterminate"));
		assertEquals(3, operations.events.size());
	}

	@Test
	void rejectsCrossDirectoryPublicationBeforeDurabilityOperations() {
		final RecordingOperations operations = new RecordingOperations(null);

		assertThrows(
						IOException.class,
						() -> CrashDurableFilePublisher.publish(
										tempDir.resolve("left").resolve(".manifest.staging"),
										tempDir.resolve("right").resolve("manifest.csv"),
										operations));

		assertTrue(operations.events.isEmpty());
	}

	private static Thread publisherThread(
					final Path staging,
					final Path target,
					final CountDownLatch ready,
					final CountDownLatch start,
					final AtomicReference<Throwable> failure) {
		return new Thread(() -> {
			try {
				ready.countDown();
				start.await();
				CrashDurableFilePublisher.publish(staging, target);
			} catch (final Throwable e) {
				failure.set(e);
			}
		});
	}

	private static final class RecordingOperations
					implements CrashDurableFilePublisher.Operations {

		private final String failOperation;
		private final List<String> events = new ArrayList<>();

		private RecordingOperations(final String failOperation) {
			this.failOperation = failOperation;
		}

		@Override
		public void syncFile(final Path path) throws IOException {
			record("sync-file:" + path, "sync-file");
		}

		@Override
		public void createLinkNoReplace(final Path source, final Path target) throws IOException {
			record("link:" + source + "->" + target, "link");
		}

		@Override
		public void delete(final Path path) throws IOException {
			record("delete:" + path, "delete");
		}

		@Override
		public void syncDirectory(final Path path) throws IOException {
			record("sync-directory:" + path, "sync-directory");
		}

		private void record(final String event, final String operation) throws IOException {
			events.add(event);
			if (operation.equals(failOperation)) {
				throw new IOException("injected " + operation + " failure");
			}
		}
	}
}
