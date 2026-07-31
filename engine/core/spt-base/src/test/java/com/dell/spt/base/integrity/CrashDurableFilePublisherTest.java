package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
	void ordersFileSyncMoveAndDirectorySync() throws Exception {
		final Path staging = tempDir.resolve(".manifest.staging");
		final Path target = tempDir.resolve("manifest.csv");
		final RecordingOperations operations = new RecordingOperations(null);

		CrashDurableFilePublisher.publish(staging, target, operations);

		assertEquals(
						List.of(
										"sync-file:" + staging.toAbsolutePath(),
										"move:" + staging.toAbsolutePath() + "->" + target.toAbsolutePath(),
										"sync-directory:" + tempDir.toAbsolutePath()),
						operations.events);
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
		public void atomicMove(final Path source, final Path target) throws IOException {
			record("move:" + source + "->" + target, "move");
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
