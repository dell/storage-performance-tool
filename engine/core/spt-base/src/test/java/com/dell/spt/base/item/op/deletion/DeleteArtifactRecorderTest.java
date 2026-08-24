package com.dell.spt.base.item.op.deletion;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.storage.Credential;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeleteArtifactRecorderTest {

	@Test
	void initializationFailureRemovesEveryPrivateStagingFile(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-init-failure-" + System.nanoTime();
		final Path artifactDirectory = path(Loggers.DELETE_OBJECTS, stepId).getParent();
		final long privateStagingBefore = privateStagingCount(artifactDirectory);
		final AtomicInteger creates = new AtomicInteger();

		assertThrows(
						IllegalStateException.class,
						() -> new DeleteArtifactRecorder(
										stepId, selection(temp), 1, 2, TimeUnit.SECONDS.toMillis(2),
										ignored -> {},
										(directory, prefix, suffix) -> {
											if (creates.incrementAndGet() == 2) {
												throw new IOException("injected second staging failure");
											}
											return Files.createTempFile(directory, prefix, suffix);
										}));

		assertEquals(2, creates.get());
		assertEquals(privateStagingBefore, privateStagingCount(artifactDirectory));
		cleanup(stepId);
	}

	@Test
	void compatibilityTargetRecordsOutsideLifecycleOwnershipWithoutBlocking(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-compat-" + System.nanoTime();
		final Path selection = selection(temp);
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		final DeleteArtifactRecorder recorder = new DeleteArtifactRecorder(
						stepId, selection, 1, 4, TimeUnit.SECONDS.toMillis(2), operation -> {
							entered.countDown();
							if (!release.await(2, TimeUnit.SECONDS)) {
								throw new IOException("test writer release timed out");
							}
						});
		final DeleteRequestOperation operation = successfulOperation(false);
		final long started = System.nanoTime();
		recorder.recordTerminal(operation);
		final long enqueueMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		assertTrue(enqueueMillis < 100, "terminal lifecycle callback blocked on artifact I/O");
		assertTrue(entered.await(1, TimeUnit.SECONDS));
		release.countDown();

		try {
			recorder.finish(lifecycle(), metrics());
			assertTrue(Files.readString(path(Loggers.DELETE_OBJECTS, stepId)).contains(",-1,"));
			assertTrue(Files.readString(path(Loggers.DELETE_RESIDUAL, stepId)).lines().count() == 1);
		} finally {
			recorder.close();
			cleanup(stepId);
		}
	}

	@Test
	void writerFailureIsSurfacedAfterLifecycleAndNeverPublishesTotals(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-failure-" + System.nanoTime();
		final Path artifactDirectory = path(Loggers.DELETE_OBJECTS, stepId).getParent();
		final long privateStagingBefore = privateStagingCount(artifactDirectory);
		final DeleteArtifactRecorder recorder = new DeleteArtifactRecorder(
						stepId, selection(temp), 1, 2, TimeUnit.SECONDS.toMillis(2),
						operation -> {
							throw new IOException("injected writer failure");
						});
		assertFalse(path(Loggers.DELETE_METRICS_TOTAL, stepId).toFile().exists());
		recorder.recordTerminal(successfulOperation(true));
		try {
			assertThrows(IllegalStateException.class, () -> recorder.finish(lifecycle(), metrics()));
			assertFalse(Files.exists(path(Loggers.DELETE_METRICS_TOTAL, stepId)));
			assertTrue(Files.isRegularFile(path(Loggers.DELETE_OBJECTS, stepId)));
		} finally {
			recorder.close();
		}
		assertEquals(privateStagingBefore, privateStagingCount(artifactDirectory));
		cleanup(stepId);
	}

	@Test
	void conflictingPublicationCanBeRetriedWithoutDuplicatingRows(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-retry-" + System.nanoTime();
		final DeleteArtifactRecorder recorder = new DeleteArtifactRecorder(stepId, selection(temp), 1);
		final Path requests = path(Loggers.DELETE_REQUESTS, stepId);
		Files.createDirectories(requests.getParent());
		Files.writeString(requests, "conflict\n");
		recorder.recordTerminal(successfulOperation(true));
		try {
			assertThrows(IllegalStateException.class, () -> recorder.finish(lifecycle(), metrics()));
			Files.delete(requests);
			recorder.finish(lifecycle(), metrics());
			assertTrue(Files.readAllLines(requests).size() == 2);
			assertTrue(Files.isRegularFile(path(Loggers.DELETE_METRICS_TOTAL, stepId)));
		} finally {
			recorder.close();
			cleanup(stepId);
		}
	}

	private static Path selection(final Path temp) throws IOException {
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(selection, "bucket,key,size,version_id\nbucket,key,1,\n");
		return selection;
	}

	private static DeleteRequestOperation successfulOperation(final boolean indexed) {
		final DeleteTarget target = new DeleteTarget(
						new IntegrityManifestDataItem("bucket", "key", 1, null), indexed ? 0 : -1);
		final var operation = new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", Credential.NONE, List.of(target)));
		operation.completeDelete(DeleteTransportResult.success(List.of(target)));
		return operation;
	}

	private static OperationLifecycleSnapshot<DeleteRequestOperation> lifecycle() {
		return new OperationLifecycleSnapshot<>(0, 0, 1, 0, 1, 0, 0, List.of(), List.of());
	}

	private static DeleteMetricsSnapshot metrics() {
		return DeleteMetricsSnapshot.builder(1)
						.identity("single", "canonical")
						.requests(1, 1, 0, 0, 0, 1)
						.objects(1, 1, 1, 0, 0, 0, 1)
						.batches(1, 1, 1, 0)
						.versions(1, 0)
						.reconciled(true)
						.build();
	}

	private static Path path(final org.apache.logging.log4j.Logger logger, final String stepId)
					throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
	}

	private static void cleanup(final String stepId) throws IOException {
		for (final var logger : List.of(
						Loggers.DELETE_METRICS_TOTAL, Loggers.DELETE_REQUESTS,
						Loggers.DELETE_OBJECTS, Loggers.DELETE_RESIDUAL)) {
			Files.deleteIfExists(path(logger, stepId));
		}
	}

	private static long privateStagingCount(final Path directory) throws IOException {
		if (!Files.isDirectory(directory)) {
			return 0;
		}
		try (final var paths = Files.list(directory)) {
			return paths.filter(path -> path.getFileName().toString().endsWith(".recording")).count();
		}
	}
}
