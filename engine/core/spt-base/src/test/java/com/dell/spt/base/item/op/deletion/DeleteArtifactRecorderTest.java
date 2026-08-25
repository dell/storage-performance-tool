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
	void writerFailureIsSurfacedAfterLifecycleAndNeverPublishesCanonicalArtifacts(
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
			assertFalse(Files.exists(path(Loggers.DELETE_REQUESTS, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_OBJECTS, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_RESIDUAL, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_VERIFICATION, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_METRICS_TOTAL, stepId)));
		} finally {
			recorder.close();
		}
		assertEquals(privateStagingBefore, privateStagingCount(artifactDirectory));
		cleanup(stepId);
	}

	@Test
	void interruptionIgnoringWriterCannotRacePublicationOrStagingCleanup(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-stuck-writer-" + System.nanoTime();
		final Path artifactDirectory = path(Loggers.DELETE_OBJECTS, stepId).getParent();
		final long privateStagingBefore = privateStagingCount(artifactDirectory);
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch interruptionIgnored = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		final DeleteArtifactRecorder recorder = new DeleteArtifactRecorder(
						stepId, selection(temp), 1, 2, 25, operation -> {
							entered.countDown();
							while (release.getCount() != 0) {
								try {
									release.await(10, TimeUnit.MILLISECONDS);
								} catch (final InterruptedException ignored) {
									interruptionIgnored.countDown();
								}
							}
						});
		recorder.recordTerminal(successfulOperation(true));
		assertTrue(entered.await(1, TimeUnit.SECONDS));

		try {
			assertThrows(IllegalStateException.class, () -> recorder.finish(lifecycle(), metrics()));
			assertTrue(interruptionIgnored.await(1, TimeUnit.SECONDS));
			assertFalse(Files.exists(path(Loggers.DELETE_REQUESTS, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_OBJECTS, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_RESIDUAL, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_VERIFICATION, stepId)));
			assertFalse(Files.exists(path(Loggers.DELETE_METRICS_TOTAL, stepId)));
			recorder.close();
			assertEquals(privateStagingBefore + 2, privateStagingCount(artifactDirectory));
		} finally {
			release.countDown();
			for (int i = 0; i < 100 && writerThreadAlive(stepId); i++) {
				Thread.sleep(10);
			}
			recorder.close();
			cleanup(stepId);
		}

		assertFalse(writerThreadAlive(stepId));
		assertEquals(privateStagingBefore, privateStagingCount(artifactDirectory));
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

	@Test
	void postVerificationRefinesResidualToPresentAndUnresolvedEvenForUnattemptedTargets(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-verified-" + System.nanoTime();
		final Path selection = temp.resolve("verify-input.csv");
		Files.writeString(
						selection,
						"bucket,key,size,version_id\n"
										+ "bucket,absent,1,\n"
										+ "bucket,present,1,\n"
										+ "bucket,unresolved,1,version-3\n");
		final var recorder = new DeleteArtifactRecorder(stepId, selection, 3);
		final var post = new DeleteVerificationReport(
						DeleteVerificationPhase.POST_DELETE,
						new DeleteVerificationProbe.Presence[]{
								DeleteVerificationProbe.Presence.ABSENT,
								DeleteVerificationProbe.Presence.PRESENT,
								DeleteVerificationProbe.Presence.UNRESOLVED
						},
						true,
						1);
		try {
			recorder.finish(
							new OperationLifecycleSnapshot<>(0, 0, 0, 0, 0, 0, 0, List.of(), List.of()),
							DeleteMetricsSnapshot.builder(1)
											.objects(3, 0, 0, 0, 3, 0, 0)
											.verification(DeleteVerificationSummary.classify(
															false, true, 30_000, null, post, new int[]{0, 0, 0
															}))
											.reconciled(true)
											.build(),
							post);
			final String residual = Files.readString(path(Loggers.DELETE_RESIDUAL, stepId));
			assertFalse(residual.contains(",absent,"));
			assertTrue(residual.contains(",present,"));
			assertTrue(residual.contains(",unresolved,"));
			assertEquals(3, residual.lines().count());
			final String verification = Files.readString(
							path(Loggers.DELETE_OBJECTS, stepId).resolveSibling("delete.verification.csv"));
			assertTrue(verification.contains(",absent,1,,unattempted,false,disabled,true,absent,false,false,false"));
			assertTrue(verification.contains(",present,1,,unattempted,false,disabled,true,present,false,false,true"));
			assertTrue(verification.contains(",unresolved,1,version-3,unattempted,false,disabled,true,unresolved,false,false,true"));
			assertEquals(4, verification.lines().count());
		} finally {
			post.close();
			recorder.close();
			cleanup(stepId);
		}
	}

	@Test
	void distributedStrictPreAbortPublishesConservativeArtifactsForLocallyPassingSlice(
					@TempDir final Path temp) throws Exception {
		final String stepId = "delete-recorder-distributed-pre-abort-" + System.nanoTime();
		final Path selection = selection(temp);
		final var recorder = new DeleteArtifactRecorder(stepId, selection, 1);
		final var pre = new DeleteVerificationReport(
						DeleteVerificationPhase.PRE_DELETE,
						new DeleteVerificationProbe.Presence[]{
								DeleteVerificationProbe.Presence.PRESENT
						},
						true,
						1);
		final var verification = DeleteVerificationSummary.classify(
						true, true, 30_000, pre, null, new int[]{0
						})
						.withPostVerificationSkipped();
		try {
			recorder.finish(
							new OperationLifecycleSnapshot<>(0, 0, 0, 0, 0, 0, 0, List.of(), List.of()),
							DeleteMetricsSnapshot.builder(1)
											.objects(1, 0, 0, 0, 1, 0, 0)
											.verification(verification)
											.reconciled(true)
											.build(),
							pre,
							null);

			assertTrue(Files.readString(path(Loggers.DELETE_RESIDUAL, stepId)).contains(",key,1,"));
			final String evidence = Files.readString(path(Loggers.DELETE_VERIFICATION, stepId));
			assertTrue(evidence.contains(",key,1,,unattempted,true,present,true,unattempted,false,false,true"));
		} finally {
			pre.close();
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
						Loggers.DELETE_OBJECTS, Loggers.DELETE_RESIDUAL,
						Loggers.DELETE_VERIFICATION)) {
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

	private static boolean writerThreadAlive(final String stepId) {
		final String threadName = "spt-delete-artifacts-" + stepId;
		return Thread.getAllStackTraces().keySet().stream()
						.anyMatch(thread -> thread.isAlive() && threadName.equals(thread.getName()));
	}
}
