package com.dell.spt.base.load.step.client;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.item.op.deletion.DeleteArtifactRecorder;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.deletion.DeleteTransportResult;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.storage.Credential;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class DeleteArtifactAggregatorTest {
	private static final List<String> DELETE_FILES = List.of(
					DeleteArtifacts.METRICS_FILE_NAME,
					DeleteArtifacts.REQUESTS_FILE_NAME,
					DeleteArtifacts.OBJECTS_FILE_NAME,
					DeleteArtifacts.RESIDUAL_FILE_NAME,
					DeleteArtifacts.VERIFICATION_FILE_NAME);

	@TempDir
	Path temp;

	@Test
	void fetchesEachContributorOncePreservesSourcesAndPublishesCompletionLast() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final Path selection = selection();
		final Path selectionCompletion = completion();
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		write(localArtifacts);
		write(remoteArtifacts);
		serve(remote, remoteArtifacts);

		new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"), selection, selectionCompletion)
						.close();

		assertEquals(2, Files.readAllLines(output.resolve(DeleteArtifacts.REQUESTS_FILE_NAME)).size() - 1);
		assertEquals(4, Files.readAllLines(output.resolve(DeleteArtifacts.OBJECTS_FILE_NAME)).size() - 1);
		assertEquals(2, Files.readAllLines(output.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME)).size() - 1);
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)).isBlank());
		for (final String artifact : List.of(
						DeleteArtifacts.METRICS_FILE_NAME,
						DeleteArtifacts.REQUESTS_FILE_NAME,
						DeleteArtifacts.OBJECTS_FILE_NAME,
						DeleteArtifacts.RESIDUAL_FILE_NAME,
						DeleteArtifacts.VERIFICATION_FILE_NAME)) {
			assertFalse(Files.readString(DeleteArtifactAggregator.nodeSourcePath(output.resolve(artifact), 0)).isBlank());
			assertFalse(Files.readString(DeleteArtifactAggregator.nodeSourcePath(output.resolve(artifact), 1)).isBlank());
		}
	}

	@Test
	void aStoppedStartFetchesTheStartedContributorsOnlyAndPublishesNothingWithoutOne() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"), selection(), completion());

		aggregator.closeAfterStoppedStart(0);
		try (var files = Files.list(output)) {
			assertEquals(List.of(), files.toList(), "a step whose slices never started published DELETE evidence");
		}
		assertThrows(IllegalArgumentException.class, () -> aggregator.closeAfterStoppedStart(3));

		// The other node's slice never started, so nothing is fetched from it; evidence that does not
		// cover the whole selection is still refused, never published as complete.
		write(localArtifacts);
		final var incomplete = assertThrows(IntegrityTerminalException.class, () -> aggregator.closeAfterStoppedStart(1));
		assertEquals("DELETE frozen selection count does not match terminal evidence", incomplete.getMessage());
		assertFalse(Files.exists(DeleteArtifactAggregator.nodeSourcePath(
						output.resolve(DeleteArtifacts.METRICS_FILE_NAME), 1)));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void failedFetchKeepsCollectedSourcesAndNeverPublishesComplete() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		write(localArtifacts);
		when(remote.readFromFile(remoteArtifacts.totals().toString(), 0)).thenThrow(new EOFException());

		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"), selection(), completion());
		assertThrows(RuntimeException.class, aggregator::close);

		assertFalse(Files.readString(DeleteArtifactAggregator.nodeSourcePath(
						output.resolve(DeleteArtifacts.METRICS_FILE_NAME), 0)).isBlank());
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.SELECTION_FILE_NAME)).isBlank());
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME)).isBlank());
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void partialRemoteFetchDoesNotPoisonRetry() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager remote = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts remoteArtifacts = nodeArtifacts(temp.resolve("remote"), 1, "worker-a");
		register(local, localArtifacts);
		register(remote, remoteArtifacts);
		write(localArtifacts);
		write(remoteArtifacts);
		final byte[] totals = Files.readAllBytes(remoteArtifacts.totals());
		final byte[] prefix = java.util.Arrays.copyOf(totals, totals.length / 2);
		when(remote.readFromFile(remoteArtifacts.totals().toString(), 0))
						.thenReturn(prefix)
						.thenReturn(totals);
		when(remote.readFromFile(remoteArtifacts.totals().toString(), prefix.length))
						.thenThrow(new IOException("interrupted fetch"));
		when(remote.readFromFile(remoteArtifacts.totals().toString(), totals.length))
						.thenThrow(new EOFException());
		for (final Path path : List.of(
						remoteArtifacts.requests(), remoteArtifacts.objects(), remoteArtifacts.residual(),
						remoteArtifacts.verification())) {
			final byte[] bytes = Files.readAllBytes(path);
			when(remote.readFromFile(path.toString(), 0)).thenReturn(bytes);
			when(remote.readFromFile(path.toString(), bytes.length)).thenThrow(new EOFException());
		}
		final Path selection = selection();
		final Path selectionCompletion = completion();

		assertThrows(RuntimeException.class, () -> new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"),
						selection, selectionCompletion).close());

		new DeleteArtifactAggregator(
						"step", List.of(local, remote), List.of("local", "worker-a"),
						selection, selectionCompletion).close();
		assertFalse(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)).isBlank());
	}

	@Test
	void aLostWorkerStillLetsTheEntryCollectTheLaterWorkersEvidence() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager lost = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts lostArtifacts = nodeArtifacts(temp.resolve("lost"), 1, "worker-a");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 2, "worker-b");
		register(local, localArtifacts);
		register(lost, lostArtifacts);
		register(later, laterArtifacts);
		write(localArtifacts);
		write(laterArtifacts);
		serve(later, laterArtifacts);
		when(lost.readFromFile(anyString(), anyLong())).thenThrow(new LostNodeFileManager.NotCollected());
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, lost, later), List.of("local", "worker-a", "worker-b"), selection(), completion());

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals("failed to collect and publish DELETE evidence", failure.getMessage());
		assertEquals("remote DELETE artifact was not collected, as the entry lost its node's step slice: "
						+ lostArtifacts.totals(), failure.getCause().getMessage());
		assertEquals(0, failure.getCause().getSuppressed().length, "a lost node gets one failure, not one per file");
		verify(lost, times(1)).readFromFile(anyString(), anyLong());
		for (final String artifact : DELETE_FILES) {
			final Path canonical = output.resolve(artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 0)), artifact);
			assertFalse(Files.exists(DeleteArtifactAggregator.nodeSourcePath(canonical, 1)), artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 2)),
							"worker 2's " + artifact + " was not collected");
		}
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void aWorkersMissingFileKeepsItsOtherFilesAndTheLaterWorkersForARetry() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager missing = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts missingArtifacts = nodeArtifacts(temp.resolve("missing"), 1, "worker-a");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 2, "worker-b");
		register(local, localArtifacts);
		register(missing, missingArtifacts);
		register(later, laterArtifacts);
		write(localArtifacts);
		write(missingArtifacts);
		write(laterArtifacts);
		serve(missing, missingArtifacts);
		serve(later, laterArtifacts);
		when(missing.readFromFile(missingArtifacts.requests().toString(), 0))
						.thenThrow(new NoSuchFileException(missingArtifacts.requests().toString()));
		when(missing.readFromFile(missingArtifacts.residual().toString(), 0))
						.thenThrow(new NoSuchFileException(missingArtifacts.residual().toString()));
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, missing, later), List.of("local", "worker-a", "worker-b"), selection(), completion());

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals("failed to fetch remote DELETE artifact " + missingArtifacts.requests(), failure.getCause().getMessage());
		assertEquals(1, failure.getCause().getSuppressed().length);
		assertEquals("failed to fetch remote DELETE artifact " + missingArtifacts.residual(),
						failure.getCause().getSuppressed()[0].getMessage());
		for (final String artifact : DELETE_FILES) {
			final Path canonical = output.resolve(artifact);
			final boolean fetched = !artifact.equals(DeleteArtifacts.REQUESTS_FILE_NAME)
							&& !artifact.equals(DeleteArtifacts.RESIDUAL_FILE_NAME);
			assertEquals(fetched, Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 1)), artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 2)),
							"worker 2's " + artifact + " was not collected");
		}
		try (var files = Files.list(output)) {
			assertEquals(List.of(), files.filter(file -> file.getFileName().toString().startsWith(".")).toList());
		}
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void anInterruptStopsTheDeleteCollectionAtOnce() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager interrupted = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 2, "worker-b");
		register(local, localArtifacts);
		register(interrupted, nodeArtifacts(temp.resolve("interrupted"), 1, "worker-a"));
		register(later, laterArtifacts);
		write(localArtifacts);
		write(laterArtifacts);
		serve(later, laterArtifacts);
		when(interrupted.readFromFile(anyString(), anyLong())).thenAnswer(invocation -> {
			throwUnchecked(new InterruptedException("stop collecting"));
			return null;
		});
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, interrupted, later), List.of("local", "worker-a", "worker-b"), selection(), completion());

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, aggregator::close));
		verify(interrupted, times(1)).readFromFile(anyString(), anyLong());
		verify(later, never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void eachFileAWorkerMissesGetsAWarningAndALostNodeOne() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager lost = mock(FileManager.class);
		final FileManager missing = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts missingArtifacts = nodeArtifacts(temp.resolve("missing"), 2, "worker-b");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 3, "worker-c");
		register(local, localArtifacts);
		register(lost, nodeArtifacts(temp.resolve("lost"), 1, "worker-a"));
		register(missing, missingArtifacts);
		register(later, laterArtifacts);
		write(localArtifacts);
		write(missingArtifacts);
		write(laterArtifacts);
		serve(missing, missingArtifacts);
		serve(later, laterArtifacts);
		when(lost.readFromFile(anyString(), anyLong())).thenThrow(new LostNodeFileManager.NotCollected());
		when(missing.readFromFile(missingArtifacts.requests().toString(), 0))
						.thenThrow(new NoSuchFileException(missingArtifacts.requests().toString()));
		when(missing.readFromFile(missingArtifacts.residual().toString(), 0))
						.thenThrow(new NoSuchFileException(missingArtifacts.residual().toString()));
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, lost, missing, later),
						List.of("local", "worker-a", "worker-b", "worker-c"), selection(), completion());
		final var warnings = new CollectionWarnings();

		final IntegrityTerminalException failure;
		try (warnings) {
			warnings.clear("step");
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
			warnings.flush("step");
		}

		assertEquals("failed to collect and publish DELETE evidence", failure.getMessage());
		final List<String> lines = warnings.lines("step");
		assertEquals(3, lines.size(), lines.toString());
		assertTrue(lines.get(0).startsWith("step: failed to collect " + DeleteArtifacts.METRICS_FILE_NAME
						+ " from node-001: remote DELETE artifact was not collected, as the entry lost its node's step slice: "),
						lines.get(0));
		assertEquals("step: failed to collect " + DeleteArtifacts.REQUESTS_FILE_NAME + " from node-002: "
						+ "failed to fetch remote DELETE artifact " + missingArtifacts.requests(), lines.get(1));
		assertEquals("step: failed to collect " + DeleteArtifacts.RESIDUAL_FILE_NAME + " from node-002: "
						+ "failed to fetch remote DELETE artifact " + missingArtifacts.residual(), lines.get(2));
		for (final String artifact : DELETE_FILES) {
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(output.resolve(artifact), 3)),
							"worker 3's " + artifact + " was not collected");
		}
	}

	@Test
	void aMissingEntryFileStillCollectsEveryWorkersEvidence() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager w1 = mock(FileManager.class);
		final FileManager w2 = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts w1Artifacts = nodeArtifacts(temp.resolve("w1"), 1, "worker-a");
		final NodeArtifacts w2Artifacts = nodeArtifacts(temp.resolve("w2"), 2, "worker-b");
		register(local, localArtifacts);
		register(w1, w1Artifacts);
		register(w2, w2Artifacts);
		write(localArtifacts);
		Files.delete(localArtifacts.requests());
		write(w1Artifacts);
		write(w2Artifacts);
		serve(w1, w1Artifacts);
		serve(w2, w2Artifacts);
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, w1, w2), List.of("local", "worker-a", "worker-b"), selection(3), completion(3));
		final var warnings = new CollectionWarnings();

		final IntegrityTerminalException failure;
		try (warnings) {
			warnings.clear("step");
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
			warnings.flush("step");
		}

		assertEquals("entry-node DELETE artifact is missing: " + output.resolve(DeleteArtifacts.REQUESTS_FILE_NAME),
						failure.getCause().getMessage());
		assertEquals(0, failure.getCause().getSuppressed().length);
		assertEquals(List.of("step: failed to collect " + DeleteArtifacts.REQUESTS_FILE_NAME + " from node-000: "
						+ failure.getCause().getMessage()), warnings.lines("step"));
		for (final String artifact : DELETE_FILES) {
			final Path canonical = output.resolve(artifact);
			assertEquals(!artifact.equals(DeleteArtifacts.REQUESTS_FILE_NAME),
							Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 0)), artifact);
			for (int node = 1; node <= 2; node++) {
				assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, node)),
								"worker " + node + "'s " + artifact + " was not collected");
			}
		}
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void aStoppedStartWithAMissingMiddleSliceCollectsTheOtherStartedSlicesOnly() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager missing = mock(FileManager.class);
		final FileManager started = mock(FileManager.class);
		final FileManager unstarted = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts missingArtifacts = nodeArtifacts(temp.resolve("missing"), 1, "worker-a");
		final NodeArtifacts startedArtifacts = nodeArtifacts(temp.resolve("started"), 2, "worker-b");
		register(local, localArtifacts);
		register(missing, missingArtifacts);
		register(started, startedArtifacts);
		register(unstarted, nodeArtifacts(temp.resolve("unstarted"), 3, "worker-c"));
		write(localArtifacts);
		write(startedArtifacts);
		serve(started, startedArtifacts);
		when(missing.readFromFile(anyString(), anyLong())).thenThrow(new NoSuchFileException("not written"));
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, missing, started, unstarted),
						List.of("local", "worker-a", "worker-b", "worker-c"), selection(3), completion(3));

		final var failure = assertThrows(IntegrityTerminalException.class, () -> aggregator.closeAfterStoppedStart(3));

		assertEquals("failed to collect and publish DELETE evidence", failure.getMessage());
		for (final String artifact : DELETE_FILES) {
			final Path canonical = output.resolve(artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 0)), artifact);
			assertFalse(Files.exists(DeleteArtifactAggregator.nodeSourcePath(canonical, 1)), artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 2)),
							"worker 2's " + artifact + " was not collected");
			assertFalse(Files.exists(DeleteArtifactAggregator.nodeSourcePath(canonical, 3)), artifact);
		}
		verify(unstarted, never()).readFromFile(anyString(), anyLong());
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void aSingleSlicesMissingFileLogsNoWarning() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		register(local, localArtifacts);
		write(localArtifacts);
		Files.delete(localArtifacts.residual());
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local), List.of("local"), selection(1), completion(1));
		final var warnings = new CollectionWarnings();

		final IntegrityTerminalException failure;
		try (warnings) {
			warnings.clear("step");
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
			warnings.flush("step");
		}

		assertEquals("entry-node DELETE artifact is missing: " + output.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME),
						failure.getCause().getMessage());
		assertEquals(List.of(), warnings.lines("step"));
	}

	@Test
	void aRetryCollectsTheFailedWorkerWithoutRefetchingTheLaterOne() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager failed = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts failedArtifacts = nodeArtifacts(temp.resolve("failed"), 1, "worker-a");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 2, "worker-b");
		register(local, localArtifacts);
		register(failed, failedArtifacts);
		register(later, laterArtifacts);
		write(localArtifacts);
		write(failedArtifacts);
		write(laterArtifacts);
		serve(failed, failedArtifacts);
		serve(later, laterArtifacts);
		final byte[] totals = Files.readAllBytes(failedArtifacts.totals());
		when(failed.readFromFile(failedArtifacts.totals().toString(), 0))
						.thenThrow(new NoSuchFileException(failedArtifacts.totals().toString()))
						.thenReturn(totals);
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, failed, later), List.of("local", "worker-a", "worker-b"), selection(3), completion(3));

		assertThrows(IntegrityTerminalException.class, aggregator::close);
		for (final String artifact : DELETE_FILES) {
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(output.resolve(artifact), 2)),
							"worker 2's " + artifact + " was not collected past worker 1");
		}
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));

		aggregator.close();

		assertFalse(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)).isBlank());
		assertEquals(6, Files.readAllLines(output.resolve(DeleteArtifacts.OBJECTS_FILE_NAME)).size() - 1);
		// Each of worker 2's five files once, and its end once: the retry reused them.
		verify(later, times(10)).readFromFile(anyString(), anyLong());
	}

	@Test
	void aNonIoFailureOfAWorkersReadCountsAsThatFileMissing() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager broken = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts brokenArtifacts = nodeArtifacts(temp.resolve("broken"), 1, "worker-a");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 2, "worker-b");
		register(local, localArtifacts);
		register(broken, brokenArtifacts);
		register(later, laterArtifacts);
		write(localArtifacts);
		write(brokenArtifacts);
		write(laterArtifacts);
		serve(broken, brokenArtifacts);
		serve(later, laterArtifacts);
		when(broken.readFromFile(brokenArtifacts.requests().toString(), 0))
						.thenThrow(new IllegalStateException("not an I/O failure"));
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, broken, later), List.of("local", "worker-a", "worker-b"), selection(3), completion(3));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals("failed to fetch remote DELETE artifact " + brokenArtifacts.requests(), failure.getCause().getMessage());
		assertInstanceOf(IllegalStateException.class, failure.getCause().getCause());
		for (final String artifact : DELETE_FILES) {
			final Path canonical = output.resolve(artifact);
			assertEquals(!artifact.equals(DeleteArtifacts.REQUESTS_FILE_NAME),
							Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 1)), artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 2)),
							"worker 2's " + artifact + " was not collected");
		}
	}

	@Test
	void aNonIoFailureOutsideAWorkersReadCountsAsThatFileMissing() throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager w1 = mock(FileManager.class);
		final FileManager w2 = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts w1Artifacts = nodeArtifacts(temp.resolve("w1"), 1, "worker-a");
		final NodeArtifacts w2Artifacts = nodeArtifacts(temp.resolve("w2"), 2, "worker-b");
		register(local, localArtifacts);
		register(w1, w1Artifacts);
		register(w2, w2Artifacts);
		write(localArtifacts);
		write(w1Artifacts);
		write(w2Artifacts);
		serve(w1, w1Artifacts);
		serve(w2, w2Artifacts);
		final Path w1Totals = DeleteArtifactAggregator.nodeSourcePath(output.resolve(DeleteArtifacts.METRICS_FILE_NAME), 1);
		final var aggregator = new DeleteArtifactAggregator(
						"step", List.of(local, w1, w2), List.of("local", "worker-a", "worker-b"), selection(3), completion(3));

		final IntegrityTerminalException failure;
		try (var moves = mockStatic(IntegrityManifestCompletion.class, Mockito.CALLS_REAL_METHODS)) {
			moves.when(() -> IntegrityManifestCompletion.atomicMove(any(), eq(w1Totals)))
							.thenThrow(new IllegalStateException("not an I/O failure"));
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
		}

		assertInstanceOf(IllegalStateException.class, failure.getCause());
		for (final String artifact : DELETE_FILES) {
			final Path canonical = output.resolve(artifact);
			assertEquals(!artifact.equals(DeleteArtifacts.METRICS_FILE_NAME),
							Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 1)), artifact);
			assertTrue(Files.isRegularFile(DeleteArtifactAggregator.nodeSourcePath(canonical, 2)),
							"worker 2's " + artifact + " was not collected");
		}
		try (var files = Files.list(output)) {
			assertEquals(List.of(), files.filter(file -> file.getFileName().toString().startsWith(".")).toList());
		}
	}

	@Test
	void anInterruptedFileReadStopsTheDeleteCollectionAtOnce() throws Exception {
		final var aggregator = interruptedCollection(invocation -> {
			throw new ClosedByInterruptException();
		});
		final var warnings = new CollectionWarnings();

		final IntegrityTerminalException failure;
		try (warnings) {
			warnings.clear("step");
			failure = assertThrows(IntegrityTerminalException.class, aggregator.aggregator()::close);
			warnings.flush("step");
		}

		assertEquals("failed to collect and publish DELETE evidence", failure.getMessage());
		assertInstanceOf(ClosedByInterruptException.class, failure.getCause().getCause());
		assertEquals(List.of(), warnings.lines("step"), "an interrupt was taken for a missing file");
		verify(aggregator.interrupted(), times(1)).readFromFile(anyString(), anyLong());
		verify(aggregator.later(), never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void anInterruptFlagStopsTheDeleteCollectionAtOnce() throws Exception {
		final var aggregator = interruptedCollection(invocation -> {
			Thread.currentThread().interrupt();
			throw new IOException("connection reset");
		});

		try {
			final var failure = assertThrows(IntegrityTerminalException.class, aggregator.aggregator()::close);
			assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag was cleared");
			assertEquals("failed to collect and publish DELETE evidence", failure.getMessage());
		} finally {
			Thread.interrupted();
		}
		verify(aggregator.interrupted(), times(1)).readFromFile(anyString(), anyLong());
		verify(aggregator.later(), never()).readFromFile(anyString(), anyLong());
	}

	private record InterruptedCollection(
					DeleteArtifactAggregator aggregator, FileManager interrupted, FileManager later) {}

	/** Three nodes; every read from node 1 runs {@code read}. */
	private InterruptedCollection interruptedCollection(final org.mockito.stubbing.Answer<byte[]> read)
					throws Exception {
		final Path output = temp.resolve("log/step");
		Files.createDirectories(output);
		final FileManager local = mock(FileManager.class);
		final FileManager interrupted = mock(FileManager.class);
		final FileManager later = mock(FileManager.class);
		final NodeArtifacts localArtifacts = nodeArtifacts(output, 0, "local");
		final NodeArtifacts laterArtifacts = nodeArtifacts(temp.resolve("later"), 2, "worker-b");
		register(local, localArtifacts);
		register(interrupted, nodeArtifacts(temp.resolve("interrupted"), 1, "worker-a"));
		register(later, laterArtifacts);
		write(localArtifacts);
		write(laterArtifacts);
		serve(later, laterArtifacts);
		when(interrupted.readFromFile(anyString(), anyLong())).thenAnswer(read);
		return new InterruptedCollection(new DeleteArtifactAggregator(
						"step", List.of(local, interrupted, later), List.of("local", "worker-a", "worker-b"),
						selection(3), completion(3)), interrupted, later);
	}

	@Test
	void realRecorderOutputAggregatesIntoAHashBoundCanonicalPublication() throws Exception {
		final String stepId = "delete-recorder-aggregate-" + System.nanoTime();
		final Path selection = temp.resolve("real-verify-input.csv");
		Files.writeString(selection, fixture(DeleteArtifacts.SELECTION_FILE_NAME));
		IntegrityManifestCompletion.create(
						selection, 777, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"spt-cli-items-stager-v1", 3, 2, 2).publish(selection);
		final Path selectionCompletion = IntegrityManifestCompletion.completionPath(selection);
		final DeleteArtifactRecorder recorder = new DeleteArtifactRecorder(stepId, selection, 2);
		final var targets = List.of(
						new DeleteTarget(new IntegrityManifestDataItem("bucket", "alpha", 9, null), 0),
						new DeleteTarget(new IntegrityManifestDataItem(
										"bucket", "comma,key", 7, "version-comma"), 1));
		final var operation = new DeleteRequestOperationImpl(
						0, new DeleteRequest("bucket", Credential.NONE, targets));
		operation.completeDelete(DeleteTransportResult.success(targets));
		recorder.recordTerminal(operation);
		final var lifecycle = new OperationLifecycleSnapshot<>(
						0, 0, 1, 0, 1, 0, 0, List.of(), List.of());
		final var metrics = DeleteMetricsSnapshot.builder(2)
						.identity("batch", "canonical")
						.requests(1, 1, 0, 0, 0, 2)
						.objects(2, 2, 2, 0, 0, 0, 2)
						.batches(1, 2, 1, 0)
						.versions(1, 1)
						.reconciled(true)
						.build();
		try {
			recorder.finish(lifecycle, metrics);
			final Path output = temp.resolve("recorder-aggregate");
			DeleteArtifactAggregation.publish(
							stepId, output,
							List.of(new DeleteArtifactAggregation.NodeSource(
											path(Loggers.DELETE_METRICS_TOTAL, stepId),
											path(Loggers.DELETE_REQUESTS, stepId),
											path(Loggers.DELETE_OBJECTS, stepId),
											path(Loggers.DELETE_RESIDUAL, stepId),
											path(Loggers.DELETE_VERIFICATION, stepId))),
							List.of("local"), selection, selectionCompletion);

			for (final String artifact : List.of(
							DeleteArtifacts.METRICS_FILE_NAME,
							DeleteArtifacts.REQUESTS_FILE_NAME,
							DeleteArtifacts.OBJECTS_FILE_NAME,
							DeleteArtifacts.RESIDUAL_FILE_NAME,
							DeleteArtifacts.VERIFICATION_FILE_NAME,
							DeleteArtifacts.SELECTION_FILE_NAME)) {
				if (DeleteArtifacts.VERIFICATION_FILE_NAME.equals(artifact)) {
					assertEquals(3, Files.readAllLines(output.resolve(artifact)).size());
				} else {
					assertEquals(fixture(artifact), Files.readString(output.resolve(artifact)));
				}
			}
			assertEquals(
							fixture(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME).strip(),
							Files.readString(output.resolve(DeleteArtifacts.SELECTION_COMPLETION_FILE_NAME)).strip());
			assertTrue(Files.readString(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME))
							.contains("\"status\" : \"complete\""));
		} finally {
			recorder.close();
			for (final var logger : List.of(
							Loggers.DELETE_METRICS_TOTAL, Loggers.DELETE_REQUESTS,
							Loggers.DELETE_OBJECTS, Loggers.DELETE_RESIDUAL,
							Loggers.DELETE_VERIFICATION)) {
				Files.deleteIfExists(path(logger, stepId));
			}
		}
	}

	private static String fixture(final String name) throws IOException {
		try (InputStream input = DeleteArtifactAggregatorTest.class.getResourceAsStream(
						"/delete-artifacts-v1/" + name)) {
			if (input == null) {
				throw new IOException("missing shared DELETE artifact fixture " + name);
			}
			return new String(input.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	@Test
	void rejectsNoncanonicalSelectionIndexBeforePublishingCompletion() throws Exception {
		final NodeArtifacts first = nodeArtifacts(temp.resolve("first"), 0, "local");
		final NodeArtifacts second = nodeArtifacts(temp.resolve("second"), 1, "worker-a");
		write(first);
		write(second);
		Files.writeString(
						first.objects(),
						first.objectsText().replace(",0,a,a,", ",+0,a,a,"));
		final Path output = temp.resolve("invalid-index");

		assertThrows(RuntimeException.class, () -> DeleteArtifactAggregation.publish(
						"step", output,
						List.of(
										new DeleteArtifactAggregation.NodeSource(
														first.totals(), first.requests(), first.objects(), first.residual()),
										new DeleteArtifactAggregation.NodeSource(
														second.totals(), second.requests(), second.objects(), second.residual())),
						List.of("local", "worker-a"), selection(), completion()));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	@Test
	void rejectsImpossiblePartialBatchShapeBeforePublishingCompletion() throws Exception {
		final NodeArtifacts first = nodeArtifacts(temp.resolve("first"), 0, "local");
		final NodeArtifacts second = nodeArtifacts(temp.resolve("second"), 1, "worker-a");
		write(first);
		write(second);
		Files.writeString(first.totals(), first.totalsText().replace(",1,0,true\n", ",0,1,true\n"));
		final Path output = temp.resolve("invalid-batch-shape");

		assertThrows(RuntimeException.class, () -> DeleteArtifactAggregation.publish(
						"step", output,
						List.of(
										new DeleteArtifactAggregation.NodeSource(
														first.totals(), first.requests(), first.objects(), first.residual()),
										new DeleteArtifactAggregation.NodeSource(
														second.totals(), second.requests(), second.objects(), second.residual())),
						List.of("local", "worker-a"), selection(), completion()));
		assertFalse(Files.exists(output.resolve(DeleteArtifacts.COMPLETION_FILE_NAME)));
	}

	private Path selection() throws Exception {
		return selection(2);
	}

	/** Two items for each of {@code nodes} nodes, as {@link NodeArtifacts} delete them. */
	private Path selection(final int nodes) throws Exception {
		final Path path = temp.resolve("verify-input.csv");
		final StringBuilder csv = new StringBuilder("bucket,key,size,version_id\n");
		for (int i = 0; i < nodes * 2; i++) {
			final char key = (char) ('a' + i);
			csv.append(key).append(',').append(key).append(",1,\n");
		}
		Files.writeString(path, csv);
		return path;
	}

	private Path completion() throws Exception {
		return completion(2);
	}

	private Path completion(final int nodes) throws Exception {
		final Path selection = temp.resolve("verify-input.csv");
		IntegrityManifestCompletion.create(
						selection, 1, IntegrityManifestCompletion.PRODUCER_CLI_STAGER,
						"delete-selection", nodes * 2, nodes * 2, nodes * 2).publish(selection);
		return IntegrityManifestCompletion.completionPath(selection);
	}

	private static void register(final FileManager manager, final NodeArtifacts artifacts) throws Exception {
		when(manager.logFileName(Loggers.DELETE_METRICS_TOTAL.getName(), "step")).thenReturn(artifacts.totals().toString());
		when(manager.logFileName(Loggers.DELETE_REQUESTS.getName(), "step")).thenReturn(artifacts.requests().toString());
		when(manager.logFileName(Loggers.DELETE_OBJECTS.getName(), "step")).thenReturn(artifacts.objects().toString());
		when(manager.logFileName(Loggers.DELETE_RESIDUAL.getName(), "step")).thenReturn(artifacts.residual().toString());
		when(manager.logFileName(Loggers.DELETE_VERIFICATION.getName(), "step"))
						.thenReturn(artifacts.verification().toString());
	}

	private static NodeArtifacts nodeArtifacts(final Path dir, final int index, final String node) {
		return new NodeArtifacts(
						dir.resolve(DeleteArtifacts.METRICS_FILE_NAME),
						dir.resolve(DeleteArtifacts.REQUESTS_FILE_NAME),
						dir.resolve(DeleteArtifacts.OBJECTS_FILE_NAME),
						dir.resolve(DeleteArtifacts.RESIDUAL_FILE_NAME),
						dir.resolve(DeleteArtifacts.VERIFICATION_FILE_NAME), index, node);
	}

	private static void write(final NodeArtifacts artifacts) throws Exception {
		Files.createDirectories(artifacts.totals().getParent());
		Files.writeString(artifacts.totals(), artifacts.totalsText());
		Files.writeString(artifacts.requests(), artifacts.requestsText());
		Files.writeString(artifacts.objects(), artifacts.objectsText());
		Files.writeString(artifacts.residual(), artifacts.residualText());
		Files.writeString(artifacts.verification(), artifacts.verificationText());
	}

	private static void serve(final FileManager manager, final NodeArtifacts artifacts) throws Exception {
		for (final Path path : List.of(
						artifacts.totals(), artifacts.requests(), artifacts.objects(), artifacts.residual(),
						artifacts.verification())) {
			final byte[] bytes = Files.readAllBytes(path);
			when(manager.readFromFile(path.toString(), 0)).thenReturn(bytes);
			when(manager.readFromFile(path.toString(), bytes.length)).thenThrow(new EOFException());
		}
	}

	private static Path path(final org.apache.logging.log4j.Logger logger, final String stepId)
					throws IOException {
		return Path.of(FileManager.INSTANCE.logFileName(logger.getName(), stepId));
	}

	private record NodeArtifacts(
			Path totals, Path requests, Path objects, Path residual, Path verification, int index, String node) {
		String requestId() { return "request-" + index; }
		String acceptedId() { return "target-" + index + "-a"; }
		String failedId() { return "target-" + index + "-b"; }
		String firstKey() { return String.valueOf((char) ('a' + index * 2)); }
		String secondKey() { return String.valueOf((char) ('b' + index * 2)); }
		String totalsText() {
			return String.join(",", DeleteArtifacts.METRICS_HEADER) + "\n"
					+ "1,logical_api_requests,object_identities,logical_api_requests,batch,2,canonical,1,0,1,0,0,2,2,1,1,0,0,1,2,1,0,true\n";
		}
		String requestsText() {
			return String.join(",", DeleteArtifacts.REQUESTS_HEADER) + "\n"
					+ "1," + requestId() + "," + requestId() + ",2,partial," + node + ",1,2,1\n";
		}
		String objectsText() {
			return String.join(",", DeleteArtifacts.OBJECTS_HEADER) + "\n"
					+ "1," + requestId() + "," + acceptedId() + "," + (index * 2) + "," + firstKey() + "," + firstKey() + ",1,,accepted,none,\n"
					+ "1," + requestId() + "," + failedId() + "," + (index * 2 + 1) + "," + secondKey() + "," + secondKey() + ",1,,failed,operational,error\n";
		}
		String residualText() {
			return "bucket,key,size,version_id\n" + secondKey() + "," + secondKey() + ",1,\n";
		}
		String verificationText() {
			return String.join(",", DeleteArtifacts.VERIFICATION_HEADER) + "\n"
					+ "1," + acceptedId() + "," + (index * 2) + "," + firstKey() + "," + firstKey()
					+ ",1,,accepted,false,disabled,false,disabled,false,false,false\n"
					+ "1," + failedId() + "," + (index * 2 + 1) + "," + secondKey() + "," + secondKey()
					+ ",1,,failed,false,disabled,false,disabled,false,false,true\n";
		}
	}
}
