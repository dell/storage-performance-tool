package com.dell.spt.base.load.step.client;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.load.step.file.FileManager;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.List;
import java.util.stream.Stream;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

class CsvLoggerArtifactAggregatorTest {

	private static final String LOGGER = "OperationLifecycle";
	private static final String ARTIFACT = OperationLifecycleArtifact.FILE_NAME;
	private static final String HEADER = OperationLifecycleArtifact.HEADER;

	private final String stepId = "missing-slice-" + java.util.UUID.randomUUID();

	@TempDir
	Path tempDir;

	@Test
	void collectsTerminalOperationEvidenceFromEveryContributor() throws Exception {
		final Path dir = Files.createTempDirectory("operation-lifecycle-aggregation-");
		final var artifact = com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.FILE_NAME;
		final var header = com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.HEADER;
		final var counters = new com.dell.spt.base.load.lifecycle.OperationLifecycleCounters(true, 4, 1, 1, 2, 1, 1, 0, 0, 0);
		final Path canonical = dir.resolve(artifact);
		final var managers = new java.util.ArrayList<FileManager>();
		try {
			for (int i = 0; i < 3; i++) {
				final var manager = mock(FileManager.class);
				managers.add(manager);
				final String source = i == 0 ? canonical.toString() : "/worker-" + i + "/" + artifact;
				when(manager.logFileName("OperationLifecycle", "step")).thenReturn(source);
				final String body = header + "\n" + com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.row(
								123, "step", "worker-" + i, List.of(counters), true) + "\n";
				if (i == 0) {
					Files.writeString(canonical, body);
				} else {
					final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
					when(manager.readFromFile(source, 0)).thenReturn(bytes);
					when(manager.readFromFile(source, bytes.length)).thenThrow(new EOFException());
				}
			}
			new CsvLoggerArtifactAggregator("step", managers, "OperationLifecycle", artifact, header).close();
			try (final var parser = CSVFormat.RFC4180.builder().setHeader().get().parse(Files.newBufferedReader(canonical))) {
				final var rows = parser.getRecords();
				assertEquals(3, rows.size());
				for (int i = 0; i < 3; i++) {
					assertEquals("worker-" + i, rows.get(i).get("worker_id"));
					org.junit.jupiter.api.Assertions.assertTrue(Files.exists(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, i)));
				}
			}
		} finally {
			try (final var files = Files.list(dir)) {
				for (final var file : files.toList())
					Files.deleteIfExists(file);
			}
			Files.deleteIfExists(dir);
		}
	}

	@Test
	void collectsRangePolicyAndOutcomesFromEveryContributor() throws Exception {
		final Path dir = Files.createTempDirectory("range-read-aggregation-");
		final var artifact = com.dell.spt.base.metrics.range.RangeReadArtifact.FILE_NAME;
		final var header = com.dell.spt.base.metrics.range.RangeReadArtifact.HEADER;
		final var counters = new com.dell.spt.base.metrics.range.RangeReadSnapshot(1,
						new com.dell.spt.base.item.op.data.range.RangeReadPolicy(3, 0L, 1),
						new com.dell.spt.base.load.lifecycle.OperationLifecycleCounters(true, 1, 1, 0, 1, 0, 0, 0, 0, 0),
						1, 1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, false);
		final Path canonical = dir.resolve(artifact);
		final var managers = new java.util.ArrayList<FileManager>();
		try {
			for (int i = 0; i < 3; i++) {
				final var manager = mock(FileManager.class);
				managers.add(manager);
				final String source = i == 0 ? canonical.toString() : "/worker-" + i + "/" + artifact;
				when(manager.logFileName("RangeRead", "step")).thenReturn(source);
				final String body = header + "\n" + com.dell.spt.base.metrics.range.RangeReadArtifact.rows(
								123, "step", "worker-" + i, List.of(counters), true) + "\n";
				if (i == 0) {
					Files.writeString(canonical, body);
				} else {
					final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
					when(manager.readFromFile(source, 0)).thenReturn(bytes);
					when(manager.readFromFile(source, bytes.length)).thenThrow(new EOFException());
				}
			}
			new CsvLoggerArtifactAggregator("step", managers, "RangeRead", artifact, header).close();
			try (final var parser = CSVFormat.RFC4180.builder().setHeader().get().parse(Files.newBufferedReader(canonical))) {
				final var rows = parser.getRecords();
				assertEquals(3, rows.size());
				for (int i = 0; i < 3; i++) {
					assertEquals("worker-" + i, rows.get(i).get("worker_id"));
					assertEquals("3", rows.get(i).get("successful_bytes"));
					assertEquals("0", rows.get(i).get("fixed_offset"));
					assertEquals("true", rows.get(i).get("terminal"));
					org.junit.jupiter.api.Assertions.assertTrue(Files.exists(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, i)));
				}
			}
		} finally {
			try (final var files = Files.list(dir)) {
				for (final var file : files.toList())
					Files.deleteIfExists(file);
			}
			Files.deleteIfExists(dir);
		}
	}

	@Test
	void preservesNodeSourcesAndMergesCompleteCsvRecords() throws Exception {
		final Path dir = Files.createTempDirectory("integrity-logger-aggregation-");
		final Path canonical = dir.resolve(IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME);
		final String local = IntegrityCsvArtifacts.PERFORMANCE_HEADER
						+ "\r\nlocal,step,s3,read_verify,sha256,1,3,0.1,30,,0\r\n";
		final String remote = IntegrityCsvArtifacts.PERFORMANCE_HEADER
						+ "\r\nremote,step,s3,read_verify,sha256,1,4,0.2,20,,0\r\n";
		Files.writeString(canonical, local, StandardCharsets.UTF_8);

		final FileManager localManager = mock(FileManager.class);
		final FileManager remoteManager = mock(FileManager.class);
		when(localManager.logFileName("logger", "step")).thenReturn(canonical.toString());
		when(remoteManager.logFileName("logger", "step")).thenReturn("/remote/integrity.performance.csv");
		final byte[] remoteBytes = remote.getBytes(StandardCharsets.UTF_8);
		when(remoteManager.readFromFile("/remote/integrity.performance.csv", 0)).thenReturn(remoteBytes);
		when(remoteManager.readFromFile("/remote/integrity.performance.csv", remoteBytes.length))
						.thenThrow(new EOFException());

		new CsvLoggerArtifactAggregator(
						"step",
						List.of(localManager, remoteManager),
						"logger",
						IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME,
						IntegrityCsvArtifacts.PERFORMANCE_HEADER)
						.close();

		final var records = CSVFormat.RFC4180.parse(Files.newBufferedReader(canonical)).getRecords();
		assertEquals(3, records.size());
		assertEquals("local", records.get(1).get(0));
		assertEquals("remote", records.get(2).get(0));
		assertEquals(local, Files.readString(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 0)));
		assertEquals(remote, Files.readString(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 1)));
	}

	@Test
	void rejectsDuplicateSourceHeaderAsData() throws Exception {
		final Path dir = Files.createTempDirectory("integrity-logger-aggregation-");
		final Path canonical = dir.resolve(IntegrityCsvArtifacts.FAILURES_FILE_NAME);
		Files.writeString(
						canonical,
						IntegrityCsvArtifacts.FAILURES_HEADER + "\r\n" + IntegrityCsvArtifacts.FAILURES_HEADER + "\r\n",
						StandardCharsets.UTF_8);
		final FileManager localManager = mock(FileManager.class);
		when(localManager.logFileName("logger", "step")).thenReturn(canonical.toString());
		final var aggregator = new CsvLoggerArtifactAggregator(
						"step",
						List.of(localManager),
						"logger",
						IntegrityCsvArtifacts.FAILURES_FILE_NAME,
						IntegrityCsvArtifacts.FAILURES_HEADER);
		assertThrows(IntegrityTerminalException.class, aggregator::close);
	}

	@Test
	void aLostNodesArtifactFailsAsNotCollectedInsteadOfEmpty() throws Exception {
		final Path dir = Files.createTempDirectory("integrity-logger-aggregation-");
		final Path canonical = dir.resolve(IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME);
		Files.writeString(canonical, IntegrityCsvArtifacts.PERFORMANCE_HEADER + "\r\n", StandardCharsets.UTF_8);
		final FileManager localManager = mock(FileManager.class);
		final FileManager remoteManager = mock(FileManager.class);
		when(localManager.logFileName("logger", "step")).thenReturn(canonical.toString());
		when(remoteManager.logFileName("logger", "step")).thenReturn("/remote/integrity.performance.csv");
		// The guard's answer for a node whose step slice the entry lost.
		when(remoteManager.readFromFile("/remote/integrity.performance.csv", 0)).thenThrow(new LostNodeFileManager.NotCollected());
		final var aggregator = new CsvLoggerArtifactAggregator(
						"step",
						List.of(localManager, remoteManager),
						"logger",
						IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME,
						IntegrityCsvArtifacts.PERFORMANCE_HEADER);

		final IntegrityTerminalException failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals(
						"remote logger artifact was not collected, as the entry lost its node's step slice: /remote/integrity.performance.csv",
						failure.getCause().getMessage());
	}

	@Test
	void aStoppedStartPublishesTheStartedSlicesOnlyAndNothingWithoutOne() throws Exception {
		final Path dir = Files.createTempDirectory("stopped-start-aggregation-");
		final var artifact = com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.FILE_NAME;
		final var header = com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.HEADER;
		final Path canonical = dir.resolve(artifact);
		try {
			final FileManager entry = mock(FileManager.class);
			when(entry.logFileName("OperationLifecycle", "step")).thenReturn(canonical.toString());
			final FileManager worker = mock(FileManager.class);
			when(worker.logFileName("OperationLifecycle", "step")).thenReturn("/worker-1/" + artifact);
			when(worker.readFromFile("/worker-1/" + artifact, 0)).thenThrow(new java.nio.file.NoSuchFileException("never started"));
			final var aggregator = new CsvLoggerArtifactAggregator("step", List.of(entry, worker), "OperationLifecycle", artifact, header);

			aggregator.closeAfterStoppedStart(0);
			try (var files = Files.list(dir)) {
				assertEquals(List.of(), files.toList(), "a step whose slices never started published its file");
			}
			assertThrows(IllegalArgumentException.class, () -> aggregator.closeAfterStoppedStart(-1));
			assertThrows(IllegalArgumentException.class, () -> aggregator.closeAfterStoppedStart(3));
			// A started slice's missing file is still a failure.
			final var missing = assertThrows(IntegrityTerminalException.class, () -> aggregator.closeAfterStoppedStart(1));
			org.junit.jupiter.api.Assertions.assertTrue(missing.getMessage().contains("entry-node logger artifact is missing"));

			final var counters = new com.dell.spt.base.load.lifecycle.OperationLifecycleCounters(true, 1, 1, 0, 1, 0, 0, 0, 0, 0);
			Files.writeString(canonical, header + "\n" + com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact.row(
							1, "step", "worker-0", List.of(counters), true) + "\n");
			aggregator.closeAfterStoppedStart(1);
			try (final var parser = CSVFormat.RFC4180.builder().setHeader().get().parse(Files.newBufferedReader(canonical))) {
				assertEquals(List.of("worker-0"), parser.getRecords().stream().map(row -> row.get("worker_id")).toList());
			}
			org.junit.jupiter.api.Assertions.assertFalse(Files.exists(CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 1)));
		} finally {
			try (final var files = Files.list(dir)) {
				for (final var file : files.toList())
					Files.deleteIfExists(file);
			}
			Files.deleteIfExists(dir);
		}
	}

	@Test
	void aLostWorkerStillLetsTheEntryCollectEveryLaterWorker() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager lost = remote(1);
		when(lost.readFromFile(remoteName(1), 0)).thenThrow(new LostNodeFileManager.NotCollected());
		final var aggregator = aggregator(entry(canonical), lost, healthy(2), healthy(3));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals(Category.AGGREGATION, failure.category());
		assertEquals("failed to aggregate " + ARTIFACT + ": not published, missing node-001; collected the other 3 of 4",
						failure.getMessage());
		assertEquals("remote logger artifact was not collected, as the entry lost its node's step slice: " + remoteName(1),
						failure.getCause().getMessage());
		assertEquals(0, failure.getSuppressed().length);
		assertNodeFiles(canonical, "w0", null, "w2", "w3");
		assertFalse(Files.exists(canonical), "a merged file without node 1 was published");
	}

	@Test
	void aMissingWorkerFileKeepsTheOtherNodesFilesAndNamesTheMissingNode() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager missing = remote(1);
		when(missing.readFromFile(remoteName(1), 0)).thenThrow(new NoSuchFileException(remoteName(1)));
		final var aggregator = aggregator(entry(canonical), missing, healthy(2));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals("failed to aggregate " + ARTIFACT + ": not published, missing node-001; collected the other 2 of 3",
						failure.getMessage());
		assertInstanceOf(NoSuchFileException.class, failure.getCause().getCause());
		assertNodeFiles(canonical, "w0", null, "w2");
		assertFalse(Files.exists(canonical));
	}

	@Test
	void aMissingEntryFileStillCollectsEveryWorker() throws Exception {
		final Path canonical = tempDir.resolve(ARTIFACT);
		final var aggregator = aggregator(entry(canonical), healthy(1), healthy(2));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals(Category.AGGREGATION, failure.category());
		assertEquals("entry-node logger artifact is missing: " + canonical
						+ "; not published, missing node-000; collected the other 2 of 3", failure.getMessage());
		assertNull(failure.getCause());
		assertEquals(0, failure.getSuppressed().length);
		assertNodeFiles(canonical, null, "w1", "w2");
		assertFalse(Files.exists(canonical));
	}

	@Test
	void aSingleSlicesMissingFileFailsAsBefore() throws Exception {
		final Path canonical = tempDir.resolve(ARTIFACT);

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator(entry(canonical))::close);

		assertEquals("entry-node logger artifact is missing: " + canonical, failure.getMessage());
		assertNull(failure.getCause());
		assertEquals(List.of(), files());
	}

	@Test
	void twoMissingSlicesAreBothNamedAndTheLaterFailureIsSuppressed() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager lost = remote(1);
		when(lost.readFromFile(remoteName(1), 0)).thenThrow(new LostNodeFileManager.NotCollected());
		final FileManager missing = remote(3);
		when(missing.readFromFile(remoteName(3), 0)).thenThrow(new NoSuchFileException(remoteName(3)));
		final var warnings = new CollectionWarnings();
		final var aggregator = aggregator(entry(canonical), lost, healthy(2), missing);

		final IntegrityTerminalException failure;
		try (warnings) {
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
			warnings.await("from node-001", "from node-003");
		}

		assertEquals("failed to aggregate " + ARTIFACT + ": not published, missing node-001, node-003; collected the other 2 of 4",
						failure.getMessage());
		assertTrue(failure.getCause().getMessage().contains(remoteName(1)), failure.getCause().getMessage());
		assertEquals(1, failure.getSuppressed().length);
		assertTrue(failure.getSuppressed()[0].getMessage().contains(remoteName(3)), failure.getSuppressed()[0].getMessage());
		assertNodeFiles(canonical, "w0", null, "w2", null);
		final List<String> lines = warnings.lines(stepId);
		assertEquals(2, lines.size(), lines.toString());
		assertTrue(lines.get(0).startsWith(stepId + ": failed to collect " + ARTIFACT + " from node-001: "), lines.get(0));
		assertTrue(lines.get(1).startsWith(stepId + ": failed to collect " + ARTIFACT + " from node-003: "), lines.get(1));
		assertTrue(lines.stream().noneMatch(line -> line.contains("failed to aggregate")), lines.toString());
	}

	@Test
	void aPartlyFetchedWorkerFileLeavesNoNodeFileAndNoTempFile() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager partial = remote(1);
		final byte[] half = (HEADER + "\n1,123," + stepId).getBytes(StandardCharsets.UTF_8);
		when(partial.readFromFile(remoteName(1), 0)).thenReturn(half);
		when(partial.readFromFile(remoteName(1), half.length)).thenThrow(new IOException("connection reset"));
		final var aggregator = aggregator(entry(canonical), partial, healthy(2));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals("failed to fetch remote logger artifact " + remoteName(1), failure.getCause().getMessage());
		assertNodeFiles(canonical, "w0", null, "w2");
		assertEquals(List.of(nodeName(0), nodeName(2)), files(), "a partial or temporary file was left");
	}

	@Test
	void aStaleLaterNodeSourceRefusesBeforeTheEntryFileMoves() throws Exception {
		final Path canonical = entryFile("w0");
		final Path stale = CsvLoggerArtifactAggregator.nodeSourcePath(canonical, 2);
		Files.writeString(stale, "stale");
		final FileManager w1 = healthy(1);
		final var aggregator = aggregator(entry(canonical), w1, healthy(2));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals(Category.PUBLICATION, failure.category());
		assertEquals("stale node source exists: " + stale, failure.getMessage());
		assertEquals("stale", Files.readString(stale));
		assertTrue(Files.isRegularFile(canonical), "the entry's file moved before the stale check");
		assertEquals(List.of(ARTIFACT, nodeName(2)), files());
		verify(w1, never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void aStoppedStartWithAMissingMiddleSliceCollectsTheOtherStartedSlicesOnly() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager missing = remote(1);
		when(missing.readFromFile(remoteName(1), 0)).thenThrow(new NoSuchFileException(remoteName(1)));
		final FileManager unstarted = healthy(3);
		final var aggregator = aggregator(entry(canonical), missing, healthy(2), unstarted);

		final var failure = assertThrows(IntegrityTerminalException.class, () -> aggregator.closeAfterStoppedStart(3));

		assertEquals("failed to aggregate " + ARTIFACT + ": not published, missing node-001; collected the other 2 of 3",
						failure.getMessage());
		assertNodeFiles(canonical, "w0", null, "w2", null);
		verify(unstarted, never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void anInterruptStopsTheCollectionAtOnce() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager interrupted = remote(1);
		when(interrupted.readFromFile(remoteName(1), 0)).thenAnswer(invocation -> {
			throwUnchecked(new InterruptedException("stop collecting"));
			return null;
		});
		final FileManager later = healthy(2);
		final var aggregator = aggregator(entry(canonical), interrupted, later);

		assertInstanceOf(InterruptedException.class, assertThrows(Throwable.class, aggregator::close));
		verify(later, never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void anInterruptedFileReadStopsTheCollectionAtOnce() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager interrupted = remote(1);
		when(interrupted.readFromFile(remoteName(1), 0)).thenThrow(new ClosedByInterruptException());
		final FileManager later = healthy(2);
		final var warnings = new CollectionWarnings();
		final var aggregator = aggregator(entry(canonical), interrupted, later);

		final IntegrityTerminalException failure;
		try (warnings) {
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
			warnings.flush(stepId);
		}

		assertEquals("failed to aggregate " + ARTIFACT, failure.getMessage());
		assertInstanceOf(ClosedByInterruptException.class, failure.getCause().getCause());
		assertEquals(List.of(), warnings.lines(stepId), "an interrupt was taken for a missing node");
		verify(later, never()).readFromFile(anyString(), anyLong());
		assertFalse(Files.exists(canonical));
	}

	@Test
	void anInterruptFlagStopsTheCollectionAtOnce() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager interrupted = remote(1);
		when(interrupted.readFromFile(remoteName(1), 0)).thenAnswer(invocation -> {
			Thread.currentThread().interrupt();
			throw new IOException("connection reset");
		});
		final FileManager later = healthy(2);
		final var warnings = new CollectionWarnings();
		final var aggregator = aggregator(entry(canonical), interrupted, later);

		final IntegrityTerminalException failure;
		try (warnings) {
			try {
				failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
				assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag was cleared");
			} finally {
				Thread.interrupted();
			}
			warnings.flush(stepId);
		}

		assertEquals("failed to aggregate " + ARTIFACT, failure.getMessage());
		assertEquals(List.of(), warnings.lines(stepId), "an interrupt was taken for a missing node");
		verify(later, never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void anInterruptFlagSetBeforeTheCloseBlamesNoNode() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager w1 = healthy(1);
		final FileManager w2 = healthy(2);
		final var warnings = new CollectionWarnings();
		final var aggregator = aggregator(entry(canonical), w1, w2);

		final IntegrityTerminalException failure;
		try (warnings) {
			Thread.currentThread().interrupt();
			try {
				failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
				assertTrue(Thread.currentThread().isInterrupted(), "the interrupt flag was cleared");
			} finally {
				Thread.interrupted();
			}
			warnings.flush(stepId);
		}

		assertEquals("failed to aggregate " + ARTIFACT, failure.getMessage());
		assertEquals(List.of(), warnings.lines(stepId), "healthy nodes were blamed for an interrupt");
		verify(w2, never()).readFromFile(anyString(), anyLong());
	}

	@Test
	void aNonIoFailureOfAWorkersReadCountsAsThatNodeMissing() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager broken = remote(1);
		when(broken.readFromFile(remoteName(1), 0)).thenThrow(new IllegalStateException("not an I/O failure"));
		final var aggregator = aggregator(entry(canonical), broken, healthy(2));

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator::close);

		assertEquals("failed to aggregate " + ARTIFACT + ": not published, missing node-001; collected the other 2 of 3",
						failure.getMessage());
		assertEquals("failed to fetch remote logger artifact " + remoteName(1), failure.getCause().getMessage());
		assertInstanceOf(IllegalStateException.class, failure.getCause().getCause());
		assertNodeFiles(canonical, "w0", null, "w2");
		assertEquals(List.of(nodeName(0), nodeName(2)), files());
	}

	@Test
	void aNonIoFailureOutsideAWorkersReadCountsAsThatNodeMissing() throws Exception {
		final Path canonical = entryFile("w0");
		final var aggregator = aggregator(entry(canonical), healthy(1), healthy(2));

		final IntegrityTerminalException failure;
		try (var moves = mockStatic(IntegrityManifestCompletion.class, Mockito.CALLS_REAL_METHODS)) {
			moves.when(() -> IntegrityManifestCompletion.atomicMove(any(), eq(nodePath(1).toAbsolutePath())))
							.thenThrow(new IllegalStateException("not an I/O failure"));
			failure = assertThrows(IntegrityTerminalException.class, aggregator::close);
		}

		assertEquals("failed to aggregate " + ARTIFACT + ": not published, missing node-001; collected the other 2 of 3",
						failure.getMessage());
		assertInstanceOf(IllegalStateException.class, failure.getCause());
		assertNodeFiles(canonical, "w0", null, "w2");
		assertEquals(List.of(nodeName(0), nodeName(2)), files(), "a partial or temporary file was left");
	}

	@Test
	void aSingleSlicesMissingFileLogsNoWarning() throws Exception {
		final Path canonical = tempDir.resolve(ARTIFACT);
		final var warnings = new CollectionWarnings();

		try (warnings) {
			assertThrows(IntegrityTerminalException.class, aggregator(entry(canonical))::close);
			warnings.flush(stepId);
		}

		assertEquals(List.of(), warnings.lines(stepId));
	}

	@Test
	void aWorkersCopyGetsTheDefaultPermissionsOfANewFile() throws Exception {
		assumeTrue(Files.getFileStore(tempDir).supportsFileAttributeView(PosixFileAttributeView.class));
		final Set<PosixFilePermission> fresh = Files.getPosixFilePermissions(Files.createFile(tempDir.resolve("fresh")));
		Files.delete(tempDir.resolve("fresh"));
		assumeTrue(fresh.contains(PosixFilePermission.GROUP_READ) || fresh.contains(PosixFilePermission.OTHERS_READ),
						"the umask makes every new file owner-only");
		final Path canonical = entryFile("w0");
		final FileManager w1 = healthy(1);

		aggregator(entry(canonical), w1, healthy(2)).close();

		assertEquals(fresh, Files.getPosixFilePermissions(nodePath(1)));
		assertEquals(fresh, Files.getPosixFilePermissions(nodePath(2)));
		verify(w1, times(2)).readFromFile(anyString(), anyLong());
	}

	@Test
	void aFailedMergeLeavesNoStagingFile() throws Exception {
		final Path canonical = entryFile("w0");
		final FileManager wrong = remote(1);
		final byte[] bytes = "wrong,header\n1,2\n".getBytes(StandardCharsets.UTF_8);
		when(wrong.readFromFile(remoteName(1), 0)).thenReturn(bytes);
		when(wrong.readFromFile(remoteName(1), bytes.length)).thenThrow(new EOFException());

		final var failure = assertThrows(IntegrityTerminalException.class, aggregator(entry(canonical), wrong)::close);

		assertEquals("failed to aggregate " + ARTIFACT, failure.getMessage());
		assertEquals(List.of(nodeName(0), nodeName(1)), files(), "the merge left its staging file");
	}

	@Test
	void theStartCheckFindsTheNodeSourcesAnEarlierLargerStepLeft() throws Exception {
		final Path canonical = tempDir.resolve(ARTIFACT);
		final FileManager entry = entry(canonical);
		assertThrows(IntegrityTerminalException.class, aggregator(entry, healthy(1), healthy(2))::close);
		Files.writeString(tempDir.resolve("operation.lifecycle.node-1000.csv"), "a later node's");
		Files.writeString(tempDir.resolve("operation.lifecycle.node-01.csv"), "not a node source");
		Files.writeString(tempDir.resolve("range.read.node-003.csv"), "another artifact's");

		// A later one-slice step with the same id must still be refused: it would publish beside them.
		assertEquals(List.of(canonical, nodePath(0), nodePath(1), nodePath(2), tempDir.resolve("operation.lifecycle.node-1000.csv")),
						CsvLoggerArtifactAggregator.publishedPaths(entry, LOGGER, stepId, 1));
		assertEquals(List.of(canonical, nodePath(0), nodePath(1), nodePath(2), nodePath(3),
						tempDir.resolve("operation.lifecycle.node-1000.csv")),
						CsvLoggerArtifactAggregator.publishedPaths(entry, LOGGER, stepId, 4));
	}

	private Path entryFile(final String workerId) throws IOException {
		final Path canonical = tempDir.resolve(ARTIFACT);
		Files.writeString(canonical, body(workerId));
		return canonical;
	}

	private String body(final String workerId) {
		final var counters = new OperationLifecycleCounters(true, 4, 4, 0, 4, 0, 0, 0, 0, 0);
		return HEADER + "\n" + OperationLifecycleArtifact.row(123, stepId, workerId, List.of(counters), true) + "\n";
	}

	private FileManager entry(final Path canonical) throws IOException {
		final FileManager manager = mock(FileManager.class);
		when(manager.logFileName(LOGGER, stepId)).thenReturn(canonical.toString());
		return manager;
	}

	private static String remoteName(final int node) {
		return "/w" + node + "/" + ARTIFACT;
	}

	private FileManager remote(final int node) throws IOException {
		final FileManager manager = mock(FileManager.class);
		when(manager.logFileName(LOGGER, stepId)).thenReturn(remoteName(node));
		return manager;
	}

	private FileManager healthy(final int node) throws IOException {
		final FileManager manager = remote(node);
		final byte[] bytes = body("w" + node).getBytes(StandardCharsets.UTF_8);
		when(manager.readFromFile(remoteName(node), 0)).thenReturn(bytes);
		when(manager.readFromFile(remoteName(node), bytes.length)).thenThrow(new EOFException());
		return manager;
	}

	private CsvLoggerArtifactAggregator aggregator(final FileManager... managers) throws IOException {
		return new CsvLoggerArtifactAggregator(stepId, List.of(managers), LOGGER, ARTIFACT, HEADER);
	}

	private Path nodePath(final int node) {
		return CsvLoggerArtifactAggregator.nodeSourcePath(tempDir.resolve(ARTIFACT), node);
	}

	private static String nodeName(final int node) {
		return String.format("operation.lifecycle.node-%03d.csv", node);
	}

	/** Each node's file holds its own rows, or is absent where {@code workerIds} has null. */
	private void assertNodeFiles(final Path canonical, final String... workerIds) throws IOException {
		for (int i = 0; i < workerIds.length; i++) {
			final Path node = CsvLoggerArtifactAggregator.nodeSourcePath(canonical, i);
			if (workerIds[i] == null) {
				assertFalse(Files.exists(node), node + " exists");
			} else {
				assertEquals(body(workerIds[i]), Files.readString(node), node.toString());
			}
		}
	}

	/** Every file name in the temporary directory, hidden ones included, sorted. */
	private List<String> files() throws IOException {
		try (Stream<Path> files = Files.list(tempDir)) {
			return files.map(file -> file.getFileName().toString()).sorted().toList();
		}
	}
}
