package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.github.akurilov.confuse.Config;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CsvArtifactAggregatorTest {

	@TempDir
	Path tempDir;

	@Test
	void publishesSingleNodeSourceAndCompletionWithoutLosingCsvRecords() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\n"
										+ "b,\"comma,key\",4,v1\r\n"
										+ "b,\"line\nkey\",5,\r\n");

		new CsvArtifactAggregator(
						"create-step",
						List.of(FileManager.INSTANCE),
						List.of(),
						manifest.toString(),
						99,
						0)
						.close();

		assertTrue(Files.isRegularFile(CsvArtifactAggregator.nodeSourcePath(manifest, 0)));
		assertTrue(Files.isRegularFile(manifest));
		assertTrue(Files.isRegularFile(IntegrityManifestCompletion.completionPath(manifest)));
		final var records = CSVFormat.RFC4180.parse(Files.newBufferedReader(manifest)).getRecords();
		assertEquals(3, records.size());
		assertEquals("line\nkey", records.get(2).get(1));
		final var completion = IntegrityManifestCompletion.validate(
						manifest, 99, IntegrityInputProvenance.ENGINE_STEP, "create-step");
		assertEquals(2, completion.selectedRecordCount());
	}

	@Test
	void zeroSuccessfulWritesPublishEvidenceThenFailTerminally() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\n");

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> new CsvArtifactAggregator(
										"create-step",
										List.of(FileManager.INSTANCE),
										List.of(),
										manifest.toString(),
										101,
										0)
										.close());

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(Files.isRegularFile(manifest));
		assertTrue(Files.isRegularFile(CsvArtifactAggregator.nodeSourcePath(manifest, 0)));
		final var completion = IntegrityManifestCompletion.validate(
						manifest, 101, IntegrityInputProvenance.ENGINE_STEP, "create-step");
		assertEquals(0, completion.selectedRecordCount());
	}

	@Test
	void discoverySortsDeduplicatesAndCaps() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\n"
										+ "b,z,1,\r\n"
										+ "b,a,2,\r\n"
										+ "b,a,2,\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "3\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(manifest), "4\n");

		new CsvArtifactAggregator(
						"list-step",
						List.of(FileManager.INSTANCE),
						List.of(),
						manifest.toString(),
						100,
						1)
						.close();

		final var records = CSVFormat.RFC4180.parse(Files.newBufferedReader(manifest)).getRecords();
		assertEquals(2, records.size());
		assertEquals("a", records.get(1).get(1));
		final var completion = IntegrityManifestCompletion.validate(
						manifest, 100, IntegrityInputProvenance.ENGINE_STEP, "list-step");
		assertEquals(3, completion.sourceRecordCount());
		assertEquals(2, completion.uniqueRecordCount());
		assertEquals(4, completion.excludedDeleteMarkerCount());
		assertEquals(1, completion.selectedRecordCount());
		assertFalse(Files.exists(tempDir.resolve("verify-input.node-001.csv")));
	}

	@Test
	void distributedDiscoverySumsDeleteMarkersAcrossNodes() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,a,1,v1\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "1\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(manifest), "2\n");
		final String remoteManifest = "/remote/verify-input.csv";
		final FileManagerService remote = remoteFileManager(remoteManifest, Map.of(
						remoteManifest, "bucket,key,size,version_id\r\nb,b,2,v2\r\n",
						remoteManifest + ".emitted.count", "1\n",
						remoteManifest + ".delete-markers.count", "3\n"));

		new CsvArtifactAggregator(
						"list-step", List.of(FileManager.INSTANCE, remote),
						List.of(mock(Config.class), mock(Config.class)),
						manifest.toString(), 105, 0, com.dell.spt.base.item.op.OpType.LIST).close();

		final var completion = IntegrityManifestCompletion.validate(
						manifest, 105, IntegrityInputProvenance.ENGINE_STEP, "list-step");
		assertEquals(2, completion.sourceRecordCount());
		assertEquals(5, completion.excludedDeleteMarkerCount());
		assertTrue(Files.isRegularFile(CsvArtifactAggregator.nodeSourcePath(manifest, 1)));
		assertEquals("3", Files.readString(IntegrityManifestCompletion.deleteMarkerCountPath(
						CsvArtifactAggregator.nodeSourcePath(manifest, 1))).trim());
	}

	@Test
	void distributedDiscoveryRejectsStaleNodeDeleteMarkerCount() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,a,1,v1\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "1\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(manifest), "0\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(
						CsvArtifactAggregator.nodeSourcePath(manifest, 0)), "stale\n");
		final FileManagerService remote = remoteFileManager("/remote/verify-input.csv", Map.of());

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> new CsvArtifactAggregator(
										"list-step", List.of(FileManager.INSTANCE, remote),
										List.of(mock(Config.class), mock(Config.class)),
										manifest.toString(), 106, 0, com.dell.spt.base.item.op.OpType.LIST).close());

		assertEquals(IntegrityTerminalException.Category.PUBLICATION, failure.category());
		assertTrue(failure.getMessage().contains("stale node delete-marker count"));
	}

	@Test
	void distributedDiscoveryRejectsMissingEntryNodeDeleteMarkerCount() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,a,1,v1\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "1\n");
		final FileManagerService remote = remoteFileManager("/remote/verify-input.csv", Map.of());

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> new CsvArtifactAggregator(
										"list-step", List.of(FileManager.INSTANCE, remote),
										List.of(mock(Config.class), mock(Config.class)),
										manifest.toString(), 107, 0, com.dell.spt.base.item.op.OpType.LIST).close());

		assertEquals(IntegrityTerminalException.Category.AGGREGATION, failure.category());
		assertTrue(failure.getMessage().contains("entry-node LIST delete-marker count is missing"));
	}

	@Test
	void discoveryRejectsEmissionCountMismatchWithoutCompletion() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(manifest, "bucket,key,size,version_id\r\nb,a,1,\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "2\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(manifest), "0\n");

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> new CsvArtifactAggregator(
										"list-step", List.of(FileManager.INSTANCE), List.of(),
										manifest.toString(), 102, 0, com.dell.spt.base.item.op.OpType.LIST).close());

		assertEquals(IntegrityTerminalException.Category.AGGREGATION, failure.category());
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	@Test
	void aggregationCrossesSortChunkBoundaryAndCleansPrivateTemp() throws Exception {
		final Path systemTemp = Path.of(System.getProperty("java.io.tmpdir"));
		final long tempDirsBefore;
		try (final var paths = Files.list(systemTemp)) {
			tempDirsBefore = paths.filter(path -> path.getFileName().toString().startsWith("spt-integrity-aggregate-")).count();
		}
		final Path manifest = tempDir.resolve("verified-large.csv");
		try (final var writer = Files.newBufferedWriter(manifest)) {
			writer.write("bucket,key,size,version_id\r\n");
			for (int i = CsvArtifactAggregator.SORT_CHUNK_RECORDS; i >= 0; i--) {
				writer.write("b,key-" + String.format(java.util.Locale.ROOT, "%08d", i) + ",1,\r\n");
			}
		}

		new CsvArtifactAggregator(
						"read-step", List.of(FileManager.INSTANCE), List.of(),
						manifest.toString(), 104, 0, com.dell.spt.base.item.op.OpType.READ).close();

		final var completion = IntegrityManifestCompletion.validate(
						manifest, 104, IntegrityInputProvenance.ENGINE_STEP, "read-step");
		assertEquals(CsvArtifactAggregator.SORT_CHUNK_RECORDS + 1L, completion.sourceRecordCount());
		assertEquals(completion.sourceRecordCount(), completion.uniqueRecordCount());
		final long tempDirsAfter;
		try (final var paths = Files.list(systemTemp)) {
			tempDirsAfter = paths.filter(path -> path.getFileName().toString().startsWith("spt-integrity-aggregate-")).count();
		}
		assertEquals(tempDirsBefore, tempDirsAfter);
	}

	@Test
	void rejectsConflictingSizesWithoutCompletion() throws Exception {
		final Path manifest = tempDir.resolve("verified.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\n"
										+ "b,a,1,v1\r\n"
										+ "b,a,2,v1\r\n");

		assertThrows(
						IntegrityTerminalException.class,
						() -> new CsvArtifactAggregator(
										"read-step", List.of(FileManager.INSTANCE), List.of(),
										manifest.toString(), 103, 0, com.dell.spt.base.item.op.OpType.READ).close());
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	@Test
	void chunkCleanupFailureIsSuppressedBehindPrimaryWriteFailure() {
		final IOException primary = new IOException("chunk write failed");
		final IOException cleanup = new IOException("chunk delete failed");

		final IOException thrown = assertThrows(
						IOException.class,
						() -> CsvArtifactAggregator.completeChunkWrite(
										() -> {
											throw primary;
										},
										() -> {
											throw cleanup;
										}));

		assertSame(primary, thrown);
		assertEquals(1, thrown.getSuppressed().length);
		assertSame(cleanup, thrown.getSuppressed()[0]);
	}

	@Test
	void aggregationCleanupFailureIsSuppressedBehindPrimaryFailure() throws Exception {
		final Path source = tempDir.resolve("invalid-source.csv");
		Files.writeString(source, "not,a,canonical,header\n");
		final IOException cleanup = new IOException("temp cleanup failed");

		final IOException thrown = assertThrows(
						IOException.class,
						() -> CsvArtifactAggregator.aggregateBoundedForTest(
										List.of(source), tempDir.resolve("staging.csv"), 0, () -> {
											throw cleanup;
										}));

		assertTrue(thrown.getMessage().contains("noncanonical header"));
		assertEquals(1, thrown.getSuppressed().length);
		assertSame(cleanup, thrown.getSuppressed()[0]);
	}

	private FileManagerService remoteFileManager(
					final String manifestName, final Map<String, String> contents) throws IOException {
		final FileManagerService remote = mock(FileManagerService.class);
		when(remote.newTmpFileName()).thenReturn(manifestName);
		when(remote.readFromFile(anyString(), anyLong())).thenAnswer(invocation -> {
			final String fileName = invocation.getArgument(0);
			final long offset = invocation.getArgument(1);
			final String content = contents.get(fileName);
			if (content == null) {
				throw new IOException("missing remote fixture " + fileName);
			}
			final byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
			if (offset >= bytes.length) {
				return FileManager.EMPTY;
			}
			return Arrays.copyOfRange(bytes, Math.toIntExact(offset), bytes.length);
		});
		return remote;
	}

	@Test
	void aggregationCleanupOnlyFailureIsReportedNormally() throws Exception {
		final Path source = tempDir.resolve("valid-source.csv");
		Files.writeString(
						source,
						"bucket,key,size,version_id\n"
										+ "b,key,1,\n");
		final Path staging = Files.createFile(tempDir.resolve("staging.csv"));
		final IOException cleanup = new IOException("temp cleanup failed");

		final IOException thrown = assertThrows(
						IOException.class,
						() -> CsvArtifactAggregator.aggregateBoundedForTest(
										List.of(source), staging, 0, () -> {
											throw cleanup;
										}));

		assertSame(cleanup, thrown);
	}
}
