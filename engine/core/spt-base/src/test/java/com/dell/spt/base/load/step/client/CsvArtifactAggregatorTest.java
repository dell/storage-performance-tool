package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.file.FileManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
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
		assertEquals(1, completion.selectedRecordCount());
		assertFalse(Files.exists(tempDir.resolve("verify-input.node-001.csv")));
	}
}
