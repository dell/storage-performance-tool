package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.file.FileManager;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

class CsvLoggerArtifactAggregatorTest {

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
}
