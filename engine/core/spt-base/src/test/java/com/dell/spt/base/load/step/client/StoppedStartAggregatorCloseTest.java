package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A stopped start's close of an aggregator can be repeated, as a close retried after an earlier
 * one failed part way: the repeat collects nothing again and does not fail.
 */
class StoppedStartAggregatorCloseTest {

	private final String stepId = "stopped-start-aggregator-close-" + UUID.randomUUID();
	private final List<FileManagerService> workers = new ArrayList<>();
	private Path tempDir;

	@AfterEach
	void cleanUp() throws IOException {
		Files.deleteIfExists(Path.of(System.getProperty("java.io.tmpdir"), "spt", "timingMetrics_" + stepId));
		if (tempDir != null) {
			StepIdReuseAtStartTest.removeFolder(tempDir);
		}
	}

	@Test
	void theItemOutputAggregatorClosesASecondTime() throws Exception {
		tempDir = Files.createTempDirectory("stopped-start-aggregator-close");
		final List<FileManager> files = files(3);
		final List<Config> configSlices = List.of(TestConfigBuilder.config(), TestConfigBuilder.config(), TestConfigBuilder.config());
		final var aggregator = new ItemOutputFileAggregator(
						stepId, files, configSlices, tempDir.resolve("items.csv").toString(), 1);

		assertClosesTwice(aggregator::closeAfterStoppedStart);
	}

	@Test
	void theTraceAggregatorClosesASecondTime() throws Exception {
		final var aggregator = new OpTraceLogFileAggregator(stepId, files(3));

		assertClosesTwice(aggregator::closeAfterStoppedStart);
	}

	@Test
	void theTimingAggregatorClosesASecondTime() throws Exception {
		final var aggregator = new ItemTimingMetricOutputFileAggregator(stepId, files(3));

		assertClosesTwice(aggregator::closeAfterStoppedStart);
	}

	/** The entry and both workers had started: the first close reads both workers, the second neither. */
	private void assertClosesTwice(final IntConsumer closeAfterStoppedStart) throws IOException {
		closeAfterStoppedStart.accept(3);
		for (final var worker : workers) {
			verify(worker, atLeastOnce()).readFromFile(anyString(), anyLong());
		}
		clearInvocations(workers.toArray());

		assertDoesNotThrow(() -> closeAfterStoppedStart.accept(3));

		for (final var worker : workers) {
			verify(worker, never()).readFromFile(anyString(), anyLong());
		}
	}

	/** The entry's own file manager and one worker per further slice, each with an empty file. */
	private List<FileManager> files(final int slices) throws IOException {
		final List<FileManager> files = new ArrayList<>();
		files.add(FileManager.INSTANCE);
		for (var i = 1; i < slices; i++) {
			final String prefix = "/" + stepId + "/node-" + i + "/";
			final FileManagerService worker = mock(FileManagerService.class);
			when(worker.logFileName(anyString(), anyString())).thenAnswer(call -> prefix + call.getArgument(0));
			when(worker.newTmpFileName()).thenReturn(prefix + "items");
			when(worker.fileSize(anyString())).thenReturn(0L);
			when(worker.readFromFile(anyString(), anyLong())).thenThrow(new EOFException());
			workers.add(worker);
			files.add(worker);
		}
		return files;
	}
}
