package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import com.dell.spt.base.item.io.CsvItemInput;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.file.FileManagerImpl;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("ItemInputFileSlicer & ItemOutputFileAggregator")
class ItemFileSlicerAggregatorTest {

	private final List<Path> filesToCleanup = new ArrayList<>();

	@AfterEach
	void cleanup() {
		for (Path p : filesToCleanup) {
			try {
				Files.deleteIfExists(p);
			} catch (final Exception e) {
				fail("Failed to delete temp file " + p, e);
			}
		}
		filesToCleanup.clear();
	}

	@Test
	@DisplayName("ItemInputFileSlicer distributes items and cleans up")
	void testItemInputFileSlicerDistributesAndCleansUp() throws Exception {
		// Arrange: 2 local file managers and 2 config slices
		List<FileManager> fileMgrs = List.of(new FileManagerImpl(), new FileManagerImpl());
		Config baseCfg = TestConfigBuilder.config();
		List<Config> configSlices = List.of(new BasicConfig(baseCfg), new BasicConfig(baseCfg));

		// Input with 5 simple items
		String data = String.join("\n", List.of("a", "b", "c", "d", "e")) + "\n";
		CsvItemInput<Item> itemInput = new CsvItemInput<>(
						new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), new ItemFactoryImpl<>());

		// Act: construct slicer which performs slicing in constructor
		int batchSize = 2;
		ItemInputFileSlicer slicer = new ItemInputFileSlicer(
						"test-step-slicer", fileMgrs, configSlices, itemInput, batchSize);

		// Assert: each config slice has an item-input-file set and files exist with some data
		for (int i = 0; i < configSlices.size(); i++) {
			String itemInputFile = configSlices.get(i).stringVal("item-input-file");
			assertNotNull(itemInputFile);
			assertFalse(itemInputFile.isEmpty());
			Path p = Path.of(itemInputFile);
			assertTrue(Files.exists(p), "Slice file should exist before close");
			assertTrue(Files.size(p) > 0, "Slice file should contain data");
			filesToCleanup.add(p); // in case close fails
		}

		// Cleanup via slicer.close() should delete the temporary files
		slicer.close();
		for (Config cfg : configSlices) {
			String f = cfg.stringVal("item-input-file");
			assertFalse(Files.exists(Path.of(f)), "Slice file should be deleted on close");
		}
	}

	@Test
	@DisplayName("ItemOutputFileAggregator collects remote slices into local output and deletes remotes")
	void testItemOutputFileAggregatorCollectsFromRemoteSlices() throws Exception {
		// Arrange: first file manager is local, second is remote (service)
		FileManager localMgr = new FileManagerImpl();
		FileManagerService remoteMgr = new FileManagerServiceImpl(0); // not started, used as a marker and delegate
		List<FileManager> fileMgrs = List.of(localMgr, remoteMgr);

		Config baseCfg = TestConfigBuilder.config();
		List<Config> configSlices = List.of(new BasicConfig(baseCfg), new BasicConfig(baseCfg));

		// Target local output file
		Path localOutput = Files.createTempFile("spt-itm-agg-", ".out");
		filesToCleanup.add(localOutput);

		// Construct aggregator to prepare remote slice filenames in configSlices[1]
		ItemOutputFileAggregator aggregator = new ItemOutputFileAggregator(
						"test-step-agg", fileMgrs, configSlices, localOutput.toString());

		// Prepare remote content: write to the remote slice file that aggregator configured
		String remoteFile = configSlices.get(1).stringVal("item-output-file");
		assertNotNull(remoteFile);
		assertFalse(remoteFile.isEmpty());
		byte[] payload = "hello\nworld\n".getBytes(StandardCharsets.UTF_8);
		remoteMgr.writeToFile(remoteFile, payload);

		// Act: close aggregator which collects remote to local and deletes remote
		aggregator.close();

		// Assert: local file contains the payload and remote file is deleted
		byte[] collected = Files.readAllBytes(localOutput);
		assertArrayEquals(payload, collected);
		assertFalse(Files.exists(Path.of(remoteFile)), "Remote slice should be deleted after collect");
	}

	@Test
	@DisplayName("ItemInputFileSlicer logs and skips when stream initialization fails")
	void itemInputFileSlicerHandlesStreamFactoryFailure() throws Exception {
		final FileManager mockMgr = mock(FileManager.class);
		final Path tmp = Files.createTempFile("spt-slicer", ".input");
		filesToCleanup.add(tmp);
		when(mockMgr.newTmpFileName()).thenReturn(tmp.toString());
		doAnswer(invocation -> {
			Files.deleteIfExists(tmp);
			return null;
		}).when(mockMgr).deleteFile(anyString());

		final Config baseCfg = TestConfigBuilder.config();
		final List<Config> slices = List.of(new BasicConfig(baseCfg));

		final String data = "alpha\n";
		final CsvItemInput<Item> itemInput = new CsvItemInput<>(
						new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)), new ItemFactoryImpl<>());

		ItemInputFileSlicer.setObjectOutputStreamFactoryForTesting(out -> {
			throw new IOException("stream header failure");
		});
		try {
			final ItemInputFileSlicer slicer = new ItemInputFileSlicer(
							"test-step-stream-failure", List.of(mockMgr), slices, itemInput, 1);
			assertEquals(tmp.toString(), slices.get(0).stringVal("item-input-file"));
			verify(mockMgr, never()).writeToFile(anyString(), any(byte[].class));
			slicer.close();
		} finally {
			ItemInputFileSlicer.resetObjectOutputStreamFactoryForTesting();
		}
	}
}
