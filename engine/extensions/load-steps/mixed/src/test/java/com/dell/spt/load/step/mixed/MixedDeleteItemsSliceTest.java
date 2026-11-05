package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.load.step.client.ItemInputFileSlicer;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Each engine deletes its own share of a DELETE queue file, so no row is deleted twice. */
class MixedDeleteItemsSliceTest {

	private static final int ENGINES = 2;
	private static final int BATCH = 2;

	@TempDir
	Path dir;

	/** What each engine's configuration was told, as its slice's setting. */
	private final Map<Integer, String> sliceSettings = new HashMap<>();

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	/** A file manager writing where an engine would read: the slices are real files. */
	private List<FileManager> fileManagers() throws IOException {
		final List<FileManager> managers = new ArrayList<>();
		for (int i = 0; i < ENGINES; i++) {
			final FileManager manager = mock(FileManager.class);
			when(manager.newTmpFileName()).thenReturn(dir.resolve("slice-" + i).toString());
			doAnswer(call -> {
				Files.write(
								Path.of((String) call.getArgument(0)),
								(byte[]) call.getArgument(1),
								StandardOpenOption.CREATE,
								StandardOpenOption.APPEND);
				return null;
			}).when(manager).writeToFile(anyString(), any());
			doAnswer(call -> {
				Files.deleteIfExists(Path.of((String) call.getArgument(0)));
				return null;
			}).when(manager).deleteFile(anyString());
			managers.add(manager);
		}
		return managers;
	}

	private List<Config> configSlices() {
		final List<Config> slices = new ArrayList<>();
		for (int i = 0; i < ENGINES; i++) {
			final int slice = i;
			final Config config = mock(Config.class);
			doAnswer(call -> {
				if (MixedLoadStepLocal.DELETE_ITEMS_FILE.equals(call.getArgument(0))) {
					sliceSettings.put(slice, call.getArgument(1));
				}
				return config;
			}).when(config).val(anyString(), any());
			slices.add(config);
		}
		return slices;
	}

	private ItemInputFileSlicer slice(final String content) throws Exception {
		return slice(dir.resolve("delete-items.csv"), content);
	}

	private ItemInputFileSlicer slice(final Path file, final String content) throws Exception {
		Files.writeString(file, content);
		return MixedLoadStepClient.sliceDeleteItems(
						"step", file.toString(), new DataItemFactoryImpl<>(), fileManagers(), configSlices(), BATCH);
	}

	/** What an engine's DELETE queue holds after it reads its slice, as a whole run would. */
	private List<String> deleteQueue(final int engine) {
		final PoolItemInput<Item> pool = new PoolItemInput<>(List.of((Item) new DataItemImpl("/b/seed", 0, 1)));
		MixedLoadStepLocal.loadDeleteItems(sliceSettings.get(engine), pool, new DataItemFactoryImpl<>());
		final List<String> names = new ArrayList<>();
		Item item;
		while ((item = pool.pollDelete()) != null) {
			names.add(item.name());
		}
		return names;
	}

	private String sliceFailure(final Path file, final String content) {
		final var failure = assertThrows(RunTerminalException.class, () -> slice(file, content));
		assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
		return failure.getMessage();
	}

	@Test
	void eachRowGoesToOneEngine() throws Exception {
		try (final var slicer = slice("/b/a,0,10,0/0\n/b/b,0,10,0/0\n/b/c,0,10,0/0\n")) {
			assertEquals(3, slicer.distributedItems());
			assertEquals(List.of("/b/a", "/b/c"), deleteQueue(0));
			assertEquals(List.of("/b/b"), deleteQueue(1));
		}
	}

	@Test
	void theConfigurationBeingDistributedNamesTheFile() throws Exception {
		final Path file = dir.resolve("appended.csv");
		Files.writeString(file, "/b/appended,0,10,0/0\n/b/other,0,10,0/0\n");
		final Config distributed = mock(Config.class);
		when(distributed.stringVal(MixedLoadStepLocal.DELETE_ITEMS_FILE)).thenReturn(file.toString());
		when(distributed.stringVal("item-type")).thenReturn("data");
		final var client = new MixedLoadStepClient(
						MixedLoadStepClientTest.newBaseConfig(), List.of(), List.of(), null);

		final var slicers = client.sliceOwnItemFiles(distributed, fileManagers(), configSlices(), BATCH);

		try {
			assertEquals(List.of("/b/appended"), deleteQueue(0));
			assertEquals(List.of("/b/other"), deleteQueue(1));
		} finally {
			for (final var slicer : slicers) {
				slicer.close();
			}
		}
	}

	@Test
	void anInterruptWhileSlicingStaysAnInterrupt() throws Exception {
		final Path file = dir.resolve("delete-items.csv");
		Files.writeString(file, "/b/a,0,10,0/0\n/b/b,0,10,0/0\n");
		final List<FileManager> managers = fileManagers();
		for (final FileManager manager : managers) {
			doAnswer(call -> {
				throw new InterruptedException();
			}).when(manager).writeToFile(anyString(), any());
		}

		assertThrows(
						InterruptedException.class,
						() -> MixedLoadStepClient.sliceDeleteItems(
										"step", file.toString(), new DataItemFactoryImpl<>(), managers, configSlices(), BATCH));
	}

	@Test
	void aManifestRowKeepsItsBucketAndVersionInItsSlice() throws Exception {
		final var manifest = "bucket,key,size,version_id\nb1,p/k1,10,\nb2,k2,20,v1\n";
		try (final var slicer = slice(manifest)) {
			assertEquals(2, slicer.distributedItems());
			final PoolItemInput<Item> pool = new PoolItemInput<>(List.of((Item) new DataItemImpl("/b/seed", 0, 1)));
			MixedLoadStepLocal.loadDeleteItems(sliceSettings.get(1), pool, new DataItemFactoryImpl<>());
			final var item = (com.dell.spt.base.item.IntegrityManifestDataItem) pool.pollDelete();
			assertEquals("b2", item.bucket());
			assertEquals("k2", item.name());
			assertEquals("v1", item.versionId());
		}
	}

	@Test
	void anEngineWithNoRowsHasAnEmptyQueue() throws Exception {
		try (final var slicer = slice("/b/a,0,10,0/0\n")) {
			assertEquals(1, slicer.distributedItems());
			assertEquals(List.of("/b/a"), deleteQueue(0));
			assertEquals(List.of(), deleteQueue(1));
		}
	}

	@Test
	void aFileWithNoRowsFailsAndLeavesNoSlices() throws Exception {
		assertEquals(
						"MixedLoad: delete items file has no items", sliceFailure(dir.resolve("delete-items.csv"), ""));
		assertTrue(Files.notExists(dir.resolve("slice-0")));
		assertTrue(Files.notExists(dir.resolve("slice-1")));
	}

	@Test
	void aMissingFileFails() {
		final var failure = assertThrows(
						RunTerminalException.class,
						() -> MixedLoadStepClient.sliceDeleteItems(
										"step",
										dir.resolve("none.csv").toString(),
										new DataItemFactoryImpl<>(),
										fileManagers(),
										configSlices(),
										BATCH));
		assertEquals("MixedLoad: delete items file was not found", failure.getMessage());
	}

	@Test
	void aBadRowFailsTheRunAndNamesItsLine() {
		final var message = sliceFailure(dir.resolve("delete-items.csv"), "/b/a,0,10,0/0\n\n/b/b,0,10,0/0\n");
		assertEquals("items file \"delete-items.csv\" line 2 is blank", message);
	}

	/** A file named as a whole keeps the check that a slice can't: an empty .csv is still a mistake. */
	@Test
	void aWholeCsvFileWithNoRowsStillFailsOnASingleEngine() throws Exception {
		final var file = Files.writeString(dir.resolve("whole.csv"), "");
		final var pool = new PoolItemInput<DataItem>(List.of(new DataItemImpl("/b/seed", 0, 1)));
		final var failure = assertThrows(
						RunTerminalException.class,
						() -> MixedLoadStepLocal.loadDeleteItems(file.toString(), pool, new DataItemFactoryImpl<>()));
		assertEquals("MixedLoad: delete items file has no items", failure.getMessage());
	}
}
