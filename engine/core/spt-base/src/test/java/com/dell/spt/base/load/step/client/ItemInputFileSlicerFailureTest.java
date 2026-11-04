package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.io.CsvItemInput;
import com.dell.spt.base.item.io.TerminalItemInputException;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.file.BinFileInput;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A slicer that can't distribute every item ends the run, and leaves no slices behind. */
class ItemInputFileSlicerFailureTest {

	private static final String GOOD = "/b/a,0,10,0/0\n";

	@TempDir
	Path dir;

	@BeforeAll
	static void loadLoggers() {
		assertNotNull(Loggers.MSG);
	}

	private static CsvItemInput<DataItem> csv(final String content) {
		return new CsvItemInput<>(
						new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), new DataItemFactoryImpl<>());
	}

	private static FileManager manager(final String slice) {
		final FileManager fileManager = mock(FileManager.class);
		try {
			when(fileManager.newTmpFileName()).thenReturn(slice);
		} catch (final IOException e) {
			throw new AssertionError(e);
		}
		return fileManager;
	}

	/** Writes the slices for real, as the file manager on an engine does. */
	private FileManager realManager(final String name) throws Exception {
		final Path slice = dir.resolve(name);
		final FileManager fileManager = manager(slice.toString());
		org.mockito.Mockito.doAnswer(call -> {
			Files.write(slice, call.<byte[]> getArgument(1), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
			return null;
		}).when(fileManager).writeToFile(anyString(), any());
		return fileManager;
	}

	private static ItemInputFileSlicer slice(
					final FileManager fileManager, final Input<? extends Item> input, final int batchSize) {
		return new ItemInputFileSlicer("step", List.of(fileManager), List.of(TestConfigBuilder.config()), input, batchSize);
	}

	@Test
	void aBlankLineEndsTheRunAndNamesTheLine() throws Exception {
		final FileManager fileManager = manager("slice-0");
		final var failure = assertThrows(RunTerminalException.class, () -> slice(fileManager, csv(GOOD + "\n" + GOOD), 8));
		assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
		assertEquals("items file line 2 is blank", failure.getMessage());
		assertEquals("step", failure.stepId());
		verify(fileManager).deleteFile("slice-0");
	}

	@Test
	void aBadLineAsTheFirstOfABatchDropsNothingSilently() throws Exception {
		final FileManager fileManager = manager("slice-0");
		final var failure = assertThrows(
						RunTerminalException.class, () -> slice(fileManager, csv(GOOD + "/b/x,zz,400,0/0\n" + GOOD), 1));
		assertEquals("items file line 2 is not a valid item", failure.getMessage());
		verify(fileManager).deleteFile("slice-0");
	}

	@Test
	@SuppressWarnings("unchecked")
	void aListingThatFailsPartWayEndsTheRunWithItsOwnMessage() throws Exception {
		final FileManager fileManager = manager("slice-0");
		final Input<Item> listing = mock(Input.class);
		when(listing.get(any(), anyInt()))
						.thenAnswer(call -> {
							call.<List<Item>> getArgument(0).add(mock(Item.class));
							return 1;
						})
						.thenAnswer(call -> {
							throw new TerminalItemInputException("S3 LIST input failed", new IOException("500 from host:9000"));
						});
		final var failure = assertThrows(RunTerminalException.class, () -> slice(fileManager, listing, 1));
		assertEquals("S3 LIST input failed", failure.getMessage());
		verify(fileManager).deleteFile("slice-0");
	}

	@Test
	@SuppressWarnings("unchecked")
	void anotherReadErrorKeepsItsDetailOutOfThePublicMessage() throws Exception {
		final FileManager fileManager = manager("slice-0");
		final Input<Item> input = mock(Input.class);
		when(input.get(any(), anyInt())).thenAnswer(call -> {
			throw new IOException("failed against host:9000?token=x");
		});
		final var failure = assertThrows(RunTerminalException.class, () -> slice(fileManager, input, 1));
		assertEquals("failed to read or distribute the items input", failure.getMessage());
		assertFalse(failure instanceof IntegrityTerminalException, "an ordinary step isn't an integrity run");
	}

	@Test
	void aSliceThatCantBeWrittenEndsTheRun() throws Exception {
		final FileManager fileManager = manager("slice-0");
		// The first write is the slice's header; the second holds its items.
		doThrow(new IOException("disk full")).when(fileManager).writeToFile(eq("slice-0"), any());
		final var failure = assertThrows(RunTerminalException.class, () -> slice(fileManager, csv(GOOD), 8));
		assertEquals("failed to read or distribute the items input", failure.getMessage());
		verify(fileManager).deleteFile("slice-0");
	}

	@Test
	void aSliceThatCantBeAllocatedEndsTheRun() throws Exception {
		final FileManager good = manager("slice-0");
		final List<FileManager> managers = new ArrayList<>();
		managers.add(good);
		managers.add(null);
		final var failure = assertThrows(
						RunTerminalException.class,
						() -> new ItemInputFileSlicer(
										"step", managers, List.of(TestConfigBuilder.config(), TestConfigBuilder.config()), csv(GOOD), 8));
		assertEquals("failed to allocate item input file for slice 1", failure.getMessage());
		verify(good).deleteFile("slice-0");
	}

	@Test
	void anEngineGivenNoItemsStillGetsASliceItCanOpen() throws Exception {
		final FileManager first = realManager("first");
		final FileManager second = realManager("second");
		try (var slicer = new ItemInputFileSlicer(
						"step",
						List.of(first, second),
						List.of(TestConfigBuilder.config(), TestConfigBuilder.config()),
						csv(GOOD),
						8)) {
			assertEquals(1, slicer.distributedItems());
			assertItems(dir.resolve("first"), 1);
			assertItems(dir.resolve("second"), 0);
		}
	}

	@Test
	void noItemsAtAllLeaveEverySliceOpenableAndCountZero() throws Exception {
		final FileManager only = realManager("only");
		try (var slicer = slice(only, csv(""), 8)) {
			assertEquals(0, slicer.distributedItems());
			assertItems(dir.resolve("only"), 0);
		}
	}

	@Test
	void everyItemIsReadBackAfterTheHeaderIsWrittenSeparately() throws Exception {
		final FileManager only = realManager("only");
		try (var slicer = slice(only, csv(GOOD + GOOD + GOOD), 2)) {
			assertEquals(3, slicer.distributedItems());
			assertItems(dir.resolve("only"), 3);
		}
	}

	private static void assertItems(final Path slice, final int expected) throws Exception {
		assertTrue(Files.exists(slice));
		final List<Object> items = new ArrayList<>();
		try (var input = new BinFileInput<>(slice)) {
			while (true) {
				try {
					final Object item = input.get();
					assertInstanceOf(DataItem.class, item);
					items.add(item);
				} catch (final Exception end) {
					assertInstanceOf(EOFException.class, end);
					break;
				}
			}
		}
		assertEquals(expected, items.size());
	}
}
