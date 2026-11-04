package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CsvItemInputBadLineTest {

	private static final String GOOD = "/b/a,0,10,0/0\n";

	private static CsvItemInput<DataItem> input(final String content) {
		return new CsvItemInput<>(
						new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), new DataItemFactoryImpl<>());
	}

	@Test
	void goodLinesAreReadToTheEnd() throws Exception {
		try (var input = input(GOOD + GOOD)) {
			final List<DataItem> items = new ArrayList<>();
			assertEquals(2, input.get(items, 8));
			assertThrows(EOFException.class, () -> input.get(items, 8));
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"no-comma", "/b/x,zz,400,0/0", "/b/x,0", "/b/x,0,zz,0/0"
	})
	void aBadLineEndsTheBatchRead(final String bad) throws Exception {
		try (var input = input(GOOD + bad + "\n" + GOOD)) {
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(new ArrayList<>(), 8));
			assertEquals(IntegrityTerminalException.Category.INPUT, failure.category());
			assertEquals("items file line 2 is not a valid item", failure.getMessage());
			assertFalse(failure.getMessage().contains(bad), "the public message must not carry the line");
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"", "   "
	})
	void aBlankLineIsNamedAsBlank(final String blank) throws Exception {
		try (var input = input(GOOD + blank + "\n" + GOOD)) {
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(new ArrayList<>(), 8));
			assertEquals("items file line 2 is blank", failure.getMessage());
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"   ", "\t", ""
	})
	void aLineOfSpacesIsNotAPathItem(final String blank) throws Exception {
		final String content = "/b/a\n" + blank + "\n/b/c\n";
		try (var input = new CsvItemInput<Item>(
						new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), new ItemFactoryImpl<>())) {
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(new ArrayList<>(), 8));
			assertEquals("items file line 2 is blank", failure.getMessage());
		}
	}

	@Test
	void aBadLineAsTheFirstOfABatchIsNotTheEndOfTheInput() throws Exception {
		try (var input = input(GOOD + "\n" + GOOD)) {
			final List<DataItem> items = new ArrayList<>();
			assertEquals(1, input.get(items, 1));
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(items, 1));
			assertTrue(failure.getMessage().contains("line 2"));
		}
	}

	@Test
	void theSingleItemReadReportsABadLineRatherThanTheEndOfTheInput() throws Exception {
		try (var input = input(GOOD + "/b/x,zz,400,0/0\n" + GOOD)) {
			input.get();
			final var failure = assertThrows(RunTerminalException.class, input::get);
			assertTrue(failure.getMessage().contains("line 2"));
		}
		try (var input = input(GOOD)) {
			input.get();
			assertNull(input.get());
		}
	}

	@Test
	void aFileIsNamedWithoutItsDirectory(@TempDir final Path dir) throws Exception {
		final Path file = Files.writeString(dir.resolve("items.csv"), GOOD + "\n");
		try (var input = new CsvFileItemInput<DataItem>(file, new DataItemFactoryImpl<>())) {
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(new ArrayList<>(), 8));
			assertEquals("items file \"items.csv\" line 2 is blank", failure.getMessage());
		}
	}

	@Test
	void aFileReadAgainCountsItsLinesAgain(@TempDir final Path dir) throws Exception {
		final Path file = Files.writeString(dir.resolve("items.csv"), GOOD + "\n");
		try (var input = new CsvFileItemInput<DataItem>(file, new DataItemFactoryImpl<>())) {
			input.get();
			input.reset();
			input.get();
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(new ArrayList<>(), 8));
			assertTrue(failure.getMessage().contains("line 2 "), failure.getMessage());
		}
	}

	@Test
	void skippedLinesStillCountTowardTheLineNamed() throws Exception {
		try (var input = input(GOOD + GOOD + "\n")) {
			assertEquals(2, input.skip(2));
			final var failure = assertThrows(RunTerminalException.class, () -> input.get(new ArrayList<>(), 8));
			assertEquals("items file line 3 is blank", failure.getMessage());
		}
	}
}
