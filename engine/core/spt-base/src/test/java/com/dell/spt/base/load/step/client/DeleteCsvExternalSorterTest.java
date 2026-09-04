package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.dell.spt.base.integrity.IntegrityCsvFormat;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DeleteCsvExternalSorterTest {
	@TempDir
	Path temp;

	@Test
	void sortsMoreThanTwoMemoryChunksAndRemovesIntermediateFiles() throws Exception {
		final List<String> header = List.of("id", "value");
		final Path source = temp.resolve("source.csv");
		final int rowCount = DeleteCsvExternalSorter.SORT_CHUNK_RECORDS * 2 + 17;
		try (CSVPrinter printer = new CSVPrinter(
						Files.newBufferedWriter(source, StandardCharsets.UTF_8),
						IntegrityCsvFormat.RFC4180_LF)) {
			printer.printRecord(header);
			for (int index = rowCount - 1; index >= 0; index--) {
				printer.printRecord(String.format("%08d", index), "value-" + index);
			}
		}

		final Path target = temp.resolve("target.csv");
		assertEquals(
						rowCount,
						DeleteCsvExternalSorter.sort(
										List.of(source), header, Comparator.comparing(row -> row.get(0)),
										target, temp, "bounded"));

		try (CSVParser parser = CSVFormat.RFC4180.parse(
						Files.newBufferedReader(target, StandardCharsets.UTF_8))) {
			final var rows = parser.iterator();
			assertEquals(header, rows.next().toList());
			for (int index = 0; index < rowCount; index++) {
				assertEquals(String.format("%08d", index), rows.next().get(0));
			}
			assertFalse(rows.hasNext());
		}
		try (final var paths = Files.list(temp)) {
			assertFalse(paths.anyMatch(path -> path.getFileName().toString().startsWith("bounded-")));
		}
	}
}
