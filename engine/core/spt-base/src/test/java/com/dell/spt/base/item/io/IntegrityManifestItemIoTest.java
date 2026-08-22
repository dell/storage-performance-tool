package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntegrityManifestItemIoTest {

	@TempDir
	Path tempDir;

	@Test
	void writerAndReaderRoundTripQuotedMultilineKeys() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		final var expected = new IntegrityManifestDataItem(
						"bucket", "prefix/a,quoted\" key\nnext-line", 42, "v,1");
		try (final var output = new IntegrityManifestItemOutput(manifest)) {
			assertTrue(output.put(expected));
		}

		assertTrue(IntegrityManifestItemInput.hasCanonicalHeader(manifest));
		try (final var input = new IntegrityManifestItemInput(manifest)) {
			final var actual = input.get();
			assertEquals(expected.bucket(), actual.bucket());
			assertEquals(expected.name(), actual.name());
			assertEquals(expected.size(), actual.size());
			assertEquals(expected.versionId(), actual.versionId());
		}
	}

	@Test
	void factorySelectsOnlyTheExactCanonicalHeader() throws Exception {
		final Path canonical = tempDir.resolve("canonical.csv");
		Files.writeString(
						canonical,
						"bucket,key,size,version_id\r\nb,k,1,\r\n",
						StandardCharsets.UTF_8);
		assertInstanceOf(
						IntegrityManifestItemInput.class,
						ItemInputFactory.createFileItemInput(new DataItemFactoryImpl<DataItemImpl>(), canonical.toString()));

		final Path legacy = tempDir.resolve("legacy.csv");
		Files.writeString(legacy, "object,0,1,0/0\n", StandardCharsets.UTF_8);
		assertInstanceOf(
						CsvFileItemInput.class,
						ItemInputFactory.createFileItemInput(new DataItemFactoryImpl<DataItemImpl>(), legacy.toString()));

		final Path almost = tempDir.resolve("almost.csv");
		Files.writeString(almost, "bucket,key,size,versionId\n", StandardCharsets.UTF_8);
		assertInstanceOf(
						CsvFileItemInput.class,
						ItemInputFactory.createFileItemInput(new DataItemFactoryImpl<DataItemImpl>(), almost.toString()));
	}

	@Test
	void readerReportsExactRemainingCountWithoutConsumingRows() throws Exception {
		final Path manifest = tempDir.resolve("counted.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\nb,a,1,\r\nb,b,2,\r\n",
						StandardCharsets.UTF_8);
		try (final var input = new IntegrityManifestItemInput(manifest)) {
			assertEquals(2, input.remainingItemCount());
			input.get();
			assertEquals(1, input.remainingItemCount());
			input.reset();
			assertEquals(2, input.remainingItemCount());
			assertEquals(1, input.skip(1));
			assertEquals(1, input.remainingItemCount());
		}
	}

	@Test
	void validatesEveryManifestRowBeforeExposingItsFrozenCount() throws Exception {
		final Path manifest = tempDir.resolve("bad.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\nbucket,valid,1,\r\nbucket,key,-1,\r\n",
						StandardCharsets.UTF_8);

		assertThrows(IOException.class, () -> new IntegrityManifestItemInput(manifest));
	}
}
