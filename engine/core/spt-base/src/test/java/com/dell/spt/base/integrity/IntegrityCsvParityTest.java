package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntegrityCsvParityTest {

	@TempDir
	Path tempDir;

	@Test
	void sharedWritersAndParsersPreserveExactIdentity() throws Exception {
		final Path fixture = sharedIntegrityFixture("canonical-v1", "writer-cases.json");
		final List<Map<String, Object>> cases = new ObjectMapper().readValue(
						fixture.toFile(), new TypeReference<>() {});
		for (final Map<String, Object> test : cases) {
			final String name = (String) test.get("name");
			final byte[] expected = Base64.getDecoder().decode((String) test.get("base64"));
			final Path manifest = tempDir.resolve(name + ".csv");
			Files.write(manifest, expected);
			if (!test.containsKey("fields")) {
				assertThrows(IOException.class, () -> IntegrityManifestValidator.validate(manifest), name);
				continue;
			}
			final List<String> fields = new ObjectMapper().convertValue(
							test.get("fields"), new TypeReference<>() {});

			final ByteArrayOutputStream output = new ByteArrayOutputStream();
			try (OutputStreamWriter text = new OutputStreamWriter(output, StandardCharsets.UTF_8);
							CSVPrinter writer = new CSVPrinter(text, IntegrityCsvFormat.RFC4180_LF)) {
				writer.printRecord(IntegrityManifestItemInput.HEADER);
				writer.printRecord(fields);
			}
			assertArrayEquals(expected, output.toByteArray(), name + " writer");

			IntegrityManifestValidator.validate(manifest);

			try (InputStreamReader text = new InputStreamReader(
							new ByteArrayInputStream(expected), StandardCharsets.UTF_8);
							CSVParser parser = IntegrityCsvFormat.RFC4180_LF.parse(text)) {
				final var records = parser.getRecords();
				assertEquals(2, records.size(), name + " record count");
				assertEquals(IntegrityManifestItemInput.HEADER, records.get(0).toList(), name + " header");
				assertEquals(fields, records.get(1).toList(), name + " fields");
			}
		}
	}

	private static Path sharedIntegrityFixture(final String... parts) {
		Path cursor = Path.of("").toAbsolutePath();
		while (cursor != null) {
			Path candidate = cursor.resolve(Path.of("testdata", "integrity"));
			for (final String part : parts) {
				candidate = candidate.resolve(part);
			}
			if (Files.exists(candidate)) {
				return candidate;
			}
			cursor = cursor.getParent();
		}
		throw new AssertionError(
						"shared integrity fixture not found: " + String.join("/", parts));
	}
}
