package com.dell.spt.base.env;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.DIR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.InvalidBooleanValueException;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** An extension's defaults file with a boolean that is not true or false stops the start. */
class ExtensionDefaultsBooleanTest {

	private static final String SCHEMA = "storage:\n  net:\n    http:\n      read:\n        metadata:\n          only: boolean\n";

	private static final String DEFAULTS_FILE = "defaults-boolean-test.yaml";

	@TempDir
	Path appHome;

	private static final class BooleanDefaultsExtension extends ExtensionBase {

		@Override
		public String id() {
			return "boolean-defaults-test";
		}

		@Override
		public SchemaProvider schemaProvider() {
			return new YamlSchemaProviderBase() {

				@Override
				protected InputStream schemaInputStream() {
					return new ByteArrayInputStream(SCHEMA.getBytes(StandardCharsets.UTF_8));
				}

				@Override
				public String id() {
					return APP_NAME;
				}
			};
		}

		@Override
		protected String defaultsFileName() {
			return DEFAULTS_FILE;
		}

		@Override
		protected List<String> resourceFilesToInstall() {
			return List.of();
		}
	}

	private void writeDefaults(final String only) throws Exception {
		final var configDir = Files.createDirectories(appHome.resolve(DIR_CONFIG));
		Files.writeString(configDir.resolve(DEFAULTS_FILE),
						"storage:\n  net:\n    http:\n      read:\n        metadata:\n          only: " + only + "\n");
	}

	@Test
	void aNonBooleanStringIsRefusedRatherThanDroppingTheFile() throws Exception {
		writeDefaults("\"yes\"");
		final var failure = assertThrows(InvalidBooleanValueException.class,
						() -> new BooleanDefaultsExtension().defaults(appHome));
		assertEquals("storage-net-http-read-metadata-only: \"yes\" is not true or false", failure.getMessage());
	}

	@Test
	void aBooleanLoads() throws Exception {
		writeDefaults("true");
		final var config = new BooleanDefaultsExtension().defaults(appHome);
		assertTrue(config.boolVal("storage-net-http-read-metadata-only"));
	}

	@Test
	void anUnreadableFileIsStillDroppedWithAWarning() throws Exception {
		writeDefaults("[unclosed");
		assertNull(new BooleanDefaultsExtension().defaults(appHome));
	}
}
