package com.dell.spt.base.config;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A YAML or JSON defaults document's number or boolean for a URI argument, request header or object
 * tag reaches the driver as text, and one for a text setting every run reads is refused with the
 * setting's name. Every other value keeps the type the reader gave it, as before.
 */
public class DocumentValueTypesTest {

	@TempDir
	Path dir;

	/** The base schema with the HTTP and S3 settings the extensions add. */
	public static Map<String, Object> schema() throws Exception {
		final var base = SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow();
		final Map<String, Object> extension = Map.of("storage", Map.of(
						"region", String.class,
						"net", Map.of("http", Map.of("headers", Map.class, "uri", Map.of("args", Map.class))),
						"object", Map.of("tagging", Map.of("enabled", boolean.class, "tags", Map.class))));
		return TreeUtil.addBranches(base, extension);
	}

	private static Config yaml(final String document) throws Exception {
		return ConfigUtil.loadConfig(document, ConfigFormat.YAML, schema());
	}

	private static Config json(final String document) throws Exception {
		return ConfigUtil.loadConfig(document, ConfigFormat.JSON, schema());
	}

	private static String uriArgs(final String value) {
		return "storage:\n  net:\n    http:\n      uri:\n        args:\n          tid: " + value + "\n";
	}

	private static String headers(final String value) {
		return "storage:\n  net:\n    http:\n      headers:\n        X-Id: " + value + "\n";
	}

	private static String tags(final String value) {
		return "storage:\n  object:\n    tagging:\n      tags:\n        k: " + value + "\n";
	}

	private static Object uriArg(final Config config) {
		return config.mapVal("storage-net-http-uri-args").get("tid");
	}

	private static Object header(final Config config) {
		return config.mapVal("storage-net-http-headers").get("X-Id");
	}

	private static Object tag(final Config config) {
		return config.mapVal("storage-object-tagging-tags").get("k");
	}

	@ParameterizedTest
	@ValueSource(strings = {"7", "0012", "007", "+7", "1_000", "0x1F", "1e3", "7.0", "7.5", "12345678901234567890",
			"true", "True", "yes", "on", "false", "off"
	})
	void aYamlNumberOrBooleanUriArgumentOrHeaderIsItsTextAsWritten(final String value) throws Exception {
		assertEquals(value, uriArg(yaml(uriArgs(value))));
		assertEquals(value, header(yaml(headers(value))));
	}

	@ParameterizedTest
	@ValueSource(strings = {"7", "7.0", "1e3", "1.50", "-0", "true", "false", "12345678901234567890"
	})
	void aJsonNumberOrBooleanUriArgumentOrHeaderIsItsTextAsWritten(final String value) throws Exception {
		assertEquals(value, uriArg(json("{\"storage\": {\"net\": {\"http\": {\"uri\": {\"args\": {\"tid\": " + value + "}}}}}}")));
		assertEquals(value, header(json("{\"storage\": {\"net\": {\"http\": {\"headers\": {\"X-Id\": " + value + "}}}}}")));
	}

	@ParameterizedTest
	@CsvSource({"7, 7", "007, 7", "True, true", "1e3, 1000.0", "7.0, 7.0", "12345678901234567890, 12345678901234567890"
	})
	void aYamlNumberOrBooleanTagIsItsCanonicalText(final String value, final String text) throws Exception {
		assertEquals(text, tag(yaml(tags(value))));
	}

	@ParameterizedTest
	@CsvSource({"7, 7", "1e3, 1000.0", "true, true", "12345678901234567890, 12345678901234567890"
	})
	void aJsonNumberOrBooleanTagIsItsCanonicalText(final String value, final String text) throws Exception {
		assertEquals(text, tag(json("{\"storage\": {\"object\": {\"tagging\": {\"tags\": {\"k\": " + value + "}}}}}")));
	}

	/** The forms that run today keep their values. */
	@ParameterizedTest
	@CsvSource(delimiter = '|', value = {
			"2026-11-15|2026-11-15",
			"2026-11-15T10:00:00Z|2026-11-15T10:00:00Z",
			"0o17|0o17",
			"1:30|1:30",
			"08|08",
			"x7|x7",
			"a b|a b",
			"'\"7\"'|7",
			"'\"0012\"'|0012",
			"!x|''",
			"a #b|a",
			"bytes=0-5|bytes=0-5",
			"'\"\"'|''"
	})
	void aTextUriArgumentOrHeaderIsUnchanged(final String value, final String text) throws Exception {
		assertEquals(text, uriArg(yaml(uriArgs(value))));
		assertEquals(text, header(yaml(headers(value))));
	}

	@ParameterizedTest
	@ValueSource(strings = {"~", "null", "", "#x", "&x"
	})
	void aUriArgumentWithNoValueIsKept(final String value) throws Exception {
		final var args = yaml(uriArgs(value)).mapVal("storage-net-http-uri-args");
		assertTrue(args.containsKey("tid"));
		assertNull(args.get("tid"));
	}

	@ParameterizedTest
	@ValueSource(strings = {"~", "null", ""
	})
	void aTagWithNoValueIsKept(final String value) throws Exception {
		final var tags = yaml(tags(value)).mapVal("storage-object-tagging-tags");
		assertTrue(tags.containsKey("k"));
		assertNull(tags.get("k"));
	}

	/**
	 * A header with no value, and a list or map URI argument or header, load as before: {@code s3-aws}
	 * and {@code fs} do not read these maps, and the Netty HTTP drivers refuse them where they do.
	 */
	@ParameterizedTest
	@ValueSource(strings = {"~", "null", ""
	})
	void aHeaderWithNoValueIsKept(final String value) throws Exception {
		final var headers = yaml(headers(value)).mapVal("storage-net-http-headers");
		assertTrue(headers.containsKey("X-Id"));
		assertNull(headers.get("X-Id"));
	}

	@Test
	void aListOrMapUriArgumentOrHeaderIsKept() throws Exception {
		assertEquals(List.of(1, 2), header(yaml(headers("[1, 2]"))));
		assertEquals(List.of(1, 2), uriArg(yaml(uriArgs("[1, 2]"))));
		assertEquals(Map.of("a", 1), uriArg(yaml(uriArgs("{a: 1}"))));
		assertEquals(List.of(7), uriArg(json("{\"storage\": {\"net\": {\"http\": {\"uri\": {\"args\": {\"tid\": [7]}}}}}}")));
	}

	/** The {@code s3-aws} driver sends a list or map tag as its text, so the configuration keeps it. */
	@Test
	void aListOrMapTagIsKept() throws Exception {
		assertEquals(List.of(1, 2), tag(yaml(tags("[1, 2]"))));
		assertEquals(Map.of("a", "b"), tag(yaml(tags("{a: b}"))));
		assertEquals(List.of(7),
						tag(json("{\"storage\": {\"object\": {\"tagging\": {\"tags\": {\"k\": [7]}}}}}")));
	}

	@ParameterizedTest
	@CsvSource({
			"load.step.id, 7, number",
			"load.step.id, 1234567890123456789012, number",
			"run.comment, 2026, number",
			"storage.integrity.mode, 1, number",
			"storage.integrity.algorithm, 256, number",
			"storage.integrity.input.provenance, 1.5, number",
			"storage.integrity.input.expectedProducerId, 12345, number",
			"run.comment, yes, boolean",
			"storage.integrity.mode, false, boolean"
	})
	void aNumberOrBooleanAtATextSettingIsRefusedWithItsName(final String setting, final String value, final String kind) {
		final var keys = setting.split("\\.");
		final var document = new StringBuilder();
		for (int i = 0; i < keys.length; i++) {
			document.append("  ".repeat(i)).append(keys[i]).append(':').append(i == keys.length - 1 ? " " + value : "").append('\n');
		}
		final var path = String.join("-", keys);
		final var failure = assertThrows(InvalidTextValueException.class, () -> yaml(document.toString()));
		assertEquals(path + ": the value is read as a " + kind + ", not text; quote it", failure.getMessage());
		assertEquals(path, failure.path());
		assertFalse(failure.getMessage().contains(value), "the message must not quote the value");
	}

	@Test
	void aNumberAtATextSettingIsRefusedInJson() {
		final var failure = assertThrows(InvalidTextValueException.class,
						() -> json("{\"run\": {\"comment\": 12345}}"));
		assertEquals("run-comment: the value is read as a number, not text; quote it", failure.getMessage());
	}

	@Test
	void aQuotedNumberAtATextSettingIsText() throws Exception {
		assertEquals("12345", yaml("load:\n  step:\n    id: \"12345\"\n").stringVal("load-step-id"));
		assertEquals("0012", json("{\"run\": {\"comment\": \"0012\"}}").stringVal("run-comment"));
	}

	/**
	 * A number at any other text setting loads as before: its reader ignores it and falls back, a
	 * scenario can set the setting for each step, or the driver or step in use does not read it.
	 */
	@ParameterizedTest
	@CsvSource({
			"run.cluster.id, 123",
			"log.level, 7",
			"storage.auth.uid, 12345",
			"storage.region, 1",
			"item.output.path, 2026",
			"item.naming.prefix, 1.5",
			"load.op.type, 7",
			"load.op.list.sharding.charset, 1"
	})
	void aNumberAtAnotherTextSettingLoadsAsBefore(final String setting, final String value) throws Exception {
		final var keys = setting.split("\\.");
		final var document = new StringBuilder();
		for (int i = 0; i < keys.length; i++) {
			document.append("  ".repeat(i)).append(keys[i]).append(':').append(i == keys.length - 1 ? " " + value : "").append('\n');
		}
		assertTrue(yaml(document.toString()).val(String.join("-", keys)) instanceof Number);
	}

	/** {@code RunIdentityConfig} ignores a number for the cluster id, so the run goes on without one. */
	@Test
	void aNumberClusterIdLoadsAndIsIgnoredAsBefore() throws Exception {
		assertNull(RunIdentityConfig.clusterId(yaml("run:\n  cluster:\n    id: 123\n")));
	}

	/** A boolean or a big integer at any other text setting is refused by the schema's own check, as before. */
	@ParameterizedTest
	@ValueSource(strings = {"run:\n  cluster:\n    id: true\n", "storage:\n  auth:\n    secret: 1234567890123456789012\n"
	})
	void aBooleanOrBigIntegerAtAnotherTextSettingFailsAsBefore(final String document) {
		final var failure = assertThrows(RuntimeException.class, () -> yaml(document));
		assertFalse(failure instanceof InvalidTextValueException, failure.toString());
	}

	/** {@code RangeReadConfig} reads a number for these text settings. */
	@Test
	void aNumberAtARangeReadSettingIsKept() throws Exception {
		final var config = yaml("load:\n  op:\n    read:\n      range:\n        size: 4096\n        offset: 0\n        align: 512\n");
		assertEquals(4096, config.val("load-op-read-range-size"));
		assertEquals(0, config.val("load-op-read-range-offset"));
		assertEquals(512, config.val("load-op-read-range-align"));
	}

	@Test
	void typedAndAnySettingsKeepTheirValues() throws Exception {
		final var config = yaml("load:\n  op:\n    limit:\n      count: 7\nitem:\n  data:\n    size: 1024\n"
						+ "    ranges:\n      threshold: 0\n");
		assertEquals(7L, config.longVal("load-op-limit-count"));
		assertEquals(1024, config.val("item-data-size"));
		assertEquals(0, config.val("item-data-ranges-threshold"));
	}

	@Test
	void aDefaultsFileIsSettledTheSameWay() throws Exception {
		final var file = dir.resolve("defaults.yaml");
		Files.writeString(file, "---\n" + uriArgs("0012") + "      headers:\n        X-Id: 7\n");
		final var config = ConfigUtil.loadConfig(file.toFile(), schema());
		assertEquals("0012", uriArg(config));
		assertEquals("7", header(config));
		final var jsonFile = dir.resolve("defaults.json");
		Files.writeString(jsonFile, "{\"storage\": {\"net\": {\"http\": {\"uri\": {\"args\": {\"tid\": 7.0}}}}}}");
		assertEquals("7.0", uriArg(ConfigUtil.loadConfig(jsonFile.toFile(), schema())));
		final var refused = dir.resolve("refused.yaml");
		Files.writeString(refused, "load:\n  step:\n    id: 12345\n");
		assertThrows(InvalidTextValueException.class, () -> ConfigUtil.loadConfig(refused.toFile(), schema()));
	}

	/** Only the first YAML document is the configuration, as the tree reader reads it. */
	@Test
	void aLaterYamlDocumentDoesNotLendItsText() throws Exception {
		final var config = yaml(uriArgs("7") + "---\n" + uriArgs("0007"));
		assertEquals("7", uriArg(config));
	}

	@Test
	void eachEntryTakesItsOwnText() throws Exception {
		final var config = yaml("storage:\n  net:\n    http:\n      headers:\n        tid: 007\n      uri:\n        args:\n"
						+ "          tid: 0012\n          n: 7\n  object:\n    tagging:\n      tags:\n        tid: 0012\n");
		assertEquals("007", config.mapVal("storage-net-http-headers").get("tid"));
		assertEquals("0012", uriArg(config));
		assertEquals("7", config.mapVal("storage-net-http-uri-args").get("n"));
		assertEquals("10", config.mapVal("storage-object-tagging-tags").get("tid"));
	}
}
