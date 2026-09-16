package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion.VersionFlag;
import com.networknt.schema.ValidationMessage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MetricsSchemaContractTest {

	private static final Path DOC_DIR = Path.of("doc", "usage", "api", "remote");
	private static final Path SCHEMA_PATH = DOC_DIR.resolve("metrics-schema-v4.json");
	private static final List<Path> PAYLOAD_PATHS = List.of(
					DOC_DIR.resolve("examples/metrics-v4-idle.json"),
					DOC_DIR.resolve("examples/metrics-v4-node-read.json"),
					DOC_DIR.resolve("examples/metrics-v4-cluster-read.json"),
					DOC_DIR.resolve("examples/metrics-v4-node-delete.json"),
					DOC_DIR.resolve("examples/metrics-v4-cluster-delete.json"),
					DOC_DIR.resolve("examples/metrics-v4-list-shards.json"),
					Path.of("src", "test", "resources", "delete-metrics-v4-cross-view.json"),
					Path.of("src", "test", "resources", "delete-metrics-v4-strict-pre-abort.json"));
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Test
	void publishedSchemaAcceptsAllSchema4ExamplesAndFixtures() throws Exception {
		final JsonSchema schema = loadSchema();
		for (final Path payloadPath : PAYLOAD_PATHS) {
			final JsonNode payload = OBJECT_MAPPER.readTree(payloadPath.toFile());
			final Set<ValidationMessage> errors = schema.validate(payload);
			assertTrue(errors.isEmpty(), () -> payloadPath + ": " + errors);
		}
	}

	@Test
	void publishedSchemaRejectsAnotherSchemaVersion() throws Exception {
		final JsonSchema schema = loadSchema();
		final JsonNode payload = OBJECT_MAPPER.readTree(PAYLOAD_PATHS.get(0).toFile());
		((com.fasterxml.jackson.databind.node.ObjectNode) payload.get(0)).put("metrics_schema", 5);

		assertFalse(schema.validate(payload).isEmpty());
	}

	private static JsonSchema loadSchema() throws Exception {
		final JsonNode schemaNode = OBJECT_MAPPER.readTree(Files.readString(SCHEMA_PATH));
		final JsonSchema schema = JsonSchemaFactory.getInstance(VersionFlag.V202012).getSchema(schemaNode);
		schema.initializeValidators();
		return schema;
	}
}
