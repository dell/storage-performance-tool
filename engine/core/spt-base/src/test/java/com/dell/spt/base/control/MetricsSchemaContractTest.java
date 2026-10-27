package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
	private static final Path SCHEMA_V4_PATH = DOC_DIR.resolve("metrics-schema-v4.json");
	private static final Path SCHEMA_V5_PATH = DOC_DIR.resolve("metrics-schema-v5.json");
	private static final List<Path> PAYLOAD_V4_PATHS = List.of(
					DOC_DIR.resolve("examples/metrics-v4-idle.json"),
					DOC_DIR.resolve("examples/metrics-v4-node-read.json"),
					DOC_DIR.resolve("examples/metrics-v4-cluster-read.json"),
					DOC_DIR.resolve("examples/metrics-v4-node-delete.json"),
					DOC_DIR.resolve("examples/metrics-v4-cluster-delete.json"),
					DOC_DIR.resolve("examples/metrics-v4-list-shards.json"),
					Path.of("src", "test", "resources", "delete-metrics-v4-cross-view.json"),
					Path.of("src", "test", "resources", "delete-metrics-v4-strict-pre-abort.json"));
	private static final List<Path> PAYLOAD_V5_PATHS = List.of(
					DOC_DIR.resolve("examples/metrics-v5-idle.json"),
					DOC_DIR.resolve("examples/metrics-v5-node-read.json"),
					DOC_DIR.resolve("examples/metrics-v5-cluster-read.json"),
					DOC_DIR.resolve("examples/metrics-v5-node-delete.json"),
					DOC_DIR.resolve("examples/metrics-v5-cluster-delete.json"),
					DOC_DIR.resolve("examples/metrics-v5-list-shards.json"),
					DOC_DIR.resolve("examples/metrics-v5-fleet-rdma-read.json"));
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	@Test
	void publishedSchemaAcceptsAllSchema4ExamplesAndFixtures() throws Exception {
		assertAllValid(loadSchema(SCHEMA_V4_PATH), PAYLOAD_V4_PATHS);
	}

	@Test
	void publishedSchemaAcceptsAllSchema5Examples() throws Exception {
		assertAllValid(loadSchema(SCHEMA_V5_PATH), PAYLOAD_V5_PATHS);
	}

	@Test
	void publishedSchemaRejectsAnotherSchemaVersion() throws Exception {
		final JsonNode v4 = OBJECT_MAPPER.readTree(PAYLOAD_V4_PATHS.get(0).toFile());
		((ObjectNode) v4.get(0)).put("metrics_schema", 5);
		assertFalse(loadSchema(SCHEMA_V4_PATH).validate(v4).isEmpty());
		final JsonNode v5 = OBJECT_MAPPER.readTree(PAYLOAD_V5_PATHS.get(0).toFile());
		((ObjectNode) v5.get(0)).put("metrics_schema", 4);
		assertFalse(loadSchema(SCHEMA_V5_PATH).validate(v5).isEmpty());
	}

	@Test
	void schema5RejectsSteadyStatisticsWithoutAStatusThatReportsThem() throws Exception {
		final JsonSchema schema = loadSchema(SCHEMA_V5_PATH);
		final JsonNode payload = OBJECT_MAPPER.readTree(DOC_DIR.resolve("examples/metrics-v5-node-read.json").toFile());
		final ObjectNode steady = (ObjectNode) payload.get(0).get("steady");
		steady.put("status", "not-reached");
		assertFalse(schema.validate(payload).isEmpty(), "a not-reached window reports no statistics");
		steady.put("status", "short");
		steady.remove("operations");
		assertFalse(schema.validate(payload).isEmpty(), "a short window reports its statistics");
	}

	@Test
	void schema5RejectsMalformedDriverStatistics() throws Exception {
		final JsonSchema schema = loadSchema(SCHEMA_V5_PATH);
		final JsonNode payload = OBJECT_MAPPER.readTree(
						DOC_DIR.resolve("examples/metrics-v5-fleet-rdma-read.json").toFile());
		final ObjectNode row = (ObjectNode) payload.get(0);
		((ObjectNode) row.get("driver_stats").get("counters")).put("rdma_transferred", -1);
		assertFalse(schema.validate(payload).isEmpty(), "a counter is never negative");
		final ObjectNode differentDrivers = OBJECT_MAPPER.createObjectNode();
		differentDrivers.put("partial", true);
		differentDrivers.putArray("drivers").add("s3").add("s3-rdma");
		row.set("driver_stats", differentDrivers);
		assertTrue(schema.validate(payload).isEmpty(), () -> "different drivers: " + schema.validate(payload));
		differentDrivers.put("partial", false);
		assertFalse(schema.validate(payload).isEmpty(), "a total without sums is partial");
	}

	private static void assertAllValid(final JsonSchema schema, final List<Path> payloadPaths) throws Exception {
		for (final Path payloadPath : payloadPaths) {
			final JsonNode payload = OBJECT_MAPPER.readTree(payloadPath.toFile());
			final Set<ValidationMessage> errors = schema.validate(payload);
			assertTrue(errors.isEmpty(), () -> payloadPath + ": " + errors);
		}
	}

	private static JsonSchema loadSchema(final Path schemaPath) throws Exception {
		final JsonNode schemaNode = OBJECT_MAPPER.readTree(Files.readString(schemaPath));
		final JsonSchema schema = JsonSchemaFactory.getInstance(VersionFlag.V202012).getSchema(schemaNode);
		schema.initializeValidators();
		return schema;
	}
}
