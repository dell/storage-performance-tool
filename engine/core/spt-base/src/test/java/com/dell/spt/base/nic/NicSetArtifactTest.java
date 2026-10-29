package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class NicSetArtifactTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static JsonNode report(final List<NicSetArtifact.Context> contexts) throws Exception {
		return MAPPER.readTree(NicSetArtifact.json(7, "step,1", "worker", 64, contexts));
	}

	private static List<String> names(final JsonNode array) {
		final var out = new ArrayList<String>();
		array.forEach(node -> out.add(node.asText()));
		return out;
	}

	@Test
	void oneDriversFieldsAreAtTheTopLevel() throws Exception {
		final var report = report(List.of(new NicSetArtifact.Context(0, new NicSetResources(0, 8, List.of("ens1f0", "ens1f1")))));

		assertEquals(List.of("schema_version", "engine_run_id", "step_id", "worker_id", "hardware_threads", "driver_threads",
						"event_loops_per_nic", "nics", "event_loops"), fieldNames(report));
		assertEquals(1, report.get("schema_version").asInt());
		assertEquals(7, report.get("engine_run_id").asLong());
		assertEquals("step,1", report.get("step_id").asText());
		assertEquals("worker", report.get("worker_id").asText());
		assertEquals(64, report.get("hardware_threads").asInt());
		assertEquals(0, report.get("driver_threads").asInt());
		assertEquals(8, report.get("event_loops_per_nic").asInt());
		assertEquals(List.of("ens1f0", "ens1f1"), names(report.get("nics")));
		assertEquals(16, report.get("event_loops").asInt());
	}

	@Test
	void aNicSetThatNeverOpenedHasNoNicsAndNoEventLoops() throws Exception {
		final var report = report(List.of(new NicSetArtifact.Context(0, new NicSetResources(6, 6, List.of()))));

		assertEquals(6, report.get("driver_threads").asInt());
		assertEquals(0, report.get("nics").size());
		assertEquals(0, report.get("event_loops").asInt());
	}

	@Test
	void severalDriversAddContextsAndKeepOnlyTheFieldsTheyAgreeOn() throws Exception {
		final var same = new NicSetResources(0, 8, List.of("ens1f0", "ens1f1"));
		final var agreeing = report(List.of(new NicSetArtifact.Context(0, same), new NicSetArtifact.Context(2, same)));
		assertEquals(8, agreeing.get("event_loops_per_nic").asInt());
		assertEquals(List.of("ens1f0", "ens1f1"), names(agreeing.get("nics")));
		assertEquals(32, agreeing.get("event_loops").asInt(), "the engine's total");
		assertEquals(2, agreeing.get("contexts").size());
		final var second = agreeing.get("contexts").get(1);
		assertEquals(List.of("context_index", "driver_threads", "event_loops_per_nic", "nics", "event_loops"), fieldNames(second));
		assertEquals(2, second.get("context_index").asInt());
		assertEquals(16, second.get("event_loops").asInt());

		final var otherNics = report(List.of(new NicSetArtifact.Context(0, same),
						new NicSetArtifact.Context(1, new NicSetResources(0, 8, List.of("ens2f0")))));
		assertEquals(0, otherNics.get("driver_threads").asInt());
		assertEquals(8, otherNics.get("event_loops_per_nic").asInt());
		assertFalse(otherNics.has("nics"));
		assertEquals(24, otherNics.get("event_loops").asInt());
		assertEquals(List.of("ens2f0"), names(otherNics.get("contexts").get(1).get("nics")));

		final var otherThreads = report(List.of(new NicSetArtifact.Context(0, same),
						new NicSetArtifact.Context(1, new NicSetResources(6, 6, List.of("ens1f0", "ens1f1")))));
		assertFalse(otherThreads.has("driver_threads"));
		assertFalse(otherThreads.has("event_loops_per_nic"));
		assertEquals(List.of("ens1f0", "ens1f1"), names(otherThreads.get("nics")));
		assertEquals(28, otherThreads.get("event_loops").asInt());
	}

	@Test
	void aContextNeedsItsResources() {
		assertThrows(NullPointerException.class, () -> new NicSetArtifact.Context(0, null));
	}

	private static List<String> fieldNames(final JsonNode node) {
		final var out = new ArrayList<String>();
		node.fieldNames().forEachRemaining(out::add);
		return out;
	}
}
