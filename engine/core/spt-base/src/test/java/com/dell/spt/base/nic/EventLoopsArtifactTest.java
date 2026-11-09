package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class EventLoopsArtifactTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	@Test
	void aDefaultRouteDriverReportsItsCountAndTheDefault() throws Exception {
		final var json = read(EventLoopsArtifact.json(7L, "read-1", "w", 192,
						List.of(new EventLoopsArtifact.Context(0, new EventLoops(EventLoopRoute.DEFAULT, 0, 24)))));

		assertEquals(1, json.get("schema_version").asInt());
		assertEquals(7L, json.get("engine_run_id").asLong());
		assertEquals("read-1", json.get("step_id").asText());
		assertEquals("w", json.get("worker_id").asText());
		assertEquals(192, json.get("hardware_threads").asInt());
		assertEquals(24, json.get("event_loops").asInt());
		assertEquals(MAPPER.readTree("""
						[{"context_index":0,"route":"default","driver_threads":0,"source":"default","event_loops":24}]"""),
						json.get("contexts"));
	}

	@Test
	void anOverrideIsNamedAsTheSource() throws Exception {
		final var json = read(EventLoopsArtifact.json(1L, "s", "w", 192,
						List.of(new EventLoopsArtifact.Context(0, new EventLoops(EventLoopRoute.DEFAULT, 12, 12)))));

		final var context = json.get("contexts").get(0);
		assertEquals(12, context.get("driver_threads").asInt());
		assertEquals("storage.driver.threads", context.get("source").asText());
		assertEquals(12, json.get("event_loops").asInt());
	}

	@Test
	void aNicSetDriverReportsEveryLoopItOwns() throws Exception {
		final var resources = new NicSetResources(0, 24, List.of("a", "b", "c", "d"), 2);
		final var json = read(EventLoopsArtifact.json(1L, "s", "w", 192,
						List.of(new EventLoopsArtifact.Context(0, EventLoops.of(resources)))));

		final var context = json.get("contexts").get(0);
		assertEquals("nic_set", context.get("route").asText());
		assertEquals("default", context.get("source").asText());
		assertEquals(98, context.get("event_loops").asInt());
		assertEquals(98, json.get("event_loops").asInt());
	}

	@Test
	void severalContextsKeepTheirIndexesAndTheTotalSumsThem() throws Exception {
		final var json = read(EventLoopsArtifact.json(1L, "s", "w", 6, List.of(
						new EventLoopsArtifact.Context(0, new EventLoops(EventLoopRoute.DEFAULT, 0, 4)),
						new EventLoopsArtifact.Context(2, new EventLoops(EventLoopRoute.DEFAULT, 8, 8)))));

		assertEquals(12, json.get("event_loops").asInt());
		assertEquals(0, json.get("contexts").get(0).get("context_index").asInt());
		assertEquals(2, json.get("contexts").get(1).get("context_index").asInt());
	}

	@Test
	void aStepWhoseDriversOwnNoLoopsReportsAnEmptyList() throws Exception {
		final var json = read(EventLoopsArtifact.json(1L, "s", "w", 6, List.of()));

		assertEquals(0, json.get("event_loops").asInt());
		assertEquals(0, json.get("contexts").size());
	}

	@Test
	void aCountIsNeverNegative() {
		assertThrows(IllegalArgumentException.class, () -> new EventLoops(EventLoopRoute.DEFAULT, -1, 4));
		assertThrows(IllegalArgumentException.class, () -> new EventLoops(EventLoopRoute.DEFAULT, 0, -1));
		assertThrows(NullPointerException.class, () -> new EventLoops(null, 0, 4));
	}

	private static JsonNode read(final String json) throws Exception {
		return MAPPER.readTree(json);
	}
}
