package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import com.dell.spt.base.nic.pool.PoolDemand;
import com.dell.spt.base.nic.pool.PoolPreparation;
import org.junit.jupiter.api.Test;

class NicSetArtifactTest {

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private static JsonNode report(final List<NicSetArtifact.Context> contexts) throws Exception {
		return MAPPER.readTree(NicSetArtifact.json(7, "step,1", "worker", 64, contexts, 0));
	}

	private static List<String> names(final JsonNode array) {
		final var out = new ArrayList<String>();
		array.forEach(node -> out.add(node.asText()));
		return out;
	}

	@Test
	void oneDriversFieldsAreAtTheTopLevel() throws Exception {
		final var report = report(List.of(new NicSetArtifact.Context(0, new NicSetResources(0, 8, List.of("ens1f0", "ens1f1"), 2))));

		assertEquals(List.of("schema_version", "engine_run_id", "step_id", "worker_id", "hardware_threads", "driver_threads",
						"event_loops_per_nic", "nics", "event_loops", "base_event_loops", "total_event_loops"), fieldNames(report));
		assertEquals(1, report.get("schema_version").asInt());
		assertEquals(7, report.get("engine_run_id").asLong());
		assertEquals("step,1", report.get("step_id").asText());
		assertEquals("worker", report.get("worker_id").asText());
		assertEquals(64, report.get("hardware_threads").asInt());
		assertEquals(0, report.get("driver_threads").asInt());
		assertEquals(8, report.get("event_loops_per_nic").asInt());
		assertEquals(List.of("ens1f0", "ens1f1"), names(report.get("nics")));
		assertEquals(16, report.get("event_loops").asInt(), "the per-NIC groups");
		assertEquals(2, report.get("base_event_loops").asInt(), "the base group");
		assertEquals(18, report.get("total_event_loops").asInt(), "every loop the driver owns");
	}

	@Test
	void contextsWithoutANicSetAreCountedOnlyWhenThereAreAny() throws Exception {
		final var contexts = List.of(new NicSetArtifact.Context(0, new NicSetResources(0, 8, List.of("ens1f0"), 2)));

		final var report = MAPPER.readTree(NicSetArtifact.json(7, "step", "worker", 64, contexts, 2));

		assertEquals(2, report.get("contexts_without_nic_set").asInt());
		assertEquals(10, report.get("total_event_loops").asInt(), "the NIC-set driver's loops only");
		assertFalse(report(contexts).has("contexts_without_nic_set"));
	}

	@Test
	void aNicSetThatNeverOpenedHasNoNicsAndOnlyItsBaseGroup() throws Exception {
		final var report = report(List.of(new NicSetArtifact.Context(0, new NicSetResources(6, 6, List.of(), 2))));

		assertEquals(6, report.get("driver_threads").asInt());
		assertEquals(0, report.get("nics").size());
		assertEquals(0, report.get("event_loops").asInt());
		assertEquals(2, report.get("base_event_loops").asInt());
		assertEquals(2, report.get("total_event_loops").asInt());
	}

	@Test
	void severalDriversAddContextsAndKeepOnlyTheFieldsTheyAgreeOn() throws Exception {
		final var same = new NicSetResources(0, 8, List.of("ens1f0", "ens1f1"), 2);
		final var agreeing = report(List.of(new NicSetArtifact.Context(0, same), new NicSetArtifact.Context(2, same)));
		assertEquals(8, agreeing.get("event_loops_per_nic").asInt());
		assertEquals(List.of("ens1f0", "ens1f1"), names(agreeing.get("nics")));
		assertEquals(32, agreeing.get("event_loops").asInt(), "the engine's per-NIC groups");
		assertEquals(4, agreeing.get("base_event_loops").asInt(), "one base group per driver");
		assertEquals(36, agreeing.get("total_event_loops").asInt(), "the engine's total");
		assertEquals(2, agreeing.get("contexts").size());
		final var second = agreeing.get("contexts").get(1);
		assertEquals(List.of("context_index", "driver_threads", "event_loops_per_nic", "nics", "event_loops", "base_event_loops",
						"total_event_loops"), fieldNames(second));
		assertEquals(2, second.get("context_index").asInt());
		assertEquals(16, second.get("event_loops").asInt());
		assertEquals(2, second.get("base_event_loops").asInt());
		assertEquals(18, second.get("total_event_loops").asInt());

		final var otherNics = report(List.of(new NicSetArtifact.Context(0, same),
						new NicSetArtifact.Context(1, new NicSetResources(0, 8, List.of("ens2f0"), 2))));
		assertEquals(0, otherNics.get("driver_threads").asInt());
		assertEquals(8, otherNics.get("event_loops_per_nic").asInt());
		assertFalse(otherNics.has("nics"));
		assertEquals(24, otherNics.get("event_loops").asInt());
		assertEquals(List.of("ens2f0"), names(otherNics.get("contexts").get(1).get("nics")));

		final var otherThreads = report(List.of(new NicSetArtifact.Context(0, same),
						new NicSetArtifact.Context(1, new NicSetResources(6, 6, List.of("ens1f0", "ens1f1"), 2))));
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

	@Test
	void aStepWithNicPoolsAddsItsChecksAndPreWarmWithoutChangingTheOtherFields() throws Exception {
		final var demand = PoolDemand.estimate(
						new PoolDemand.Workload(4, 1L << 20, 0, 0, List.of(new PoolDemand.Operation(
										PoolDemand.OpKind.CREATE, 0, false, new PoolDemand.Range(1L << 30, 1L << 30, "item.data.size")))),
						List.of("ens1", "ens2"));
		final var step = new PoolDemand.StepDemand(List.of(demand));
		final var check = PoolDemand.check(step, 64L << 30, 8L << 30);
		final var nics = List.of(
						new PoolPreparation.NicReport("ens1", PoolDemand.State.KNOWN, 2, 3, 1, 16L << 30, 2, 1 << 30, 2L << 30, 2, 40, ""),
						new PoolPreparation.NicReport("ens2", PoolDemand.State.KNOWN, 2, 3, 1, 16L << 30, 2, 1 << 30, 2L << 30, 1, 50, "buffer registration failed"));
		final var pool = new PoolPreparation.Report(
						List.of(new PoolPreparation.DriverReport(0, "warn", demand, nics)), check, 12L << 30, false, List.of(), 12L << 30, 50);
		final var resources = new NicSetResources(0, 8, List.of("ens1", "ens2"), 4);
		final var without = NicSetArtifact.json(1, "step", "worker", 16, List.of(new NicSetArtifact.Context(0, resources)), 0);
		final var with = NicSetArtifact.json(1, "step", "worker", 16, List.of(new NicSetArtifact.Context(0, resources)), 0, pool);

		final var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
		final var document = mapper.readTree(with);
		final var stripped = (com.fasterxml.jackson.databind.node.ObjectNode) mapper.readTree(with);
		stripped.remove("pool");
		assertEquals(mapper.readTree(without), stripped);
		final var section = document.get("pool");
		assertEquals(12L << 30, section.get("required_limit_bytes").asLong());
		assertEquals(64L << 30, section.get("max_direct_memory_bytes").asLong());
		assertEquals(12L << 30, section.get("cli_estimate_bytes").asLong());
		assertEquals(50, section.get("prewarm_ms").asLong());
		assertTrue(section.at("/checks/per_pool/passed").asBoolean());
		assertTrue(section.at("/checks/process/passed").asBoolean());
		assertEquals("warn", section.at("/drivers/0/shortfall").asText());
		assertEquals("ens1", section.at("/drivers/0/nics/0/nic").asText());
		assertEquals(2, section.at("/drivers/0/nics/0/prewarmed_buffers").asInt());
		assertEquals(40, section.at("/drivers/0/nics/0/prewarm_ms").asInt());
		assertEquals("buffer registration failed", section.at("/drivers/0/nics/1/prewarm_note").asText());
		assertEquals("PUT", section.at("/drivers/0/operations/0/op").asText());
	}
}
