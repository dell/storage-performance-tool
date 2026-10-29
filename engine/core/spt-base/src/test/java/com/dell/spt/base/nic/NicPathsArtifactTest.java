package com.dell.spt.base.nic;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

class NicPathsArtifactTest {

	private static final NicPathRows.Nic NIC_A = new NicPathRows.Nic("ens1", "192.0.2.10", null, null, null, 0, "p0", "4");
	private static final NicPathRows.Nic NIC_B = new NicPathRows.Nic("ens2", "192.0.2.11", null, null, null, 1, null, "shared");
	private static final NicPathRows.Endpoint ENDPOINT = new NicPathRows.Endpoint("s3.example:9000", "198.51.100.5", 9000);

	private static List<CSVRecord> records(final List<NicPathsArtifact.Context> contexts) throws Exception {
		return CSVFormat.RFC4180.builder().setHeader().get().parse(new StringReader(
						NicPathsArtifact.HEADER + "\n" + NicPathsArtifact.rows(7, "step,1", "worker", contexts))).getRecords();
	}

	@Test
	void everyContextsRowsComeFirstAndTheEndRecordCarriesTheTotals() throws Exception {
		final var first = new NicPathRows(List.of(
						new NicPathRows.Row(NicPathRows.Kind.PATH, NIC_A, ENDPOINT, 10, 7, 1, 1, 700),
						new NicPathRows.Row(NicPathRows.Kind.NIC, NIC_A, null, 2, 0, 2, 0, 0),
						new NicPathRows.Row(NicPathRows.Kind.NONE, null, null, 1, 0, 0, 1, 0)), true);
		final var second = new NicPathRows(List.of(
						new NicPathRows.Row(NicPathRows.Kind.PATH, NIC_B, ENDPOINT, 5, 5, 0, 0, 50)), true);

		final var records = records(List.of(new NicPathsArtifact.Context(0, first), new NicPathsArtifact.Context(2, second)));

		assertEquals(5, records.size());
		assertEquals(List.of("path", "nic", "none", "path", "end"), records.stream().map(r -> r.get("row")).toList());
		assertEquals(List.of("0", "0", "0", "2", ""), records.stream().map(r -> r.get("context_index")).toList());
		final var path = records.get(0);
		assertEquals("1", path.get("schema_version"));
		assertEquals("7", path.get("engine_run_id"));
		assertEquals("step,1", path.get("step_id"));
		assertEquals("ens1", path.get("interface"));
		assertEquals("192.0.2.10", path.get("ipv4"));
		assertEquals("", path.get("rdma_device"));
		assertEquals("0", path.get("numa_node"));
		assertEquals("p0", path.get("shared_port"));
		assertEquals("4", path.get("permit_share"));
		assertEquals("s3.example:9000", path.get("endpoint"));
		assertEquals("198.51.100.5", path.get("endpoint_ip"));
		assertEquals("9000", path.get("endpoint_port"));
		assertEquals("1", path.get("unresolved"), "attempts less every outcome");
		assertEquals("", path.get("complete"));
		assertEquals("", records.get(1).get("endpoint"));
		assertEquals("ens1", records.get(1).get("interface"));
		assertEquals("", records.get(2).get("interface"));
		assertEquals("shared", records.get(3).get("permit_share"));
		final var end = records.get(4);
		assertEquals("", end.get("interface"));
		assertEquals(List.of("18", "12", "3", "2", "1", "750"), List.of(end.get("attempts"), end.get("succeeded"),
						end.get("failed"), end.get("interrupted"), end.get("unresolved"), end.get("bytes")));
		assertEquals("true", end.get("complete"));
		assertEquals("", end.get("missing_contexts"));
	}

	@Test
	void aContextWithoutRowsOrWithLateUpdatesMakesTheEndRecordIncomplete() throws Exception {
		final var late = new NicPathRows(List.of(new NicPathRows.Row(NicPathRows.Kind.PATH, NIC_A, ENDPOINT, 3, 3, 0, 0, 30)), false);

		final var incomplete = records(List.of(new NicPathsArtifact.Context(0, late)));
		assertEquals("false", incomplete.get(1).get("complete"));
		assertEquals("", incomplete.get(1).get("missing_contexts"));

		final var missing = records(Arrays.asList(new NicPathsArtifact.Context(0, null), new NicPathsArtifact.Context(1, null)));
		assertEquals(1, missing.size(), "only the end record");
		assertEquals("end", missing.get(0).get("row"));
		assertEquals("false", missing.get(0).get("complete"));
		assertEquals("0;1", missing.get(0).get("missing_contexts"));
		assertEquals("0", missing.get(0).get("attempts"));
	}
}
