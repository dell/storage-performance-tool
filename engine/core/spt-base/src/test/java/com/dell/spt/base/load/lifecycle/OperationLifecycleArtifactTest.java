package com.dell.spt.base.load.lifecycle;

import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

class OperationLifecycleArtifactTest {
	@Test
	void combinesContextsOnlyWhenAllAreTerminalAndReconciled() throws Exception {
		final var c = new OperationLifecycleCounters(true, 4, 1, 1, 2, 1, 1, 0, 0, 0);
		final var rows = CSVFormat.RFC4180.builder().setHeader().get().parse(new java.io.StringReader(
						OperationLifecycleArtifact.HEADER + "\n" + OperationLifecycleArtifact.row(123, "step,read", "worker", List.of(c, c), true))).getRecords();
		assertEquals("step,read", rows.get(0).get("step_id"));
		assertEquals("true", rows.get(0).get("terminal"));
		assertEquals("8", rows.get(0).get("selected"));
		assertTrue(OperationLifecycleArtifact.row(123, "step", "worker", List.of(c), false).contains(",false,"));
		assertTrue(OperationLifecycleArtifact.row(123, "step", "worker", Arrays.asList(c, null), true).contains(",false,"));
		assertTrue(OperationLifecycleArtifact.row(123, "step", "worker", List.of(), true).contains(",false,"));
		assertFalse(new OperationLifecycleCounters(true, 5, 1, 1, 2, 1, 1, 0, 0, 0).reconciled());
		assertFalse(new OperationLifecycleCounters(true, 4, 1, 1, 3, 1, 1, 0, 0, 0).reconciled());
		assertFalse(new OperationLifecycleCounters(true, 4, 1, 1, 2, 1, 1, 0, 0, 1).reconciled());
	}
}
