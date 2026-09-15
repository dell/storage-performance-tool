package com.dell.spt.base.metrics.range;

import static org.junit.jupiter.api.Assertions.*;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;

class RangeReadArtifactTest {
	private RangeReadSnapshot success(RangeReadPolicy policy) {
		return new RangeReadSnapshot(1, policy,
						new OperationLifecycleCounters(true, 1, 1, 0, 1, 0, 0, 0, 0, 0),
						1, 1, policy.length(), 0, 0, 0, 0, 0, 0, 0, 0, 0, false);
	}

	@Test
	void retainsExactPolicyContextIdentityAndCounters() throws Exception {
		var fixed = success(new RangeReadPolicy(1, Long.MAX_VALUE, 1));
		var random = success(new RangeReadPolicy(3, null, 8));
		var records = CSVFormat.RFC4180.builder().setHeader().get().parse(new StringReader(
						RangeReadArtifact.HEADER + "\n" + RangeReadArtifact.rows(42, "step,read", "worker", Arrays.asList(fixed, null, random), true))).getRecords();
		assertEquals(2, records.size());
		assertEquals("step,read", records.get(0).get("step_id"));
		assertEquals("9223372036854775807", records.get(0).get("fixed_offset"));
		assertEquals("true", records.get(0).get("fixed_offset_present"));
		assertEquals("fixed", records.get(0).get("mode"));
		assertEquals("2", records.get(1).get("context_index"));
		assertEquals("random", records.get(1).get("mode"));
		assertEquals("", records.get(1).get("fixed_offset"));
		assertEquals("false", records.get(1).get("fixed_offset_present"));
		assertEquals("8", records.get(1).get("alignment"));
		assertEquals("3", records.get(1).get("successful_bytes"));
		assertEquals("true", records.get(1).get("terminal"));
	}

	@Test
	void incompleteContextsCannotClaimTerminalAndOrdinaryContextsEmitNoRows() throws Exception {
		assertEquals("", RangeReadArtifact.rows(1, "step", "worker", Arrays.asList(null, null), true));
		var records = CSVFormat.RFC4180.builder().setHeader().get().parse(new StringReader(
						RangeReadArtifact.HEADER + "\n" + RangeReadArtifact.rows(1, "step", "worker", List.of(success(new RangeReadPolicy(3, 0L, 1))), false))).getRecords();
		assertEquals("false", records.get(0).get("terminal"));
	}
}
