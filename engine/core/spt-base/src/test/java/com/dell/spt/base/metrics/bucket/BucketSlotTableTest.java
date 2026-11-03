package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The correctness list of bucket metrics spec item 5a, on the lookup candidate. */
class BucketSlotTableTest {

	private final BucketSlotTable table = new BucketSlotTable(List.of("alpha", "beta", "Aa", "BB", "ALPHA"));

	@Test
	void hashCollisionIsResolvedByStringComparison() {
		assertEquals("Aa".hashCode(), "BB".hashCode());
		assertEquals(2, table.slotOfPath("/Aa/k"));
		assertEquals(3, table.slotOfPath("/BB/k"));
		final String colliding = "AaAa";
		assertEquals(colliding.hashCode(), "BBBB".hashCode());
		final var collidingTable = new BucketSlotTable(List.of(colliding));
		assertEquals(collidingTable.catchAllSlot(), collidingTable.slotOfPath("/BBBB/k"));
	}

	@Test
	void namesThatDifferOnlyInCaseAreDistinct() {
		assertEquals(0, table.slotOfPath("/alpha"));
		assertEquals(4, table.slotOfPath("/ALPHA"));
		assertEquals(table.catchAllSlot(), table.slotOfPath("/Alpha"));
	}

	@Test
	void onlyTheFirstSegmentCounts() {
		assertEquals(1, table.slotOfPath("/beta/s0000001"));
		assertEquals(1, table.slotOfPath("beta/prefix/deep"));
		assertEquals(table.catchAllSlot(), table.slotOfPath("/betamax/k"));
		assertEquals(table.catchAllSlot(), table.slotOfPath("/bet/k"));
		assertEquals(table.catchAllSlot(), table.slotOfPath("/other/beta/k"));
	}

	@Test
	void nonMembersAndMissingPathsGoToTheCatchAll() {
		assertEquals(table.catchAllSlot(), table.slotOfPath("/gamma/k"));
		assertEquals(table.catchAllSlot(), table.slotOfPath(null));
		assertEquals(table.catchAllSlot(), table.slotOfPath(""));
		assertEquals(table.catchAllSlot(), table.slotOfPath("/"));
	}

	@Test
	void retryRecycleAndChildResultsKeepTheirBucket() {
		final var item = new DataItemImpl("beta-key", 0, 1024);
		final var op = new DataOperationImpl<>(0, OpType.CREATE, item, null, "/beta", null, null, 0);
		final int first = table.slotOfPath(op.dstPath());
		assertEquals(1, first);
		final var recycled = op.result();
		assertEquals(first, table.slotOfPath(recycled.dstPath()));
		final var reader = new DataOperationImpl<>(0, OpType.READ, recycled.item(), null, null, null, null, 0);
		assertEquals(first, table.slotOfPath(reader.srcPath()));
		assertEquals(first, table.slotOfPath(reader.dstPath()));
	}

	@Test
	void eachStepUsesItsOwnSet() {
		final var otherStep = new BucketSlotTable(List.of("beta"));
		assertEquals(1, table.slotOfPath("/beta/k"));
		assertEquals(0, otherStep.slotOfPath("/beta/k"));
		assertEquals(otherStep.catchAllSlot(), otherStep.slotOfPath("/alpha/k"));
		assertNotEquals(table.catchAllSlot(), otherStep.catchAllSlot());
	}

	@Test
	void largestSetResolvesEveryMember() {
		final List<String> members = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			members.add("bucket-" + i);
		}
		final var large = new BucketSlotTable(members);
		for (int i = 0; i < members.size(); i++) {
			assertEquals(i, large.slotOfPath("/" + members.get(i) + "/s0000001"));
		}
		assertEquals(1000, large.slotOfPath("/bucket-1000/k"));
	}
}
