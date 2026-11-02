package com.dell.spt.base.metrics.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BucketRecorderTest {

	private static final List<String> MEMBERS = List.of("alpha", "beta", "other");

	private static Operation<DataItem> create(final String path) {
		return new DataOperationImpl<>(0, OpType.CREATE, new DataItemImpl("key", 0, 1), null, path, null, null, 0);
	}

	private static Operation<DataItem> read(final String name) {
		return new DataOperationImpl<>(0, OpType.READ, new DataItemImpl(name, 0, 1), null, null, null, null, 0);
	}

	@Test
	void writesGoByDestinationAndReadsBySource() {
		final var meters = new BucketMeters(OpType.CREATE, MEMBERS);
		final var recorder = new BucketRecorder(MEMBERS, meters, null);
		assertSame(meters.sets()[0], recorder.meterSet(create("/alpha")));
		assertSame(meters.sets()[1], recorder.meterSet(create("/beta/prefix")));
		assertSame(meters.sets()[1], recorder.meterSet(read("/beta/s0000001/key")));
		assertSame(meters.sets()[0], recorder.meterSet(read("alpha/key")));
	}

	@Test
	void aResultOutsideTheSetGoesToTheCatchAllAndABucketNamedOtherStaysItself() {
		final var meters = new BucketMeters(OpType.CREATE, MEMBERS);
		final var recorder = new BucketRecorder(MEMBERS, meters, null);
		assertSame(meters.sets()[2], recorder.meterSet(create("/other")));
		assertSame(meters.sets()[3], recorder.meterSet(create("/gamma")));
		assertSame(meters.sets()[3], recorder.meterSet(read("key")));
	}

	@Test
	void aRecycledResultKeepsItsBucket() {
		final var meters = new BucketMeters(OpType.CREATE, MEMBERS);
		final var recorder = new BucketRecorder(MEMBERS, meters, null);
		final var op = create("/beta");
		final var first = recorder.meterSet(op);
		assertSame(first, recorder.meterSet(op.result()));
		assertSame(first, recorder.meterSet(read(op.result().item().name())));
	}

	@Test
	void aMixedStepRoutesLikeItsMetricsContexts() {
		final var creates = new BucketMeters(OpType.CREATE, MEMBERS);
		final var reads = new BucketMeters(OpType.READ, MEMBERS);
		final var recorder = new BucketRecorder(MEMBERS, creates, Map.of(OpType.CREATE, creates, OpType.READ, reads));
		assertSame(creates.sets()[0], recorder.meterSet(create("/alpha")));
		assertSame(reads.sets()[1], recorder.meterSet(read("/beta/key")));
		// A type with no context of its own falls to the primary, as resolveMetrics does.
		assertSame(creates.sets()[1], recorder.meterSet(OpType.DELETE, "beta"));
		// A site that updates the primary context always reaches the primary's meters.
		assertSame(creates.sets()[1], recorder.primaryMeterSet(read("/beta/key")));
	}

	@Test
	void aBucketNamedByARequestResolvesWithoutALeadingSlash() {
		final var meters = new BucketMeters(OpType.DELETE, MEMBERS);
		final var recorder = new BucketRecorder(MEMBERS, meters, null);
		assertEquals(meters.sets()[1], recorder.meterSet(OpType.DELETE, "beta"));
		assertEquals(meters.sets()[3], recorder.meterSet(OpType.DELETE, "betamax"));
	}
}
