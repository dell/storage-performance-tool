package com.dell.spt.base.nic.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.nic.pool.PoolDemand.DriverDemand;
import com.dell.spt.base.nic.pool.PoolDemand.Listed;
import com.dell.spt.base.nic.pool.PoolDemand.NotKnown;
import com.dell.spt.base.nic.pool.PoolDemand.OpKind;
import com.dell.spt.base.nic.pool.PoolDemand.Operation;
import com.dell.spt.base.nic.pool.PoolDemand.Range;
import com.dell.spt.base.nic.pool.PoolDemand.Sizes;
import com.dell.spt.base.nic.pool.PoolDemand.State;
import com.dell.spt.base.nic.pool.PoolDemand.StepDemand;
import com.dell.spt.base.nic.pool.PoolDemand.Workload;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The estimate's rules (item 14a), without an engine. */
final class PoolDemandTest {

	private static final long MIB = 1L << 20;
	private static final long GIB = 1L << 30;
	private static final long THRESHOLD = MIB;
	private static final List<String> FOUR_NICS = List.of("a", "b", "c", "d");

	private static Range size(final long bytes) {
		return new Range(bytes, bytes, "item.data.size");
	}

	private static Operation put(final Sizes sizes) {
		return new Operation(OpKind.CREATE, 0, false, sizes);
	}

	private static Operation get(final Sizes sizes) {
		return new Operation(OpKind.READ, 0, false, sizes);
	}

	private static Listed listed(final long... sizes) {
		final var recorder = new ItemSizeSummary.Recorder("items.csv");
		for (final long size : sizes) {
			recorder.record(size);
		}
		recorder.finish();
		return new Listed(recorder.summary());
	}

	private static Listed incomplete(final ItemSizeSummary.Reason reason, final long... sizes) {
		final var recorder = new ItemSizeSummary.Recorder("items.csv");
		for (final long size : sizes) {
			recorder.record(size);
		}
		recorder.fail(reason, "detail");
		return new Listed(recorder.summary());
	}

	private static DriverDemand demand(final int concurrency, final List<String> nics, final Operation... operations) {
		return PoolDemand.estimate(new Workload(concurrency, THRESHOLD, 0, 0, List.of(operations)), nics);
	}

	private static DriverDemand demand(final Operation... operations) {
		return demand(96, FOUR_NICS, operations);
	}

	@Test
	void aDeleteARangedReadAndASizeBelowTheThresholdHaveZeroDemand() {
		assertEquals(State.ZERO, demand(new Operation(OpKind.OTHER, 0, false, new NotKnown(""))).state());
		assertEquals(State.ZERO, demand(new Operation(OpKind.READ, 0, true, size(GIB))).state());
		assertEquals(State.ZERO, demand(put(size(THRESHOLD - 1))).state());
		assertEquals(0, demand(put(size(THRESHOLD - 1))).maxNeedBytes());
	}

	@Test
	void aMultipartUploadCountsAtItsPartSizeWithinItsLimits() {
		final var upload = new Operation(OpKind.CREATE, 64 * MIB, false, size(8 * GIB));
		final var unlimited = demand(upload);
		assertEquals(State.KNOWN, unlimited.state());
		assertEquals(64 * MIB, unlimited.nics().get(0).sizeClass());
		assertEquals(24, unlimited.nics().get(0).count());

		final var limited = PoolDemand.estimate(new Workload(96, THRESHOLD, 2, 4, List.of(upload)), FOUR_NICS);
		assertEquals(8, limited.nics().get(0).count(), "2 objects x 4 parts is below the NIC's share of 24");
		assertEquals(24, limited.nics().get(0).minimum(), "the minimum stays the share; the limits lower only the count");

		final var objectsOnly = PoolDemand.estimate(new Workload(96, THRESHOLD, 1, 0, List.of(upload)), FOUR_NICS);
		assertEquals(24, objectsOnly.nics().get(0).count(), "128 parts per object fill the share");
		final var smallObject = new Operation(OpKind.CREATE, 64 * MIB, false, size(128 * MIB));
		final var fewParts = PoolDemand.estimate(new Workload(96, THRESHOLD, 3, 0, List.of(smallObject)), FOUR_NICS);
		assertEquals(6, fewParts.nics().get(0).count(), "3 objects of 2 parts");

		final var partsOnly = PoolDemand.estimate(new Workload(96, THRESHOLD, 0, 4, List.of(upload)), FOUR_NICS);
		assertEquals(24, partsOnly.nics().get(0).count(), "a part limit alone still fills the concurrency limit");

		final var belowThreshold = new Operation(OpKind.CREATE, 512 * 1024, false, size(8 * MIB));
		assertEquals(State.ZERO, demand(belowThreshold).state(), "parts below the threshold go over HTTP");
	}

	@Test
	void anObjectAtOrBelowThePartSizeIsNotSplit() {
		final var whole = new Operation(OpKind.CREATE, 64 * MIB, false, new Range(2 * MIB, 2 * MIB, "item.data.size"));
		assertEquals(2 * MIB, demand(whole).nics().get(0).sizeClass());
	}

	@Test
	void aReadAboveThePartSizeIsSplitIntoRangeRequestsOverHttp() {
		final var split = new Operation(OpKind.READ, 64 * MIB, false, size(GIB));
		assertEquals(State.ZERO, demand(split).state(), "a composite read's parts are partial reads");
		final var whole = new Operation(OpKind.READ, 64 * MIB, false, size(32 * MIB));
		assertEquals(32 * MIB, demand(whole).nics().get(0).sizeClass(), "at or below the part size it stays whole");
	}

	@Test
	void aRangeThatStraddlesTheThresholdCountsOnlyItsSizesAtOrAboveIt() {
		final var straddling = demand(put(new Range(4096, 4 * MIB, "item.data.size")));
		assertEquals(4 * MIB, straddling.nics().get(0).sizeClass());
		assertEquals(THRESHOLD, straddling.contributions().get(0).smallest());
		assertEquals(State.ZERO, demand(put(new Range(4096, THRESHOLD - 1, "item.data.size"))).state());
	}

	@Test
	void aTransferAbove1GibHasNoPoolDemand() {
		assertEquals(State.ZERO, demand(put(size(GIB + 1))).state());
		assertEquals(GIB, demand(put(size(GIB))).nics().get(0).sizeClass());
		final var straddling = demand(put(new Range(512 * MIB, 2 * GIB, "item.data.size")));
		assertEquals(GIB, straddling.nics().get(0).sizeClass(), "the eligible part of the range ends at 1 GiB");
	}

	@Test
	void aFixedSizeARangeInOneClassAndARangeSpanningClasses() {
		final var fixed = demand(put(size(3 * MIB)));
		assertEquals(4 * MIB, fixed.nics().get(0).sizeClass());
		assertFalse(fixed.spansClasses());
		final var oneClass = demand(put(new Range(3 * MIB, 4 * MIB, "item.data.size")));
		assertEquals(4 * MIB, oneClass.nics().get(0).sizeClass());
		assertFalse(oneClass.spansClasses());
		final var spanning = demand(put(new Range(2 * MIB, 8 * MIB, "item.data.size")));
		assertEquals(8 * MIB, spanning.nics().get(0).sizeClass(), "checked with its largest class");
		assertTrue(spanning.spansClasses());
	}

	@Test
	void listedSizesInOneClassAreAKnownDemandInIt() {
		final var demand = demand(get(listed(3 * MIB, 4 * MIB)));
		assertEquals(State.KNOWN, demand.state());
		assertEquals(4 * MIB, demand.nics().get(0).sizeClass());
		assertFalse(demand.spansClasses());
		assertEquals("items.csv", demand.contributions().get(0).source());
	}

	@Test
	void listedSizesSpanningClassesAreCheckedWithTheLargest() {
		final var demand = demand(get(listed(2 * MIB, 64 * MIB)));
		assertEquals(64 * MIB, demand.nics().get(0).sizeClass());
		assertTrue(demand.spansClasses());
	}

	@Test
	void listedSizesBelowTheThresholdOrAbove1GibAreZero() {
		assertEquals(State.ZERO, demand(get(listed(4096, 1024))).state());
		assertEquals(State.ZERO, demand(get(listed(8 * GIB))).state());
		final var mixedSizes = demand(get(listed(4096, 4 * MIB, 8 * GIB)));
		assertEquals(4 * MIB, mixedSizes.nics().get(0).sizeClass(), "only the eligible one counts");
		assertFalse(mixedSizes.spansClasses());
	}

	@Test
	void aRangedReadOfListedItemsIsZero() {
		assertEquals(State.ZERO, demand(new Operation(OpKind.READ, 0, true, listed(4 * MIB))).state());
	}

	@Test
	void aMixedDriverTakesTheLargerOfItsPutsAndItsListedGets() {
		final var demand = demand(put(size(16 * MIB)), get(listed(64 * MIB)));
		assertEquals(64 * MIB, demand.nics().get(0).sizeClass());
		assertTrue(demand.spansClasses(), "two classes, so it is not pre-warmed");
		final var sameClass = demand(put(size(64 * MIB)), get(listed(40 * MIB)));
		assertFalse(sameClass.spansClasses());
	}

	/** The fixture ticket 13's multipart seed shares. */
	@Test
	void listedLargeObjectsInPartsHavePutDemandInThePartClassAndNoWholeObjectGetDemand() {
		final var puts = demand(new Operation(OpKind.CREATE, 64 * MIB, false, listed(8 * GIB)));
		assertEquals(64 * MIB, puts.nics().get(0).sizeClass());
		assertEquals(State.KNOWN, puts.state());
		assertEquals(State.ZERO, demand(get(listed(8 * GIB))).state());
	}

	@Test
	void anIncompleteSummaryIsUnknownWithItsReasonAndNeverUsesItsBounds() {
		for (final var reason : ItemSizeSummary.Reason.values()) {
			final var demand = demand(get(incomplete(reason, 4 * MIB)));
			assertEquals(State.UNKNOWN, demand.state(), reason.name());
			assertEquals(0, demand.maxNeedBytes(), reason.name());
			final var why = demand.unknownReasons().get(0);
			assertTrue(why.contains(reason.text()) && why.contains("items.csv"), why);
		}
	}

	@Test
	void aMixedDriverKeepsItsKnownPutsWhenItsItemFileIsUnknown() {
		final var demand = demand(put(size(GIB)), get(incomplete(ItemSizeSummary.Reason.MALFORMED_ITEM)));
		assertEquals(State.KNOWN, demand.state());
		assertTrue(demand.hasUnknown());
		assertEquals(24 * GIB, demand.maxNeedBytes());
	}

	@Test
	void aCompleteFileThatIsEmptyOrAllIneligibleIsZero() {
		assertEquals(State.ZERO, demand(get(listed())).state());
		assertEquals(State.ZERO, demand(get(listed(1024))).state());
	}

	@Test
	void aBucketListingAndUnlimitedConcurrencyAreUnknown() {
		assertEquals(State.UNKNOWN, demand(get(new NotKnown("its items come from the bucket listing /b"))).state());
		final var unlimited = demand(0, FOUR_NICS, put(size(GIB)));
		assertEquals(State.UNKNOWN, unlimited.state());
		assertEquals(List.of("PUT: unlimited concurrency"), unlimited.unknownReasons());
		assertEquals(PoolDemand.UNLIMITED_CAP, unlimited.nics().get(0).cap());
		assertEquals(State.ZERO, demand(0, FOUR_NICS, put(size(4096))).state(), "below the threshold it stays zero");
	}

	@Test
	void theMinimumIsTheShareAnd96OnFourNicsOf1GibFitsIn192Gib() {
		final var demand = demand(put(size(GIB)));
		final var nic = demand.nics().get(0);
		assertEquals(24, nic.minimum());
		assertEquals(25, nic.cap());
		assertEquals(1, nic.headroom());
		assertEquals(24, nic.count());
		assertEquals(24 * GIB, nic.needBytes());
		final var step = new StepDemand(List.of(demand));
		final var check = PoolDemand.check(step, 192 * GIB, PoolDemand.PROCESS_HEADROOM_BYTES);
		assertTrue(check.passed(), check::toString);
		assertEquals(192 * GIB, step.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES));
		assertEquals(24 * GIB, PoolDemand.nicBudgetBytes(192 * GIB, 4));
		assertFalse(PoolDemand.check(step, 191 * GIB, PoolDemand.PROCESS_HEADROOM_BYTES).passed());
	}

	@Test
	void theRemainderOfTheSharesGoesToTheFirstNics() {
		final var demand = demand(6, List.of("a", "b", "c", "d"), put(size(4 * MIB)));
		assertEquals(List.of(2, 2, 1, 1), demand.nics().stream().map(PoolDemand.NicDemand::minimum).toList());
	}

	@Test
	void theSharedModeKeepsTheWholeLimitOnEachNic() {
		final var demand = demand(2, FOUR_NICS, put(size(4 * MIB)));
		assertTrue(demand.sharedMode());
		assertEquals(2, demand.nics().get(3).minimum());
		assertEquals(3, demand.nics().get(3).cap());
		assertEquals(8 * MIB, demand.nics().get(3).needBytes());
	}

	/** The pipeline fixture: ticket 13's sizing relies on it for concurrent drivers. */
	@Test
	void twoDriversThatFitTheirOwnBudgetsButNotTheLimitTogetherFailTheProcessCheckOnly() {
		final var one = demand(24, List.of("a", "b"), put(size(GIB)));
		final var two = demand(24, List.of("c", "d"), get(listed(GIB)));
		final var step = new StepDemand(List.of(one, two));
		// 24 GiB per driver's two pools fits half of a 48 GiB limit, but two drivers and the headroom do not.
		final var check = PoolDemand.check(step, 48 * GIB, PoolDemand.PROCESS_HEADROOM_BYTES);
		assertTrue(check.poolsShort().isEmpty(), () -> "the pools fit their budgets: " + check);
		assertTrue(check.processShort());
		assertEquals(48 * GIB + PoolDemand.PROCESS_HEADROOM_BYTES, check.processNeedBytes());
		assertEquals(48 * GIB + PoolDemand.PROCESS_HEADROOM_BYTES, step.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES), "the step's limit adds both");
		assertTrue(PoolDemand.check(step, step.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES), PoolDemand.PROCESS_HEADROOM_BYTES).passed());
	}

	@Test
	void aKnownDriverBesideAnUnknownOneKeepsItsRequirement() {
		final var known = demand(24, List.of("a", "b"), put(size(GIB)));
		final var unknown = demand(24, List.of("c", "d"), get(new NotKnown("its items come from the bucket listing /b")));
		final var step = new StepDemand(List.of(known, unknown));
		assertTrue(step.hasKnown());
		assertTrue(step.hasUnknown());
		assertEquals(48 * GIB, step.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES));
		assertEquals(List.of("GET: its items come from the bucket listing /b"), step.unknownReasons());
		assertFalse(PoolDemand.check(step, 40 * GIB, PoolDemand.PROCESS_HEADROOM_BYTES).passed(), "the known part still fails a short limit");
	}

	@Test
	void zeroDemandNeedsNoLimit() {
		final var step = new StepDemand(List.of(demand(new Operation(OpKind.OTHER, 0, false, new NotKnown("")))));
		assertEquals(0, step.requiredLimitBytes(PoolDemand.PROCESS_HEADROOM_BYTES));
		assertTrue(PoolDemand.check(step, 1, PoolDemand.PROCESS_HEADROOM_BYTES).passed());
	}

	@Test
	void sizeClassesMatchTheirBounds() {
		assertEquals(1 << 20, PoolSizeClass.of(0));
		assertEquals(1 << 20, PoolSizeClass.of(1 << 20));
		assertEquals(1 << 21, PoolSizeClass.of((1 << 20) + 1));
		assertEquals(1 << 30, PoolSizeClass.of(1 << 30));
	}
}
