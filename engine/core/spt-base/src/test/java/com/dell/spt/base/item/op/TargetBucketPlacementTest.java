package com.dell.spt.base.item.op;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.naming.ItemNameInput;
import com.dell.spt.base.item.naming.ItemNameInput.ItemNamingType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Placement of new objects in a target bucket set by the operation builders. */
class TargetBucketPlacementTest {

	private static final List<String> FOUR = TargetBucketSet.memberPaths("/bench", 4);
	private static final List<String> LISTED_NAMES = List.of("ps-eu", "PS_US", "ps-ap");
	private static final List<String> LISTED = TargetBucketSet.memberPaths("/placeholder/p", 3, LISTED_NAMES);

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static DataOperationsBuilderImpl<DataItem, DataOperation<DataItem>> createBuilder(
					final List<String> members, final int shards) {
		final var builder = new DataOperationsBuilderImpl(0);
		builder.opType(OpType.CREATE);
		builder.outputPathInput(new ConstantValueInputImpl<>("/bench"));
		builder.credentialInput(null);
		builder.sizeThreshold(Long.MAX_VALUE);
		if (members != null) {
			builder.targetBucketMembers(members, shards);
		}
		return builder;
	}

	private static DataItem item(final long id) {
		return new DataItemImpl(Long.toString(id, 36), id, 1024);
	}

	@Test
	void oneBucketKeepsTheConstantOutputPath() throws Exception {
		for (final var builder : List.of(createBuilder(null, 0), createBuilder(List.of("/bench"), 0))) {
			assertEquals("/bench", builder.buildOp(item(7)).dstPath());
			final var batch = new ArrayList<DataOperation<DataItem>>();
			builder.buildOps(List.of(item(1), item(2), item(3)), batch);
			for (final var op : batch) {
				assertEquals("/bench", op.dstPath());
			}
		}
	}

	@Test
	void singleAndBatchConstructionPlaceAnItemInTheMemberOfItsId() throws Exception {
		final var builder = createBuilder(FOUR, 0);
		final var batch = new ArrayList<DataOperation<DataItem>>();
		final var items = new ArrayList<DataItem>();
		for (long id = 0; id < 8; id++) {
			items.add(item(id));
		}

		builder.buildOps(items, batch);

		for (int i = 0; i < 8; i++) {
			assertEquals("/bench-" + (i % 4), batch.get(i).dstPath());
			assertEquals("/bench-" + (i % 4), builder.buildOp(items.get(i)).dstPath());
		}
		assertEquals("/bench-3", builder.buildOp(item(-1)).dstPath(), "a negative id still selects a member");
	}

	@Test
	void anOperationsBuilderOfAnyItemTypePlacesNewDataItemsToo() throws Exception {
		final var builder = new OperationsBuilderImpl<DataItem, Operation<DataItem>>(0);
		builder.opType(OpType.CREATE);
		builder.outputPathInput(new ConstantValueInputImpl<>("/bench"));
		builder.targetBucketMembers(FOUR, 0);
		final var batch = new ArrayList<Operation<DataItem>>();

		builder.buildOps(List.of(item(5), item(6)), batch);

		assertEquals("/bench-1", builder.buildOp(item(5)).dstPath());
		assertEquals(List.of("/bench-1", "/bench-2"), batch.stream().map(Operation::dstPath).toList());
	}

	@Test
	void withoutShardsAContiguousSerialRangeIsExactlyBalanced() {
		final var counts = new int[4];
		for (long id = 1000; id < 1000 + 4 * 250; id++) {
			counts[TargetBucketSet.memberIndex(id, 1, 4)]++;
		}
		assertArrayEquals(new int[]{250, 250, 250, 250
		}, counts);
	}

	@Test
	void withShardsEveryMemberGetsEveryLabelOverAFullCycle() {
		final int shards = 64;
		final Map<Integer, Set<Long>> labels = new HashMap<>();
		final var counts = new int[4];
		for (long id = 0; id < shards * 4L; id++) {
			final int member = TargetBucketSet.memberIndex(id, shards, 4);
			counts[member]++;
			labels.computeIfAbsent(member, key -> new HashSet<>()).add(Math.floorMod(id, (long) shards));
		}
		assertArrayEquals(new int[]{64, 64, 64, 64
		}, counts);
		for (int member = 0; member < 4; member++) {
			assertEquals(64, labels.get(member).size(), "member " + member);
		}
	}

	@Test
	void documentedStrideCaveatsHold() {
		final Set<Integer> strideSixtyFourMembers = new HashSet<>();
		final Set<Long> strideSixtyFourLabels = new HashSet<>();
		final Set<Integer> strideTwoFiftySixMembers = new HashSet<>();
		for (long k = 0; k < 64; k++) {
			strideSixtyFourMembers.add(TargetBucketSet.memberIndex(64 * k, 64, 4));
			strideSixtyFourLabels.add(Math.floorMod(64 * k, 64L));
			strideTwoFiftySixMembers.add(TargetBucketSet.memberIndex(256 * k, 64, 4));
		}
		assertEquals(Set.of(0, 1, 2, 3), strideSixtyFourMembers, "stride 64 still rotates through the buckets");
		assertEquals(Set.of(0L), strideSixtyFourLabels, "but uses only shard label 0");
		assertEquals(Set.of(0), strideTwoFiftySixMembers, "stride 256 reaches one bucket");
	}

	@Test
	void serialSlicesAtUnequalProgressConcentrateWhenTheSliceCountEqualsTheBucketCount() {
		final var counts = new int[4];
		final int[] created = {100, 10, 10, 10
		};
		for (int slice = 0; slice < 4; slice++) {
			for (int k = 0; k < created[slice]; k++) {
				counts[TargetBucketSet.memberIndex(slice + 4L * k, 1, 4)]++;
			}
		}
		assertArrayEquals(new int[]{100, 10, 10, 10
		}, counts, "each slice addresses one bucket, as documented");
	}

	@Test
	void randomNamingSpreadsAFixedSeedWithinTheDeclaredBound() throws Exception {
		final var names = ItemNameInput.Builder.newInstance()
						.type(ItemNamingType.RANDOM)
						.seed(20261026L)
						.radix(36)
						.length(12)
						.build();
		final int buckets = 16;
		final int ids = 100_000;
		final var counts = new int[buckets];
		for (int i = 0; i < ids; i++) {
			names.get();
			counts[TargetBucketSet.memberIndex(names.lastId(), 1, buckets)]++;
		}
		// Fixed before the run: each member within 5% of its expected share (about four standard
		// deviations for 100,000 draws over 16 members).
		final double expected = ids / (double) buckets;
		for (int member = 0; member < buckets; member++) {
			assertEquals(expected, counts[member], expected * 0.05, "member " + member);
		}
	}

	@Test
	void multipartPartsKeepTheirParentsMember() throws Exception {
		final var builder = createBuilder(FOUR, 0);
		builder.sizeThreshold(1024);
		final var parent = builder.buildOp(new DataItemImpl("big", 6, 4096 + 100));

		final var composite = assertInstanceOf(CompositeDataOperation.class, parent);
		assertEquals("/bench-2", parent.dstPath());
		assertEquals(5, composite.subOperations().size());
		for (final Object part : composite.subOperations()) {
			assertEquals("/bench-2", ((Operation<?>) part).dstPath());
		}
	}

	@Test
	void aListedSetPlacesAnItemInTheNameAtItsIndexInListOrder() throws Exception {
		final var builder = createBuilder(LISTED, 0);
		final var batch = new ArrayList<DataOperation<DataItem>>();
		final var items = new ArrayList<DataItem>();
		for (long id = 0; id < 7; id++) {
			items.add(item(id));
		}

		builder.buildOps(items, batch);

		for (int i = 0; i < 7; i++) {
			final var expected = "/" + LISTED_NAMES.get(i % 3) + "/p";
			assertEquals(expected, batch.get(i).dstPath());
			assertEquals(expected, builder.buildOp(items.get(i)).dstPath());
		}
	}

	@Test
	void aListedSetUsesTheSamePlacementFunctionWithShards() throws Exception {
		final int shards = 64;
		final var builder = createBuilder(LISTED, shards);

		for (final long id : new long[]{0, 63, 64, 127, 128, 255, 1000, -1
		}) {
			assertEquals("/" + LISTED_NAMES.get(TargetBucketSet.memberIndex(id, shards, 3)) + "/p",
							builder.buildOp(item(id)).dstPath(), "id " + id);
		}
		assertEquals("/PS_US/p", builder.buildOp(item(64)).dstPath(), "ids of one shard cycle share a member");
	}

	@Test
	void aListedSetKeepsMultipartPartsInTheirParentsBucket() throws Exception {
		final var builder = createBuilder(LISTED, 0);
		builder.sizeThreshold(1024);
		final var parent = builder.buildOp(new DataItemImpl("big", 4, 4096 + 100));

		final var composite = assertInstanceOf(CompositeDataOperation.class, parent);
		assertEquals("/PS_US/p", parent.dstPath());
		assertEquals(5, composite.subOperations().size());
		for (final Object part : composite.subOperations()) {
			assertEquals("/PS_US/p", ((Operation<?>) part).dstPath());
		}
	}

	@Test
	void existingObjectsKeepTheirRecordedBucketWhateverItsName() throws Exception {
		@SuppressWarnings({"rawtypes", "unchecked"
		})
		final DataOperationsBuilderImpl<DataItem, DataOperation<DataItem>> read = new DataOperationsBuilderImpl(0);
		read.opType(OpType.READ);
		read.inputPath("/placeholder");

		final var recorded = read.buildOp(new DataItemImpl("/PS_US/prefix/key", 99, 1024));
		final var manifest = read.buildOp(new IntegrityManifestDataItem("ps-ap", "prefix/key", 1024, null));

		assertEquals("/PS_US/prefix", recorded.srcPath());
		assertEquals("/ps-ap", manifest.srcPath());
	}

	@Test
	void existingObjectsKeepTheirRecordedBucket() throws Exception {
		@SuppressWarnings({"rawtypes", "unchecked"
		})
		final DataOperationsBuilderImpl<DataItem, DataOperation<DataItem>> read = new DataOperationsBuilderImpl(0);
		read.opType(OpType.READ);
		read.inputPath("/bench");

		final var recorded = read.buildOp(new DataItemImpl("/bench-2/prefix/key", 99, 1024));
		final var manifest = read.buildOp(new IntegrityManifestDataItem("bench-3", "prefix/key", 1024, null));
		final var relative = read.buildOp(new DataItemImpl("key", 1, 1024));

		assertEquals("/bench-2/prefix", recorded.srcPath());
		assertEquals("/bench-2/prefix", recorded.dstPath());
		assertEquals("/bench-3", manifest.srcPath());
		assertEquals("/bench", relative.srcPath(), "item.input.path stays literal");
	}
}
