package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TargetBucketSetTest {

	@Test
	void shippedDefaultIsOneBucket() throws Exception {
		assertEquals(1, TargetBucketSet.count(TargetBucketProvisioningTest.stepConfig("create", "/bench")));
	}

	@Test
	void aCommandLineStringIsCoercedThroughTheMergedSchema() throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		// --storage-targetBucket-count=12 reaches the configuration as this dashed path and string.
		config.val("storage-targetBucket-count", "12");

		assertEquals(12, TargetBucketSet.count(config));
		assertEquals(12, config.configVal("storage").intVal("targetBucket-count"));
	}

	@ParameterizedTest
	@ValueSource(ints = {0, -1, 1001
	})
	void countsOutsideOneToAThousandAreRejected(final int count) throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		config.val(TargetBucketSet.COUNT, count);

		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketSet.count(config));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("between 1 and 1000, not " + count), failure.getMessage());
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 2, 10, 11, 1000
	})
	void countsFromOneToAThousandAreAccepted(final int count) throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		config.val(TargetBucketSet.COUNT, count);

		assertEquals(count, TargetBucketSet.count(config));
		assertEquals(count, TargetBucketSet.memberPaths("/bench", count).size());
	}

	@Test
	void oneBucketKeepsThePathExactlyAsConfigured() {
		for (final var path : Arrays.asList("/bench", "bench/", "//bench//a/", "", null)) {
			assertEquals(Arrays.asList(path), TargetBucketSet.memberPaths(path, 1));
		}
	}

	@Test
	void membersReplaceOnlyTheBucketSegmentOfTheNormalizedPath() {
		final var expected = List.of("/bench-0", "/bench-1", "/bench-2", "/bench-3");
		assertEquals(expected, TargetBucketSet.memberPaths("/bench", 4));
		assertEquals(expected, TargetBucketSet.memberPaths("bench/", 4));
		assertEquals(expected, TargetBucketSet.memberPaths("//bench//", 4));
		assertEquals(
						List.of("/bench-0/a/b", "/bench-1/a/b"), TargetBucketSet.memberPaths("/bench/a/b/", 2));
	}

	@Test
	void indicesAreUnpaddedSoAMemberNameNeverDependsOnTheCount() {
		final var ten = TargetBucketSet.memberPaths("/ps", 10);
		final var eleven = TargetBucketSet.memberPaths("/ps", 11);

		assertEquals("/ps-1", ten.get(1));
		assertEquals("/ps-1", eleven.get(1));
		assertEquals("/ps-10", eleven.get(10));
		assertEquals(ten, eleven.subList(0, 10));
	}

	@Test
	void theNameRulesApplyToTheExpandedNameNotTheBase() {
		assertEquals("/ab-0", TargetBucketSet.memberPaths("/ab", 2).get(0));
		assertEquals("/a-0", TargetBucketSet.memberPaths("/a", 2).get(0));
	}

	@Test
	void sixtyThreeCharactersIsTheLongestMemberName() {
		final var base = "b".repeat(61);
		assertEquals("/" + base + "-9", TargetBucketSet.memberPaths("/" + base, 10).get(9));

		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketSet.memberPaths("/" + base, 11));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("\"" + base + "-10\""), failure.getMessage());
		assertTrue(failure.getMessage().contains("3 to 63 characters"), failure.getMessage());
	}

	@Test
	void eachForbiddenRuleRejectsTheGeneratedName() {
		assertNameRejected("/a..b", "two adjacent periods");
		assertNameRejected("/xn--bench", "reserved prefix \"xn--\"");
		assertNameRejected("/sthree-bench", "reserved prefix \"sthree-\"");
		assertNameRejected("/amzn-s3-demo-bench", "reserved prefix \"amzn-s3-demo-\"");
		assertNameRejected("/Bench", "only lowercase letters");
		assertNameRejected("/bench_x", "only lowercase letters");
		assertNameRejected("/-bench", "start and end with a lowercase letter or digit");
		assertNameRejected("/", "requires item.output.path to name a base bucket");
	}

	@Test
	void rulesAGeneratedSuffixCannotReachAreStillChecked() {
		assertEquals("it must not be formatted as an IP address", TargetBucketSet.nameProblem("192.168.5.4"));
		assertTrue(TargetBucketSet.nameProblem("bench-s3alias").contains("-s3alias"));
		assertTrue(TargetBucketSet.nameProblem("bench--ol-s3").contains("--ol-s3"));
		assertTrue(TargetBucketSet.nameProblem("bench.mrap").contains(".mrap"));
		assertTrue(TargetBucketSet.nameProblem("bench--x-s3").contains("--x-s3"));
		assertTrue(TargetBucketSet.nameProblem("bench--table-s3").contains("--table-s3"));
		assertNull(TargetBucketSet.nameProblem("bench.v2-0"));
	}

	private static void assertNameRejected(final String base, final String reason) {
		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketSet.memberPaths(base, 4));
		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains(reason), failure.getMessage());
	}
}
