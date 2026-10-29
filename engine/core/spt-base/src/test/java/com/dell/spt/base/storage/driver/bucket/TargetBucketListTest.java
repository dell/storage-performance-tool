package com.dell.spt.base.storage.driver.bucket;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/** The bucket list, storage.targetBucket.names, as the source of a target bucket set. */
class TargetBucketListTest {

	@TempDir
	Path tempDir;

	private static Config config(final String... names) throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/placeholder/prefix");
		config.val(TargetBucketSet.NAMES, List.of(names));
		return config;
	}

	@Test
	void shippedDefaultIsAnEmptyListSoTheSetIsGenerated() throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/bench");

		assertEquals(List.of(), TargetBucketSet.names(config));
		assertEquals(List.of(), TargetBucketSet.namesOfStorage(config.configVal("storage")));
		assertEquals(1, TargetBucketSet.count(config));
		config.val(TargetBucketSet.COUNT, 4);
		assertEquals(4, TargetBucketSet.count(config), "an empty list falls back to the generated count");
	}

	@Test
	void aCommandLineStringIsCoercedThroughTheMergedSchemaToAList() throws Exception {
		final var config = TargetBucketProvisioningTest.stepConfig("create", "/bench");
		// --storage-targetBucket-names=alpha,beta reaches the configuration as this dashed path and string.
		config.val("storage-targetBucket-names", "alpha,beta");

		assertEquals(List.of("alpha", "beta"), TargetBucketSet.names(config));
		assertEquals(List.of("alpha", "beta"), TargetBucketSet.namesOfStorage(config.configVal("storage")));
		assertEquals(2, TargetBucketSet.count(config));
		assertEquals(2, TargetBucketSet.countOfStorage(config.configVal("storage")));
	}

	@Test
	void aYamlListAndTheDashedSetterReachTheSamePath() throws Exception {
		final var defaults = Files.writeString(tempDir.resolve("defaults.yaml"),
						"storage:\n  targetBucket:\n    names:\n      - ps-eu\n      - ps-us\n");
		final var schema = TargetBucketProvisioningTest.stepConfig("create", "/bench").schema();

		final var loaded = ConfigUtil.loadConfig(defaults.toFile(), schema);

		assertEquals(List.of("ps-eu", "ps-us"), TargetBucketSet.names(loaded));
		assertEquals(List.of("ps-eu", "ps-us"), loaded.configVal("storage").listVal("targetBucket-names"));
	}

	@Test
	void aConfigurationWithoutTheSettingHasNoList() {
		final Config bare = new BasicConfig("-", Map.of(), Map.of());

		assertEquals(List.of(), TargetBucketSet.names(bare));
		assertEquals(1, TargetBucketSet.count(bare));
	}

	@ParameterizedTest
	@ValueSource(ints = {1, 2, 10, 1000
	})
	void listsFromOneToAThousandNamesAreAccepted(final int size) throws Exception {
		final var names = IntStream.range(0, size).mapToObj(i -> "bucket-" + i).toArray(String[]::new);
		final var config = config(names);

		assertEquals(size, TargetBucketSet.count(config));
		assertEquals(List.of(names), TargetBucketSet.names(config));
		assertEquals(size, TargetBucketSet.memberPaths("/placeholder", size, List.of(names)).size());
	}

	@Test
	void aListOfAThousandAndOneNamesIsRejected() throws Exception {
		final var names = IntStream.range(0, 1001).mapToObj(i -> "bucket-" + i).toArray(String[]::new);

		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketSet.count(config(names)));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("lists 1001 buckets, at most 1000"), failure.getMessage());
	}

	@ParameterizedTest
	@CsvSource(delimiter = '|', ignoreLeadingAndTrailingWhitespace = false, value = {
			"''|must not be empty",
			"a/b|must not contain '/'",
			"a,b|must not contain ','",
			"a b|must not contain whitespace",
			"a\tb|must not contain whitespace",
			" a|must not contain whitespace",
			"a${x}|expression marker ${",
			"#{x}|expression marker #{",
			"%{\"x\"}|expression marker %{"
	})
	void eachRejectedShapeFailsAtConfigurationTime(final String name, final String reason) throws Exception {
		final var config = config("good-one", name);

		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketSet.count(config));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("storage.targetBucket.names[1]"), failure.getMessage());
		assertTrue(failure.getMessage().contains(reason), failure.getMessage());
	}

	@Test
	void anExactDuplicateIsRejectedAndNamesThatDifferByCaseAreDistinct() throws Exception {
		final var failure = assertThrows(RunTerminalException.class,
						() -> TargetBucketSet.count(config("alpha", "beta", "alpha")));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("[2] \"alpha\" repeats an earlier name"), failure.getMessage());
		assertEquals(2, TargetBucketSet.count(config("Alpha", "alpha")));
	}

	@Test
	void namesTheAwsRulesRejectAreAcceptedAsWritten() throws Exception {
		final var names = List.of("PS_EU", "ab", "under_score", "UPPER", "xn--x", "a..b", "-lead");
		final var config = config(names.toArray(String[]::new));

		assertEquals(names, TargetBucketSet.names(config));
		assertEquals(names.size(), TargetBucketSet.count(config));
		assertEquals(List.of("/PS_EU/p", "/ab/p", "/under_score/p", "/UPPER/p", "/xn--x/p", "/a..b/p", "/-lead/p"),
						TargetBucketSet.memberPaths("/ignored/p", names.size(), names));
	}

	@Test
	void aListTogetherWithACountAboveOneIsRejectedButACountOfOneIsNot() throws Exception {
		final var config = config("alpha", "beta");
		config.val(TargetBucketSet.COUNT, 4);

		final var failure = assertThrows(RunTerminalException.class, () -> TargetBucketSet.count(config));

		assertEquals(Category.CONFIGURATION, failure.category());
		assertTrue(failure.getMessage().contains("storage.targetBucket.names lists 2 buckets and cannot be combined "
						+ "with storage.targetBucket.count 4"), failure.getMessage());
		assertThrows(RunTerminalException.class,
						() -> TargetBucketSet.countOfStorage(config.configVal("storage")));
		config.val(TargetBucketSet.COUNT, 1);
		assertEquals(2, TargetBucketSet.count(config), "the shipped count of 1 is no conflict");
	}

	@Test
	void membersAreTheNamesInOrderUnderTheNormalizedPath() {
		final var names = List.of("zeta", "Alpha", "mid_1");

		assertEquals(List.of("/zeta/a/b", "/Alpha/a/b", "/mid_1/a/b"),
						TargetBucketSet.memberPaths("placeholder/a/b/", 3, names));
		assertEquals(List.of("/zeta/a/b", "/Alpha/a/b", "/mid_1/a/b"),
						TargetBucketSet.memberPaths("//placeholder/a/b//", 3, names));
		assertEquals(
						TargetBucketSet.memberPaths("//bench//a/b//", 3).stream().map(path -> path.replaceFirst("^/bench-\\d+", "")).toList(),
						TargetBucketSet.memberPaths("//placeholder//a/b//", 3, names).stream().map(path -> path.replaceFirst("^/[^/]+", "")).toList(),
						"the rest of the path is kept exactly as a generated set keeps it");
		assertEquals(List.of("/zeta", "/Alpha", "/mid_1"), TargetBucketSet.memberPaths("/placeholder", 3, names));
		assertEquals(List.of("/zeta", "/Alpha", "/mid_1"), TargetBucketSet.memberPaths(null, 3, names));
		assertEquals(List.of("/zeta", "/Alpha", "/mid_1"), TargetBucketSet.memberPaths("", 3, names));
		assertEquals(names, TargetBucketSet.memberPaths("/p", 3, names).stream().map(path -> path.substring(1)).toList(),
						"the order of the list is the member index order");
	}

	@Test
	void theListReplacesTheGeneratedNamesNotTheirPathRules() {
		assertEquals(List.of("/bench-0", "/bench-1"), TargetBucketSet.memberPaths("/bench", 2, List.of()),
						"an empty list generates the set from the count");
		assertEquals(List.of("/bench-0/a", "/bench-1/a"), TargetBucketSet.memberPaths("/bench/a", 2, List.of()));
	}

	@Test
	void aPlaceholderWithAnExpressionIsNeverEvaluatedForAMultiNameList() {
		assertEquals(List.of("/one/p", "/two/p"),
						TargetBucketSet.memberPaths("/%{\"a/b\"}-x/p", 2, List.of("one", "two")));
		assertEquals(List.of("/one/p", "/two/p"),
						TargetBucketSet.memberPaths("/${int64:random()}/p", 2, List.of("one", "two")));
	}

	@Test
	void aOneNameListReplacesOnlyTheBucketSegmentAndKeepsTheRestAsWritten() {
		for (final var path : Arrays.asList("/ps", "ps/", "/ps/a/b/", "ps/a/${int64:random()}")) {
			assertEquals(Arrays.asList(path), TargetBucketSet.memberPaths(path, 1, List.of("ps")),
							"a placeholder equal to the name leaves the path exactly as written");
		}
		assertEquals(List.of("/ps-eu/a/b/"), TargetBucketSet.memberPaths("/other/a/b/", 1, List.of("ps-eu")));
		assertEquals(List.of("ps-eu/"), TargetBucketSet.memberPaths("other/", 1, List.of("ps-eu")));
		assertEquals(List.of("/ps-eu/a/${int64:random()}"),
						TargetBucketSet.memberPaths("/other/a/${int64:random()}", 1, List.of("ps-eu")),
						"a one-name list keeps today's single-bucket expression support after the bucket");
		assertEquals(List.of("/ps-eu"), TargetBucketSet.memberPaths(null, 1, List.of("ps-eu")));
	}

	@Test
	void theSetIsTheSameForTheStepAndTheStorageViews() throws Exception {
		final var config = config("alpha", "beta", "gamma");
		final var storage = config.configVal("storage");

		assertEquals(TargetBucketSet.count(config), TargetBucketSet.countOfStorage(storage));
		assertEquals(TargetBucketSet.names(config), TargetBucketSet.namesOfStorage(storage));
		final var all = new ArrayList<>(TargetBucketSet.names(config));
		assertEquals(List.of("alpha", "beta", "gamma"), all);
	}
}
