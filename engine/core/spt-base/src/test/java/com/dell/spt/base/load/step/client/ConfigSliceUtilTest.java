package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.TestConfigBuilder;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ConfigSliceUtil slicing")
class ConfigSliceUtilTest {

	private static final List<String> ENDPOINTS = List.of(
					"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000",
					"10.0.0.4:9000", "10.0.0.5:9000", "10.0.0.6:9000");

	/**
	 * Each row: slice count, endpoint count, and for every slice the indexes of the endpoints it
	 * targets after slicing. Slices are split into gcd(slices, endpoints) groups, and every slice of
	 * group g targets group g's endpoints.
	 */
	static Stream<Arguments> storageNodeAddrsSlices() {
		return Stream.of(
						Arguments.of(1, 1, List.of(List.of(0))),
						Arguments.of(2, 4, List.of(List.of(0, 1), List.of(2, 3))),
						Arguments.of(4, 2, List.of(List.of(0), List.of(0), List.of(1), List.of(1))),
						Arguments.of(4, 4, List.of(List.of(0), List.of(1), List.of(2), List.of(3))),
						Arguments.of(
										4, 6,
										List.of(List.of(0, 1, 2), List.of(0, 1, 2), List.of(3, 4, 5), List.of(3, 4, 5))),
						Arguments.of(3, 2, List.of(List.of(0, 1), List.of(0, 1), List.of(0, 1))),
						Arguments.of(2, 3, List.of(List.of(0, 1, 2), List.of(0, 1, 2))));
	}

	@ParameterizedTest(name = "{0} slices, {1} endpoints")
	@MethodSource("storageNodeAddrsSlices")
	@DisplayName("Every slice targets its own endpoint group")
	void sliceStorageNodeAddrsAssignsEverySliceItsGroup(
					final int sliceCount, final int endpointCount, final List<List<Integer>> expectedIndexes) {
		final List<String> endpoints = ENDPOINTS.subList(0, endpointCount);
		final Config base = configWithStorageNodeAddrs(endpoints);
		final List<Config> slices = new ArrayList<>();
		for (int i = 0; i < sliceCount; i++) {
			slices.add(ConfigSliceUtil.initSlice(base));
		}

		ConfigSliceUtil.sliceStorageNodeAddrs(slices, endpoints);

		assertEquals(sliceCount, slices.size());
		for (int i = 0; i < sliceCount; i++) {
			final List<String> expected = expectedIndexes.get(i).stream().map(endpoints::get).toList();
			assertEquals(
							expected,
							slices.get(i).<String> listVal("storage-net-node-addrs"),
							"slice #" + i + " of " + sliceCount + " with " + endpointCount + " endpoints");
		}
	}

	/**
	 * The base schema leaves storage.net.node.addrs to the Netty driver extension, so the test adds
	 * it, copying each map on the way so the schema cached by {@link TestConfigBuilder} is untouched.
	 */
	@SuppressWarnings("unchecked")
	private static Config configWithStorageNodeAddrs(final List<String> endpoints) {
		final Config base = TestConfigBuilder.config();
		final Map<String, Object> schemaTree = new HashMap<>(base.schema());
		final Map<String, Object> storageSchema = new HashMap<>((Map<String, Object>) schemaTree.get("storage"));
		final Map<String, Object> netSchema = new HashMap<>((Map<String, Object>) storageSchema.get("net"));
		final Map<String, Object> nodeSchema = new HashMap<>((Map<String, Object>) netSchema.get("node"));
		nodeSchema.put("addrs", List.class);
		netSchema.put("node", nodeSchema);
		storageSchema.put("net", netSchema);
		schemaTree.put("storage", storageSchema);
		final Map<String, Object> tree = new HashMap<>(Config.deepToMap(base));
		final Map<String, Object> storage = new HashMap<>((Map<String, Object>) tree.get("storage"));
		final Map<String, Object> net = new HashMap<>((Map<String, Object>) storage.get("net"));
		final Map<String, Object> node = new HashMap<>((Map<String, Object>) net.get("node"));
		node.put("addrs", endpoints);
		node.put("slice", true);
		net.put("node", node);
		storage.put("net", net);
		tree.put("storage", storage);
		return new BasicConfig(base.pathSep(), schemaTree, tree);
	}

	@Test
	@DisplayName("Serial item naming distributes seed and step per slice")
	void sliceSerialNamingDistributesSeedAndStep() {
		final Config base = TestConfigBuilder.config();
		final List<Config> slices = List.of(new BasicConfig(base), new BasicConfig(base), new BasicConfig(base));
		slices.forEach(
						config -> {
							config.val("item-naming-type", "serial");
							config.val("item-naming-seed", 100L);
							config.val("item-naming-step", 5);
						});
		assertEquals("serial", slices.get(0).stringVal("item-naming-type"));
		assertEquals(100L, slices.get(0).longVal("item-naming-seed"));
		assertEquals(5, slices.get(0).intVal("item-naming-step"));

		ConfigSliceUtil.sliceItemNaming(slices);

		assertEquals(100L, slices.get(0).longVal("item-naming-seed"));
		assertEquals(105L, slices.get(1).longVal("item-naming-seed"));
		assertEquals(110L, slices.get(2).longVal("item-naming-seed"));
		assertEquals(15L, slices.get(0).longVal("item-naming-step"));
		assertEquals(15L, slices.get(1).longVal("item-naming-step"));
		assertEquals(15L, slices.get(2).longVal("item-naming-step"));
	}

	@Test
	@DisplayName("Base slice preserves the positive run ID")
	void initSlicePreservesRunId() {
		final Config base = TestConfigBuilder.config();
		base.val("run-id", 987654321L);

		final Config slice = ConfigSliceUtil.initSlice(base);

		assertEquals(987654321L, slice.longVal("run-id"));
		assertTrue(slice.<String> listVal("load-step-node-addrs").isEmpty());
	}

	@Test
	@DisplayName("Missing item naming config is tolerated without side effects")
	@SuppressWarnings("unchecked")
	void sliceItemNamingMissingConfigNoop() {
		final Config base = TestConfigBuilder.config();
		final Map<String, Object> tree = new HashMap<>(Config.deepToMap(base));
		final Map<String, Object> itemMap = new HashMap<>((Map<String, Object>) tree.get("item"));
		itemMap.remove("naming");
		tree.put("item", itemMap);
		final Map<String, Object> schemaTree = new HashMap<>(base.schema());
		final Map<String, Object> itemSchema = new HashMap<>((Map<String, Object>) schemaTree.get("item"));
		itemSchema.remove("naming");
		schemaTree.put("item", itemSchema);
		final Config withoutNaming = new BasicConfig(base.pathSep(), schemaTree, tree);
		final List<Config> slices = List.of(new BasicConfig(withoutNaming), new BasicConfig(withoutNaming));

		assertDoesNotThrow(() -> ConfigSliceUtil.sliceItemNaming(slices));

		assertThrows(NoSuchElementException.class, () -> slices.get(0).val("item-naming-seed"));
		assertThrows(NoSuchElementException.class, () -> slices.get(1).val("item-naming-step"));
	}
}
