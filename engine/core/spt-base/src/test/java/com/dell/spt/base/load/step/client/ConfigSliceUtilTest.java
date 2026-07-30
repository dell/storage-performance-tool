package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.TestConfigBuilder;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ConfigSliceUtil item naming slicing")
class ConfigSliceUtilTest {

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
