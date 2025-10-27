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
		base.val("item-naming-type", "serial");
		base.val("item-naming-seed", 100L);
		base.val("item-naming-step", 5);

		final List<Config> slices = List.of(new BasicConfig(base), new BasicConfig(base), new BasicConfig(base));

		ConfigSliceUtil.sliceItemNaming(slices);

		assertEquals(100L, slices.get(0).longVal("item-naming-seed"));
		assertEquals(105L, slices.get(1).longVal("item-naming-seed"));
		assertEquals(110L, slices.get(2).longVal("item-naming-seed"));
		assertEquals(15L, slices.get(0).longVal("item-naming-step"));
		assertEquals(15L, slices.get(1).longVal("item-naming-step"));
		assertEquals(15L, slices.get(2).longVal("item-naming-step"));
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
		final Config withoutNaming = new BasicConfig(base.pathSep(), base.schema(), tree);
		final List<Config> slices = List.of(new BasicConfig(withoutNaming), new BasicConfig(withoutNaming));

		assertThrows(NoSuchElementException.class, () -> slices.get(0).configVal("item-naming"));

		assertDoesNotThrow(() -> ConfigSliceUtil.sliceItemNaming(slices));

		assertThrows(NoSuchElementException.class, () -> slices.get(0).configVal("item-naming"));
		assertThrows(NoSuchElementException.class, () -> slices.get(0).val("item-naming-seed"));
		assertThrows(NoSuchElementException.class, () -> slices.get(1).val("item-naming-step"));
	}
}
