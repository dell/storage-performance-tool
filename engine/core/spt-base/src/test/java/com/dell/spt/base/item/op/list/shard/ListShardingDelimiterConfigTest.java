package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ListShardingDelimiterConfigTest {

	@Test
	void parsesDelimitersSplitPagesAndQueueMax() throws IllegalConfigurationException {
		final Map<String, Object> split = new HashMap<>();
		split.put("pages", 42);
		final Map<String, Object> sharding = new HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("delimiters", "/._-");
		sharding.put("queue_max", 123456);
		sharding.put("split", split);
		final Map<String, Object> listCfg = Map.of("sharding", sharding);
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"delimiters", String.class,
										"queue_max", Integer.class,
										"split", Map.of("pages", Integer.class)));
		final Config list = new BasicConfig("/", schema, listCfg);
		final ListShardingConfig cfg = ListShardingConfig.parse(list);

		assertEquals("/._-", cfg.delimiters());
		assertEquals(42, cfg.splitPages());
		assertEquals(123456, cfg.queueMax());
	}

	@Test
	void blankDelimitersFallbackToDefault() throws IllegalConfigurationException {
		final Map<String, Object> sharding = new HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("delimiters", "");
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"delimiters", String.class));
		final Config list = new BasicConfig("/", schema, Map.of("sharding", sharding));

		final ListShardingConfig cfg = ListShardingConfig.parse(list);
		assertEquals("/-_.", cfg.delimiters());
	}
}
