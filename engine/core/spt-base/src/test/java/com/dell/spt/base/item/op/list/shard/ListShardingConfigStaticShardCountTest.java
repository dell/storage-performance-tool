package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ListShardingConfigStaticShardCountTest {

	@Test
	void radixDefaultsToCharsetLength() throws Exception {
		final Map<String, Object> listCfg = new HashMap<>();
		final Map<String, Object> sharding = new HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("charset", "abcd"); // length = 4
		sharding.put("radix", 0);         // should default to charset length
		listCfg.put("sharding", sharding);

		final java.util.Map<String, Object> schema = java.util.Map.of(
						"sharding",
						java.util.Map.of(
										"mode", String.class,
										"charset", String.class,
										"radix", Integer.class));
		final Config list = new BasicConfig("/", schema, listCfg);
		final ListShardingConfig cfg = ListShardingConfig.parse(list);

		final List<ListShard> shards = cfg.createStaticShards("logs/");
		assertEquals(4, shards.size());
		assertEquals("logs/a", shards.get(0).prefix());
	}
}
