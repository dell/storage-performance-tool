package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for default AUTO behavior in ListShardingConfig. */
class ListShardingConfigAutoTest {

	@Test
	void defaultModeIsAutoWhenUnset() throws Exception {
		final Map<String, Object> listCfg = new HashMap<>();
		final Map<String, Object> sharding = new HashMap<>();
		// no explicit mode; keep profile default to exercise auto
		sharding.put("profile", "safe");
		listCfg.put("sharding", sharding);
		final Map<String, Object> schema = Map.of("sharding", Map.of("profile", String.class));
		final Config list = new BasicConfig("/", schema, listCfg);

		final ListShardingConfig cfg = ListShardingConfig.parse(list);
		assertNotNull(cfg);
		assertEquals(ListShardingMode.AUTO, cfg.mode());
	}
}
