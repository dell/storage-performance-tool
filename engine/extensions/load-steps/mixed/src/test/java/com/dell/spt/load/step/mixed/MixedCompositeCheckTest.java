package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The entry engine checks a mixed step's composite configuration for the op types its weights
 * make, not for its {@code load-op-type}: READ for a GET weight, CREATE for a PUT weight.
 */
class MixedCompositeCheckTest {

	@Test
	void theCheckedOpTypesFollowTheGetAndPutWeights() throws Exception {
		final Config config = defaultConfig();
		config.val("load-op-type", "create");
		final var client = new MixedLoadStepClient(config, List.of(), List.of(), mock(MetricsManager.class));
		assertEquals(Set.of(OpType.READ, OpType.CREATE), types(client, config, 1, 1, 1, 1));
		assertEquals(Set.of(OpType.READ), types(client, config, 1, 0, 1, 0));
		assertEquals(Set.of(OpType.CREATE), types(client, config, 0, 1, 0, 1));
		assertEquals(Set.of(), types(client, config, 0, 0, 1, 1));
	}

	private static Set<OpType> types(
					final MixedLoadStepClient client, final Config config, final int get, final int put, final int delete,
					final int stat) {
		config.val("load-op-weights-get", get);
		config.val("load-op-weights-put", put);
		config.val("load-op-weights-delete", delete);
		config.val("load-op-weights-stat", stat);
		return client.compositeCheckedOpTypes(config);
	}

	/** The shipped defaults of the engine and of this extension, as the engine merges them. */
	private static Config defaultConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		return ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedCompositeCheckTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
