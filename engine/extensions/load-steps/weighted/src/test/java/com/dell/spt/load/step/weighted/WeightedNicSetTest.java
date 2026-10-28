package com.dell.spt.load.step.weighted;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.nic.NicSetDriverFactory;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** A context outside the NIC-set matrix fails before the step constructs its driver. */
@SuppressWarnings({"rawtypes", "unchecked"
})
class WeightedNicSetTest {
	@Test
	void copyContextFailsBeforeTheDriver() throws Exception {
		final var baseSchema = SchemaProvider.resolveAndReduce(Constants.APP_NAME, getClass().getClassLoader());
		final var schema = TreeUtil.reduceForest(List.of(baseSchema, new WeightedLoadStepExtension().schemaProvider().schema()));
		final com.github.akurilov.confuse.Config baseConfig;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			baseConfig = ConfigUtil.loadConfig(new String(defaults.readAllBytes(), StandardCharsets.UTF_8), ConfigFormat.YAML, schema);
		}
		baseConfig.val("load-step-id", "nic-set-" + java.util.UUID.randomUUID());
		baseConfig.val("load-op-type", "read");
		baseConfig.val("storage-driver-type", "s3");
		baseConfig.val("storage-net-interfaces", List.of("ens2np0"));
		baseConfig.val("load-op-weight", 1);
		final var copy = new BasicConfig(baseConfig.pathSep(), schema, Map.of(
						"load", Map.of("op", Map.of("type", "create")),
						"item", Map.of("input", Map.of("path", "/source"))));
		final NicSetDriverFactory factory = mock(NicSetDriverFactory.class);
		when(factory.id()).thenReturn("s3");
		final var step = new WeightedLoadStepLocal(baseConfig, List.of(factory), List.of(copy), mock(MetricsManager.class));
		try {
			final var failure = assertThrows(IllegalStateException.class, step::init);
			assertInstanceOf(IllegalConfigurationException.class, failure.getCause());
			assertTrue(failure.getCause().getMessage().contains("storage.net.interfaces"), failure.getCause().getMessage());
			verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
		} finally {
			step.close();
		}
	}
}
