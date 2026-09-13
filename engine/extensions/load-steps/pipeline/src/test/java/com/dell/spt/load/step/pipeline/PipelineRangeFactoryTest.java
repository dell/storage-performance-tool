package com.dell.spt.load.step.pipeline;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.range.RangeReadDriverFactory;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.charset.StandardCharsets;
import com.dell.spt.base.config.ConfigFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "unchecked"
})
class PipelineRangeFactoryTest {
	@Test
	void mergedContextPolicyReachesRangeFactory() throws Exception {
		final var baseSchema = SchemaProvider.resolveAndReduce(Constants.APP_NAME, getClass().getClassLoader());
		final var schema = TreeUtil.reduceForest(List.of(baseSchema, new PipelineLoadStepExtension().schemaProvider().schema()));
		final com.github.akurilov.confuse.Config baseConfig;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			baseConfig = ConfigUtil.loadConfig(new String(defaults.readAllBytes(), StandardCharsets.UTF_8), ConfigFormat.YAML, schema);
		}
		baseConfig.val("load-step-id", "range-factory-" + java.util.UUID.randomUUID());
		baseConfig.val("load-op-type", "read");
		baseConfig.val("storage-driver-type", "s3");
		baseConfig.val("load-op-read-range-size", "13");

		final var override = new BasicConfig(baseConfig.pathSep(), schema,
						Map.of("load", Map.of("op", Map.of("read", Map.of("range",
										Map.of("size", "17", "offset", "6", "align", "3"))))));
		final RangeReadDriverFactory factory = mock(RangeReadDriverFactory.class);
		when(factory.id()).thenReturn("s3");
		final var observed = new AtomicReference<RangeReadPolicy>();
		when(factory.createRangeRead(anyString(), any(), any(), anyInt(), any())).thenAnswer(call -> {
			observed.set(call.getArgument(4));
			throw new IllegalConfigurationException("construction probe");
		});
		final var step = new PipelineLoadStepLocal(baseConfig, List.of(factory), List.of(override), mock(MetricsManager.class));
		try {
			assertThrows(IllegalStateException.class, step::init);
			assertEquals(new RangeReadPolicy(17, 6L, 3), observed.get());
			verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
		} finally {
			step.close();
		}
	}
}
