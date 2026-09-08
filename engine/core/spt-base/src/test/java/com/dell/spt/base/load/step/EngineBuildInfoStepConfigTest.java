package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.buildinfo.EngineBuildInfoProvider;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.metrics.MetricsManager;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EngineBuildInfoStepConfigTest {
	@Test
	void scenarioOverridesCannotRedefineStepOrAppendedContextIdentity() {
		final var config = TestConfigBuilder.config();
		EngineBuildInfoProvider.global().projectVersion(config, false);
		final var client = new LinearLoadStepClient(config, List.of(), null, mock(MetricsManager.class));
		final LoadStepBase configured = client.config(Map.of("run", Map.of("version", "9.9.9")));
		final LoadStepBase appended = client.append(Map.of("run", Map.of("version", "8.8.8")));
		assertEquals(EngineBuildInfoProvider.global().snapshot().version(), configured.config.stringVal("run-version"));
		assertEquals(EngineBuildInfoProvider.global().snapshot().version(), appended.ctxConfigs.get(0).stringVal("run-version"));
	}

	@Test
	void workerStepProjectsReceivedConfigurationWithoutMutatingCaller() {
		final var config = TestConfigBuilder.config();
		config.val("run-version", "9.9.9");
		final var context = TestConfigBuilder.config();
		context.val("run-version", "8.8.8");
		final LoadStepBase local = new LinearLoadStepLocal(config, List.of(), List.of(context), mock(MetricsManager.class));
		assertEquals(EngineBuildInfoProvider.global().snapshot().version(), local.config.stringVal("run-version"));
		assertEquals(EngineBuildInfoProvider.global().snapshot().version(), local.ctxConfigs.get(0).stringVal("run-version"));
		assertEquals("9.9.9", config.stringVal("run-version"));
		assertEquals("8.8.8", context.stringVal("run-version"));
	}
}
