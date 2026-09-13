package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.script.ScriptEngine;
import org.junit.jupiter.api.Test;

class ScenarioRangeRegistrationTest {
	@Test
	void dormantAliasesRegisterButInvalidSelectedClientsRejectBeforeInitialization() throws Exception {
		final Config config;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			config = ConfigUtil.loadConfig(new String(defaults.readAllBytes(), StandardCharsets.UTF_8), ConfigFormat.YAML,
							SchemaProvider.resolveAndReduce(Constants.APP_NAME, getClass().getClassLoader()));
		}
		config.val("load-step-id", "range-bindings-" + UUID.randomUUID());
		config.val("load-op-type", "read");
		config.val("load-op-read-range-size", "3");
		config.val("storage-driver-type", "s3");
		final var initialized = new AtomicBoolean();
		final var metrics = mock(MetricsManager.class);
		class Client extends LinearLoadStepClient {
			Client(Config value) {
				super(value, List.of(), List.of(), metrics);
			}

			@Override
			protected void init() {
				initialized.set(true);
				throw new IllegalStateException("initialization probe");
			}
		}
		final LoadStepFactory<?, LinearLoadStepClient> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("Load");
		when(factory.createClient(any(), anyList(), any())).thenAnswer(call -> new Client(call.getArgument(0)));
		final LoadStepFactory<?, LinearLoadStepClient> mixedFactory = mock(LoadStepFactory.class);
		when(mixedFactory.id()).thenReturn("MixedLoad");
		when(mixedFactory.createClient(any(), anyList(), any())).thenAnswer(call -> new Client(call.getArgument(0)) {
			@Override
			public String getTypeName() {
				return "MixedLoad";
			}
		});
		final Map<String, Object> bindings = new HashMap<>();
		final var engine = mock(ScriptEngine.class);
		doAnswer(call -> {
			bindings.put(call.getArgument(0), call.getArgument(1));
			return null;
		}).when(engine).put(anyString(), any());
		try {
			assertDoesNotThrow(() -> ScenarioUtil.registerStepTypes(engine, List.of(factory, mixedFactory), config, metrics));
			for (String alias : List.of("CreateLoad", "ReadVerifyLoad", "ReadRandomRangeLoad", "UpdateLoad", "MixedLoad")) {
				assertThrows(IllegalConfigurationException.class, () -> ((LoadStep) bindings.get(alias)).start());
				assertFalse(initialized.get());
			}
			var read = (LoadStep) bindings.get("ReadLoad");
			assertThrows(IllegalStateException.class, read::start);
			assertTrue(initialized.get(), "A selected valid READ must reach initialization");
			verifyNoInteractions(metrics);
		} finally {
			for (Object binding : bindings.values())
				((LoadStep) binding).close();
		}
	}
}
