package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.storage.driver.range.RangeReadDriverFactory;
import java.util.List;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"rawtypes", "unchecked"
})
class RangeReadDriverFactoryTest {
	@Test
	void oldSignatureAndDisabledOverloadUseTheExistingFactory() throws Exception {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val("driver-type", "third-party");
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		when(factory.id()).thenReturn("third-party");
		when(factory.create("step", null, config, false, 32)).thenReturn(driver);
		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step"));
		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step", null));
		verify(factory, times(2)).create("step", null, config, false, 32);
	}

	@Test
	void rangePolicyIsDeliveredOnlyToExplicitRangeConstruction() throws Exception {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val("driver-type", "s3");
		final RangeReadDriverFactory factory = mock(RangeReadDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		final var policy = new RangeReadPolicy(65536, 0L, 4096);
		when(factory.id()).thenReturn("s3");
		when(factory.createRangeRead("step", null, config, 32, policy)).thenReturn(driver);
		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step", policy));
		verify(factory).createRangeRead("step", null, config, 32, policy);
		verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
	}

	@Test
	void realLinearStepDeliversTheConfiguredPolicyBeforeGeneratorInitialization() throws Exception {
		final com.github.akurilov.confuse.Config stepConfig;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			stepConfig = com.dell.spt.base.config.ConfigUtil.loadConfig(
							new String(defaults.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8),
							com.dell.spt.base.config.ConfigFormat.YAML,
							com.github.akurilov.confuse.SchemaProvider.resolveAndReduce(com.dell.spt.base.Constants.APP_NAME, getClass().getClassLoader()));
		}
		stepConfig.val("load-step-id", "range-factory-" + java.util.UUID.randomUUID());
		stepConfig.val("storage-driver-type", "s3");
		stepConfig.val("load-op-type", "read");
		stepConfig.val("load-op-read-range-size", "13");
		stepConfig.val("load-op-read-range-offset", "6");
		stepConfig.val("load-op-read-range-align", "3");
		stepConfig.val("item-data-ranges-threshold", 0);
		stepConfig.val("item-data-verify", false);
		stepConfig.val("item-data-input-compressibility", 0.0);
		final RangeReadDriverFactory factory = mock(RangeReadDriverFactory.class);
		when(factory.id()).thenReturn("s3");
		final var observed = new java.util.concurrent.atomic.AtomicReference<RangeReadPolicy>();
		when(factory.createRangeRead(anyString(), any(), any(), anyInt(), any())).thenAnswer(call -> {
			observed.set(call.getArgument(4));
			throw new IllegalConfigurationException("construction probe");
		});
		class Step extends com.dell.spt.base.load.step.linear.LinearLoadStepLocal {
			Step() {
				super(stepConfig, List.of(factory), List.of(), mock(com.dell.spt.base.metrics.MetricsManager.class));
			}

			void initialize() {
				init();
			}
		}
		final var step = new Step();
		try {
			final var failure = assertThrows(IllegalStateException.class, step::initialize);
			assertNotNull(observed.get(), () -> failure.toString() + " cause: " + failure.getCause());
			assertEquals(new RangeReadPolicy(13, 6L, 3), observed.get());
			verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
		} finally {
			step.close();
		}
	}

	@Test
	void disabledRangeCapableFactoryStillUsesOrdinaryConstruction() throws Exception {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val("driver-type", "s3");
		final RangeReadDriverFactory factory = mock(RangeReadDriverFactory.class);
		final StorageDriver driver = mock(StorageDriver.class);
		when(factory.id()).thenReturn("s3");
		when(factory.create("step", null, config, false, 32)).thenReturn(driver);
		assertSame(driver, StorageDriver.instance(List.of(factory), config, null, false, 32, "step", null));
		verify(factory, never()).createRangeRead(anyString(), any(), any(), anyInt(), any());
	}

	@Test
	void absentOptInRejectsBeforeOrdinaryConstruction() throws Exception {
		final var config = TestConfigBuilder.config().configVal("storage");
		config.val("driver-type", "s3");
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		when(factory.id()).thenReturn("s3");
		assertThrows(IllegalConfigurationException.class, () -> StorageDriver.instance(
						List.of(factory), config, null, false, 32, "step", new RangeReadPolicy(1, null, 1)));
		verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
	}

	@Test
	void verificationAndDeferredDriversCannotUseRangeConstruction() throws Exception {
		final var config = TestConfigBuilder.config().configVal("storage");
		final RangeReadDriverFactory factory = mock(RangeReadDriverFactory.class);
		for (final String id : List.of("s3", "s3-aws", "s3-rdma", "other")) {
			config.val("driver-type", id);
			when(factory.id()).thenReturn(id);
			assertThrows(IllegalConfigurationException.class, () -> StorageDriver.instance(
							List.of(factory), config, null, id.equals("s3"), 32, "step", new RangeReadPolicy(1, null, 1)));
		}
		verify(factory, never()).createRangeRead(anyString(), any(), any(), anyInt(), any());
		verify(factory, never()).create(anyString(), any(), any(), anyBoolean(), anyInt());
	}
}
