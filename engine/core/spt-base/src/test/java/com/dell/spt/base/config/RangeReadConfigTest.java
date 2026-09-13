package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.Constants;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

class RangeReadConfigTest {
	private Config defaults() throws Exception {
		final var schema = SchemaProvider.resolve(Constants.APP_NAME, getClass().getClassLoader()).stream().findFirst().orElseThrow();
		return ConfigUtil.loadConfig(Path.of(getClass().getResource("/config/defaults.yaml").toURI()).toFile(), schema);
	}

	private Config enabled() throws Exception {
		final var config = defaults();
		config.val("load-op-type", "read");
		config.val("storage-driver-type", "s3");
		config.val("load-op-read-range-size", "64KiB");
		return config;
	}

	@Test
	void shippedDefaultsAndLegacyConfigsRemainDisabled() throws Exception {
		final var config = defaults();
		for (final var leaf : List.of("size", "offset", "align")) {
			assertNull(config.stringVal("load-op-read-range-" + leaf));
		}
		assertNull(RangeReadConfig.validate(config));
		assertNull(RangeReadConfig.fromLoad(TestConfigBuilder.config().configVal("load")));
	}

	@Test
	void engineCliCoercionPreservesExplicitZeroAndArbitraryAlignment() throws Exception {
		final var config = enabled();
		CliArgUtil.parseArgs("--load-op-read-range-size=64KiB", "--load-op-read-range-offset=0",
						"--load-op-read-range-align=3").forEach(config::val);
		assertEquals(new RangeReadPolicy(65536, 0L, 3), RangeReadConfig.validate(config));
		config.val("load-op-read-range-align", 0);
		assertEquals(0, config.val("load-op-read-range-align"));
		assertEquals(1, RangeReadConfig.validate(config).alignment());
		config.val("load-op-read-range-offset", null);
		assertNull(RangeReadConfig.validate(config).fixedOffset());
	}

	@Test
	void offsetAndAlignmentWithoutSizeAreNotSilentlyIgnored() throws Exception {
		for (final var leaf : List.of("offset", "align")) {
			final var config = defaults();
			config.val("load-op-read-range-" + leaf, "0");
			assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
		}
	}

	@Test
	void malformedAndOverflowingPolicyFailsAtConfiguration() throws Exception {
		for (final var size : List.of("0", "-1", "1.5KiB", "8EiB", "9223372036854775808", "")) {
			final var config = enabled();
			config.val("load-op-read-range-size", size);
			assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config), size);
		}
		final var config = enabled();
		config.val("load-op-read-range-offset", Long.toString(Long.MAX_VALUE));
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
		config.val("load-op-read-range-offset", "1");
		config.val("load-op-read-range-align", "3");
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
	}

	@Test
	void unsupportedDriversFailInRealStepConstructorBeforeInitialization() throws Exception {
		for (final var driver : List.of("s3-aws", "s3-rdma", "fs", "swift")) {
			final var config = enabled();
			config.val("storage-driver-type", driver);
			final var failure = assertThrows(IllegalConfigurationException.class,
							() -> new LinearLoadStepLocal(config, List.of(), null, mock(MetricsManager.class)));
			assertTrue(failure.getMessage().contains("Netty s3"));
		}
	}

	@Test
	void enabledReadConstructsButWorkloadAndLegacyConflictsFail() throws Exception {
		final var config = enabled();
		assertDoesNotThrow(() -> new LinearLoadStepLocal(config, List.of(), null, mock(MetricsManager.class)));
		for (final var op : List.of("create", "update", "list", "delete", "noop")) {
			config.val("load-op-type", op);
			assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
		}
		config.val("load-op-type", "read");
		for (final var path : List.of("item-data-verify", "load-op-recycle-content-update")) {
			config.val(path, true);
			assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
			config.val(path, false);
		}
		config.val("item-data-ranges-threshold", "1KiB");
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
		config.val("item-data-ranges-threshold", 0);
		config.val("item-data-ranges-random", 1);
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
		config.val("item-data-ranges-random", 0);
		config.val("item-data-ranges-fixed", List.of("0-1"));
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
	}

	@Test
	void mixedStepAndAppendedUnsupportedContextFailBeforeInitialization() throws Exception {
		final var config = enabled();
		assertThrows(IllegalConfigurationException.class, () -> new LinearLoadStepLocal(config, List.of(), null, mock(MetricsManager.class)) {
			@Override
			public String getTypeName() {
				return ConfigUtil.MIXED_LOAD_STEP_TYPE;
			}
		});
		final var context = enabled();
		context.val("storage-driver-type", "s3-aws");
		assertThrows(IllegalConfigurationException.class, () -> new LinearLoadStepLocal(config, List.of(), List.of(context), mock(MetricsManager.class)));
		config.val("storage-integrity-mode", "metadata");
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.validate(config));
	}

	@Test
	void configuredPolicyCannotFallThroughToOrdinaryDriver() {
		final var policy = new RangeReadPolicy(1, null, 1);
		assertThrows(IllegalConfigurationException.class, () -> RangeReadConfig.requireMatchingRuntime(policy, null));
		assertThrows(IllegalConfigurationException.class,
						() -> RangeReadConfig.requireMatchingRuntime(policy, new RangeReadPolicy(2, null, 1)));
		assertDoesNotThrow(() -> RangeReadConfig.requireMatchingRuntime(policy, policy));
		assertDoesNotThrow(() -> RangeReadConfig.requireMatchingRuntime(null, null));
	}
}
