package com.dell.spt.base.metrics.steady;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.config.CliArgUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.github.akurilov.confuse.Config;
import org.junit.jupiter.api.Test;

class SteadySettingsTest {

	@Test
	void defaultsAreTheCalibratedValues() {
		assertEquals(new SteadySettings(1, 20, 20_000), SteadySettings.DEFAULTS);
		assertEquals(
						SteadySettings.DEFAULTS,
						SteadySettings.fromMetricsConfig(TestConfigBuilder.config().configVal("output-metrics")));
	}

	@Test
	void engineCommandLineArgumentsOverrideEachSetting() {
		final Config config = TestConfigBuilder.config();
		CliArgUtil.parseArgs(
						"--output-metrics-steady-warmup-turnovers=2",
						"--output-metrics-steady-min-turnovers=0",
						"--output-metrics-steady-min-duration=5s")
						.forEach(config::val);
		assertEquals(
						new SteadySettings(2, 0, 5_000),
						SteadySettings.fromMetricsConfig(config.configVal("output-metrics")));
	}

	@Test
	void durationsAcceptUnitsAndPlainSeconds() {
		final Config config = TestConfigBuilder.config();
		config.val("output-metrics-steady-min-duration", "2m");
		assertEquals(120_000, SteadySettings.fromMetricsConfig(config.configVal("output-metrics")).minDurationMillis());
		config.val("output-metrics-steady-min-duration", "0s");
		assertEquals(0, SteadySettings.fromMetricsConfig(config.configVal("output-metrics")).minDurationMillis());
		config.val("output-metrics-steady-min-duration", 7);
		assertEquals(7_000, SteadySettings.fromMetricsConfig(config.configVal("output-metrics")).minDurationMillis());
	}

	@Test
	void negativeOrMalformedValuesAreConfigurationErrors() {
		assertThrows(IllegalConfigurationException.class, () -> new SteadySettings(-1, 20, 20_000));
		assertThrows(IllegalConfigurationException.class, () -> new SteadySettings(1, -0.5, 20_000));
		assertThrows(IllegalConfigurationException.class, () -> new SteadySettings(1, 20, -1));
		assertThrows(IllegalConfigurationException.class, () -> new SteadySettings(Double.NaN, 20, 20_000));
		final Config config = TestConfigBuilder.config();
		config.val("output-metrics-steady-min-duration", "soon");
		assertThrows(
						IllegalConfigurationException.class,
						() -> SteadySettings.fromMetricsConfig(config.configVal("output-metrics")));
	}
}
