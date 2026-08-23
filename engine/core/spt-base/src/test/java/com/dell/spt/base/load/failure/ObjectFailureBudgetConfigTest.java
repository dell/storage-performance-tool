package com.dell.spt.base.load.failure;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_FIXED;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_FAILURE_POLICY_MODE_PERCENTAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ObjectFailureBudgetConfigTest {
	@Test
	void wireModesUseSharedVocabularyAndRejectEnumSpellingAliases() {
		final var config = TestConfigBuilder.config();
		config.val("load-op-failureBudget-mode", DELETE_FAILURE_POLICY_MODE_FIXED);
		assertEquals(
						DELETE_FAILURE_POLICY_MODE_FIXED,
						ObjectFailureBudgetConfig.from(config.configVal("load")).mode().wireValue());

		config.val("load-op-failureBudget-mode", DELETE_FAILURE_POLICY_MODE_PERCENTAGE);
		assertEquals(
						DELETE_FAILURE_POLICY_MODE_PERCENTAGE,
						ObjectFailureBudgetConfig.from(config.configVal("load")).mode().wireValue());

		config.val("load-op-failureBudget-mode", "PERCENTAGE");
		assertThrows(
						IllegalConfigurationException.class,
						() -> ObjectFailureBudgetConfig.from(config.configVal("load")));
	}

	@Test
	void shippedDefaultsAreNewFixedObjectUnitsAndLeaveLegacyControlsUntouched() {
		final var config = TestConfigBuilder.config();
		final var budget = ObjectFailureBudgetConfig.from(config.configVal("load"));
		assertEquals(ObjectFailureBudgetMode.FIXED, budget.mode());
		assertEquals(100_000L, budget.maxFailedObjects());
		assertEquals(Duration.ofSeconds(30), budget.grace());
		assertEquals(100_000L, config.longVal("load-op-limit-fail-count"));
		assertEquals(false, config.boolVal("load-op-limit-fail-rate"));
	}

	@Test
	void percentageAcceptsInclusiveRangeAndRejectsInvalidConfiguration() {
		final var config = TestConfigBuilder.config();
		config.val("load-op-failureBudget-mode", "percentage");
		config.val("load-op-failureBudget-maxFailurePercent", 0.0);
		assertEquals(0.0, ObjectFailureBudgetConfig.from(config.configVal("load")).maxFailurePercent());
		config.val("load-op-failureBudget-maxFailurePercent", 100.0);
		assertEquals(100.0, ObjectFailureBudgetConfig.from(config.configVal("load")).maxFailurePercent());
		config.val("load-op-failureBudget-maxFailurePercent", 100.1);
		assertThrows(IllegalConfigurationException.class,
						() -> ObjectFailureBudgetConfig.from(config.configVal("load")));
	}

	@Test
	void percentageDescriptionDistinguishesStrictZeroFromPositiveGrace() {
		assertEquals(
						"failed-object percentage limit 0.0% enforced immediately",
						ObjectFailureBudgetConfig.percentage(0, Duration.ofSeconds(30)).description());
		assertEquals(
						"failed-object percentage limit 1.5% after PT30S",
						ObjectFailureBudgetConfig.percentage(1.5, Duration.ofSeconds(30)).description());
	}
}
