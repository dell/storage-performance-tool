package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The one-time warning that a size limit cannot stop a load whose operations transfer no body. */
class LoadStepContextSizeLimitWarningTest {
	private static final String WARNING = "load.step.limit.size counts transferred bytes";
	private static final String FENCE = "size-limit-warning-fence";

	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"DELETE", "STAT", "NOOP"
	})
	void bodylessPrimaryTypeWarnsOnce(final OpType type) throws Exception {
		final List<String> warnings = warningsWhileBuilding(
						"bodyless-" + type, config(type, "1MB"), metrics(type), null);
		assertEquals(1, warnings.size(), warnings.toString());
		final String warning = warnings.get(0);
		assertTrue(warning.startsWith("bodyless-" + type + ": "), warning);
		assertTrue(warning.contains(type + " operations transfer none"), warning);
		assertTrue(warning.contains("size limit of 1MiB"), warning);
		assertTrue(warning.contains(
						"it ends at its count limit, the step's time limit, or the end of its items"), warning);
	}

	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"CREATE", "READ", "UPDATE"
	})
	void bodyTransferringTypeDoesNotWarn(final OpType type) throws Exception {
		assertEquals(List.of(), warningsWhileBuilding("body-" + type, config(type, "1MB"), metrics(type), null));
	}

	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"DELETE", "STAT", "NOOP"
	})
	void zeroSizeLimitDoesNotWarn(final OpType type) throws Exception {
		assertEquals(List.of(), warningsWhileBuilding("unlimited-" + type, config(type, "0"), metrics(type), null));
	}

	@Test
	void standaloneDeleteWarns() throws Exception {
		final Config config = config(OpType.DELETE, "1MB");
		config.val("item-output-file", "");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", 2);
		config.val("load-op-delete-duration", false);
		config.val("load-step-limit-time", "0s");
		config.val("load-op-recycle-mode", false);
		config.val("load-op-limit-count", 0L);
		// The standalone flag decides here: the primary context reports no type.
		final MetricsContext metrics = mock(MetricsContext.class, RETURNS_DEEP_STUBS);
		when(metrics.opType()).thenReturn(null);
		final List<String> warnings = warningsWhileBuilding("standalone-delete", config, metrics, null);
		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).contains("DELETE operations transfer none"), warnings.get(0));
	}

	@Test
	void mixedStepWarnsOnlyForABodylessPrimaryType() throws Exception {
		final Map<OpType, MetricsContext> deleteFirst = new EnumMap<>(OpType.class);
		deleteFirst.put(OpType.DELETE, metrics(OpType.DELETE));
		deleteFirst.put(OpType.STAT, metrics(OpType.STAT));
		final List<String> deleteWarnings = warningsWhileBuilding(
						"mixed-delete", config(OpType.CREATE, "1MB"), deleteFirst.get(OpType.DELETE), deleteFirst);
		assertEquals(1, deleteWarnings.size(), deleteWarnings.toString());
		assertTrue(deleteWarnings.get(0).contains("DELETE operations transfer none"), deleteWarnings.get(0));

		final Map<OpType, MetricsContext> readFirst = new EnumMap<>(OpType.class);
		readFirst.put(OpType.READ, metrics(OpType.READ));
		readFirst.put(OpType.DELETE, metrics(OpType.DELETE));
		assertEquals(List.of(), warningsWhileBuilding(
						"mixed-read", config(OpType.CREATE, "1MB"), readFirst.get(OpType.READ), readFirst));
	}

	@Test
	void contextWithoutMetricsDoesNotThrow() throws Exception {
		final List<String> warnings = assertDoesNotThrow(
						() -> warningsWhileBuilding("no-metrics", config(OpType.DELETE, "1MB"), null, null));
		assertEquals(List.of(), warnings);
	}

	private static Config config(final OpType type, final String sizeLimit) {
		final Config config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-data-ranges-concat", null);
		config.val("load-op-type", type.name().toLowerCase(Locale.ROOT));
		config.val("load-op-retry", false);
		config.val("load-op-wait-finish", false);
		config.val("load-step-limit-size", sizeLimit);
		return config;
	}

	private static MetricsContext metrics(final OpType type) {
		final MetricsContext metrics = mock(MetricsContext.class, RETURNS_DEEP_STUBS);
		when(metrics.opType()).thenReturn(type);
		return metrics;
	}

	@SuppressWarnings("unchecked")
	private static List<String> warningsWhileBuilding(
					final String id,
					final Config config,
					final MetricsContext metrics,
					final Map<OpType, MetricsContext> metricsByType) throws Exception {
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		when(driver.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		when(driver.supportsStandaloneDeleteRequests()).thenReturn(true);
		doNothing().when(driver).operationResultOutput(any());
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		when(generator.isNothingPendingRetry()).thenReturn(true);
		final Logger logger = (Logger) Loggers.ERR;
		final Appender appender = mock(Appender.class);
		when(appender.getName()).thenReturn("size-limit-warning-" + id);
		when(appender.isStarted()).thenReturn(true);
		final List<String> warnings = new ArrayList<>();
		final CountDownLatch fence = new CountDownLatch(1);
		doAnswer(call -> {
			final String message = ((LogEvent) call.getArgument(0)).getMessage().getFormattedMessage();
			if (message.equals(FENCE + " " + id)) {
				fence.countDown();
			} else if (message.contains(WARNING)) {
				synchronized (warnings) {
					warnings.add(message);
				}
			}
			return null;
		}).when(appender).append(any());
		final Level previousLevel = logger.getLevel();
		Configurator.setLevel(logger.getName(), Level.WARN);
		logger.addAppender(appender);
		try {
			final var context = new LoadStepContextImpl<>(
							id, generator, driver, metrics, metricsByType, config.configVal("load"), false, ListShardMetricsRecorder.NO_OP);
			context.close();
			// Same-thread fence: drains asynchronous logging before the captured warnings are read.
			logger.warn("{} {}", FENCE, id);
			assertTrue(fence.await(5, TimeUnit.SECONDS), "log fence not seen");
		} finally {
			logger.removeAppender(appender);
			Configurator.setLevel(logger.getName(), previousLevel);
		}
		synchronized (warnings) {
			return List.copyOf(warnings);
		}
	}
}
