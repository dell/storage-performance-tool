package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The limits each metrics row of a mixed step carries. The rows are operation types of the one
 * context, which runs on every sub-load over the base, so every row takes the merged count and
 * bucket settings, and the base's time limit, which the step stops on.
 */
class MixedLoadStepMetricsLimitsTest {

	private static final long BASE_COUNT = 20_000L;
	private static final String BASE_TIME = "12s";

	@TempDir
	Path home;

	@BeforeEach
	void setHome() {
		ThreadContext.put("home_dir", home.toString());
	}

	@AfterEach
	void clearHome() {
		ThreadContext.remove("home_dir");
	}

	@Test
	void everyRowTakesAnAppendedCount() throws Exception {
		final List<DistributedMetricsContext> rows = initializedRows(Map.of("load", Map.of("op", Map.of("limit", Map.of("count", 3_000L)))));

		assertEquals(List.of(OpType.READ, OpType.CREATE, OpType.DELETE), rows.stream().map(MetricsContext::opType).toList());
		for (final DistributedMetricsContext row : rows) {
			assertEquals(3_000L, limit(row, MetricsConstants.METADATA_LIMIT_OP_COUNT), row.opType().name());
			assertEquals(12L, limit(row, MetricsConstants.METADATA_LIMIT_TIME_SEC), row.opType().name());
		}
	}

	@Test
	void everyRowKeepsTheBasesTimeOverAnAppendedTime() throws Exception {
		final List<DistributedMetricsContext> rows = initializedRows(Map.of("load", Map.of("step", Map.of("limit", Map.of("time", "4s")))));

		assertEquals(3, rows.size());
		for (final DistributedMetricsContext row : rows) {
			assertEquals(12L, limit(row, MetricsConstants.METADATA_LIMIT_TIME_SEC), row.opType().name());
			assertEquals(BASE_COUNT, limit(row, MetricsConstants.METADATA_LIMIT_OP_COUNT), row.opType().name());
		}
	}

	@Test
	void withoutAppendEveryRowTakesTheBasesLimits() throws Exception {
		final List<DistributedMetricsContext> rows = initializedRows(null);

		assertEquals(3, rows.size());
		for (final DistributedMetricsContext row : rows) {
			assertEquals(BASE_COUNT, limit(row, MetricsConstants.METADATA_LIMIT_OP_COUNT), row.opType().name());
			assertEquals(12L, limit(row, MetricsConstants.METADATA_LIMIT_TIME_SEC), row.opType().name());
		}
	}

	@Test
	void everyRowPastTheContextsHasABucketSourceFromAnAppendedBucketCount() throws Exception {
		final List<DistributedMetricsContext> rows = initializedRows(Map.of("storage", Map.of("targetBucket", Map.of("count", 2))));

		assertEquals(3, rows.size());
		for (final DistributedMetricsContext row : rows) {
			assertNotNull(row.bucketMetricsSource(), row.opType().name());
		}
	}

	@Test
	void aSlicesRowsTakeTheMergedCount() throws Exception {
		final Config sliceBase = stepConfig();
		final var slice = new MixedLoadStepLocal(
						sliceBase,
						List.of(new MixedLoadStepExtension(), new DummyStorageDriverMockExtension<>()),
						appendedContexts(Map.of("load", Map.of("op", Map.of("limit", Map.of("count", 1_500L))))),
						mock(MetricsManager.class));
		try {
			slice.init();
			final List<MetricsContext<?>> rows = metricsContextsOf(slice);
			assertEquals(3, rows.size());
			for (final MetricsContext<?> row : rows) {
				assertEquals(1_500L, limit(row, MetricsConstants.METADATA_LIMIT_OP_COUNT), row.opType().name());
				assertEquals(12L, limit(row, MetricsConstants.METADATA_LIMIT_TIME_SEC), row.opType().name());
			}
		} finally {
			slice.close();
		}
	}

	/** The rows of a client built with {@code append(context)}, or with no append when it is null. */
	private List<DistributedMetricsContext> initializedRows(final Map<String, Object> context) throws Exception {
		MixedLoadStepClient client = new MixedLoadStepClient(stepConfig(), List.of(), null, mock(MetricsManager.class));
		if (context != null) {
			client = client.append(context);
		}
		client.init();
		return metricsContextsOf(client).stream().map(row -> (DistributedMetricsContext) row).toList();
	}

	private static long limit(final MetricsContext<?> row, final String key) {
		final Object value = row.metadata().get(key);
		assertNotNull(value, key);
		return ((Number) value).longValue();
	}

	/** The contexts of a mixed step after one {@code append(context)}, as sent to a slice. */
	@SuppressWarnings("unchecked")
	private List<Config> appendedContexts(final Map<String, Object> context) throws Exception {
		final MixedLoadStepClient step = new MixedLoadStepClient(stepConfig(), List.of(), null, null).append(context);
		final Field field = LoadStepBase.class.getDeclaredField("ctxConfigs");
		field.setAccessible(true);
		return ((List<Config>) field.get(step)).stream().map(ctx -> (Config) new BasicConfig(ctx)).toList();
	}

	@SuppressWarnings("unchecked")
	private static List<MetricsContext<?>> metricsContextsOf(final LoadStepBase step) throws ReflectiveOperationException {
		final Field field = LoadStepBase.class.getDeclaredField("metricsContexts");
		field.setAccessible(true);
		return (List<MetricsContext<?>>) field.get(step);
	}

	/** GET, PUT and DELETE over a pool of seed items, with a count and a time limit on the base. */
	private Config stepConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		final Config config = ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
		config.val("load-step-id", "mixed-limits-" + System.nanoTime());
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", BASE_TIME);
		config.val("load-op-limit-count", BASE_COUNT);
		config.val("load-op-weights-get", 1);
		config.val("load-op-weights-put", 2);
		config.val("load-op-weights-delete", 1);
		config.val("load-op-weights-stat", 0);
		config.val("storage-driver-type", "dummy-mock");
		config.val("storage-targetBucket-provision", false);
		config.val("item-data-size", "1KB");
		config.val("item-output-path", "/bucket");
		final Path seeds = home.resolve("seeds.csv");
		if (!Files.exists(seeds)) {
			Files.write(seeds, List.of(new DataItemImpl("/bucket/seed-0", 0, 1024).toString()));
		}
		config.val("item-input-file", seeds.toString());
		return config;
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedLoadStepMetricsLimitsTest.class.getResourceAsStream(name)) {
			return new String(Objects.requireNonNull(stream, name).readAllBytes(), StandardCharsets.UTF_8);
		}
	}
}
