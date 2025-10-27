package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ListShardingConfigTest {

	private static final int DEFAULT_BASE62_LEN = 62;

	@Test
	void defaultModeIsAutoWhenUnset() throws IllegalConfigurationException {
		final Config listConfig = new BasicConfig(
						"-",
						Map.of("sharding", Map.of()),
						Map.of("sharding", Map.of()));
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(ListShardingMode.AUTO, parsed.mode());
		assertEquals(DEFAULT_BASE62_LEN, parsed.alphabet().length);
		assertEquals(DEFAULT_BASE62_LEN, parsed.radix());
	}

	@Test
	void offProfileDisablesShardingByDefault() throws IllegalConfigurationException {
		final Config listConfig = new BasicConfig(
						"-",
						Map.of("sharding", Map.of("profile", String.class)),
						Map.of("sharding", Map.of("profile", "off")));
		final var parsed = ListShardingConfig.parse(listConfig);
		// Profiles are ignored for defaulting; we default to AUTO now
		assertEquals(ListShardingMode.AUTO, parsed.mode());
	}

	// profile-based behavior removed

	@Test
	void explicitModeOverridesDefault() throws IllegalConfigurationException {
		final Map<String, Object> shardingSchema = Map.of("mode", String.class);
		final Map<String, Object> shardingValues = Map.of("mode", "auto");
		final Config listConfig = new BasicConfig("-", Map.of("sharding", shardingSchema), Map.of("sharding", shardingValues));
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(ListShardingMode.AUTO, parsed.mode());
	}

	@Test
	void staticShardsRespectRadixAndPrefix() throws IllegalConfigurationException {
		final Config listConfig = new BasicConfig(
						"-",
						Map.of(
										"sharding",
										Map.of(
														"mode", String.class,
														"radix", Integer.class,
														"charset", String.class)),
						Map.of("sharding", Map.of("mode", "static", "radix", 4, "charset", "abcd")));
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(ListShardingMode.STATIC, parsed.mode());
		assertEquals(Duration.ofSeconds(30), parsed.stallTimeout());
		assertEquals(Duration.ofSeconds(10), parsed.progressLogInterval());
		assertEquals(
						ListShardingConfig.AdaptiveHeuristicsConfig.DEFAULT_FULL_PAGE_THRESHOLD,
						parsed.adaptive().fullPageThreshold());
		final List<ListShard> shards = parsed.createStaticShards("logs/");
		assertEquals(4, shards.size());
		assertEquals("logs/a", shards.get(0).prefix());
		assertEquals("logs/d", shards.get(3).prefix());
	}

	@Test
	void defaultsFallbackWhenShardingAbsent() throws IllegalConfigurationException {
		final Config listConfig = new BasicConfig("-", Map.of(), Map.of());
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(ListShardingMode.AUTO, parsed.mode());
		assertEquals(Duration.ZERO, parsed.stallTimeout());
		assertEquals(Duration.ZERO, parsed.progressLogInterval());
		assertEquals(
						ListShardingConfig.AdaptiveHeuristicsConfig.DEFAULT_FULL_PAGE_THRESHOLD,
						parsed.adaptive().fullPageThreshold());
	}

	@Test
	void parsesAdaptiveHeuristicOverrides() throws IllegalConfigurationException {
		final var adaptiveSchema = new java.util.HashMap<String, Object>();
		adaptiveSchema.put("full_page_threshold", Integer.class);
		adaptiveSchema.put("stall_warning_threshold", Integer.class);
		adaptiveSchema.put("backlog_ratio", Double.class);
		adaptiveSchema.put("min_pending_shards", Integer.class);
		adaptiveSchema.put("cooldown", String.class);
		final var shardingSchema = new java.util.HashMap<String, Object>();
		shardingSchema.put("mode", String.class);
		shardingSchema.put("radix", Integer.class);
		shardingSchema.put("adaptive", adaptiveSchema);
		final var schema = new java.util.HashMap<String, Object>();
		schema.put("sharding", shardingSchema);

		final var adaptiveValues = new java.util.HashMap<String, Object>();
		adaptiveValues.put("full_page_threshold", 6);
		adaptiveValues.put("stall_warning_threshold", 3);
		adaptiveValues.put("backlog_ratio", 2.5);
		adaptiveValues.put("min_pending_shards", 10);
		adaptiveValues.put("cooldown", "5s");
		final var shardingValues = new java.util.HashMap<String, Object>();
		shardingValues.put("mode", "static");
		shardingValues.put("radix", 0);
		shardingValues.put("adaptive", adaptiveValues);
		final var values = new java.util.HashMap<String, Object>();
		values.put("sharding", shardingValues);
		final Config listConfig = new BasicConfig("-", schema, values);
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(6, parsed.adaptive().fullPageThreshold());
		assertEquals(3, parsed.adaptive().stallWarningThreshold());
		assertEquals(2.5d, parsed.adaptive().backlogSplitRatio(), 1e-6);
		assertEquals(10, parsed.adaptive().minPendingShards());
		assertEquals(Duration.ofSeconds(5), parsed.adaptive().cooldown());
	}

	@Test
	void splitLogRateParsesFromNestedLogSection() throws IllegalConfigurationException {
		final var logSchema = new java.util.HashMap<String, Object>();
		logSchema.put("rate", Integer.class);
		final var splitSchema = new java.util.HashMap<String, Object>();
		splitSchema.put("log", logSchema);
		final var shardingSchema = new java.util.HashMap<String, Object>();
		shardingSchema.put("mode", String.class);
		shardingSchema.put("radix", Integer.class);
		shardingSchema.put("charset", String.class);
		shardingSchema.put("split", splitSchema);
		final var schema = new java.util.HashMap<String, Object>();
		schema.put("sharding", shardingSchema);

		final var shardingValues = new java.util.HashMap<String, Object>();
		shardingValues.put("mode", "static");
		shardingValues.put("radix", 2);
		shardingValues.put("charset", "ab");
		shardingValues.put("split", Map.of("log", Map.of("rate", 5)));
		final var values = new java.util.HashMap<String, Object>();
		values.put("sharding", shardingValues);
		final Config listConfig = new BasicConfig("-", schema, values);
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(5, parsed.splitLogRate());
	}

	@Test
	void splitLogRateFallsBackToLegacySampleKey() throws IllegalConfigurationException {
		final var logSchema = new java.util.HashMap<String, Object>();
		logSchema.put("sample", Integer.class);
		final var splitSchema = new java.util.HashMap<String, Object>();
		splitSchema.put("log", logSchema);
		final var shardingSchema = new java.util.HashMap<String, Object>();
		shardingSchema.put("mode", String.class);
		shardingSchema.put("radix", Integer.class);
		shardingSchema.put("charset", String.class);
		shardingSchema.put("split", splitSchema);
		final var schema = new java.util.HashMap<String, Object>();
		schema.put("sharding", shardingSchema);

		final var shardingValues = new java.util.HashMap<String, Object>();
		shardingValues.put("mode", "static");
		shardingValues.put("radix", 2);
		shardingValues.put("charset", "ab");
		shardingValues.put("split", Map.of("log", Map.of("sample", 7)));
		final var values = new java.util.HashMap<String, Object>();
		values.put("sharding", shardingValues);
		final Config listConfig = new BasicConfig("-", schema, values);
		final var parsed = ListShardingConfig.parse(listConfig);
		assertEquals(7, parsed.splitLogRate());
	}

	@Test
	void splitPagesFallsBackToTopLevelKey() throws Exception {
		final Map<String, Object> sharding = new java.util.HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("split_pages", 99);
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"split_pages", Integer.class));
		final Config list = new BasicConfig("/", schema, Map.of("sharding", sharding));

		final ListShardingConfig cfg = ListShardingConfig.parse(list);
		assertEquals(99, cfg.splitPages());
	}

	@Test
	void splitPagesZeroDefaultsToBaseline() throws Exception {
		final Map<String, Object> sharding = new java.util.HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("split", Map.of("pages", 0));
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"split", Map.of("pages", Integer.class)));
		final Config list = new BasicConfig("/", schema, Map.of("sharding", sharding));

		final ListShardingConfig cfg = ListShardingConfig.parse(list);
		assertEquals(50, cfg.splitPages());
	}

	@Test
	void summaryEnabledSupportsAllSynonyms() throws Exception {
		final Map<String, Object> baseSchema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"summary", Boolean.class,
										"summary_enabled", Boolean.class,
										"summary-enabled", Boolean.class));

		final Map<String, Object> summaryOnly = new java.util.HashMap<>();
		summaryOnly.put("mode", "auto");
		summaryOnly.put("summary", true);
		final ListShardingConfig summaryCfg = ListShardingConfig.parse(new BasicConfig("/", baseSchema, Map.of("sharding", summaryOnly)));
		assertTrue(summaryCfg.summaryEnabled());

		final Map<String, Object> underscore = new java.util.HashMap<>();
		underscore.put("mode", "auto");
		underscore.put("summary_enabled", true);
		final ListShardingConfig underscoreCfg = ListShardingConfig.parse(new BasicConfig("/", baseSchema, Map.of("sharding", underscore)));
		assertTrue(underscoreCfg.summaryEnabled());

		final Map<String, Object> hyphen = new java.util.HashMap<>();
		hyphen.put("mode", "auto");
		hyphen.put("summary-enabled", true);
		final ListShardingConfig hyphenCfg = ListShardingConfig.parse(new BasicConfig("/", baseSchema, Map.of("sharding", hyphen)));
		assertTrue(hyphenCfg.summaryEnabled());

		final Map<String, Object> absent = new java.util.HashMap<>();
		absent.put("mode", "auto");
		final ListShardingConfig absentCfg = ListShardingConfig.parse(new BasicConfig("/", baseSchema, Map.of("sharding", absent)));
		assertFalse(absentCfg.summaryEnabled());
	}

	@Test
	void queueMaxDefaultsWhenMissing() throws Exception {
		final Map<String, Object> sharding = new java.util.HashMap<>();
		sharding.put("mode", "auto");
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of("mode", String.class));
		final Config list = new BasicConfig("/", schema, Map.of("sharding", sharding));

		final ListShardingConfig cfg = ListShardingConfig.parse(list);
		assertEquals(10_000_000, cfg.queueMax());
	}

	@Test
	void splitPagesMalformedShouldFail() {
		final Map<String, Object> sharding = new java.util.HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("split", Map.of("pages", "not-a-number"));
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"split", Map.of("pages", String.class)));
		final Config list = new BasicConfig("/", schema, Map.of("sharding", sharding));

		assertThrows(IllegalConfigurationException.class, () -> ListShardingConfig.parse(list));
	}

	@Test
	void queueMaxMalformedShouldFail() {
		final Map<String, Object> sharding = new java.util.HashMap<>();
		sharding.put("mode", "auto");
		sharding.put("queue_max", "NaN");
		final Map<String, Object> schema = Map.of(
						"sharding",
						Map.of(
										"mode", String.class,
										"queue_max", String.class));
		final Config list = new BasicConfig("/", schema, Map.of("sharding", sharding));

		assertThrows(IllegalConfigurationException.class, () -> ListShardingConfig.parse(list));
	}
}
