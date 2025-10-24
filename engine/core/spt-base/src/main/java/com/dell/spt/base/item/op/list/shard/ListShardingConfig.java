package com.dell.spt.base.item.op.list.shard;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TimeUtil;
import com.github.akurilov.confuse.Config;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;

public final class ListShardingConfig {

	private static final String DEFAULT_CHARSET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
	// Profiles are retained for backward-compatibility of config parsing but do not affect
	// defaults anymore. We keep DEFAULT_CHARSET as the base62 set for static seeding.

	private final ListShardingMode mode;
	private final int radix;
	private final char[] alphabet;
	private final Duration stallTimeout;
	private final Duration progressLogInterval;
	private final AdaptiveHeuristicsConfig adaptive;
	private final int splitLogRate;
	private final boolean summaryEnabled;
	// New delimiter-first algorithm settings
	private final String delimiters; // ordered list of delimiter characters to probe
	private final int splitPages;    // pages of 1000 keys before considering a split
	private final int queueMax;      // soft cap on queued shards

	private ListShardingConfig(
					final ListShardingMode mode,
					final int radix,
					final char[] alphabet,
					final Duration stallTimeout,
					final Duration progressLogInterval,
					final AdaptiveHeuristicsConfig adaptive,
					final int splitLogRate,
					final boolean summaryEnabled,
					final String delimiters,
					final int splitPages,
					final int queueMax) {
		this.mode = mode;
		this.radix = radix;
		this.alphabet = alphabet;
		this.stallTimeout = stallTimeout;
		this.progressLogInterval = progressLogInterval;
		this.adaptive = adaptive;
		this.splitLogRate = splitLogRate;
		this.summaryEnabled = summaryEnabled;
		this.delimiters = (delimiters == null || delimiters.isEmpty()) ? "/-_." : delimiters;
		this.splitPages = splitPages <= 0 ? 50 : splitPages;
		this.queueMax = queueMax <= 0 ? 10_000_000 : queueMax;
	}

	public ListShardingMode mode() {
		return mode;
	}

	public int radix() {
		return radix;
	}

	public char[] alphabet() {
		return alphabet;
	}

	public Duration stallTimeout() {
		return stallTimeout;
	}

	public Duration progressLogInterval() {
		return progressLogInterval;
	}

	public AdaptiveHeuristicsConfig adaptive() {
		return adaptive;
	}

	public int splitLogRate() {
		return splitLogRate;
	}

	public boolean summaryEnabled() {
		return summaryEnabled;
	}

	public String delimiters() {
		return delimiters;
	}

	public int splitPages() {
		return splitPages;
	}

	public int queueMax() {
		return queueMax;
	}

	public static ListShardingConfig parse(final Config listConfig) throws IllegalConfigurationException {
		if (listConfig == null) {
			return new ListShardingConfig(
							ListShardingMode.AUTO,
							0,
							DEFAULT_CHARSET.toCharArray(),
							Duration.ZERO,
							Duration.ZERO,
							AdaptiveHeuristicsConfig.defaults(),
							1,
							false,
							"/-_.",
							50,
							10_000_000);
		}
		final var sharding = listConfig.configVal("sharding");
		if (sharding == null) {
			return new ListShardingConfig(
							ListShardingMode.AUTO,
							0,
							DEFAULT_CHARSET.toCharArray(),
							Duration.ZERO,
							Duration.ZERO,
							AdaptiveHeuristicsConfig.defaults(),
							1,
							false,
							"/-_.",
							50,
							10_000_000);
		}
		final var mode = resolveMode(sharding);
		final var alphabet = resolveAlphabet(sharding);
		final int radix = resolveRadix(sharding, alphabet.length);
		if (radix > alphabet.length) {
			throw new IllegalConfigurationException(
							"Sharding radix "
											+ radix
											+ " exceeds available charset size "
											+ alphabet.length
											+ "; provide a larger charset or reduce the radix");
		}
		final Duration stallTimeout = parseDuration(sharding.val("stall_timeout"), Duration.ofSeconds(30));
		final Duration progressLogInterval = parseDuration(sharding.val("progress_log_interval"), Duration.ofSeconds(10));
		final AdaptiveHeuristicsConfig adaptive = AdaptiveHeuristicsConfig.parse(sharding);
		final Config splitConfig = sharding.configVal("split");
		final Config splitLogConfig = splitConfig != null ? splitConfig.configVal("log") : null;

		final Integer splitLogRateCandidate = firstNonNull(
						optionalInt(splitLogConfig, "rate"),
						optionalInt(splitLogConfig, "sample"),
						optionalInt(sharding, "split_log_rate"),
						optionalInt(sharding, "split_log_sample"));
		int splitLogRate = splitLogRateCandidate == null ? 1 : Math.max(1, splitLogRateCandidate);

		final Boolean summaryCandidate = firstNonNull(
						optionalBoolean(sharding, "summary"),
						optionalBoolean(sharding, "summary_enabled"),
						optionalBoolean(sharding, "summary-enabled"));
		final boolean summary = summaryCandidate != null ? summaryCandidate : false;

		final String delimiters = sanitizeDelimiters(optionalString(sharding, "delimiters"));

		final Integer splitPagesCandidate = firstNonNull(
						optionalInt(splitConfig, "pages"),
						optionalInt(splitConfig, "split_pages"),
						optionalInt(sharding, "split_pages"));
		int splitPages = splitPagesCandidate == null ? 50 : splitPagesCandidate;
		if (splitPages <= 0) {
			splitPages = 50;
		}

		final Integer queueMaxCandidate = optionalInt(sharding, "queue_max");
		int queueMax = queueMaxCandidate == null ? 10_000_000 : queueMaxCandidate;
		if (queueMax <= 0) {
			queueMax = 10_000_000;
		}

		return new ListShardingConfig(
						mode,
						radix,
						alphabet,
						stallTimeout,
						progressLogInterval,
						adaptive,
						splitLogRate,
						summary,
						delimiters,
						splitPages,
						queueMax);
	}

	private static ListShardingMode resolveMode(final Config sharding)
					throws IllegalConfigurationException {
		final String rawMode = stringOrNull(sharding, "mode");
		if (rawMode == null || rawMode.isEmpty()) {
			// Default to AUTO irrespective of profile to simplify semantics.
			return ListShardingMode.AUTO;
		}
		try {
			return ListShardingMode.valueOf(rawMode.toUpperCase(Locale.ROOT));
		} catch (final IllegalArgumentException e) {
			throw new IllegalConfigurationException("Unknown LIST sharding mode: " + rawMode, e);
		}
	}

	private static char[] resolveAlphabet(final Config sharding)
					throws IllegalConfigurationException {
		final String rawCharset = stringOrNull(sharding, "charset");
		final String deduped = (rawCharset == null || rawCharset.isEmpty()) ? dedupe(DEFAULT_CHARSET) : dedupe(rawCharset);
		if (deduped.isEmpty()) {
			throw new IllegalConfigurationException("Sharding charset must contain at least one character");
		}
		return deduped.toCharArray();
	}

	private static int resolveRadix(
					final Config sharding, final int alphabetLength)
					throws IllegalConfigurationException {
		int radix;
		try {
			radix = sharding.intVal("radix");
		} catch (final Exception ignore) {
			radix = 0;
		}
		if (radix <= 0) {
			radix = alphabetLength;
		}
		return radix;
	}

	// profiles removed

	private static String stringOrNull(final Config config, final String key) {
		try {
			return config.stringVal(key);
		} catch (final Exception ignore) {
			return null;
		}
	}

	private static Integer optionalInt(final Config config, final String key) throws IllegalConfigurationException {
		if (config == null || key == null) {
			return null;
		}
		final Object raw = readValue(config, key);
		if (raw == null) {
			return null;
		}
		if (raw instanceof Number number) {
			return Integer.valueOf(number.intValue());
		}
		if (raw instanceof String str) {
			if (str.isBlank()) {
				return null;
			}
			try {
				return Integer.parseInt(str.trim());
			} catch (final NumberFormatException e) {
				throw new IllegalConfigurationException(
								"Unable to parse integer value \"" + str + "\" for key \"" + key + "\"", e);
			}
		}
		throw new IllegalConfigurationException(
						"Unsupported integer value for key \"" + key + "\": " + raw);
	}

	private static Boolean optionalBoolean(final Config config, final String key)
					throws IllegalConfigurationException {
		if (config == null || key == null) {
			return null;
		}
		final Object raw = readValue(config, key);
		if (raw == null) {
			return null;
		}
		if (raw instanceof Boolean b) {
			return b;
		}
		if (raw instanceof String str) {
			if (str.isBlank()) {
				return null;
			}
			final String normalized = str.trim();
			if ("true".equalsIgnoreCase(normalized)) {
				return Boolean.TRUE;
			}
			if ("false".equalsIgnoreCase(normalized)) {
				return Boolean.FALSE;
			}
			throw new IllegalConfigurationException(
							"Unable to parse boolean value \"" + str + "\" for key \"" + key + "\"");
		}
		throw new IllegalConfigurationException(
						"Unsupported boolean value for key \"" + key + "\": " + raw);
	}

	private static String optionalString(final Config config, final String key)
					throws IllegalConfigurationException {
		if (config == null || key == null) {
			return null;
		}
		final Object raw = readValue(config, key);
		if (raw == null) {
			return null;
		}
		if (raw instanceof String str) {
			return str;
		}
		throw new IllegalConfigurationException(
						"Unsupported string value for key \"" + key + "\": " + raw);
	}

	private static Object readValue(final Config config, final String key) throws IllegalConfigurationException {
		try {
			return config.val(key);
		} catch (final NoSuchElementException e) {
			return null;
		} catch (final Exception e) {
			throw new IllegalConfigurationException("Unable to read value for key \"" + key + "\"", e);
		}
	}

	@SafeVarargs
	private static <T> T firstNonNull(final T... candidates) {
		for (final T candidate : candidates) {
			if (candidate != null) {
				return candidate;
			}
		}
		return null;
	}

	private static String sanitizeDelimiters(final String raw) {
		if (raw == null || raw.isBlank()) {
			return "/-_.";
		}
		return raw;
	}

	public List<ListShard> createStaticShards(final String seedPrefix) {
		final var normalizedBase = normalizeSeedPrefix(seedPrefix);
		final var shards = new ArrayList<ListShard>(radix);
		for (var i = 0; i < radix; i++) {
			final var suffix = Character.toString(alphabet[i]);
			final var prefix = concat(normalizedBase, suffix);
			shards.add(new ListShard(prefix, null, null, null));
		}
		return shards;
	}

	private static String dedupe(final String charsetRaw) {
		final var seen = new LinkedHashMap<Character, Boolean>();
		for (final char c : charsetRaw.toCharArray()) {
			seen.putIfAbsent(c, Boolean.TRUE);
		}
		final var builder = new StringBuilder(seen.size());
		seen.keySet().forEach(builder::append);
		return builder.toString();
	}

	// ShardingProfile concept removed entirely

	private static String normalizeSeedPrefix(final String value) {
		if (value == null || value.isEmpty()) {
			return "/";
		}
		return value;
	}

	private static String concat(final String base, final String suffix) {
		final boolean baseIsRoot = base == null || base.equals("/");
		if (suffix == null || suffix.isEmpty()) {
			return baseIsRoot ? "" : trimLeadingSlash(base);
		}
		final var sanitizedBase = baseIsRoot ? "" : trimLeadingSlash(base);
		if (sanitizedBase.isEmpty()) {
			return suffix;
		}
		return sanitizedBase.endsWith("/") ? sanitizedBase + suffix : sanitizedBase + suffix;
	}

	private static String trimLeadingSlash(final String value) {
		if (value == null) {
			return "";
		}
		var result = value;
		while (result.startsWith("/")) {
			result = result.substring(1);
		}
		return result;
	}

	private static Duration parseDuration(final Object raw, final Duration defaultValue)
					throws IllegalConfigurationException {
		if (raw == null) {
			return defaultValue;
		}
		if (raw instanceof Number) {
			final long millis = ((Number) raw).longValue();
			if (millis < 0) {
				throw new IllegalConfigurationException("Duration value must be non-negative");
			}
			return Duration.ofMillis(millis);
		}
		if (raw instanceof String) {
			final String str = ((String) raw).trim();
			if (str.isEmpty()) {
				return Duration.ZERO;
			}
			final long seconds = TimeUtil.getTimeInSeconds(str);
			if (seconds < 0) {
				throw new IllegalConfigurationException("Duration value must be non-negative");
			}
			return Duration.ofSeconds(seconds);
		}
		throw new IllegalConfigurationException("Unsupported duration value: " + raw);
	}

	public static final class AdaptiveHeuristicsConfig {

		public static final int DEFAULT_FULL_PAGE_THRESHOLD = 4;
		public static final int DEFAULT_STALL_WARNING_THRESHOLD = 2;
		public static final double DEFAULT_BACKLOG_SPLIT_RATIO = 1.5d;
		public static final int DEFAULT_MIN_PENDING_SHARDS = 4;

		private final int fullPageThreshold;
		private final int stallWarningThreshold;
		private final double backlogSplitRatio;
		private final int minPendingShards;
		private final Duration cooldown;

		private AdaptiveHeuristicsConfig(
						final int fullPageThreshold,
						final int stallWarningThreshold,
						final double backlogSplitRatio,
						final int minPendingShards,
						final Duration cooldown) {
			this.fullPageThreshold = Math.max(0, fullPageThreshold);
			this.stallWarningThreshold = Math.max(0, stallWarningThreshold);
			this.backlogSplitRatio = backlogSplitRatio <= 0 ? DEFAULT_BACKLOG_SPLIT_RATIO : backlogSplitRatio;
			this.minPendingShards = Math.max(0, minPendingShards);
			this.cooldown = cooldown == null || cooldown.isNegative() ? Duration.ZERO : cooldown;
		}

		public int fullPageThreshold() {
			return fullPageThreshold;
		}

		public int stallWarningThreshold() {
			return stallWarningThreshold;
		}

		public double backlogSplitRatio() {
			return backlogSplitRatio;
		}

		public int minPendingShards() {
			return minPendingShards;
		}

		public Duration cooldown() {
			return cooldown;
		}

		public static AdaptiveHeuristicsConfig defaults() {
			return new AdaptiveHeuristicsConfig(
							DEFAULT_FULL_PAGE_THRESHOLD,
							DEFAULT_STALL_WARNING_THRESHOLD,
							DEFAULT_BACKLOG_SPLIT_RATIO,
							DEFAULT_MIN_PENDING_SHARDS,
							Duration.ZERO);
		}

		static AdaptiveHeuristicsConfig parse(final Config shardingConfig)
						throws IllegalConfigurationException {
			if (shardingConfig == null) {
				return defaults();
			}
			final Config adaptiveConfig = shardingConfig.configVal("adaptive");
			if (adaptiveConfig == null) {
				return defaults();
			}
			final int fullPageThreshold = intOrDefault(
							takeValue(adaptiveConfig, "full_page_threshold"), DEFAULT_FULL_PAGE_THRESHOLD);
			final int stallWarningThreshold = intOrDefault(
							takeValue(adaptiveConfig, "stall_warning_threshold"), DEFAULT_STALL_WARNING_THRESHOLD);
			final double backlogRatio = doubleOrDefault(
							takeValue(adaptiveConfig, "backlog_ratio"), DEFAULT_BACKLOG_SPLIT_RATIO);
			final int minPending = intOrDefault(
							takeValue(adaptiveConfig, "min_pending_shards"), DEFAULT_MIN_PENDING_SHARDS);
			final Duration cooldown = parseDuration(
							adaptiveConfig.val("cooldown"), Duration.ZERO);
			if (Double.isNaN(backlogRatio) || Double.isInfinite(backlogRatio) || backlogRatio <= 0.0d) {
				throw new IllegalConfigurationException("backlog_ratio must be > 0");
			}
			return new AdaptiveHeuristicsConfig(
							fullPageThreshold,
							stallWarningThreshold,
							backlogRatio,
							minPending,
							cooldown);
		}

		private static Object takeValue(final Config config, final String key) {
			return config != null ? config.val(key) : null;
		}

		private static int intOrDefault(final Object raw, final int defaultValue)
						throws IllegalConfigurationException {
			if (raw == null) {
				return defaultValue;
			}
			if (raw instanceof Number) {
				return ((Number) raw).intValue();
			}
			if (raw instanceof String str) {
				if (str.isBlank()) {
					return defaultValue;
				}
				try {
					return Integer.parseInt(str.trim());
				} catch (final NumberFormatException e) {
					throw new IllegalConfigurationException(
									"Unable to parse integer value \"" + str + "\" for adaptive sharding setting", e);
				}
			}
			throw new IllegalConfigurationException("Unsupported integer value: " + raw);
		}

		private static double doubleOrDefault(final Object raw, final double defaultValue)
						throws IllegalConfigurationException {
			if (raw == null) {
				return defaultValue;
			}
			if (raw instanceof Number) {
				return ((Number) raw).doubleValue();
			}
			if (raw instanceof String str) {
				if (str.isBlank()) {
					return defaultValue;
				}
				try {
					return Double.parseDouble(str.trim());
				} catch (final NumberFormatException e) {
					throw new IllegalConfigurationException(
									"Unable to parse double value \"" + str + "\" for adaptive sharding setting", e);
				}
			}
			throw new IllegalConfigurationException("Unsupported double value: " + raw);
		}
	}
}
