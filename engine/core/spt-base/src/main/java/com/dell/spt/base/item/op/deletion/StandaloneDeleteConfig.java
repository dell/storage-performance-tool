package com.dell.spt.base.item.op.deletion;

import static com.dell.spt.base.metrics.MetricsConstants.DELETE_SELECTION_ORDER_CANONICAL;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TimeUtil;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.TransferConvertBuffer;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.commons.reflection.TypeUtil;
import java.util.NoSuchElementException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/** Parsed fail-closed engine settings for the standalone DELETE request spine. */
public final class StandaloneDeleteConfig {
	/** Shipped per-phase verification retry bound. */
	public static final long DEFAULT_VERIFICATION_TIMEOUT_MILLIS = 30_000;

	private static final String DELETE_CONFIG_PATH = "op-delete";
	private static final String OP_CONFIG_PATH = "op";
	private static final String SHUFFLE_KEY = "shuffle";
	private static final String STANDALONE_KEY = "standalone";
	private static final String BATCH_SIZE_KEY = "batchSize";
	private static final String DURATION_KEY = "duration";
	private static final String SELECTION_ORDER_KEY = "selectionOrder";
	private static final String SELECTED_KEY = "selected";
	private static final String SELECTED_CURRENT_KEY = "selectedCurrentKey";
	private static final String SELECTED_EXACT_VERSION_KEY = "selectedExactVersion";
	private static final String SELECTED_BUCKETS_KEY = "selectedBuckets";
	private static final String SEED_MILLIS_KEY = "seedMillis";
	private static final String DISCOVERY_MILLIS_KEY = "discoveryMillis";
	private static final String WORKFLOW_STARTED_EPOCH_NANOS_KEY = "workflowStartedEpochNanos";
	private static final String PRE_VALIDATION_KEY = "preValidation";
	private static final String POST_VERIFICATION_KEY = "postVerification";
	private static final String VERIFICATION_TIMEOUT_MILLIS_KEY = "verificationTimeoutMillis";
	private static final String OUTPUT_FILE_KEY = "output-file";

	private final boolean enabled;
	private final int batchSize;
	private final boolean durationMode;
	private final String selectionOrder;
	private final long selected;
	private final long selectedCurrentKey;
	private final long selectedExactVersion;
	private final Map<String, Long> selectedBuckets;
	private final long seedMillis;
	private final long discoveryMillis;
	private final long workflowStartedEpochNanos;
	private final boolean preValidation;
	private final boolean postVerification;
	private final long verificationTimeoutMillis;

	private StandaloneDeleteConfig(
					final boolean enabled,
					final int batchSize,
					final boolean durationMode,
					final String selectionOrder,
					final long selected,
					final long selectedCurrentKey,
					final long selectedExactVersion,
					final Map<String, Long> selectedBuckets,
					final long seedMillis,
					final long discoveryMillis,
					final long workflowStartedEpochNanos,
					final boolean preValidation,
					final boolean postVerification,
					final long verificationTimeoutMillis) {
		this.enabled = enabled;
		this.batchSize = batchSize;
		this.durationMode = durationMode;
		this.selectionOrder = selectionOrder;
		this.selected = selected;
		this.selectedCurrentKey = selectedCurrentKey;
		this.selectedExactVersion = selectedExactVersion;
		this.selectedBuckets = Collections.unmodifiableMap(new TreeMap<>(selectedBuckets));
		this.seedMillis = seedMillis;
		this.discoveryMillis = discoveryMillis;
		this.workflowStartedEpochNanos = workflowStartedEpochNanos;
		this.preValidation = preValidation;
		this.postVerification = postVerification;
		this.verificationTimeoutMillis = verificationTimeoutMillis;
	}

	/** Parses the optional standalone DELETE node, preserving disabled compatibility if absent. */
	public static StandaloneDeleteConfig from(final Config loadConfig) {
		if (loadConfig == null) {
			return disabled();
		}
		try {
			final var deleteConfig = loadConfig.configVal(DELETE_CONFIG_PATH);
			if (deleteConfig == null || !deleteConfig.boolVal(STANDALONE_KEY)) {
				return disabled();
			}
			if (optionalBoolean(loadConfig.configVal(OP_CONFIG_PATH), SHUFFLE_KEY, false)) {
				throw new IllegalConfigurationException(
								"Standalone DELETE cannot use load-op-shuffle");
			}
			boolean durationMode = false;
			try {
				durationMode = deleteConfig.boolVal(DURATION_KEY);
			} catch (final NoSuchElementException ignored) {
				// Compatibility with extension-supplied schemas created before duration mode.
			}
			final Object rawDuration = loadConfig.configVal("step-limit").val("time");
			final long durationSeconds = rawDuration instanceof String
							? TimeUtil.getTimeInSeconds((String) rawDuration)
							: TypeUtil.typeConvert(rawDuration, long.class);
			if (durationMode) {
				if (durationSeconds <= 0) {
					throw new IllegalConfigurationException(
									"Standalone DELETE duration mode requires a positive load-step-limit-time");
				}
				if (loadConfig.configVal("op-limit").longVal("count") > 0) {
					throw new IllegalConfigurationException(
									"Standalone DELETE duration mode cannot use load-op-limit-count");
				}
			} else if (durationSeconds > 0) {
				throw new IllegalConfigurationException(
								"Standalone DELETE with a positive load-step-limit-time requires "
												+ "load-op-delete-duration=true");
			}
			return new StandaloneDeleteConfig(
							true,
							deleteConfig.intVal(BATCH_SIZE_KEY),
							durationMode,
							canonicalSelectionOrder(deleteConfig),
							optionalLong(deleteConfig, SELECTED_KEY, -1),
							optionalLong(deleteConfig, SELECTED_CURRENT_KEY, -1),
							optionalLong(deleteConfig, SELECTED_EXACT_VERSION_KEY, -1),
							selectedBuckets(deleteConfig),
							optionalLong(deleteConfig, SEED_MILLIS_KEY, -1),
							optionalLong(deleteConfig, DISCOVERY_MILLIS_KEY, -1),
							optionalLong(deleteConfig, WORKFLOW_STARTED_EPOCH_NANOS_KEY, -1),
							optionalBoolean(deleteConfig, PRE_VALIDATION_KEY, false),
							optionalBoolean(deleteConfig, POST_VERIFICATION_KEY, false),
							optionalLong(
											deleteConfig, VERIFICATION_TIMEOUT_MILLIS_KEY,
											DEFAULT_VERIFICATION_TIMEOUT_MILLIS));
		} catch (final NoSuchElementException e) {
			// Compatibility with extension-supplied schemas created before this optional node.
			return disabled();
		}
	}

	private static StandaloneDeleteConfig disabled() {
		return new StandaloneDeleteConfig(
						false, 0, false, DELETE_SELECTION_ORDER_CANONICAL,
						-1, -1, -1, Map.of(), -1, -1, -1, false, false,
						DEFAULT_VERIFICATION_TIMEOUT_MILLIS);
	}

	private static boolean optionalBoolean(
					final Config config, final String key, final boolean fallback) {
		try {
			return config.boolVal(key);
		} catch (final RuntimeException ignored) {
			return fallback;
		}
	}

	private static String optionalString(final Config config, final String key, final String fallback) {
		try {
			final String value = config.stringVal(key);
			return value == null || value.isBlank() ? fallback : value;
		} catch (final RuntimeException ignored) {
			return fallback;
		}
	}

	private static String canonicalSelectionOrder(final Config config) {
		final String selectionOrder = optionalString(
						config, SELECTION_ORDER_KEY, DELETE_SELECTION_ORDER_CANONICAL);
		if (!DELETE_SELECTION_ORDER_CANONICAL.equals(selectionOrder)) {
			throw new IllegalConfigurationException(
							"load-op-delete-selectionOrder must be " + DELETE_SELECTION_ORDER_CANONICAL);
		}
		return selectionOrder;
	}

	private static long optionalLong(final Config config, final String key, final long fallback) {
		try {
			return config.longVal(key);
		} catch (final RuntimeException ignored) {
			return fallback;
		}
	}

	private static Map<String, Long> selectedBuckets(final Config config) {
		final var result = new TreeMap<String, Long>();
		final java.util.List<?> values;
		try {
			values = config.listVal(SELECTED_BUCKETS_KEY);
		} catch (final RuntimeException ignored) {
			return result;
		}
		if (values == null) {
			return result;
		}
		for (final Object raw : values) {
			final String value = String.valueOf(raw);
			final int separator = value.lastIndexOf('=');
			if (separator <= 0 || separator == value.length() - 1) {
				throw new IllegalConfigurationException("Invalid DELETE selected bucket metric: " + value);
			}
			try {
				result.put(value.substring(0, separator), Long.parseLong(value.substring(separator + 1)));
			} catch (final NumberFormatException failure) {
				throw new IllegalConfigurationException("Invalid DELETE selected bucket metric: " + value, failure);
			}
		}
		return result;
	}

	/** Returns whether the standalone request spine is explicitly enabled. */
	public boolean enabled() {
		return enabled;
	}

	/** Returns the validated target count requested per logical DELETE operation. */
	public int batchSize() {
		return batchSize;
	}

	/** Returns whether natural finite-input completion before the step deadline is invalid. */
	public boolean durationMode() {
		return durationMode;
	}

	/** Returns the canonical target selection order. */
	public String selectionOrder() {
		return selectionOrder;
	}

	/** Returns the selected target count, or {@code -1} when unavailable. */
	public long selected() {
		return selected;
	}

	/** Returns the selected current-key count, or {@code -1} when unavailable. */
	public long selectedCurrentKey() {
		return selectedCurrentKey;
	}

	/** Returns the selected exact-version count, or {@code -1} when unavailable. */
	public long selectedExactVersion() {
		return selectedExactVersion;
	}

	/** Returns immutable selected-target counts keyed by bucket. */
	public Map<String, Long> selectedBuckets() {
		return selectedBuckets;
	}

	/** Returns whether immutable selected/version/bucket identities are internally complete. */
	public boolean frozenSelectionAvailable() {
		if (selected < 0 || selectedCurrentKey < 0 || selectedExactVersion < 0) {
			return false;
		}
		try {
			if (Math.addExact(selectedCurrentKey, selectedExactVersion) != selected) {
				return false;
			}
			long bucketTotal = 0;
			for (final long count : selectedBuckets.values()) {
				if (count < 0) {
					return false;
				}
				bucketTotal = Math.addExact(bucketTotal, count);
			}
			return bucketTotal == selected;
		} catch (final ArithmeticException ignored) {
			return false;
		}
	}

	/** Returns measured seed time in milliseconds, or {@code -1} when not applicable. */
	public long seedMillis() {
		return seedMillis;
	}

	/** Returns measured discovery time in milliseconds, or {@code -1} when not applicable. */
	public long discoveryMillis() {
		return discoveryMillis;
	}

	/** Returns the full-workflow monotonic epoch-relative start in nanoseconds, or {@code -1}. */
	public long workflowStartedEpochNanos() {
		return workflowStartedEpochNanos;
	}

	/** Returns whether strict full inventory validation runs before timed DELETE. */
	public boolean preValidation() {
		return preValidation;
	}

	/** Returns whether full absence verification runs after bounded drain. */
	public boolean postVerification() {
		return postVerification;
	}

	/** Returns the independent retry timeout for each enabled verification phase. */
	public long verificationTimeoutMillis() {
		return verificationTimeoutMillis;
	}

	/** Validates operation-type, item-type, recycle, retry, and cardinality settings. */
	public void validateSettings(
					final OpType opType,
					final ItemType itemType,
					final boolean recycle,
					final boolean retry) {
		if (!enabled) {
			return;
		}
		if (opType != OpType.DELETE || (itemType != null && itemType != ItemType.DATA)) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires load-op-type=delete and item-type=data");
		}
		if (batchSize < 1 || batchSize > DeleteRequest.MAX_TARGET_COUNT) {
			throw new IllegalConfigurationException(
							"load-op-delete-batchSize must be between 1 and "
											+ DeleteRequest.MAX_TARGET_COUNT);
		}
		if (recycle) {
			throw new IllegalConfigurationException("Standalone DELETE does not support recycle mode");
		}
		if (retry) {
			throw new IllegalConfigurationException(
							"Standalone DELETE does not support SPT operation retries");
		}
		if ((preValidation || postVerification) && verificationTimeoutMillis <= 0) {
			throw new IllegalConfigurationException(
							"load-op-delete-verificationTimeoutMillis must be positive when verification is enabled");
		}
	}

	/** Validates that the step is terminal and its driver explicitly supports the request type. */
	public void validateTopology(
					final Config itemConfig, final Object itemInput, final Object operationOutput) {
		if (!enabled) {
			return;
		}
		if (itemInput instanceof TransferConvertBuffer<?, ?>) {
			throw new IllegalConfigurationException(
							"Standalone DELETE is terminal and cannot consume a single-item pipeline");
		}
		if (!(itemInput instanceof RemainingItemCountInput<?>)) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires a finite input with an exact remaining-item count");
		}
		final var itemOutputFile = itemConfig.stringVal(OUTPUT_FILE_KEY);
		if (itemOutputFile != null && !itemOutputFile.isEmpty()) {
			throw new IllegalConfigurationException(
							"Standalone DELETE cannot use ordinary successful-item output");
		}
		if (!(operationOutput instanceof StorageDriver<?, ?>)) {
			throw unsupportedDriver();
		}
		final var driver = (StorageDriver<?, ?>) operationOutput;
		if (!driver.supportsStandaloneDeleteRequests()) {
			throw unsupportedDriver();
		}
		if ((preValidation || postVerification) && !(driver instanceof DeleteVerificationProbe)) {
			throw new IllegalConfigurationException(
							"Configured storage driver does not support standalone DELETE verification");
		}
	}

	private static IllegalConfigurationException unsupportedDriver() {
		return new IllegalConfigurationException(
						"Configured storage driver does not support standalone DELETE requests");
	}
}
