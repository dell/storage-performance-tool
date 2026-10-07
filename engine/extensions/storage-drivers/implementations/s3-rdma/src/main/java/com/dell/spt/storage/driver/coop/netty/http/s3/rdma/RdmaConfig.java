package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.github.akurilov.confuse.Config;

public final class RdmaConfig {

	private static final long DEFAULT_THRESHOLD_BYTES = 1_048_576; // 1MB
	private static final String DEFAULT_DEVICE = "auto";
	private static final String DEFAULT_LOG_LEVEL = "WARN";
	private static final long DEFAULT_TIMEOUT_MS = 30_000; // 30 seconds

	private final boolean enabled;
	private final long thresholdBytes;
	private final boolean fallbackEnabled;
	private final String device;
	private final String localIp;
	private final String logLevel;
	private final long timeoutMs;
	private final boolean allowMissingBytesHeader;

	public RdmaConfig(final Config rdmaConfig) {
		if (rdmaConfig == null) {
			// Use all defaults if no config provided
			this.enabled = true;
			this.thresholdBytes = DEFAULT_THRESHOLD_BYTES;
			this.fallbackEnabled = false;
			this.device = DEFAULT_DEVICE;
			this.localIp = "";
			this.logLevel = DEFAULT_LOG_LEVEL;
			this.timeoutMs = DEFAULT_TIMEOUT_MS;
			this.allowMissingBytesHeader = false;
		} else {
			this.enabled = getBoolean(rdmaConfig, "enabled", true);
			this.thresholdBytes = getLong(rdmaConfig, "thresholdBytes", DEFAULT_THRESHOLD_BYTES);
			this.fallbackEnabled = getBoolean(rdmaConfig, "fallback", false);
			this.device = getString(rdmaConfig, "device", DEFAULT_DEVICE);
			this.localIp = getString(rdmaConfig, "localIp", "");
			this.logLevel = getString(rdmaConfig, "logLevel", DEFAULT_LOG_LEVEL);
			this.timeoutMs = getLong(rdmaConfig, "timeoutMs", DEFAULT_TIMEOUT_MS);
			this.allowMissingBytesHeader = getBoolean(rdmaConfig, "allowMissingBytesHeader", false);
		}
	}

	private static boolean getBoolean(final Config config, final String key, final boolean defaultValue) {
		try {
			return config.boolVal(key);
		} catch (final Exception e) {
			return defaultValue;
		}
	}

	private static long getLong(final Config config, final String key, final long defaultValue) {
		try {
			return config.longVal(key);
		} catch (final Exception e) {
			return defaultValue;
		}
	}

	private static String getString(final Config config, final String key, final String defaultValue) {
		try {
			final String val = config.stringVal(key);
			return val != null ? val : defaultValue;
		} catch (final Exception e) {
			return defaultValue;
		}
	}

	public RdmaConfig(
					final boolean enabled,
					final long thresholdBytes,
					final boolean fallbackEnabled,
					final String device,
					final String localIp,
					final String logLevel) {
		this(enabled, thresholdBytes, fallbackEnabled, device, localIp, logLevel, DEFAULT_TIMEOUT_MS);
	}

	public RdmaConfig(
					final boolean enabled,
					final long thresholdBytes,
					final boolean fallbackEnabled,
					final String device,
					final String localIp,
					final String logLevel,
					final long timeoutMs) {
		this(enabled, thresholdBytes, fallbackEnabled, device, localIp, logLevel, timeoutMs, false);
	}

	/**
	 * @param allowMissingBytesHeader legacy compatibility: accept an RDMA GET success that omits
	 *                                {@code x-amz-rdma-bytes-transferred} and count the requested size
	 */
	public RdmaConfig(
					final boolean enabled,
					final long thresholdBytes,
					final boolean fallbackEnabled,
					final String device,
					final String localIp,
					final String logLevel,
					final long timeoutMs,
					final boolean allowMissingBytesHeader) {
		this.enabled = enabled;
		this.thresholdBytes = thresholdBytes;
		this.fallbackEnabled = fallbackEnabled;
		this.device = device;
		this.localIp = localIp;
		this.logLevel = logLevel;
		this.timeoutMs = timeoutMs;
		this.allowMissingBytesHeader = allowMissingBytesHeader;
	}

	/** Returns a copy with {@code localIp} replaced. */
	public RdmaConfig withLocalIp(final String localIp) {
		return new RdmaConfig(
						enabled, thresholdBytes, fallbackEnabled, device, localIp, logLevel, timeoutMs,
						allowMissingBytesHeader);
	}

	public boolean isEnabled() {
		return enabled;
	}

	public long getThresholdBytes() {
		return thresholdBytes;
	}

	public boolean isFallbackEnabled() {
		return fallbackEnabled;
	}

	public String getDevice() {
		return device;
	}

	public String getLocalIp() {
		return localIp;
	}

	public String getLogLevel() {
		return logLevel;
	}

	public long getTimeoutMs() {
		return timeoutMs;
	}

	/** Whether an RDMA GET success without a bytes-transferred header is accepted (legacy servers). */
	public boolean isAllowMissingBytesHeader() {
		return allowMissingBytesHeader;
	}

	/** Whether {@code device} names a specific RDMA device rather than requesting auto-detection. */
	public boolean hasExplicitDevice() {
		return device != null && !device.isEmpty() && !DEFAULT_DEVICE.equals(device);
	}

	@Override
	public String toString() {
		return "RdmaConfig{" +
						"enabled=" + enabled +
						", thresholdBytes=" + thresholdBytes +
						", fallbackEnabled=" + fallbackEnabled +
						", device='" + device + '\'' +
						", localIp='" + localIp + '\'' +
						", logLevel='" + logLevel + '\'' +
						", timeoutMs=" + timeoutMs +
						", allowMissingBytesHeader=" + allowMissingBytesHeader +
						'}';
	}
}
