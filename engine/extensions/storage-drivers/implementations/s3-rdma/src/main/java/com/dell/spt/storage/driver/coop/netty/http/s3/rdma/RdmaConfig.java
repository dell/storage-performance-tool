package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.github.akurilov.confuse.Config;

public final class RdmaConfig {

	private static final long DEFAULT_THRESHOLD_BYTES = 1_048_576; // 1MB
	private static final String DEFAULT_DEVICE = "auto";
	private static final String DEFAULT_LOG_LEVEL = "WARN";

	private final boolean enabled;
	private final long thresholdBytes;
	private final boolean fallbackEnabled;
	private final String device;
	private final String localIp;
	private final String logLevel;

	public RdmaConfig(final Config rdmaConfig) {
		if (rdmaConfig == null) {
			// Use all defaults if no config provided
			this.enabled = true;
			this.thresholdBytes = DEFAULT_THRESHOLD_BYTES;
			this.fallbackEnabled = true;
			this.device = DEFAULT_DEVICE;
			this.localIp = "";
			this.logLevel = DEFAULT_LOG_LEVEL;
		} else {
			this.enabled = getBoolean(rdmaConfig, "enabled", true);
			this.thresholdBytes = getLong(rdmaConfig, "threshold-bytes", DEFAULT_THRESHOLD_BYTES);
			this.fallbackEnabled = getBoolean(rdmaConfig, "fallback-enabled", true);
			this.device = getString(rdmaConfig, "device", DEFAULT_DEVICE);
			this.localIp = getString(rdmaConfig, "local-ip", "");
			this.logLevel = getString(rdmaConfig, "log-level", DEFAULT_LOG_LEVEL);
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
		this.enabled = enabled;
		this.thresholdBytes = thresholdBytes;
		this.fallbackEnabled = fallbackEnabled;
		this.device = device;
		this.localIp = localIp;
		this.logLevel = logLevel;
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

	@Override
	public String toString() {
		return "RdmaConfig{" +
						"enabled=" + enabled +
						", thresholdBytes=" + thresholdBytes +
						", fallbackEnabled=" + fallbackEnabled +
						", device='" + device + '\'' +
						", localIp='" + localIp + '\'' +
						", logLevel='" + logLevel + '\'' +
						'}';
	}
}
