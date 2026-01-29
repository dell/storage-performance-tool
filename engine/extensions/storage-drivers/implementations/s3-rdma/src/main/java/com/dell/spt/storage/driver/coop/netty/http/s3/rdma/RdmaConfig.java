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
		this.enabled = rdmaConfig.boolVal("enabled");
		this.thresholdBytes = rdmaConfig.longVal("threshold-bytes");
		this.fallbackEnabled = rdmaConfig.boolVal("fallback-enabled");
		this.device = rdmaConfig.stringVal("device");
		this.localIp = rdmaConfig.stringVal("local-ip");
		this.logLevel = rdmaConfig.stringVal("log-level");
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
