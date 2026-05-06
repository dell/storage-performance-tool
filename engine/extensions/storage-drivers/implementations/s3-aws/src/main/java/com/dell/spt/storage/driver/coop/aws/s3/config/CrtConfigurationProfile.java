package com.dell.spt.storage.driver.coop.aws.s3.config;

import java.time.Duration;

/**
 * CRT configuration profile for a specific object size range.
 * Contains optimized CRT parameters for that size category.
 */
public class CrtConfigurationProfile {
	private final ObjectSizeProfile sizeProfile;
	private final double targetThroughputInGbps;
	private final long minimumPartSizeInBytes;
	private final int maxConcurrency;
	private final int maxConcurrentRequestStreams;
	private final Duration connectionTimeout;
	private final Duration connectionAcquisitionTimeout;
	private final Duration connectionMaxIdleTime;
	private final Duration connectionTimeToLive;
	private final boolean useVirtualThreads;
	private final int smallObjectThreadCount;
	private final long byteBufferThresholdBytes;
	private final int readBufferSizeBytes;

	public CrtConfigurationProfile(
					final ObjectSizeProfile sizeProfile,
					final double targetThroughputInGbps,
					final long minimumPartSizeInBytes,
					final int maxConcurrency,
					final int maxConcurrentRequestStreams,
					final Duration connectionTimeout,
					final Duration connectionAcquisitionTimeout,
					final Duration connectionMaxIdleTime,
					final Duration connectionTimeToLive,
					final boolean useVirtualThreads,
					final int smallObjectThreadCount,
					final long byteBufferThresholdBytes,
					final int readBufferSizeBytes) {

		this.sizeProfile = sizeProfile;
		this.targetThroughputInGbps = targetThroughputInGbps;
		this.minimumPartSizeInBytes = minimumPartSizeInBytes;
		this.maxConcurrency = maxConcurrency;
		this.maxConcurrentRequestStreams = maxConcurrentRequestStreams;
		this.connectionTimeout = connectionTimeout;
		this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
		this.connectionMaxIdleTime = connectionMaxIdleTime;
		this.connectionTimeToLive = connectionTimeToLive;
		this.useVirtualThreads = useVirtualThreads;
		this.smallObjectThreadCount = smallObjectThreadCount;
		this.byteBufferThresholdBytes = byteBufferThresholdBytes;
		this.readBufferSizeBytes = readBufferSizeBytes;
	}

	public ObjectSizeProfile getSizeProfile() {
		return sizeProfile;
	}

	public double getTargetThroughputInGbps() {
		return targetThroughputInGbps;
	}

	public long getMinimumPartSizeInBytes() {
		return minimumPartSizeInBytes;
	}

	public int getMaxConcurrency() {
		return maxConcurrency;
	}

	public int getMaxConcurrentRequestStreams() {
		return maxConcurrentRequestStreams;
	}

	public Duration getConnectionTimeout() {
		return connectionTimeout;
	}

	public Duration getConnectionAcquisitionTimeout() {
		return connectionAcquisitionTimeout;
	}

	public Duration getConnectionMaxIdleTime() {
		return connectionMaxIdleTime;
	}

	public Duration getConnectionTimeToLive() {
		return connectionTimeToLive;
	}

	public boolean isUseVirtualThreads() {
		return useVirtualThreads;
	}

	public int getSmallObjectThreadCount() {
		return smallObjectThreadCount;
	}

	public long getByteBufferThresholdBytes() {
		return byteBufferThresholdBytes;
	}

	public int getReadBufferSizeBytes() {
		return readBufferSizeBytes;
	}

	@Override
	public String toString() {
		return "CrtConfigurationProfile{" +
						"sizeProfile=" + sizeProfile +
						", targetThroughputInGbps=" + targetThroughputInGbps +
						", minimumPartSizeInBytes=" + minimumPartSizeInBytes +
						", maxConcurrency=" + maxConcurrency +
						", maxConcurrentRequestStreams=" + maxConcurrentRequestStreams +
						", connectionTimeout=" + connectionTimeout +
						", connectionAcquisitionTimeout=" + connectionAcquisitionTimeout +
						", connectionMaxIdleTime=" + connectionMaxIdleTime +
						", connectionTimeToLive=" + connectionTimeToLive +
						", useVirtualThreads=" + useVirtualThreads +
						", smallObjectThreadCount=" + smallObjectThreadCount +
						", byteBufferThresholdBytes=" + byteBufferThresholdBytes +
						", readBufferSizeBytes=" + readBufferSizeBytes +
						'}';
	}
}
