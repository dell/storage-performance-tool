package com.dell.spt.storage.driver.coop.aws.s3.config;

import java.time.Duration;
import java.util.Map;

/**
 * Smart CRT configuration selector that provides optimized profiles
 * for different object size categories.
 */
public class SmartCrtConfigurator {

	/**
	 * Predefined configuration profiles for each object size category.
	 * These profiles are optimized based on performance analysis.
	 */
	public static final Map<ObjectSizeProfile, CrtConfigurationProfile> PROFILES;

	static {
		PROFILES = Map.ofEntries(
						// Small Objects (< 64KB): Optimized for low latency and high operation count
						Map.entry(ObjectSizeProfile.SMALL, new CrtConfigurationProfile(
										ObjectSizeProfile.SMALL,
										0.5,  // targetThroughputInGbps - realistic for 1KB objects
										256 * 1024,  // minimumPartSizeInBytes - 256KB
										128,  // maxConcurrency - moderate to prevent pool exhaustion
										4,  // maxConcurrentRequestStreams - multiple requests per connection
										Duration.ofMillis(2000),  // connectionTimeout
										Duration.ofMillis(1000),  // connectionAcquisitionTimeout
										Duration.ofSeconds(60),  // connectionMaxIdleTime
										Duration.ofMinutes(5),  // connectionTimeToLive
										false,  // useVirtualThreads - platform threads for small objects
										128,  // smallObjectThreadCount
										64 * 1024,  // byteBufferThresholdBytes - 64KB
										8192  // readBufferSizeBytes - 8KB
						)),

						// Medium Objects (64KB - 8MB): Balanced latency and throughput
						Map.entry(ObjectSizeProfile.MEDIUM, new CrtConfigurationProfile(
										ObjectSizeProfile.MEDIUM,
										2.0,  // targetThroughputInGbps
										8 * 1024 * 1024,  // minimumPartSizeInBytes - 8MB
										256,  // maxConcurrency
										2,  // maxConcurrentRequestStreams
										Duration.ofMillis(3000),  // connectionTimeout
										Duration.ofMillis(2000),  // connectionAcquisitionTimeout
										Duration.ofSeconds(120),  // connectionMaxIdleTime
										Duration.ofMinutes(10),  // connectionTimeToLive
										true,  // useVirtualThreads - virtual threads for medium objects
										64,  // smallObjectThreadCount
										64 * 1024,  // byteBufferThresholdBytes - 64KB
										8192  // readBufferSizeBytes - 8KB
						)),

						// Large Objects (8MB - 100MB): Optimized for throughput
						Map.entry(ObjectSizeProfile.LARGE, new CrtConfigurationProfile(
										ObjectSizeProfile.LARGE,
										10.0,  // targetThroughputInGbps
										8 * 1024 * 1024,  // minimumPartSizeInBytes - 8MB
										512,  // maxConcurrency
										1,  // maxConcurrentRequestStreams - one stream per connection for large transfers
										Duration.ofMillis(5000),  // connectionTimeout
										Duration.ofMillis(3000),  // connectionAcquisitionTimeout
										Duration.ofSeconds(300),  // connectionMaxIdleTime
										Duration.ofMinutes(15),  // connectionTimeToLive
										true,  // useVirtualThreads
										32,  // smallObjectThreadCount
										64 * 1024,  // byteBufferThresholdBytes - 64KB
										8192  // readBufferSizeBytes - 8KB
						)),

						// Very Large Objects (> 100MB): Maximum throughput with larger parts
						Map.entry(ObjectSizeProfile.VERY_LARGE, new CrtConfigurationProfile(
										ObjectSizeProfile.VERY_LARGE,
										20.0,  // targetThroughputInGbps
										16 * 1024 * 1024,  // minimumPartSizeInBytes - 16MB
										256,  // maxConcurrency - rely on part parallelism
										1,  // maxConcurrentRequestStreams
										Duration.ofMillis(10000),  // connectionTimeout
										Duration.ofMillis(5000),  // connectionAcquisitionTimeout
										Duration.ofSeconds(600),  // connectionMaxIdleTime
										Duration.ofMinutes(30),  // connectionTimeToLive
										true,  // useVirtualThreads
										16,  // smallObjectThreadCount
										64 * 1024,  // byteBufferThresholdBytes - 64KB
										8192  // readBufferSizeBytes - 8KB
						)));
	}

	/**
	 * Get the configuration profile for a given object size.
	 *
	 * @param objectSize The object size in bytes
	 * @return The appropriate configuration profile
	 */
	public static CrtConfigurationProfile getProfile(final long objectSize) {
		ObjectSizeProfile sizeProfile = ObjectSizeProfile.fromSize(objectSize);
		return PROFILES.get(sizeProfile);
	}

	/**
	 * Get the configuration profile for a specific size category.
	 *
	 * @param profile The size profile
	 * @return The configuration profile for that size category
	 */
	public static CrtConfigurationProfile getProfile(final ObjectSizeProfile profile) {
		return PROFILES.get(profile);
	}

	/**
	 * Create a custom configuration profile by overriding specific values
	 * from a base profile.
	 *
	 * @param baseProfile The base profile to customize
	 * @param overrides Map of parameter names to override values
	 * @return A new configuration profile with overrides applied
	 */
	public static CrtConfigurationProfile customizeProfile(
					final CrtConfigurationProfile baseProfile,
					final Map<String, Object> overrides) {

		double targetThroughputInGbps = baseProfile.getTargetThroughputInGbps();
		long minimumPartSizeInBytes = baseProfile.getMinimumPartSizeInBytes();
		int maxConcurrency = baseProfile.getMaxConcurrency();
		int maxConcurrentRequestStreams = baseProfile.getMaxConcurrentRequestStreams();
		Duration connectionTimeout = baseProfile.getConnectionTimeout();
		Duration connectionAcquisitionTimeout = baseProfile.getConnectionAcquisitionTimeout();
		Duration connectionMaxIdleTime = baseProfile.getConnectionMaxIdleTime();
		Duration connectionTimeToLive = baseProfile.getConnectionTimeToLive();
		boolean useVirtualThreads = baseProfile.isUseVirtualThreads();
		int smallObjectThreadCount = baseProfile.getSmallObjectThreadCount();
		long byteBufferThresholdBytes = baseProfile.getByteBufferThresholdBytes();
		int readBufferSizeBytes = baseProfile.getReadBufferSizeBytes();

		if (overrides.containsKey("targetThroughputInGbps")) {
			targetThroughputInGbps = ((Number) overrides.get("targetThroughputInGbps")).doubleValue();
		}
		if (overrides.containsKey("minimumPartSizeInBytes")) {
			minimumPartSizeInBytes = ((Number) overrides.get("minimumPartSizeInBytes")).longValue();
		}
		if (overrides.containsKey("maxConcurrency")) {
			maxConcurrency = ((Number) overrides.get("maxConcurrency")).intValue();
		}
		if (overrides.containsKey("maxConcurrentRequestStreams")) {
			maxConcurrentRequestStreams = ((Number) overrides.get("maxConcurrentRequestStreams")).intValue();
		}
		if (overrides.containsKey("connectionTimeoutMs")) {
			connectionTimeout = Duration.ofMillis(((Number) overrides.get("connectionTimeoutMs")).longValue());
		}
		if (overrides.containsKey("connectionAcquisitionTimeoutMs")) {
			connectionAcquisitionTimeout = Duration.ofMillis(((Number) overrides.get("connectionAcquisitionTimeoutMs")).longValue());
		}
		if (overrides.containsKey("connectionMaxIdleTimeMs")) {
			connectionMaxIdleTime = Duration.ofMillis(((Number) overrides.get("connectionMaxIdleTimeMs")).longValue());
		}
		if (overrides.containsKey("connectionTimeToLiveMs")) {
			connectionTimeToLive = Duration.ofMillis(((Number) overrides.get("connectionTimeToLiveMs")).longValue());
		}
		if (overrides.containsKey("useVirtualThreads")) {
			useVirtualThreads = (Boolean) overrides.get("useVirtualThreads");
		}
		if (overrides.containsKey("smallObjectThreadCount")) {
			smallObjectThreadCount = ((Number) overrides.get("smallObjectThreadCount")).intValue();
		}
		if (overrides.containsKey("byteBufferThresholdBytes")) {
			byteBufferThresholdBytes = ((Number) overrides.get("byteBufferThresholdBytes")).longValue();
		}
		if (overrides.containsKey("readBufferSizeBytes")) {
			readBufferSizeBytes = ((Number) overrides.get("readBufferSizeBytes")).intValue();
		}

		return new CrtConfigurationProfile(
						baseProfile.getSizeProfile(),
						targetThroughputInGbps,
						minimumPartSizeInBytes,
						maxConcurrency,
						maxConcurrentRequestStreams,
						connectionTimeout,
						connectionAcquisitionTimeout,
						connectionMaxIdleTime,
						connectionTimeToLive,
						useVirtualThreads,
						smallObjectThreadCount,
						byteBufferThresholdBytes,
						readBufferSizeBytes);
	}
}
