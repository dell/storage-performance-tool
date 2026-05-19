package com.dell.spt.storage.driver.coop.aws.s3.config;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Smart S3 client pool that maintains multiple S3AsyncClient instances,
 * each configured for a different object size profile.
 * This enables optimal CRT settings for different workload characteristics.
 */
public class SmartS3ClientPool {
	private static final Logger LOG = LoggerFactory.getLogger(SmartS3ClientPool.class);

	private final Map<ObjectSizeProfile, S3AsyncClient> clients;
	private final AwsBasicCredentials credentials;
	private final Region region;
	private final URI endpoint;
	private final boolean pathStyle;
	private volatile boolean closed = false;

	/**
	 * Create a new smart client pool.
	 *
	 * @param credentials AWS credentials
	 * @param region AWS region
	 * @param endpoint S3 endpoint URI
	 * @param pathStyle Whether to use path-style access
	 */
	public SmartS3ClientPool(
					final AwsBasicCredentials credentials,
					final Region region,
					final URI endpoint,
					final boolean pathStyle) {

		this.credentials = credentials;
		this.region = region;
		this.endpoint = endpoint;
		this.pathStyle = pathStyle;
		this.clients = new ConcurrentHashMap<>();

		LOG.info("SmartS3ClientPool initialized with endpoint: {}, region: {}, pathStyle: {}",
						endpoint, region, pathStyle);
	}

	/**
	 * Get the appropriate S3AsyncClient for a given object size.
	 * Clients are created lazily on first access.
	 *
	 * @param objectSize The object size in bytes
	 * @return The S3AsyncClient configured for that object size
	 */
	public S3AsyncClient getClient(final long objectSize) {
		if (closed) {
			throw new IllegalStateException("SmartS3ClientPool is closed");
		}

		ObjectSizeProfile profile = ObjectSizeProfile.fromSize(objectSize);
		return clients.computeIfAbsent(profile, this::createClient);
	}

	/**
	 * Get the S3AsyncClient for a specific profile.
	 *
	 * @param profile The object size profile
	 * @return The S3AsyncClient for that profile
	 */
	public S3AsyncClient getClient(final ObjectSizeProfile profile) {
		if (closed) {
			throw new IllegalStateException("SmartS3ClientPool is closed");
		}

		return clients.computeIfAbsent(profile, this::createClient);
	}

	/**
	 * Create a new S3AsyncClient configured for a specific profile.
	 *
	 * @param profile The object size profile
	 * @return A new S3AsyncClient instance
	 */
	private S3AsyncClient createClient(final ObjectSizeProfile profile) {
		CrtConfigurationProfile config = SmartCrtConfigurator.getProfile(profile);

		LOG.info("Creating S3AsyncClient for profile: {} with config: {}", profile, config);

		var crtBuilder = S3AsyncClient.crtBuilder()
						.credentialsProvider(StaticCredentialsProvider.create(credentials))
						.region(region)
						.endpointOverride(endpoint)
						.forcePathStyle(pathStyle)
						.targetThroughputInGbps(config.getTargetThroughputInGbps())
						.minimumPartSizeInBytes(config.getMinimumPartSizeInBytes())
						.maxConcurrency(config.getMaxConcurrency());

		return crtBuilder.build();
	}

	/**
	 * Create a custom client with a specific configuration profile.
	 * This is useful for testing or advanced use cases.
	 *
	 * @param profile The object size profile
	 * @param customConfig Custom configuration overrides
	 * @return A new S3AsyncClient instance with custom configuration
	 */
	public S3AsyncClient createCustomClient(
					final ObjectSizeProfile profile,
					final Map<String, Object> customConfig) {

		if (closed) {
			throw new IllegalStateException("SmartS3ClientPool is closed");
		}

		CrtConfigurationProfile baseConfig = SmartCrtConfigurator.getProfile(profile);
		CrtConfigurationProfile config = SmartCrtConfigurator.customizeProfile(baseConfig, customConfig);

		LOG.info("Creating custom S3AsyncClient for profile: {} with custom config: {}", profile, customConfig);

		var crtBuilder = S3AsyncClient.crtBuilder()
						.credentialsProvider(StaticCredentialsProvider.create(credentials))
						.region(region)
						.endpointOverride(endpoint)
						.forcePathStyle(pathStyle)
						.targetThroughputInGbps(config.getTargetThroughputInGbps())
						.minimumPartSizeInBytes(config.getMinimumPartSizeInBytes())
						.maxConcurrency(config.getMaxConcurrency());

		return crtBuilder.build();
	}

	/**
	 * Close all clients in the pool and release resources.
	 */
	public void close() {
		if (closed) {
			return;
		}

		closed = true;
		LOG.info("Closing SmartS3ClientPool with {} clients", clients.size());

		for (Map.Entry<ObjectSizeProfile, S3AsyncClient> entry : clients.entrySet()) {
			try {
				LOG.debug("Closing client for profile: {}", entry.getKey());
				entry.getValue().close();
			} catch (Exception e) {
				LOG.warn("Failed to close client for profile: {}", entry.getKey(), e);
			}
		}

		clients.clear();
	}

	/**
	 * Check if the pool is closed.
	 *
	 * @return true if closed, false otherwise
	 */
	public boolean isClosed() {
		return closed;
	}

	/**
	 * Get the number of clients currently in the pool.
	 *
	 * @return The number of clients
	 */
	public int getClientCount() {
		return clients.size();
	}
}
