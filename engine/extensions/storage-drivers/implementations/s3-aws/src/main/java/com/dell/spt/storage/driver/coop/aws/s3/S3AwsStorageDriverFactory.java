package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.ExtensionBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.io.yaml.YamlSchemaProviderBase;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.URI;
import java.util.List;

/**
 * ServiceLoader entry point for the AWS SDK based S3 storage driver.
 */
public final class S3AwsStorageDriverFactory<I extends Item, O extends Operation<I>>
				extends ExtensionBase
				implements StorageDriverFactory<I, O, S3AwsStorageDriver<I, O>> {

	private static final Logger LOG = LoggerFactory.getLogger(S3AwsStorageDriverFactory.class);
	private static final String NAME = "s3-aws";
	private static final String DEFAULTS_FILE_NAME = "defaults-storage-s3-aws.yaml";

	@Override
	public String id() {
		return NAME;
	}

	@Override
	public S3AwsStorageDriver<I, O> create(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {

		return createInternal(stepId, dataInput, storageConfig, verifyFlag, batchSize);
	}

	private S3AwsStorageDriver<I, O> createInternal(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {

		// ---------------------------
		// Authentication — confuse splits "auth-uid" into path auth→uid,
		// which matches the standard storage.auth.uid config path.
		// ---------------------------
		final String accessKey;
		final String secretKey;
		try {
			accessKey = storageConfig.stringVal("auth-uid");
		} catch (Exception e) {
			throw new IllegalConfigurationException(
							"Missing required config: storage.auth.uid (access key)");
		}
		try {
			secretKey = storageConfig.stringVal("auth-secret");
		} catch (Exception e) {
			throw new IllegalConfigurationException(
							"Missing required config: storage.auth.secret (secret key)");
		}

		// Region — optional for S3-compatible services
		String region;
		try {
			region = storageConfig.stringVal("region");
		} catch (Exception e) {
			region = null;
		}
		if (region == null || region.isEmpty()) {
			region = "eu-west-2";
		}

		// Note: bucket resolution is handled by the driver constructor,
		// not by the factory — the driver reads it from config directly.

		// ---------------------------
		// Endpoint — mirror the netty driver's config resolution:
		//   storage.net.node.addrs  (list of hostnames or host:port)
		//   storage.net.node.port   (shared port when addrs are bare hostnames)
		//   storage.net.ssl.enabled (scheme selection)
		// ---------------------------
		final String endpoint = resolveEndpoint(storageConfig);

		// Path-style access — S3-compatible stores (SeaweedFS, MinIO) require this.
		// Default to true since this driver targets non-AWS endpoints.
		final boolean pathStyle = true;

		// ---------------------------
		// CRT Performance Tuning
		// ---------------------------
		// Target throughput in Gbps - adjust based on network capacity
		// Using aggressive defaults for high-performance environments
		double targetThroughputInGbps = 20.0;  // 20 Gbps for high-throughput scenarios
		try {
			targetThroughputInGbps = storageConfig.configVal("crt").doubleVal("targetThroughputGbps");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.targetThroughputGbps from config, using default: {}", targetThroughputInGbps);
		}

		// Minimum part size for multipart uploads (16 MB for better performance with large objects)
		long minimumPartSizeInBytes = 16 * 1024 * 1024L;
		try {
			minimumPartSizeInBytes = storageConfig.configVal("crt").longVal("minimumPartSizeBytes");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.minimumPartSizeBytes from config, using default: {}", minimumPartSizeInBytes);
		}

		// Maximum concurrent connections (CRT manages this internally, but higher values help parallelism)
		final int maxConcurrency = 256;  // Increased from 128 for better parallelism

		// ---------------------------
		// Build AWS S3 Async Client with CRT
		// ---------------------------
		final var creds = AwsBasicCredentials.create(accessKey, secretKey);

		LOG.info("Creating AWS S3 Async Client with CRT");
		LOG.info("CRT Configuration:");
		LOG.info("  Target Throughput: {} Gbps", targetThroughputInGbps);
		LOG.info("  Minimum Part Size: {} bytes ({} MB)", minimumPartSizeInBytes, minimumPartSizeInBytes / (1024 * 1024));
		LOG.info("  Max Concurrency: {} (internal CRT management)", maxConcurrency);
		LOG.info("  Endpoint: {}", endpoint);
		LOG.info("  Region: {}", region);
		LOG.info("  Path Style: {}", pathStyle);

		S3AsyncClient s3AsyncClient = S3AsyncClient.crtBuilder()
						.credentialsProvider(StaticCredentialsProvider.create(creds))
						.region(Region.of(region))
						.endpointOverride(URI.create(endpoint))
						.forcePathStyle(pathStyle)
						.targetThroughputInGbps(targetThroughputInGbps)
						.minimumPartSizeInBytes(minimumPartSizeInBytes)
						.build();

		LOG.info("AWS S3 Async Client with CRT created successfully: {}", s3AsyncClient.getClass().getSimpleName());

		return new S3AwsStorageDriver<>(
						stepId,
						dataInput,
						storageConfig,
						verifyFlag,
						batchSize,
						s3AsyncClient);
	}

	/**
	 * Resolve the S3 endpoint URL from storage config, mirroring the netty
	 * driver's config paths: storage.net.node.addrs, storage.net.node.port,
	 * and storage.net.ssl.enabled.
	 *
	 * @return fully-qualified endpoint URL (e.g. "http://10.0.0.1:8333")
	 * @throws IllegalConfigurationException if no endpoint can be resolved
	 */
	static String resolveEndpoint(final Config storageConfig)
					throws IllegalConfigurationException {
		String endpoint = null;
		boolean sslEnabled = false;

		try {
			final Config netConfig = storageConfig.configVal("net");
			final Config nodeConfig = netConfig.configVal("node");
			final List<String> addrs = nodeConfig.listVal("addrs");

			int port = 0;
			try {
				port = nodeConfig.intVal("port");
			} catch (Exception ignored) {}

			try {
				sslEnabled = netConfig.configVal("ssl").boolVal("enabled");
			} catch (Exception ignored) {}

			if (addrs != null && !addrs.isEmpty()) {
				final String addr = addrs.get(0);
				final String scheme = sslEnabled ? "https" : "http";

				if (addr.startsWith("http://") || addr.startsWith("https://")) {
					endpoint = addr;
				} else if (!addr.contains(":") && port > 0) {
					endpoint = scheme + "://" + addr + ":" + port;
				} else {
					endpoint = scheme + "://" + addr;
				}
			}
		} catch (Exception ignored) {}

		if (endpoint == null) {
			throw new IllegalConfigurationException(
							"Missing required config: storage.net.node.addrs " +
											"(S3 endpoint address)");
		}
		return endpoint;
	}

	@Override
	public SchemaProvider schemaProvider() {
		return new YamlSchemaProviderBase() {
			@Override
			protected InputStream schemaInputStream() {
				return getClass().getResourceAsStream(
								"/config-schema-storage-s3-aws.yaml");
			}

			@Override
			public String id() {
				return Constants.APP_NAME;
			}
		};
	}

	@Override
	protected String defaultsFileName() {
		return DEFAULTS_FILE_NAME;
	}

	@Override
	protected List<String> resourceFilesToInstall() {
		return List.of("config/" + DEFAULTS_FILE_NAME);
	}
}
