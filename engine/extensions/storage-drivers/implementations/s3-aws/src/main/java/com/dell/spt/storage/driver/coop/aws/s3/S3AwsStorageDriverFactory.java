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
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
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
		// CRT Configuration
		// ---------------------------
		boolean optimizeForSmallObjects = true;
		try {
			optimizeForSmallObjects = storageConfig.configVal("crt").boolVal("optimizeForSmallObjects");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.optimizeForSmallObjects from config, using default: {}", optimizeForSmallObjects);
		}

		double targetThroughputInGbps;
		long minimumPartSizeInBytes;
		int maxConcurrency;

		if (optimizeForSmallObjects) {
			targetThroughputInGbps = 10.0;
			minimumPartSizeInBytes = 8 * 1024 * 1024L;
			maxConcurrency = 512;
		} else {
			targetThroughputInGbps = 20.0;
			minimumPartSizeInBytes = 16 * 1024 * 1024L;
			maxConcurrency = 256;
		}

		// Allow config override
		try {
			targetThroughputInGbps = storageConfig.configVal("crt").doubleVal("targetThroughputGbps");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.targetThroughputGbps from config, using default: {}", targetThroughputInGbps);
		}

		try {
			minimumPartSizeInBytes = storageConfig.configVal("crt").longVal("minimumPartSizeBytes");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.minimumPartSizeBytes from config, using default: {}", minimumPartSizeInBytes);
		}

		try {
			maxConcurrency = storageConfig.configVal("crt").intVal("maxConcurrency");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.maxConcurrency from config, using default: {}", maxConcurrency);
		}

		long partSizeBytes = 8 * 1024 * 1024L;  // 8MB default part size
		try {
			partSizeBytes = storageConfig.configVal("crt").longVal("partSizeBytes");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.partSizeBytes from config, using default: {}", partSizeBytes);
		}

		long smallObjectThresholdBytes = 100 * 1024L;
		try {
			smallObjectThresholdBytes = storageConfig.configVal("crt").longVal("smallObjectThresholdBytes");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.smallObjectThresholdBytes from config, using default: {}", smallObjectThresholdBytes);
		}

		// Connection timeout settings
		int connectionTimeoutMs = 5000;  // 5 seconds
		try {
			connectionTimeoutMs = storageConfig.configVal("crt").intVal("connectionTimeoutMs");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.connectionTimeoutMs from config, using default: {}", connectionTimeoutMs);
		}

		// Socket timeout settings
		int socketTimeoutMs = 30000;  // 30 seconds
		try {
			socketTimeoutMs = storageConfig.configVal("crt").intVal("socketTimeoutMs");
		} catch (Exception e) {
			LOG.debug("Could not read storage.crt.socketTimeoutMs from config, using default: {}", socketTimeoutMs);
		}

		// ---------------------------
		// Build AWS S3 Async Client with CRT
		// ---------------------------
		final var creds = AwsBasicCredentials.create(accessKey, secretKey);

		var crtBuilder = S3AsyncClient.crtBuilder()
						.credentialsProvider(StaticCredentialsProvider.create(creds))
						.region(Region.of(region))
						.endpointOverride(URI.create(endpoint))
						.forcePathStyle(pathStyle)
						.targetThroughputInGbps(targetThroughputInGbps)
						.minimumPartSizeInBytes(minimumPartSizeInBytes)
						.maxConcurrency(maxConcurrency);

		// Note: partSizeBytes is used for driver-level multipart decisions
		// The CRT manages part size internally based on minimumPartSizeInBytes

		S3AsyncClient s3AsyncClient = crtBuilder.build();
		S3AsyncClient exactVersionS3Client = null;
		try {
			// The CRT S3 transfer client may synthesize conditional multipart GETs from a
			// current-object HEAD. Use the ordinary protocol client when a version ID is exact.
			exactVersionS3Client = S3AsyncClient.builder()
							.credentialsProvider(StaticCredentialsProvider.create(creds))
							.region(Region.of(region))
							.endpointOverride(URI.create(endpoint))
							.forcePathStyle(pathStyle)
							.httpClientBuilder(AwsCrtAsyncHttpClient.builder()
											.maxConcurrency(maxConcurrency))
							.build();

			return new S3AwsStorageDriver<>(
							stepId,
							dataInput,
							storageConfig,
							verifyFlag,
							batchSize,
							s3AsyncClient,
							exactVersionS3Client,
							smallObjectThresholdBytes,
							partSizeBytes);
		} catch (RuntimeException e) {
			try {
				if (exactVersionS3Client != null) {
					exactVersionS3Client.close();
				}
			} catch (RuntimeException closeFailure) {
				e.addSuppressed(closeFailure);
			}
			try {
				s3AsyncClient.close();
			} catch (RuntimeException closeFailure) {
				e.addSuppressed(closeFailure);
			}
			throw e;
		}
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
			} catch (Exception e) {
				LOG.debug("Could not read storage.net.node.port from config", e);
			}

			try {
				sslEnabled = netConfig.configVal("ssl").boolVal("enabled");
			} catch (Exception e) {
				LOG.debug("Could not read storage.net.ssl.enabled from config", e);
			}

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
		} catch (Exception e) {
			LOG.debug("Could not resolve endpoint from storage.net config", e);
		}

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
