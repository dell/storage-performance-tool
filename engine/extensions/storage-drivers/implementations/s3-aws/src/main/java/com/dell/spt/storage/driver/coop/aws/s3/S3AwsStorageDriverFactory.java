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
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;

/**
 * ServiceLoader entry point for the AWS SDK based S3 storage driver.
 */
public final class S3AwsStorageDriverFactory<I extends Item, O extends Operation<I>>
				extends ExtensionBase
				implements StorageDriverFactory<I, O, S3AwsStorageDriver<I, O>> {

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
		String endpoint = null;
		boolean sslEnabled = false;

		try {
			final Config netConfig = storageConfig.configVal("net");
			final Config nodeConfig = netConfig.configVal("node");
			final List<String> addrs = nodeConfig.listVal("addrs");

			// Read port (same field the netty driver uses)
			int port = 0;
			try {
				port = nodeConfig.intVal("port");
			} catch (Exception ignored) { }

			// Read SSL flag for scheme selection
			try {
				sslEnabled = netConfig.configVal("ssl").boolVal("enabled");
			} catch (Exception ignored) { }

			if (addrs != null && !addrs.isEmpty()) {
				final String addr = addrs.get(0);
				final String scheme = sslEnabled ? "https" : "http";

				if (addr.startsWith("http://") || addr.startsWith("https://")) {
					// Address already includes scheme (and presumably port)
					endpoint = addr;
				} else if (!addr.contains(":") && port > 0) {
					// Bare hostname — append the shared port
					endpoint = scheme + "://" + addr + ":" + port;
				} else {
					// host:port pair (non-uniform ports) or port is unknown
					endpoint = scheme + "://" + addr;
				}
			}
		} catch (Exception ignored) { }

		if (endpoint == null) {
			throw new IllegalConfigurationException(
							"Missing required config: storage.net.node.addrs " +
											"(S3 endpoint address)");
		}

		// Path-style access — S3-compatible stores (SeaweedFS, MinIO) require this.
		// Default to true since this driver targets non-AWS endpoints.
		final boolean pathStyle = true;

		// Connection tuning
		final int maxConnections = 128;
		final int socketTimeout = 30_000;
		final int connTimeout = 10_000;

		// ---------------------------
		// Build AWS S3 client
		// ---------------------------
		final var creds = AwsBasicCredentials.create(accessKey, secretKey);

		final var httpClient = ApacheHttpClient.builder()
						.maxConnections(maxConnections)
						.connectionTimeout(Duration.ofMillis(connTimeout))
						.socketTimeout(Duration.ofMillis(socketTimeout))
						.connectionAcquisitionTimeout(Duration.ofSeconds(3))
						.connectionMaxIdleTime(Duration.ofSeconds(30))
						.connectionTimeToLive(Duration.ofMinutes(2))
						.tcpKeepAlive(true)
						.build();

		S3Client s3Client = S3Client.builder()
						.region(Region.of(region))
						.credentialsProvider(StaticCredentialsProvider.create(creds))
						.httpClient(httpClient)
						.endpointOverride(URI.create(endpoint))
						.forcePathStyle(pathStyle)
						.serviceConfiguration(S3Configuration.builder()
										.chunkedEncodingEnabled(false)
										.dualstackEnabled(false)
										.accelerateModeEnabled(false)
										.useArnRegionEnabled(true)
										.build())
						.build();

		return new S3AwsStorageDriver<>(
						stepId,
						dataInput,
						storageConfig,
						verifyFlag,
						batchSize,
						s3Client);
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
