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

		try {
			return createInternal(stepId, dataInput, storageConfig, verifyFlag, batchSize);
		} catch (Exception e) {
			System.err.println("DEBUG: Factory create failed: " + e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	private S3AwsStorageDriver<I, O> createInternal(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {

		// ---------------------------
		// Required config values - support both legacy S3 and AWS formats
		// ---------------------------
		String accessKey;
		String secretKey;
		String region;
		String bucket;

		// Try legacy S3 parameters first (for backward compatibility)
		try {
			accessKey = storageConfig.stringVal("auth-uid");
		} catch (Exception e1) {
			try {
				accessKey = storageConfig.stringVal("storage-auth-uid");
			} catch (Exception e2) {
				// Try AWS-specific parameter
				try {
					accessKey = storageConfig.stringVal("access-key");
				} catch (Exception e3) {
					throw new IllegalConfigurationException("Missing required parameter: access-key (or storage-auth-uid)");
				}
			}
		}

		try {
			secretKey = storageConfig.stringVal("auth-secret");
		} catch (Exception e1) {
			try {
				secretKey = storageConfig.stringVal("storage-auth-secret");
			} catch (Exception e2) {
				// Try AWS-specific parameter
				try {
					secretKey = storageConfig.stringVal("secret-key");
				} catch (Exception e3) {
					throw new IllegalConfigurationException("Missing required parameter: secret-key (or storage-auth-secret)");
				}
			}
		}

		// Region is AWS-specific but optional for some S3-compatible services
		try {
			region = storageConfig.stringVal("region");
		} catch (Exception e) {
			// For S3-compatible services like SeaweedFS, use a default region
			region = "eu-west-2";
		}

		// Ensure region is never null for AWS SDK
		if (region == null || region.isEmpty()) {
			region = "eu-west-2";
		}

		// Bucket name - try different parameter names
		try {
			bucket = storageConfig.stringVal("bucket");
		} catch (Exception e1) {
			try {
				bucket = storageConfig.stringVal("storage-net-node-addrs");
				// Extract bucket from node addresses if it's the first part
				if (bucket != null && bucket.contains("/")) {
					bucket = bucket.split("/")[0];
				}
			} catch (Exception e2) {
				bucket = "default-bucket"; // Fallback
			}
		}

		// ---------------------------
		// Optional config values (confuse-style)
		// ---------------------------
		String endpoint = null;
		boolean pathStyle;
		int maxConnections;
		int socketTimeout;
		int connTimeout;

		try {
			endpoint = storageConfig.stringVal("endpoint");
			if (endpoint == null) {
				throw new Exception("endpoint parameter is null");
			}
		} catch (Exception e1) {
			try {
				// Try accessing nested config objects
				Config netConfig = storageConfig.configVal("net");
				Config nodeConfig = netConfig.configVal("node");
				List<String> addrs = nodeConfig.listVal("addrs");

				if (addrs != null && !addrs.isEmpty()) {
					String nodeAddrs = addrs.get(0);

					// Convert node address to endpoint URL
					if (!nodeAddrs.startsWith("http://") && !nodeAddrs.startsWith("https://")) {
						endpoint = "http://" + nodeAddrs;
					} else {
						endpoint = nodeAddrs;
					}
				} else {
					endpoint = null;
				}
			} catch (Exception e2) {
				endpoint = null;
			}
		}

		try {
			pathStyle = storageConfig.boolVal("path-style-access");
		} catch (Exception e1) {
			try {
				// Try legacy S3 path style access parameter
				pathStyle = storageConfig.boolVal("storage-net-s3-path-style-access");
			} catch (Exception e2) {
				pathStyle = false; // Default for AWS S3
			}
		}

		try {
			maxConnections = storageConfig.intVal("max-connections");
		} catch (Exception e) {
			maxConnections = 128; // Increased from 64 for better concurrency
		}

		try {
			socketTimeout = storageConfig.intVal("socket-timeout-ms");
		} catch (Exception e) {
			socketTimeout = 30000; // Reduced from 60000 for faster failure detection
		}

		try {
			connTimeout = storageConfig.intVal("connection-timeout-ms");
		} catch (Exception e) {
			connTimeout = 10000; // Reverted to 10s for more stable connections
		}

		// ---------------------------
		// Build AWS client
		// ---------------------------
		final var creds = AwsBasicCredentials.create(accessKey, secretKey);

		final var httpClient = ApacheHttpClient.builder()
						.maxConnections(maxConnections)
						.socketTimeout(Duration.ofMillis(socketTimeout))
						.connectionTimeout(Duration.ofMillis(connTimeout))
						.connectionAcquisitionTimeout(Duration.ofSeconds(5))
						.connectionMaxIdleTime(Duration.ofMinutes(1))
						.connectionTimeToLive(Duration.ofMinutes(5))
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
