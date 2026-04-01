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

		// ---------------------------
		// Required config values
		// ---------------------------
		final String accessKey = storageConfig.stringVal("access-key");
		final String secretKey = storageConfig.stringVal("secret-key");
		final String region = storageConfig.stringVal("region");

		// ---------------------------
		// Optional config values (confuse-style)
		// ---------------------------
		String endpoint;
		boolean pathStyle;
		int maxConnections;
		int socketTimeout;
		int connTimeout;

		try {
			endpoint = storageConfig.stringVal("endpoint");
		} catch (Exception e) {
			endpoint = null;
		}

		try {
			pathStyle = storageConfig.boolVal("path-style-access");
		} catch (Exception e) {
			pathStyle = false;
		}

		try {
			maxConnections = storageConfig.intVal("max-connections");
		} catch (Exception e) {
			maxConnections = 64;
		}

		try {
			socketTimeout = storageConfig.intVal("socket-timeout-ms");
		} catch (Exception e) {
			socketTimeout = 60000;
		}

		try {
			connTimeout = storageConfig.intVal("connection-timeout-ms");
		} catch (Exception e) {
			connTimeout = 10000;
		}

		// ---------------------------
		// Build AWS client
		// ---------------------------
		final var creds = AwsBasicCredentials.create(accessKey, secretKey);

		final var httpClient = ApacheHttpClient.builder()
						.maxConnections(maxConnections)
						.socketTimeout(Duration.ofMillis(socketTimeout))
						.connectionTimeout(Duration.ofMillis(connTimeout))
						.build();

		final var s3Builder = S3Client.builder()
						.region(Region.of(region))
						.credentialsProvider(
										StaticCredentialsProvider.create(creds))
						.httpClient(httpClient);

		if (endpoint != null && !endpoint.isEmpty()) {
			s3Builder.endpointOverride(URI.create(endpoint));
		}

		if (pathStyle) {
			s3Builder.forcePathStyle(true);
		}

		return new S3AwsStorageDriver<>(
						stepId,
						dataInput,
						storageConfig,
						verifyFlag,
						batchSize,
						s3Builder.build());
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
