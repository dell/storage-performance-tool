package com.dell.spt.storage.driver.coop.aws.s3;

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * The three kinds of S3 client the driver builds, made as the driver makes them from a set of
 * timeouts but without the driver, so that a test can look at the failure a client reports.
 */
final class S3AwsRawClients {

	private static final StaticCredentialsProvider CREDENTIALS = StaticCredentialsProvider.create(AwsBasicCredentials.create("access-key", "secret-key"));

	private S3AwsRawClients() {}

	/** The CRT transfer client that serves most operations. */
	static S3AsyncClient transfer(final int port, final S3AwsClientTimeouts timeouts) {
		return S3AsyncClient.crtBuilder()
						.credentialsProvider(CREDENTIALS)
						.region(Region.US_EAST_1)
						.endpointOverride(endpoint(port))
						.forcePathStyle(true)
						.maxConcurrency(8)
						.httpConfiguration(timeouts.crtHttpConfiguration())
						.build();
	}

	/** The ordinary SDK client over the CRT HTTP client, which serves exact-version requests. */
	static S3AsyncClient exactVersion(final int port, final S3AwsClientTimeouts timeouts) {
		return S3AsyncClient.builder()
						.credentialsProvider(CREDENTIALS)
						.region(Region.US_EAST_1)
						.endpointOverride(endpoint(port))
						.forcePathStyle(true)
						.httpClientBuilder(timeouts.applyTo(AwsCrtAsyncHttpClient.builder().maxConcurrency(8)))
						.build();
	}

	/** The resources of the standalone DELETE client. */
	static S3AwsStandaloneDeleteResources standaloneDelete(final int port, final S3AwsClientTimeouts timeouts) {
		return S3AwsStandaloneDeleteResources.create(new S3AwsStandaloneDeleteResources.Configuration(
						CREDENTIALS, Region.US_EAST_1, endpoint(port), true, 8, timeouts));
	}

	private static URI endpoint(final int port) {
		return URI.create("http://127.0.0.1:" + port);
	}
}
