package com.dell.spt.storage.driver.coop.aws.s3;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/** Owns the AWS SDK and native HTTP resources used only by standalone DELETE. */
final class S3AwsStandaloneDeleteResources implements AutoCloseable {

	@FunctionalInterface
	interface Factory {
		S3AwsStandaloneDeleteResources create(Configuration configuration);
	}

	record Configuration(
				AwsCredentialsProvider credentialsProvider,
				Region region,
				URI endpoint,
				boolean pathStyle,
				int maxConcurrency,
				Duration connectionTimeout) {

		Configuration {
			Objects.requireNonNull(credentialsProvider);
			Objects.requireNonNull(region);
			Objects.requireNonNull(endpoint);
			Objects.requireNonNull(connectionTimeout);
		}
	}

	private final S3AsyncClient s3Client;
	private final SdkAsyncHttpClient httpClient;

	S3AwsStandaloneDeleteResources(
					final S3AsyncClient s3Client, final SdkAsyncHttpClient httpClient) {
		this.s3Client = Objects.requireNonNull(s3Client);
		this.httpClient = Objects.requireNonNull(httpClient);
	}

	static S3AwsStandaloneDeleteResources create(final Configuration configuration) {
		return create(
						() -> new DeleteTimingAsyncHttpClient(
										CrtDeleteTimingAsyncHttpClient.builder()
														.operationAttribute(DeleteTimingAsyncHttpClient.DELETE_OPERATION)
														.maxConcurrency(configuration.maxConcurrency())
														.connectionTimeout(configuration.connectionTimeout())
														.build()),
						httpClient -> S3AsyncClient.builder()
										.credentialsProvider(configuration.credentialsProvider())
										.region(configuration.region())
										.endpointOverride(configuration.endpoint())
										.forcePathStyle(configuration.pathStyle())
										.httpClient(httpClient)
										.build());
	}

	static S3AwsStandaloneDeleteResources create(
					final Supplier<SdkAsyncHttpClient> httpClientFactory,
					final Function<SdkAsyncHttpClient, S3AsyncClient> s3ClientFactory) {
		SdkAsyncHttpClient httpClient = null;
		try {
			httpClient = Objects.requireNonNull(
							httpClientFactory.get(), "Standalone DELETE HTTP client factory returned null");
			final S3AsyncClient s3Client = Objects.requireNonNull(
							s3ClientFactory.apply(httpClient),
							"Standalone DELETE S3 client factory returned null");
			return new S3AwsStandaloneDeleteResources(s3Client, httpClient);
		} catch (final RuntimeException | Error constructionFailure) {
			closeAfterConstructionFailure(httpClient, constructionFailure);
			throw constructionFailure;
		}
	}

	S3AsyncClient s3Client() {
		return s3Client;
	}

	@Override
	public void close() {
		Throwable closeFailure = null;
		try {
			s3Client.close();
		} catch (final RuntimeException | Error failure) {
			closeFailure = failure;
		}
		try {
			httpClient.close();
		} catch (final RuntimeException | Error failure) {
			if (closeFailure == null) {
				closeFailure = failure;
			} else {
				closeFailure.addSuppressed(failure);
			}
		}
		rethrow(closeFailure);
	}

	private static void closeAfterConstructionFailure(
					final SdkAsyncHttpClient httpClient, final Throwable constructionFailure) {
		if (httpClient == null) {
			return;
		}
		try {
			httpClient.close();
		} catch (final RuntimeException | Error closeFailure) {
			constructionFailure.addSuppressed(closeFailure);
		}
	}

	private static void rethrow(final Throwable failure) {
		if (failure instanceof RuntimeException runtimeFailure) {
			throw runtimeFailure;
		}
		if (failure instanceof Error error) {
			throw error;
		}
	}

	/** Serializes first construction with close and prevents any post-close creation. */
	static final class Lazy implements AutoCloseable {
		private Supplier<S3AwsStandaloneDeleteResources> supplier;
		private S3AwsStandaloneDeleteResources resources;
		private RuntimeException constructionRuntimeFailure;
		private Error constructionError;
		private boolean closed;

		Lazy(final Supplier<S3AwsStandaloneDeleteResources> supplier) {
			this.supplier = Objects.requireNonNull(supplier);
		}

		synchronized S3AsyncClient client() {
			if (closed) {
				throw new IllegalStateException("Standalone DELETE resources are already closed");
			}
			if (constructionRuntimeFailure != null) {
				throw constructionRuntimeFailure;
			}
			if (constructionError != null) {
				throw constructionError;
			}
			if (resources == null) {
				try {
					resources = Objects.requireNonNull(
									supplier.get(), "Standalone DELETE resource supplier returned null");
					supplier = null;
				} catch (final RuntimeException failure) {
					constructionRuntimeFailure = failure;
					supplier = null;
					throw failure;
				} catch (final Error failure) {
					constructionError = failure;
					supplier = null;
					throw failure;
				}
			}
			return resources.s3Client();
		}

		@Override
		public void close() {
			final S3AwsStandaloneDeleteResources resourcesToClose;
			synchronized (this) {
				if (closed) {
					return;
				}
				closed = true;
				supplier = null;
				resourcesToClose = resources;
				resources = null;
			}
			if (resourcesToClose != null) {
				resourcesToClose.close();
			}
		}
	}
}
