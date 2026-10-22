package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.bucket.AwsS3Region;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse;
import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Target-bucket control client of the {@code s3-aws} driver.
 *
 * <p>It owns an ordinary SDK client built with the driver's endpoint, path-style addressing,
 * static credentials, region and connection bounds, and closes it with the client. Per-path
 * credentials ({@code storage.auth.file}) are not supported by this driver, so every request uses
 * the configured credentials.
 */
final class S3AwsTargetBucketControl implements TargetBucketControl {

	private static final Logger LOG = LoggerFactory.getLogger(S3AwsTargetBucketControl.class);

	/** The wait bound when {@code storage.crt.requestTimeoutMs} is 0. */
	static final Duration UNBOUNDED_REQUEST_REPLACEMENT = Duration.ofSeconds(30);

	private final S3AsyncClient client;
	private final String region;
	private final String endpoint;
	private final Duration requestBound;

	S3AwsTargetBucketControl(
					final S3AsyncClient client, final String region, final String endpoint, final Duration requestTimeout) {
		this.client = client;
		this.region = region;
		this.endpoint = endpoint;
		this.requestBound = requestTimeout.isZero() ? UNBOUNDED_REQUEST_REPLACEMENT : requestTimeout;
	}

	@Override
	public TargetBucketResponse check(final String bucket, final Credential credential) throws InterruptedException {
		return await(client.headBucket(request -> request.bucket(bucket)));
	}

	@Override
	public TargetBucketResponse create(final String bucket, final Credential credential) throws InterruptedException {
		final var request = CreateBucketRequest.builder().bucket(bucket);
		if (AwsS3Region.needsLocationConstraint(region, endpoint)) {
			request.createBucketConfiguration(configuration -> configuration.locationConstraint(region));
		}
		final var response = await(client.createBucket(request.build()));
		// Absence is a check outcome only; a create that reports 404 failed.
		return response.kind() == TargetBucketResponse.Kind.NOT_FOUND ? TargetBucketResponse.failed("HTTP 404") : response;
	}

	/**
	 * Waits for the SDK's own request future and cancels that future, not a dependent stage, on a
	 * timeout or interruption: cancelling a dependent stage leaves the request running.
	 */
	private TargetBucketResponse await(final CompletableFuture<?> future) throws InterruptedException {
		try {
			future.get(requestBound.toMillis(), TimeUnit.MILLISECONDS);
			return TargetBucketResponse.ok();
		} catch (final InterruptedException e) {
			future.cancel(true);
			throw e;
		} catch (final TimeoutException e) {
			future.cancel(true);
			return TargetBucketResponse.failed("timed out after " + requestBound.toMillis() + " ms");
		} catch (final CancellationException e) {
			return TargetBucketResponse.failed("request cancelled");
		} catch (final ExecutionException e) {
			LOG.debug("target bucket request failed: {}", String.valueOf(e.getCause()));
			return classify(e.getCause());
		}
	}

	/** Classifies a failed request without exposing SDK messages, which may name the endpoint. */
	static TargetBucketResponse classify(final Throwable failure) {
		if (failure instanceof NoSuchBucketException) {
			return TargetBucketResponse.notFound();
		}
		if (failure instanceof S3Exception s3Failure && s3Failure.statusCode() > 0) {
			return TargetBucketResponse.ofHttpStatus(s3Failure.statusCode());
		}
		if (failure instanceof ApiCallTimeoutException || failure instanceof ApiCallAttemptTimeoutException
						|| S3AwsClientTimeouts.isStall(failure) || S3AwsClientTimeouts.isSocketTimeout(failure)) {
			return TargetBucketResponse.failed("timed out");
		}
		if (failure instanceof SdkClientException) {
			return TargetBucketResponse.failed("connection failed");
		}
		return TargetBucketResponse.failed("request failed");
	}

	@Override
	public void close() {
		client.close();
	}
}
