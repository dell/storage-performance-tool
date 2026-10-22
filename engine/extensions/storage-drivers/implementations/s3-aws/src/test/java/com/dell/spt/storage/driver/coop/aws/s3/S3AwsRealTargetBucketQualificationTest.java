package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Opt-in qualification of target-bucket provisioning against a real S3-compatible endpoint.
 *
 * <p>Enable it with {@code SPT_REAL_S3_TEST=true} and the variables of the local test profile:
 * {@code S3_ENDPOINTS} (or {@code S3_ENDPOINT}), {@code S3_ACCESS_KEY}, {@code S3_SECRET_KEY} and
 * optionally {@code S3_REGION}. Once enabled, missing configuration, permissions or connectivity
 * fail the test. It creates only uniquely named {@code spt-tbq-*} buckets and removes them and
 * their objects in {@code finally}. Assertions use portable S3 semantics only.
 */
@EnabledIfEnvironmentVariable(named = "SPT_REAL_S3_TEST", matches = "true")
class S3AwsRealTargetBucketQualificationTest {

	@TempDir
	Path home;

	private record Target(URI endpoint, String accessKey, String secretKey, String region) {

		static Target fromEnvironment() {
			var endpoints = System.getenv("S3_ENDPOINTS");
			if (endpoints == null || endpoints.isBlank()) {
				endpoints = System.getenv("S3_ENDPOINT");
			}
			final var accessKey = System.getenv("S3_ACCESS_KEY");
			final var secretKey = System.getenv("S3_SECRET_KEY");
			if (endpoints == null || endpoints.isBlank() || accessKey == null || accessKey.isBlank()
							|| secretKey == null || secretKey.isBlank()) {
				fail("SPT_REAL_S3_TEST=true requires S3_ENDPOINTS (or S3_ENDPOINT), S3_ACCESS_KEY and S3_SECRET_KEY");
			}
			var first = endpoints.split(",")[0].trim();
			if (!first.contains("://")) {
				first = "http://" + first;
			}
			final var region = System.getenv("S3_REGION");
			return new Target(URI.create(first), accessKey, secretKey, region == null ? "" : region.trim());
		}

		int port() {
			if (endpoint.getPort() > 0) {
				return endpoint.getPort();
			}
			return "https".equals(endpoint.getScheme()) ? 443 : 80;
		}

		S3AsyncClient fixtureClient() {
			return S3AsyncClient.builder()
							.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
							.region(Region.of(region.isEmpty() ? "us-east-1" : region))
							.endpointOverride(endpoint)
							.forcePathStyle(true)
							.httpClientBuilder(AwsCrtAsyncHttpClient.builder())
							.build();
		}
	}

	@Test
	void provisioningCreatesUsesAndRefusesBucketsOnTheConfiguredEndpoint() throws Exception {
		final var target = Target.fromEnvironment();
		final var suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 16).toLowerCase(Locale.ROOT);
		final var created = "spt-tbq-" + suffix;
		final var refused = "spt-tbq-off-" + suffix;
		final List<String> cleanupFailures = new ArrayList<>();
		try (final var fixture = target.fixtureClient()) {
			try {
				assertFalse(exists(fixture, created), "the qualification bucket must start absent");
				assertFalse(exists(fixture, refused), "the refused bucket must start absent");

				run(target, created, true, "tbq-create");
				assertTrue(exists(fixture, created), "the absent bucket was not created");
				assertEquals(1, objects(fixture, created).size(), "exactly one measured object operation");

				run(target, created, true, "tbq-existing");
				assertEquals(2, objects(fixture, created).size(), "the existing bucket keeps its contents");

				final var failure = assertThrows(RunTerminalException.class, () -> run(target, refused, false, "tbq-refused"));
				assertFalse(failure instanceof IntegrityTerminalException);
				assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
				assertEquals("tbq-refused", failure.stepId());
				assertTrue(failure.getMessage().contains("target bucket \"" + refused + "\" does not exist"),
								failure.getMessage());
				assertFalse(exists(fixture, refused), "a disabled policy must leave the bucket absent");
			} finally {
				for (final var bucket : List.of(created, refused)) {
					try {
						remove(fixture, bucket);
					} catch (final RuntimeException e) {
						cleanupFailures.add(bucket + ": " + e.getClass().getSimpleName());
					}
				}
			}
		}
		assertTrue(cleanupFailures.isEmpty(), "qualification cleanup failed: " + cleanupFailures);
	}

	private void run(final Target target, final String bucket, final boolean provision, final String stepId)
					throws Exception {
		final var overrides = new java.util.HashMap<String, Object>(Map.of(
						"storage-auth-uid", target.accessKey(),
						"storage-auth-secret", target.secretKey(),
						"storage-region", target.region(),
						"storage-net-node-addrs", List.of(target.endpoint().getHost()),
						"storage-net-ssl-enabled", "https".equals(target.endpoint().getScheme())));
		final var config = S3AwsMergedConfig.create(home, target.port(), overrides);
		final var scenario = """
						CreateLoad.config({
						  "storage": {"driver": {"type": "s3-aws", "limit": {"concurrency": 1}},
						    "targetBucket": {"provision": %s}},
						  "item": {"data": {"size": "1KiB"}, "output": {"path": "/%s"}},
						  "load": {"op": {"type": "create", "limit": {"count": 1}}, "step": {"id": "%s"}}
						}).run();
						""".formatted(provision, bucket, stepId);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			ScenarioUtil.configure(engine, Extension.load(classLoader), config, metrics);
			new RunImpl("target-bucket qualification", scenario, engine, 1L).run();
		}
	}

	private static boolean exists(final S3AsyncClient fixture, final String bucket) {
		try {
			fixture.headBucket(request -> request.bucket(bucket)).join();
			return true;
		} catch (final CompletionException e) {
			if (e.getCause() instanceof NoSuchBucketException
							|| (e.getCause() instanceof S3Exception s3 && s3.statusCode() == 404)) {
				return false;
			}
			throw e;
		}
	}

	private static List<S3Object> objects(final S3AsyncClient fixture, final String bucket) {
		return fixture.listObjectsV2(request -> request.bucket(bucket)).join().contents();
	}

	/** Removes a bucket this test created, with its objects; an absent bucket needs nothing. */
	private static void remove(final S3AsyncClient fixture, final String bucket) {
		if (!exists(fixture, bucket)) {
			return;
		}
		for (final var object : objects(fixture, bucket)) {
			fixture.deleteObject(request -> request.bucket(bucket).key(object.key())).join();
		}
		fixture.deleteBucket(request -> request.bucket(bucket)).join();
	}
}
