package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControlFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketProvisioning;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse.Kind;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.core.exception.ApiCallTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

class S3AwsTargetBucketControlTest {

	@TempDir
	Path home;

	private static S3Exception status(final int code) {
		return (S3Exception) S3Exception.builder().statusCode(code).message("raw SDK text with 192.0.2.3").build();
	}

	@Test
	void failuresAreClassifiedWithoutSdkText() {
		assertEquals(Kind.NOT_FOUND, S3AwsTargetBucketControl.classify(NoSuchBucketException.builder().build()).kind());
		assertEquals(Kind.NOT_FOUND, S3AwsTargetBucketControl.classify(status(404)).kind());
		for (final int code : new int[]{301, 400, 401, 403, 409, 500, 503
		}) {
			final var response = S3AwsTargetBucketControl.classify(status(code));
			assertEquals(Kind.FAILED, response.kind(), "status " + code);
			assertEquals("HTTP " + code, response.detail());
		}
		assertEquals("timed out", S3AwsTargetBucketControl.classify(ApiCallTimeoutException.create(1000)).detail());
		assertEquals("connection failed",
						S3AwsTargetBucketControl.classify(SdkClientException.create("Unable to connect to 192.0.2.3")).detail());
		assertEquals("request failed", S3AwsTargetBucketControl.classify(new IllegalStateException("x")).detail());
	}

	@Test
	void createSendsALocationConstraintOnlyToAwsOutsideUsEast1() throws Exception {
		final Object[][] cases = {
				{"https://s3.eu-west-1.amazonaws.com", "eu-west-1", "eu-west-1"
				},
				{"https://s3.amazonaws.com", "us-east-1", null
				},
				{"http://minio.example:9000", "eu-west-1", null
				}
		};
		for (final var c : cases) {
			final var client = mock(S3AsyncClient.class);
			when(client.createBucket(any(CreateBucketRequest.class)))
							.thenReturn(CompletableFuture.completedFuture(CreateBucketResponse.builder().build()));
			final var control = new S3AwsTargetBucketControl(client, (String) c[1], (String) c[0], Duration.ofSeconds(1));

			assertEquals(Kind.OK, control.create("bench", null).kind());

			final var request = ArgumentCaptor.forClass(CreateBucketRequest.class);
			verify(client).createBucket(request.capture());
			assertEquals("bench", request.getValue().bucket());
			final var configuration = request.getValue().createBucketConfiguration();
			if (c[2] == null) {
				assertNull(configuration, (String) c[0]);
			} else {
				assertEquals(c[2], configuration.locationConstraintAsString());
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void checkUsesHeadBucketAndCreateNeverReportsAbsence() throws Exception {
		final var client = mock(S3AsyncClient.class);
		when(client.headBucket(any(Consumer.class)))
						.thenReturn(CompletableFuture.completedFuture(HeadBucketResponse.builder().build()))
						.thenReturn(CompletableFuture.failedFuture(NoSuchBucketException.builder().build()));
		when(client.createBucket(any(CreateBucketRequest.class)))
						.thenReturn(CompletableFuture.failedFuture(status(404)));
		final var control = new S3AwsTargetBucketControl(client, "us-east-1", "http://minio.example", Duration.ofSeconds(1));

		assertEquals(Kind.OK, control.check("bench", null).kind());
		assertEquals(Kind.NOT_FOUND, control.check("bench", null).kind());
		final var create = control.create("bench", null);
		assertEquals(Kind.FAILED, create.kind());
		assertEquals("HTTP 404", create.detail());
		control.close();
		verify(client).close();
	}

	@Test
	@SuppressWarnings("unchecked")
	void waitsAreBoundedAndInterruptible() throws Exception {
		final var client = mock(S3AsyncClient.class);
		final var pending = new CompletableFuture<HeadBucketResponse>();
		when(client.headBucket(any(Consumer.class))).thenReturn(pending);
		final var control = new S3AwsTargetBucketControl(client, "us-east-1", "http://minio.example", Duration.ofMillis(50));

		final var timeout = control.check("bench", null);
		assertEquals(Kind.FAILED, timeout.kind());
		assertEquals("timed out after 50 ms", timeout.detail());

		final var unbounded = new S3AwsTargetBucketControl(client, "us-east-1", "http://minio.example", Duration.ZERO);
		Thread.currentThread().interrupt();
		try {
			assertThrows(InterruptedException.class, () -> unbounded.check("bench", null));
		} finally {
			Thread.interrupted();
		}
		assertEquals(Duration.ofSeconds(30), S3AwsTargetBucketControl.UNBOUNDED_REQUEST_REPLACEMENT);
	}

	@Test
	void regionPrecedenceReplacesTheEuWest2Fallback() {
		final var config = mock(Config.class);
		when(config.stringVal("region")).thenReturn(null, "", "ap-south-1", "ap-south-1");

		assertEquals("us-east-1", S3AwsStorageDriverFactory.effectiveRegion(config, "http://minio.example:9000"));
		assertEquals("eu-west-1", S3AwsStorageDriverFactory.effectiveRegion(config, "https://s3.eu-west-1.amazonaws.com"));
		assertEquals("ap-south-1", S3AwsStorageDriverFactory.effectiveRegion(config, "http://minio.example:9000"));
		assertEquals("ap-south-1", S3AwsStorageDriverFactory.effectiveRegion(config, "https://s3.eu-west-1.amazonaws.com"),
						"an explicit region wins over endpoint inference");
	}

	@Test
	void factoryOptsIntoTheCapability() {
		final var factory = Extension.load(Thread.currentThread().getContextClassLoader()).stream()
						.filter(e -> e instanceof StorageDriverFactory<?, ?, ?> f && "s3-aws".equals(f.id()))
						.findFirst()
						.orElseThrow();
		assertInstanceOf(TargetBucketControlFactory.class, factory);
	}

	@Test
	void controllerProvisionsThroughTheRealSdkClient() throws Exception {
		final var requests = new CopyOnWriteArrayList<String>();
		final var created = new AtomicBoolean();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			final var method = exchange.getRequestMethod();
			final var path = exchange.getRequestURI().getRawPath();
			final var body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
			requests.add(method + " " + path + (body.isEmpty() ? "" : " with-body")
							+ (exchange.getRequestHeaders().containsKey("Authorization") ? " signed" : ""));
			final int code;
			if ("PUT".equals(method) && "/aws-canary".equals(path)) {
				created.set(true);
				code = 200;
			} else if ("HEAD".equals(method) && "/aws-canary".equals(path)) {
				code = created.get() ? 200 : 404;
			} else if ("HEAD".equals(method) && "/aws-denied".equals(path)) {
				code = 403;
			} else {
				code = 500;
			}
			exchange.sendResponseHeaders(code, -1);
			exchange.close();
		});
		server.start();
		try {
			final Config config = S3AwsMergedConfig.create(home, server.getAddress().getPort(), Map.of());
			config.val("storage-region", "");
			config.val("load-op-type", "create");
			config.val("item-output-path", "/aws-canary/prefix");
			final List<Extension> extensions = Extension.load(Thread.currentThread().getContextClassLoader());

			TargetBucketProvisioning.provision("aws-canary", config, null, extensions);

			assertEquals(List.of("HEAD /aws-canary signed", "PUT /aws-canary signed"), requests,
							"a generic endpoint gets an empty CreateBucket");

			requests.clear();
			config.val("item-output-path", "/aws-denied");
			final var failure = assertThrows(RunTerminalException.class,
							() -> TargetBucketProvisioning.provision("aws-canary", config, null, extensions));
			assertEquals("target bucket \"aws-denied\" is not accessible (HTTP 403)", failure.getMessage());
			assertEquals(List.of("HEAD /aws-denied signed"), requests);
		} finally {
			server.stop(0);
		}
	}
}
