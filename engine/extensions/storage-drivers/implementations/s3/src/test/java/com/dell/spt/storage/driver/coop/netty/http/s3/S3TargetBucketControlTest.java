package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketControlFactory;
import com.dell.spt.base.storage.driver.bucket.TargetBucketProvisioning;
import com.dell.spt.base.storage.driver.bucket.TargetBucketResponse.Kind;
import com.dell.spt.storage.driver.coop.netty.endpoint.ScriptedDnsServer;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class S3TargetBucketControlTest {

	private static S3StorageDriverTest.TestS3Driver driver(final Config config) throws Exception {
		return new S3StorageDriverTest.TestS3Driver(config);
	}

	@Test
	void checkClassifiesTheBucketHeadResponse() throws Exception {
		final Object[][] cases = {
				{200, Kind.OK, ""
				}, {204, Kind.OK, ""
				}, {404, Kind.NOT_FOUND, "not found"
				},
				{301, Kind.FAILED, "HTTP 301"
				}, {307, Kind.FAILED, "HTTP 307"
				}, {400, Kind.FAILED, "HTTP 400"
				},
				{401, Kind.FAILED, "HTTP 401"
				}, {403, Kind.FAILED, "HTTP 403"
				}, {500, Kind.FAILED, "HTTP 500"
				},
				{503, Kind.FAILED, "HTTP 503"
				}
		};
		for (final var c : cases) {
			final var drv = driver(S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1"));
			drv.enqueueResponse(new DefaultFullHttpResponse(
							HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf((Integer) c[0])));

			final var response = drv.checkTargetBucket("bench", null);

			assertEquals(c[1], response.kind(), "status " + c[0]);
			assertEquals(c[2], response.detail(), "status " + c[0]);
			final var head = drv.log().poll();
			assertEquals(HttpMethod.HEAD, head.method());
			assertEquals("/bench", head.uri());
			assertEquals(0, head.content().readableBytes());
		}
	}

	@Test
	void transportFailuresAreNeverAbsence() throws Exception {
		final var refused = driver(S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1"));
		refused.failRequestsForTest(new ConnectException("Connection refused: 192.0.2.3:9024"));
		final var connect = refused.checkTargetBucket("bench", null);
		assertEquals(Kind.FAILED, connect.kind());
		assertEquals("connection failed", connect.detail(), "the public detail never names the endpoint");

		final var silent = new S3StorageDriverTest.TestS3Driver(
						S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1")) {
			@Override
			protected FullHttpResponse executeHttpRequest(final FullHttpRequest httpRequest) {
				return null;
			}
		};
		final var timeout = silent.checkTargetBucket("bench", null);
		assertEquals(Kind.FAILED, timeout.kind());
		assertTrue(timeout.detail().startsWith("timed out"), timeout.detail());
	}

	@Test
	void controlRequestsKeepGlobalHeadersAndUriArgumentsButNoObjectState() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, true, "crc32", "127.0.0.1");
		config.val("storage-net-http-headers", new HashMap<>(Map.of("X-Spt-Global", "on")));
		config.val("storage-net-http-uri-args", Map.of("tenant", "t1"));
		config.val("storage-object-tagging-enabled", true);
		config.val("storage-object-tagging-tags", Map.of("k", "v"));
		final var drv = driver(config);

		assertEquals(Kind.OK, drv.checkTargetBucket("bench", null).kind());
		assertEquals(Kind.OK, drv.createTargetBucket("bench", null).kind());

		for (final var request : List.of(drv.log().poll(), drv.log().poll())) {
			assertEquals("/bench?tenant=t1", request.uri());
			assertEquals("on", request.headers().get("X-Spt-Global"));
			assertTrue(request.headers().contains(HttpHeaderNames.AUTHORIZATION));
			for (final var header : request.headers()) {
				final var name = header.getKey().toLowerCase(java.util.Locale.ROOT);
				assertFalse(name.startsWith("x-amz-meta-") || name.startsWith("x-amz-checksum")
								|| name.equals("x-amz-tagging") || name.equals("content-md5")
								|| name.startsWith("x-amz-rdma") || name.equals("x-amz-copy-source"),
								"object state leaked into a bucket request: " + name);
			}
		}
	}

	@Test
	void genericEndpointsGetAnEmptyCreateBucketEvenWithASigningRegion() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "minio.example");
		config.val("storage-region", "eu-west-1");
		final var drv = driver(config);

		drv.createTargetBucket("bench", null);

		final var put = drv.log().poll();
		assertEquals(HttpMethod.PUT, put.method());
		assertEquals(0, put.content().readableBytes());
		assertEquals("0", put.headers().get(HttpHeaderNames.CONTENT_LENGTH));
		assertTrue(put.headers().get(HttpHeaderNames.AUTHORIZATION).contains("/eu-west-1/s3/aws4_request"));
	}

	@Test
	void awsEndpointsGetARegionalLocationConstraint() throws Exception {
		final String[][] cases = {
				{"s3.eu-west-1.amazonaws.com", "", "eu-west-1"
				},
				{"s3.amazonaws.com", "", ""
				},
				{"s3.us-east-1.amazonaws.com", "", ""
				},
				{"s3.eu-west-1.amazonaws.com", "us-west-2", "us-west-2"
				}
		};
		for (final var c : cases) {
			final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, c[0]);
			config.val("storage-region", c[1]);
			final var drv = driver(config);

			drv.createTargetBucket("bench", null);

			final var put = drv.log().poll();
			final var body = put.content().toString(StandardCharsets.UTF_8);
			if (c[2].isEmpty()) {
				assertEquals("", body, c[0]);
			} else {
				assertTrue(body.contains("<LocationConstraint>" + c[2] + "</LocationConstraint>"), body);
				assertEquals(String.valueOf(put.content().readableBytes()),
								put.headers().get(HttpHeaderNames.CONTENT_LENGTH));
				assertTrue(put.headers().get(HttpHeaderNames.AUTHORIZATION).contains("/" + c[2] + "/s3/aws4_request"));
			}
		}
	}

	@Test
	void perPathCredentialSignsTheBucketRequest() throws Exception {
		final var drv = driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));

		drv.checkTargetBucket("bench", Credential.getInstance("writer", "secret-w"));
		drv.checkTargetBucket("bench", null);

		assertTrue(drv.log().poll().headers().get(HttpHeaderNames.AUTHORIZATION).startsWith("AWS writer:"));
		assertTrue(drv.log().poll().headers().get(HttpHeaderNames.AUTHORIZATION).startsWith("AWS user1:"));
	}

	@Test
	void unboundedNetworkTimeoutIsBoundedForControlRequests() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		config.val("storage-net-endpoint-selection", "round-robin");
		final var control = S3TargetBucketControl.controlConfig(config.configVal("storage"));

		assertEquals(S3TargetBucketControl.UNBOUNDED_TIMEOUT_REPLACEMENT_MILLIS, control.intVal("net-timeoutMilliSec"));
		assertEquals("round-robin", control.stringVal("net-endpoint-selection"), "endpoint selection is preserved");
		assertEquals(0, config.intVal("storage-net-timeoutMilliSec"), "the step configuration is not modified");
	}

	@Test
	void roundRobinControlRequestsUseTheLogicalHostname() throws Exception {
		final var hosts = new CopyOnWriteArrayList<String>();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			hosts.add(exchange.getRequestHeaders().getFirst("Host"));
			exchange.sendResponseHeaders(200, -1);
			exchange.close();
		});
		server.start();
		try {
			final int port = server.getAddress().getPort();
			final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
			config.val("storage-net-node-port", port);
			config.val("storage-net-timeoutMilliSec", 2000);
			config.val("storage-net-endpoint-selection", "round-robin");
			config.val("storage-net-endpoint-hostname", "buckets.example.test");
			config.val("storage-net-endpoint-connect-timeoutMilliSec", 1000);

			try (final var control = new S3StorageDriverExtension<>().openTargetBucketControl("step", config.configVal("storage"))) {
				assertEquals(Kind.OK, control.check("bench", null).kind());
			}

			assertEquals(List.of("buckets.example.test:" + port), hosts,
							"bucket checks use the logical hostname for Host and signing");
		} finally {
			server.stop(0);
		}
	}

	@Test
	void perRequestDnsControlRequestsResolveThroughTheConfiguredServer() throws Exception {
		final var hosts = new CopyOnWriteArrayList<String>();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			hosts.add(exchange.getRequestHeaders().getFirst("Host"));
			exchange.sendResponseHeaders(404, -1);
			exchange.close();
		});
		server.start();
		try (final var dns = new ScriptedDnsServer(query -> ScriptedDnsServer.Reply.answers(query.name(), "127.0.0.1"))) {
			final int port = server.getAddress().getPort();
			final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, EndpointSelectionTestSupport.HOSTNAME);
			config.val("storage-net-node-port", port);
			config.val("storage-net-timeoutMilliSec", 2000);
			config.val("storage-net-endpoint-selection", "per-request-dns");
			config.val("storage-net-endpoint-dns-server", "127.0.0.1:" + dns.address().getPort());
			config.val("storage-net-endpoint-dns-timeoutMilliSec", 2000);
			config.val("storage-net-endpoint-connect-timeoutMilliSec", 1000);

			try (final var control = new S3StorageDriverExtension<>().openTargetBucketControl("step", config.configVal("storage"))) {
				assertEquals(Kind.NOT_FOUND, control.check("bench", null).kind());
			}

			assertFalse(dns.received().isEmpty(), "the controller must resolve through the configured DNS server");
			assertEquals(List.of(EndpointSelectionTestSupport.HOSTNAME + ":" + port), hosts);
		} finally {
			server.stop(0);
		}
	}

	@Test
	void onlyTheShippedS3FactoryOptsIn() {
		final List<Extension> extensions = Extension.load(Thread.currentThread().getContextClassLoader());
		final var s3 = extensions.stream()
						.filter(e -> e instanceof StorageDriverFactory<?, ?, ?> f && "s3".equals(f.id()))
						.findFirst()
						.orElseThrow();
		assertInstanceOf(TargetBucketControlFactory.class, s3);
	}

	@Test
	void contextsWithDifferentRegionsSignWithTheirOwnRegion() throws Exception {
		final var authorizations = new CopyOnWriteArrayList<String>();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			authorizations.add(exchange.getRequestURI().getRawPath() + " " + exchange.getRequestHeaders().getFirst("Authorization"));
			exchange.sendResponseHeaders(200, -1);
			exchange.close();
		});
		server.start();
		try {
			final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
			config.val("storage-net-node-port", server.getAddress().getPort());
			config.val("storage-net-timeoutMilliSec", 2000);
			config.val("storage-targetBucket-provision", true);
			config.val("storage-auth-file", null);
			config.val("load-op-type", "create");
			config.val("item-type", "data");
			config.val("item-output-path", "/unused");
			config.val("item-input-path", null);
			config.val("item-input-file", null);
			final var east = new BasicConfig(config.pathSep(), config.schema(), Map.of(
							"item", Map.of("output", Map.of("path", "/bucket-east")),
							"storage", Map.of("region", "us-east-1")));
			final var west = new BasicConfig(config.pathSep(), config.schema(), Map.of(
							"item", Map.of("output", Map.of("path", "/bucket-west")),
							"storage", Map.of("region", "eu-west-1")));

			TargetBucketProvisioning.provision("regions", config, List.of(east, west),
							Extension.load(Thread.currentThread().getContextClassLoader()));

			assertEquals(2, authorizations.size(), authorizations.toString());
			assertTrue(authorizations.get(0).startsWith("/bucket-east ") && authorizations.get(0).contains("/us-east-1/s3/"),
							authorizations.get(0));
			assertTrue(authorizations.get(1).startsWith("/bucket-west ") && authorizations.get(1).contains("/eu-west-1/s3/"),
							authorizations.get(1));
		} finally {
			server.stop(0);
		}
	}

	@Test
	void controllerProvisionsThroughTheRealFactoryAndNettyTransport() throws Exception {
		final var requests = new CopyOnWriteArrayList<String>();
		final var created = new AtomicBoolean();
		final var server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", exchange -> {
			final var method = exchange.getRequestMethod();
			final var path = exchange.getRequestURI().getRawPath();
			requests.add(method + " " + path + (exchange.getRequestHeaders().containsKey("Authorization") ? " signed" : ""));
			exchange.getRequestBody().readAllBytes();
			final int status;
			if ("PUT".equals(method) && "/canary".equals(path)) {
				created.set(true);
				status = 200;
			} else if ("HEAD".equals(method) && "/canary".equals(path)) {
				status = created.get() ? 200 : 404;
			} else if ("HEAD".equals(method) && "/denied".equals(path)) {
				status = 403;
			} else {
				status = 500;
			}
			exchange.sendResponseHeaders(status, -1);
			exchange.close();
		});
		server.start();
		try {
			final Config config = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
			config.val("storage-net-node-port", server.getAddress().getPort());
			config.val("storage-net-timeoutMilliSec", 5000);
			config.val("storage-targetBucket-provision", true);
			config.val("storage-auth-file", null);
			config.val("load-op-type", "create");
			config.val("item-type", "data");
			config.val("item-output-path", "/canary/prefix");
			config.val("item-input-path", null);
			config.val("item-input-file", null);
			config.val("load-step-id", "canary");
			final List<Extension> extensions = Extension.load(Thread.currentThread().getContextClassLoader());

			TargetBucketProvisioning.provision("canary", config, null, extensions);
			TargetBucketProvisioning.provision("canary", config, null, extensions);

			assertEquals(List.of("HEAD /canary signed", "PUT /canary signed", "HEAD /canary signed"), requests);

			requests.clear();
			config.val("item-output-path", "/denied");
			final var failure = assertThrows(
							RunTerminalException.class, () -> TargetBucketProvisioning.provision("canary", config, null, extensions));
			assertEquals("target bucket \"denied\" is not accessible (HTTP 403)", failure.getMessage());
			assertEquals(List.of("HEAD /denied signed"), requests, "an access failure never leads to CreateBucket");
		} finally {
			server.stop(0);
		}
	}
}
