package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.DateUtil;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.storage.Credential;
import static com.dell.spt.base.Constants.APP_NAME;

import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;

import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

@Tag("integration")
public class S3VersioningTest
				extends S3StorageDriver {

	private static final Credential CREDENTIAL = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	private static Config getConfig() {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(
											schemaProvider -> {
												try {
													return schemaProvider.schema();
												} catch (final Exception e) {
													throw new RuntimeException("Failed to get schema", e);
												}
											})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			final Map<String, Object> configSchema = TreeUtil.reduceForest(configSchemas);
			final Config config = new BasicConfig("-", configSchema);
			config.val("load-batch-size", 4096);
			config.val("storage-driver-limit-concurrency", 0);
			config.val("storage-net-transport", "epoll");
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl-enabled", false);
			config.val("storage-net-ssl-protocols", Collections.<String> emptyList());
			config.val("storage-net-ssl-provider", "OPENSSL");
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 0);
			config.val("storage-net-ioRatio", 50);
			config.val("storage-net-node-addrs", Collections.singletonList("127.0.0.1"));
			config.val("storage-net-node-port", 9024);
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-object-fsAccess", true);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", new HashMap<>());
			config.val("storage-object-versioning", true);
			config.val(
							"storage-net-http-headers",
							new HashMap<String, String>() {
								{
									put("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}");
								}
							});
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-uri-args", Collections.EMPTY_MAP);
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
			config.val("storage-auth-version", 2);
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1_000_000);
			return config;
		} catch (final Throwable cause) {
			throw new RuntimeException(cause);
		}
	}

	private final Queue<FullHttpRequest> httpRequestsLog = new ArrayDeque<>();

	public S3VersioningTest()
					throws Exception {
		this(getConfig());
	}

	private S3VersioningTest(final Config config)
					throws Exception {
		super(
						"test-storage-driver-s3", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("4MB"), 16, false),
						config.configVal("storage"), false, config.intVal("load-batch-size"));
	}

	@Override
	protected FullHttpResponse executeHttpRequest(final FullHttpRequest httpRequest) {
		httpRequestsLog.add(httpRequest);
		return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
	}

	@BeforeEach
	public void setUp() {
		start();
	}

	@AfterEach
	public void tearDown()
					throws Exception {
		httpRequestsLog.clear();
		close();
	}

	@Test
	public void testRequestNewVersionedPath()
					throws Exception {

		final String bucketName = "/bucket0";
		final String result = requestNewPath(bucketName);
		assertEquals(bucketName, result);
		assertEquals(3, httpRequestsLog.size());

		final FullHttpRequest req0 = httpRequestsLog.poll();
		assertEquals(HttpMethod.HEAD, req0.method());
		assertEquals(bucketName, req0.uri());
		final HttpHeaders reqHeaders0 = req0.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders0.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders0.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		final Date reqDate0 = DateUtil.FMT_DATE_RFC1123.parse(
						reqHeaders0.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate0.getTime(), 10_000);
		final String authHeaderValue0 = reqHeaders0.get(HttpHeaderNames.AUTHORIZATION);
		assertTrue(authHeaderValue0.startsWith("AWS " + CREDENTIAL.getUid() + ":"));

		final FullHttpRequest req1 = httpRequestsLog.poll();
		assertEquals(HttpMethod.GET, req1.method());
		assertEquals(bucketName + "?versioning", req1.uri());
		final HttpHeaders reqHeaders1 = req1.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders1.get(HttpHeaderNames.HOST));
		assertEquals(0, reqHeaders1.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
		final Date reqDate1 = DateUtil.FMT_DATE_RFC1123.parse(
						reqHeaders1.get(HttpHeaderNames.DATE));
		assertEquals(
						new Date().getTime(), reqDate1.getTime(), 10_000,
						"Date differs from now " + new Date() + " more than 10 sec: " + reqDate1);
		final String authHeaderValue1 = reqHeaders1.get(HttpHeaderNames.AUTHORIZATION);
		assertTrue(authHeaderValue1.startsWith("AWS " + CREDENTIAL.getUid() + ":"));

		final FullHttpRequest req2 = httpRequestsLog.poll();
		assertEquals(HttpMethod.PUT, req2.method());
		assertEquals(bucketName + "?versioning", req2.uri());
		final HttpHeaders reqHeaders2 = req2.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders2.get(HttpHeaderNames.HOST));
		final Date reqDate2 = DateUtil.FMT_DATE_RFC1123.parse(
						reqHeaders2.get(HttpHeaderNames.DATE));
		assertEquals(new Date().getTime(), reqDate2.getTime(), 10_000);
		final String authHeaderValue2 = reqHeaders2.get(HttpHeaderNames.AUTHORIZATION);
		assertTrue(authHeaderValue2.startsWith("AWS " + CREDENTIAL.getUid() + ":"));
		final byte[] reqContent2 = req2.content().array();
		assertEquals(S3Api.VERSIONING_ENABLE_CONTENT, reqContent2);
		assertEquals(
						reqContent2.length, reqHeaders2.getInt(HttpHeaderNames.CONTENT_LENGTH).intValue());
	}

}
