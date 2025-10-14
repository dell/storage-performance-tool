package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
public class S3ObjectTaggingTest
				extends S3StorageDriver<DataItem, DataOperation<DataItem>> {

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
			config.val("storage-object-tagging-enabled", true);
			config.val(
							"storage-object-tagging-tags",
							new HashMap<String, String>() {
								{
									put("tag1", "value1");
									put("tag2", "value2");
								}
							});
			config.val("storage-object-versioning", true);
			config.val("storage-net-http-headers", new HashMap<>());
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

	public S3ObjectTaggingTest()
					throws Exception {
		this(getConfig());
	}

	private S3ObjectTaggingTest(final Config config)
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
	public void testPutTaggingRequest()
					throws Exception {
		final String bucketName = "/bucket2";
		final long itemSize = 10240;
		final String itemId = "00003brre8lgz";
		final DataItem dataItem = new DataItemImpl(
						itemId, Long.parseLong(itemId, Character.MAX_RADIX), itemSize);
		final DataOperation<DataItem> op = new DataOperationImpl<>(
						hashCode(), OpType.UPDATE, dataItem, null, bucketName,
						Credential.getInstance(CREDENTIAL.getUid(), CREDENTIAL.getSecret()), null, 0);
		final FullHttpRequest httpRequest = (FullHttpRequest) httpRequest(op, storageNodeAddrs[0]);
		assertEquals(HttpMethod.PUT, httpRequest.method());
		assertEquals(bucketName + "/" + itemId + "?tagging", httpRequest.uri());
		final HttpHeaders reqHeaders = httpRequest.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertEquals("sH5WWyhmHz+o5yoXD5DChg==", reqHeaders.get(HttpHeaderNames.CONTENT_MD5));
		assertTrue(reqHeaders.get(HttpHeaderNames.AUTHORIZATION).startsWith("AWS " + CREDENTIAL.getUid() + ":"));
		final String canonicalReq = getCanonical(reqHeaders, httpRequest.method(), httpRequest.uri());
		assertEquals("PUT\nsH5WWyhmHz+o5yoXD5DChg==\n\n\n" + bucketName + '/' + itemId + "?tagging", canonicalReq);
		final var contentStr = httpRequest.content().toString(StandardCharsets.US_ASCII);
		assertEquals(
						"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
										"<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n" +
										"\t<TagSet>\n" +
										"\t\t<Tag><Key>tag1</Key><Value>value1</Value></Tag>\n" +
										"\t\t<Tag><Key>tag2</Key><Value>value2</Value></Tag>\n" +
										"\t</TagSet>\n" +
										"</Tagging>\n",
						contentStr);
	}

	@Test
	public void testGetTaggingRequest()
					throws Exception {
		final String bucketName = "/bucket2";
		final long itemSize = 10240;
		final String itemId = "00003brre8lgz";
		final DataItem dataItem = new DataItemImpl(
						itemId, Long.parseLong(itemId, Character.MAX_RADIX), itemSize);
		final DataOperation<DataItem> op = new DataOperationImpl<>(
						hashCode(), OpType.READ, dataItem, null, bucketName,
						Credential.getInstance(CREDENTIAL.getUid(), CREDENTIAL.getSecret()), null, 0);
		final FullHttpRequest httpRequest = (FullHttpRequest) httpRequest(op, storageNodeAddrs[0]);
		assertEquals(HttpMethod.GET, httpRequest.method());
		assertEquals(bucketName + "/" + itemId + "?tagging", httpRequest.uri());
		final HttpHeaders reqHeaders = httpRequest.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertTrue(reqHeaders.get(HttpHeaderNames.AUTHORIZATION).startsWith("AWS " + CREDENTIAL.getUid() + ":"));
		final String canonicalReq = getCanonical(reqHeaders, httpRequest.method(), httpRequest.uri());
		assertEquals("GET\n\n\n\n" + bucketName + '/' + itemId + "?tagging", canonicalReq);
	}

	@Test
	public void testDeleteTaggingRequest()
					throws Exception {
		final String bucketName = "/bucket2";
		final long itemSize = 10240;
		final String itemId = "00003brre8lgz";
		final DataItem dataItem = new DataItemImpl(
						itemId, Long.parseLong(itemId, Character.MAX_RADIX), itemSize);
		final DataOperation<DataItem> op = new DataOperationImpl<>(
						hashCode(), OpType.DELETE, dataItem, null, bucketName,
						Credential.getInstance(CREDENTIAL.getUid(), CREDENTIAL.getSecret()), null, 0);
		final FullHttpRequest httpRequest = (FullHttpRequest) httpRequest(op, storageNodeAddrs[0]);
		assertEquals(HttpMethod.DELETE, httpRequest.method());
		assertEquals(bucketName + "/" + itemId + "?tagging", httpRequest.uri());
		final HttpHeaders reqHeaders = httpRequest.headers();
		assertEquals(storageNodeAddrs[0], reqHeaders.get(HttpHeaderNames.HOST));
		assertTrue(reqHeaders.get(HttpHeaderNames.AUTHORIZATION).startsWith("AWS " + CREDENTIAL.getUid() + ":"));
		final String canonicalReq = getCanonical(reqHeaders, httpRequest.method(), httpRequest.uri());
		assertEquals("DELETE\n\n\n\n" + bucketName + '/' + itemId + "?tagging", canonicalReq);
	}
}
