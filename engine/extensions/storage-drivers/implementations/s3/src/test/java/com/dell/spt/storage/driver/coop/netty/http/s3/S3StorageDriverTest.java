package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemFactoryImpl;
import com.dell.spt.base.item.ItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.path.PathOperation;
import com.dell.spt.base.item.op.path.PathOperationsBuilderImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.Attribute;
import software.amazon.awssdk.crt.checksums.CRC64NVME;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;
import static com.dell.spt.base.Constants.APP_NAME;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

public class S3StorageDriverTest {

	private S3StorageDriver<Item, Operation<Item>> newDriverMock() {
		// Create a mock that calls real methods; constructor is not invoked
		return Mockito.mock(S3StorageDriver.class, Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));
	}

	@Test
	void canonicalV4_noQuery_usesEmptyBodyShaWhenNoContent() throws Exception {
		S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set("host", "s3.test:443");
		headers.set("date", "Thu, 21 Sep 2023 12:00:00 GMT");
		headers.set("content-length", "0");

		Map<String, String> extra = new HashMap<>();
		Method m = S3StorageDriver.class.getDeclaredMethod("getCanonicalV4", HttpHeaders.class, Map.class, HttpMethod.class, String.class);
		m.setAccessible(true);
		String canonical = (String) m.invoke(drv, headers, extra, HttpMethod.GET, "/bucket/object");

		assertTrue(canonical.contains(S3Api.AMZ_EMPTY_BODY_SHA256), "Expected empty body SHA256 marker when content-length=0");
		assertTrue(canonical.startsWith("GET\n"), "Method should lead canonical string");
		assertTrue(canonical.contains("\n/bucket/object\n"), "Path should be present with newline separation");
	}

	@Test
	void canonicalV4_queryWithoutEquals_appendsEquals() throws Exception {
		S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set("host", "s3.test:443");
		headers.set("date", "Thu, 21 Sep 2023 12:00:00 GMT");
		headers.set("content-length", "0");

		Map<String, String> extra = new HashMap<>();
		Method m = S3StorageDriver.class.getDeclaredMethod("getCanonicalV4", HttpHeaders.class, Map.class, HttpMethod.class, String.class);
		m.setAccessible(true);
		String canonical = (String) m.invoke(drv, headers, extra, HttpMethod.GET, "/bucket/object?uploads");

		assertTrue(canonical.contains("uploads="), "Canonical query string should contain '=' when missing");
	}

	@Test
	void nonCanonicalHeaders_filtersOnlyXAmz() throws Exception {
		S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set("x-amz-date", "20230921T120000Z");
		headers.set("x-amz-meta-foo", "bar");
		headers.set("Content-Type", "application/octet-stream");

		Method m = S3StorageDriver.class.getDeclaredMethod("getNonCanonicalHeaders", HttpHeaders.class);
		m.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<String, String> map = (Map<String, String>) m.invoke(drv, headers);

		assertTrue(map.containsKey("x-amz-date"));
		assertTrue(map.containsKey("x-amz-meta-foo"));
		assertFalse(map.containsKey("content-type"), "Non x-amz headers must not be included");
		assertEquals("20230921T120000Z", map.get("x-amz-date"));
		assertEquals("bar", map.get("x-amz-meta-foo"));
	}

	// ---------- Helpers for full driver-based tests ----------
	private static final Credential TEST_CRED = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");

	private static Config baseConfig(boolean versioning, int authVersion, boolean checksumEnabled, String checksumAlg, String host) {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(sp -> {
								try {
									return sp.schema();
								} catch (Exception e) {
									throw new RuntimeException(e);
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
			config.val("load-batch-size", 1024);
			config.val("storage-driver-limit-concurrency", 0);
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 1024);
			// Net: use NIO in tests to avoid native epoll dependency
			config.val("storage-net-transport", "nio");
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl-enabled", false);
			config.val("storage-net-ssl-protocols", List.of());
			config.val("storage-net-ssl-provider", "OPENSSL");
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-writeSpinCount", 1);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 0);
			config.val("storage-net-ioRatio", 50);
			config.val("storage-net-node-addrs", List.of(host));
			config.val("storage-net-node-port", 9024);
			config.val("storage-net-node-connAttemptsLimit", 0);
			// HTTP
			config.val("storage-net-http-headers", new HashMap<String, String>() {
				{
					put("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}");
				}
			});
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", Map.of());
			// Object
			config.val("storage-object-fsAccess", true);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", versioning);
			// Auth
			config.val("storage-auth-uid", TEST_CRED.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", TEST_CRED.getSecret());
			config.val("storage-auth-version", authVersion);
			// Checksum
			config.val("storage-checksum-enabled", checksumEnabled);
			if (checksumEnabled) {
				config.val("storage-checksum-algorithm", checksumAlg);
			}
			return config;
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static DataItem mockDataItemFromBytes(final byte[] payload) throws Exception {
		final DataItem dataItem = Mockito.mock(DataItem.class);
		final int[] readOffset = new int[]{0
		};
		Mockito.when(dataItem.size()).thenReturn((long) payload.length);
		Mockito.doAnswer(invocation -> {
			final ByteBuffer dst = invocation.getArgument(0);
			if (readOffset[0] >= payload.length) {
				return 0;
			}
			final int n = Math.min(dst.remaining(), payload.length - readOffset[0]);
			dst.put(payload, readOffset[0], n);
			readOffset[0] += n;
			return n;
		}).when(dataItem).read(Mockito.any(ByteBuffer.class));
		Mockito.doAnswer(invocation -> {
			readOffset[0] = 0;
			return null;
		}).when(dataItem).reset();
		return dataItem;
	}

	private static String checksumHeaderName(final String algorithm) {
		if ("md5".equals(algorithm)) {
			return HttpHeaderNames.CONTENT_MD5.toString();
		}
		if ("crc64-nvme".equals(algorithm)) {
			return S3Api.AMZ_CHECKSUM_PREFIX + "crc64nvme";
		}
		return S3Api.AMZ_CHECKSUM_PREFIX + algorithm;
	}

	private static String checksumHeaderFor(final String algorithm, final byte[] payload) throws Exception {
		final Config cfg = baseConfig(false, 4, true, algorithm, "s3.us-east-1.amazonaws.com:443");
		final TestS3Driver drv = new TestS3Driver(cfg);
		final DataItem dataItem = mockDataItemFromBytes(payload);
		final Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, dataItem, null, "/bucket", TEST_CRED);
		final HttpHeaders headers = new DefaultHttpHeaders();
		drv.applyChecksum(headers, op);
		return headers.get(checksumHeaderName(algorithm));
	}

	private static void assertChecksumHeader(final String algorithm, final byte[] payload, final String expected) throws Exception {
		assertEquals(expected, checksumHeaderFor(algorithm, payload));
	}

	private static String reference32BitChecksum(final Checksum checksum, final byte[] payload) {
		checksum.reset();
		checksum.update(payload, 0, payload.length);
		final byte[] checksumBytes = ByteBuffer.allocate(Integer.BYTES).putInt((int) (checksum.getValue() & 0xFFFFFFFFL)).array();
		return Base64.getEncoder().encodeToString(checksumBytes);
	}

	private static String reference64BitChecksum(final Checksum checksum, final byte[] payload) {
		checksum.reset();
		checksum.update(payload, 0, payload.length);
		final byte[] checksumBytes = ByteBuffer.allocate(Long.BYTES).putLong(checksum.getValue()).array();
		return Base64.getEncoder().encodeToString(checksumBytes);
	}

	private static class TestS3Driver extends S3StorageDriver<Item, Operation<Item>> {
		private final Queue<FullHttpRequest> requests = new ArrayDeque<>();
		private final ArrayDeque<FullHttpResponse> stubResponses = new ArrayDeque<>();

		TestS3Driver(Config cfg) throws Exception {
			super("test-s3", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true), cfg.configVal("storage"), false, cfg.intVal("load-batch-size"));
		}

		void enqueueResponse(FullHttpResponse resp) {
			stubResponses.add(resp);
		}

		Queue<FullHttpRequest> log() {
			return requests;
		}

		@Override
		protected FullHttpResponse executeHttpRequest(final FullHttpRequest httpRequest) {
			requests.add(httpRequest);
			FullHttpResponse next = stubResponses.poll();
			if (next != null) {
				return next;
			}
			return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		}
	}

	// ---------- requestNewPath ----------
	@Test
	void requestNewPathLogHeaderValue_redactsSensitiveHeaders() {
		assertEquals(
						"<redacted>",
						S3StorageDriver.safeHeaderValueForLogging(
										HttpHeaderNames.AUTHORIZATION.toString(), "AWS access:signature"));
		assertEquals(
						"<redacted>",
						S3StorageDriver.safeHeaderValueForLogging(
										HttpHeaderNames.COOKIE.toString(), "session=secret"));
		assertEquals(
						"<redacted>",
						S3StorageDriver.safeHeaderValueForLogging(
										"x-amz-security-token", "temporary-token"));
		assertEquals(
						"bucket.example.com",
						S3StorageDriver.safeHeaderValueForLogging(
										HttpHeaderNames.HOST.toString(), "bucket.example.com"));
	}

	@Test
	void requestNewPath_createsBucketWhenMissing() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		// Simulate: HEAD -> 404, PUT -> 200
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND));
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));

		String path = "/bucket-create";
		String res = drv.requestNewPath(path);
		assertEquals(path, res);
		Queue<FullHttpRequest> log = drv.log();
		assertEquals(2, log.size());
		FullHttpRequest head = log.poll();
		assertEquals(HttpMethod.HEAD, head.method());
		assertEquals(path, head.uri());
		assertTrue(head.headers().contains(HttpHeaderNames.AUTHORIZATION));
		FullHttpRequest put = log.poll();
		assertEquals(HttpMethod.PUT, put.method());
		assertEquals(path, put.uri());
		assertTrue(put.headers().contains(HttpHeaderNames.AUTHORIZATION));
	}

	// ---------- applyAuthHeaders (v2 and v4) ----------
	@Test
	void applyAuthHeaders_v2_setsAwsAuthorization() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "127.0.0.1");
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);

		Method m = S3StorageDriver.class.getDeclaredMethod("applyAuthHeaders", HttpHeaders.class, HttpMethod.class, String.class, Credential.class);
		m.setAccessible(true);
		m.invoke(drv, headers, HttpMethod.GET, "/bucket/obj", TEST_CRED);
		String auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		assertNotNull(auth);
		assertTrue(auth.startsWith(S3Api.AUTH_PREFIX + TEST_CRED.getUid() + ":"));
	}

	@Test
	void applyAuthHeaders_v4_setsSignedHeaders() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-2.amazonaws.com:80");
		TestS3Driver drv = new TestS3Driver(cfg);
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "s3.us-east-2.amazonaws.com:80");
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);

		Method m = S3StorageDriver.class.getDeclaredMethod("applyAuthHeaders", HttpHeaders.class, HttpMethod.class, String.class, Credential.class);
		m.setAccessible(true);
		m.invoke(drv, headers, HttpMethod.GET, "/bucket/obj", TEST_CRED);
		assertNotNull(headers.get(S3Api.AMZ_DATE_HEADER));
		assertEquals(S3Api.AMZ_EMPTY_BODY_SHA256, headers.get(S3Api.AMZ_PAYLOAD_HEADER));
		String auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		assertNotNull(auth);
		assertTrue(auth.startsWith(S3Api.AUTH_V4_PREFIX + "Credential=" + TEST_CRED.getUid() + "/"));
		// Host header should be without port per implementation
		assertEquals("s3.us-east-2.amazonaws.com", headers.get(HttpHeaderNames.HOST));
	}

	@Test
	void applyAuthHeaders_v4_setsUnsignedPayloadForBodies() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "s3.us-east-1.amazonaws.com:443");
		headers.setInt(HttpHeaderNames.CONTENT_LENGTH, 1024);

		Method m = S3StorageDriver.class.getDeclaredMethod("applyAuthHeaders", HttpHeaders.class, HttpMethod.class, String.class, Credential.class);
		m.setAccessible(true);
		m.invoke(drv, headers, HttpMethod.PUT, "/bucket/obj", TEST_CRED);
		assertEquals(S3Api.AMZ_UNSIGNED_PAYLOAD, headers.get(S3Api.AMZ_PAYLOAD_HEADER));
	}

	@Test
	void canonicalV4_insertsEmptyQueryLineWhenAbsent() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "127.0.0.1:9020");
		TestS3Driver drv = new TestS3Driver(cfg);
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "127.0.0.1:9020");
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		headers.set(S3Api.AMZ_DATE_HEADER, "20251004T000000Z");
		headers.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_EMPTY_BODY_SHA256);

		Method m = S3StorageDriver.class.getDeclaredMethod(
						"getCanonicalV4", HttpHeaders.class, Map.class, HttpMethod.class, String.class);
		m.setAccessible(true);
		@SuppressWarnings("unchecked")
		final String canonical = (String) m.invoke(drv, headers, new TreeMap<String, String>(), HttpMethod.HEAD, "/bucket");

		assertTrue(
						canonical.contains("/bucket\n\nhost:"),
						() -> "canonical form missing empty query line: " + canonical.replace('\n', '|'));
	}

	@Test
	void create_withPrefix_isNotCopy() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "127.0.0.1:8080");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var item = new com.dell.spt.base.item.DataItemImpl("logs/AA/fileX", 0, 1024);
		// Ensure the data item can produce bytes
		item.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var op = new com.dell.spt.base.item.op.data.DataOperationImpl<>(0, OpType.CREATE, item, null, "/bucketC", TEST_CRED, null, 0);
		final var req = (HttpRequest) drv.httpRequest((Operation<Item>) (Operation<?>) op, "127.0.0.1");
		assertEquals(HttpMethod.PUT, req.method());
		assertEquals("/bucketC/logs/AA/fileX", req.uri());
		assertNull(req.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE), "CREATE without srcPath must not be a COPY");
		assertNotNull(req.headers().get(HttpHeaderNames.CONTENT_LENGTH), "CREATE should set Content-Length > 0");
	}

	@Test
	void copy_onlyWhenSrcPathPresent() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "127.0.0.1:8080");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var item = new com.dell.spt.base.item.DataItemImpl("fileY", 0, 512);
		final var op = new com.dell.spt.base.item.op.data.DataOperationImpl<>(0, OpType.CREATE, item, "/srcBucket/srcKey", "/dstBucket", TEST_CRED, null, 0);
		final var req = (HttpRequest) drv.httpRequest((Operation<Item>) (Operation<?>) op, "127.0.0.1");
		assertEquals(HttpMethod.PUT, req.method());
		assertEquals("/dstBucket/fileY", req.uri());
		assertEquals("/srcBucket/srcKey/fileY", req.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE), "COPY must be used for CREATE when srcPath present");
		assertEquals("0", req.headers().get(HttpHeaderNames.CONTENT_LENGTH));
	}

	@Test
	void applyAuthHeaders_v4_preservesNonDefaultPort() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.test.example:9090");
		TestS3Driver drv = new TestS3Driver(cfg);
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "s3.test.example:9090");
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		Method m = S3StorageDriver.class.getDeclaredMethod("applyAuthHeaders", HttpHeaders.class, HttpMethod.class, String.class, Credential.class);
		m.setAccessible(true);
		m.invoke(drv, headers, HttpMethod.HEAD, "/bucket", TEST_CRED);
		assertEquals("s3.test.example:9090", headers.get(HttpHeaderNames.HOST), "Non-default port must be preserved in Host header");
	}

	@Test
	void canonicalizeQueryString_sortsAndEncodes() throws Exception {
		Method qm = S3StorageDriver.class.getDeclaredMethod("canonicalizeQueryString", String.class);
		qm.setAccessible(true);
		final String canonical = (String) qm.invoke(null, "z=~&b=two+three&a=1&b=two three");
		assertEquals("a=1&b=two%20three&b=two%2Bthree&z=~", canonical);
	}

	// ---------- normalizeHeaderValue ----------

	@Test
	void normalizeHeaderValue_collapsesConsecutiveWhitespace() throws Exception {
		Method m = S3StorageDriver.class.getDeclaredMethod("normalizeHeaderValue", String.class);
		m.setAccessible(true);
		assertEquals("foo bar", m.invoke(null, "foo   bar"));
		assertEquals("a b c", m.invoke(null, "a  b  c"));
		assertEquals("a b", m.invoke(null, "a \t b"));
	}

	@Test
	void normalizeHeaderValue_trimsAndPassesThroughSimpleValues() throws Exception {
		Method m = S3StorageDriver.class.getDeclaredMethod("normalizeHeaderValue", String.class);
		m.setAccessible(true);
		assertEquals("simple", m.invoke(null, "simple"));
		assertEquals("no-collapse", m.invoke(null, "  no-collapse  "));
		assertEquals("", m.invoke(null, (String) null));
		assertEquals("", m.invoke(null, ""));
		assertEquals("", m.invoke(null, "   "));
	}

	@Test
	void canonical_headers_lowercasedAndTrimmed() throws Exception {
		S3StorageDriver<Item, Operation<Item>> drv = newDriverMock();
		HttpHeaders headers = new DefaultHttpHeaders();
		headers.set("X-Amz-Meta-Foo", "bar    baz");
		headers.set("x-amz-date", "20251004T000000Z");
		headers.set("host", "s3.test:443");
		headers.set("content-length", "0");
		headers.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_EMPTY_BODY_SHA256);
		// Build the x-amz-* map explicitly and pass into canonicalizer
		Method mHdrs = S3StorageDriver.class.getDeclaredMethod("getNonCanonicalHeaders", HttpHeaders.class);
		mHdrs.setAccessible(true);
		@SuppressWarnings("unchecked")
		Map<String, String> xamz = (Map<String, String>) mHdrs.invoke(drv, headers);
		Method m = S3StorageDriver.class.getDeclaredMethod("getCanonicalV4", HttpHeaders.class, Map.class, HttpMethod.class, String.class);
		m.setAccessible(true);
		final String canonical = (String) m.invoke(drv, headers, xamz, HttpMethod.GET, "/b");
		assertTrue(canonical.contains("x-amz-meta-foo:bar baz\n"), () -> canonical);
	}

	// ---------- list() ----------
	@Test
	void list_parsesContentsAndAddsPoison() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
						"<ListBucketResult>" +
						"<IsTruncated>false</IsTruncated>" +
						"<Contents><Key>a1</Key><Size>10</Size></Contents>" +
						"<Contents><Key>a2</Key><Size>20</Size></Contents>" +
						"</ListBucketResult>";
		drv.enqueueResponse(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(xml, StandardCharsets.US_ASCII)));

		ItemFactory<Item> f = new ItemFactoryImpl<>();
		List<Item> items = drv.list(f, "/bucketL", "a", 36, null, 10);
		// Expect 2 items + poison null
		assertEquals(3, items.size());
		assertEquals("/bucketL/a1", ((ItemImpl) items.get(0)).name());
		assertEquals("/bucketL/a2", ((ItemImpl) items.get(1)).name());
		assertNull(items.get(2));
	}

	@Test
	void httpRequest_listBuildsContinuationQuery() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);

		ListOperation<PathItemImpl> listOp = newListOperation(
						"/bucketL",
						"/prefix/",
						ListOptions.builder()
										.delimiter("/")
										.startAfter("last-obj")
										.continuationToken("token-xyz")
										.maxKeys(700)
										.build());

		final var req = httpRequestFor(listOp, drv);
		final var uri = req.uri();
		assertTrue(uri.startsWith("/bucketL?list-type=2"), () -> "URI=" + uri);
		assertTrue(uri.contains("prefix=prefix%2F"), () -> "URI=" + uri);
		assertTrue(uri.contains("delimiter=%2F"), () -> "URI=" + uri);
		assertTrue(uri.contains("continuation-token=token-xyz"), () -> "URI=" + uri);
		assertTrue(uri.contains("max-keys=700"), () -> "URI=" + uri);
	}

	@Test
	void httpRequest_listVersionsUsesMarkers() throws Exception {
		Config cfg = baseConfig(true, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		ListOperation<PathItemImpl> listOp = newListOperation(
						"/bucketV",
						"versions",
						ListOptions.builder()
										.includeVersions(true)
										.keyMarker("key-marker")
										.versionIdMarker("vid-marker")
										.maxKeys(1200)
										.build());

		final var req = httpRequestFor(listOp, drv);
		final var uri = req.uri();
		assertTrue(uri.startsWith("/bucketV?versions"), () -> "URI=" + uri);
		assertTrue(uri.contains("prefix=versions"), () -> "URI=" + uri);
		assertTrue(uri.contains("key-marker=key-marker"), () -> "URI=" + uri);
		assertTrue(uri.contains("version-id-marker=vid-marker"), () -> "URI=" + uri);
		assertTrue(uri.contains("max-keys=1000"), () -> "URI=" + uri);
	}

	// ---------- constructor behavior ----------
	@Test
	void constructor_rejectsInvalidChecksum() {
		Config cfg = baseConfig(false, 2, true, "badalgo", "127.0.0.1");
		assertThrows(IllegalArgumentException.class, () -> new TestS3Driver(cfg));
	}

	@Test
	void constructor_acceptsCrc64NvmeChecksum() {
		Config cfg = baseConfig(false, 2, true, "crc64-nvme", "127.0.0.1");
		assertDoesNotThrow(() -> new TestS3Driver(cfg));
	}

	@Test
	void constructor_extractsAwsRegionFromHost() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "s3.us-west-2.amazonaws.com:80");
		TestS3Driver drv = new TestS3Driver(cfg);
		Field regionF = S3StorageDriver.class.getDeclaredField("awsRegion");
		regionF.setAccessible(true);
		assertEquals("us-west-2", regionF.get(drv));
	}

	// ---------- checksum characterization ----------
	@Test
	void applyChecksum_md5_knownVector_123456789_setsContentMd5Header() throws Exception {
		assertChecksumHeader("md5", "123456789".getBytes(StandardCharsets.UTF_8), "JfnnlDI7RTiF9RgfG2JNCw==");
	}

	@Test
	void applyChecksum_crc32_knownVector_123456789_setsAmzChecksumHeader() throws Exception {
		assertChecksumHeader("crc32", "123456789".getBytes(StandardCharsets.UTF_8), "y/Q5Jg==");
	}

	@Test
	void applyChecksum_crc32c_knownVector_123456789_setsAmzChecksumHeader() throws Exception {
		assertChecksumHeader("crc32c", "123456789".getBytes(StandardCharsets.UTF_8), "4waSgw==");
	}

	@Test
	void applyChecksum_sha1_knownVector_123456789_setsAmzChecksumHeader() throws Exception {
		assertChecksumHeader("sha1", "123456789".getBytes(StandardCharsets.UTF_8), "98O8HYCOBHMq32eZZczDTKeuNEE=");
	}

	@Test
	void applyChecksum_sha256_knownVector_123456789_setsAmzChecksumHeader() throws Exception {
		assertChecksumHeader("sha256", "123456789".getBytes(StandardCharsets.UTF_8), "FeKw08M4keuw8e9gnsQZQgwg4yDOlMZfvIwzEkSOsiU=");
	}

	@Test
	void applyChecksum_crc64nvme_knownVector_123456789_setsAmzChecksumHeader() throws Exception {
		assertChecksumHeader("crc64-nvme", "123456789".getBytes(StandardCharsets.UTF_8), "rosUhgp5mIg=");
	}

	@Test
	void applyChecksum_emptyPayload_vectors_areStable() throws Exception {
		final byte[] payload = new byte[0];
		assertChecksumHeader("md5", payload, "1B2M2Y8AsgTpgAmY7PhCfg==");
		assertChecksumHeader("crc32", payload, "AAAAAA==");
		assertChecksumHeader("crc32c", payload, "AAAAAA==");
		assertChecksumHeader("crc64-nvme", payload, "AAAAAAAAAAA=");
		assertChecksumHeader("sha1", payload, "2jmj7l5rSw0yVb/vlWAYkK/YBwk=");
		assertChecksumHeader("sha256", payload, "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=");
	}

	@Test
	void applyChecksum_resetsDataItem_afterSuccess() throws Exception {
		final Config cfg = baseConfig(false, 4, true, "crc32", "s3.us-east-1.amazonaws.com:443");
		final TestS3Driver drv = new TestS3Driver(cfg);
		final DataItem dataItem = mockDataItemFromBytes("abc".getBytes(StandardCharsets.UTF_8));
		final Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, dataItem, null, "/bucket", TEST_CRED);
		final HttpHeaders headers = new DefaultHttpHeaders();
		drv.applyChecksum(headers, op);
		assertNotNull(headers.get(S3Api.AMZ_CHECKSUM_PREFIX + "crc32"));
		Mockito.verify(dataItem, Mockito.times(1)).reset();
	}

	@Test
	void applyChecksum_resetsDataItem_afterReadFailure() throws Exception {
		final Config cfg = baseConfig(false, 4, true, "crc32", "s3.us-east-1.amazonaws.com:443");
		final TestS3Driver drv = new TestS3Driver(cfg);
		final DataItem dataItem = Mockito.mock(DataItem.class);
		Mockito.when(dataItem.size()).thenReturn(1024L);
		Mockito.doThrow(new IOException("test read failure")).when(dataItem).read(Mockito.any(ByteBuffer.class));
		final Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, dataItem, null, "/bucket", TEST_CRED);
		final HttpHeaders headers = new DefaultHttpHeaders();
		drv.applyChecksum(headers, op);
		assertFalse(headers.names().stream().anyMatch(name -> name.toString().toLowerCase().startsWith("x-amz-checksum-")));
		assertNull(headers.get(HttpHeaderNames.CONTENT_MD5));
		Mockito.verify(dataItem, Mockito.times(1)).reset();
	}

	@Test
	void applyChecksum_disabled_doesNotEmitChecksumHeaders() throws Exception {
		final Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		final TestS3Driver drv = new TestS3Driver(cfg);
		final DataItem dataItem = mockDataItemFromBytes("abc".getBytes(StandardCharsets.UTF_8));
		final Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, dataItem, null, "/bucket", TEST_CRED);
		final HttpHeaders headers = new DefaultHttpHeaders();
		drv.applyChecksum(headers, op);
		assertFalse(headers.names().stream().anyMatch(name -> name.toString().toLowerCase().startsWith("x-amz-checksum-")));
		assertNull(headers.get(HttpHeaderNames.CONTENT_MD5));
		Mockito.verify(dataItem, Mockito.never()).read(Mockito.any(ByteBuffer.class));
		Mockito.verify(dataItem, Mockito.never()).reset();
	}

	@Test
	void applyChecksum_nonDataItem_noHeadersEmitted() throws Exception {
		final Config cfg = baseConfig(false, 4, true, "crc32", "s3.us-east-1.amazonaws.com:443");
		final TestS3Driver drv = new TestS3Driver(cfg);
		final Item item = new ItemImpl("/bucket/obj");
		final Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, item, null, "/bucket", TEST_CRED);
		final HttpHeaders headers = new DefaultHttpHeaders();
		drv.applyChecksum(headers, op);
		assertTrue(headers.isEmpty(), "No checksum headers should be emitted for non-DataItem operations");
	}

	@Test
	void applyChecksum_crc32_largePayload_over64KiB_matchesReference() throws Exception {
		final byte[] payload = new byte[64 * 1024 + 513];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) (i * 31 + 7);
		}
		final String expected = reference32BitChecksum(new CRC32(), payload);
		assertChecksumHeader("crc32", payload, expected);
	}

	@Test
	void applyChecksum_crc32c_largePayload_over64KiB_matchesReference() throws Exception {
		final byte[] payload = new byte[64 * 1024 + 513];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) (i * 17 + 11);
		}
		final String expected = reference32BitChecksum(new CRC32C(), payload);
		assertChecksumHeader("crc32c", payload, expected);
	}

	@Test
	void applyChecksum_crc64nvme_largePayload_over64KiB_matchesReference() throws Exception {
		final byte[] payload = new byte[64 * 1024 + 513];
		for (int i = 0; i < payload.length; i++) {
			payload[i] = (byte) (i * 13 + 5);
		}
		final String expected = reference64BitChecksum(new CRC64NVME(), payload);
		assertChecksumHeader("crc64-nvme", payload, expected);
	}

	@Test
	void responseHandler_md5_mode_doesNotCaptureChecksumFromEtagOnly() throws Exception {
		Config cfg = baseConfig(false, 4, true, "md5", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-resp-md5");
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		final var handler = new S3ResponseHandler<>(
						drv, false, false, S3Api.AMZ_CHECKSUM_PREFIX + "md5");
		final var headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.ETAG, "\"etag-part1\"");
		Method m = S3ResponseHandler.class.getDeclaredMethod(
						"handleResponseHeaders", Channel.class, Operation.class, HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(handler, null, partOp, headers);
		assertEquals("\"etag-part1\"", parent.get("1"), "ETag should still be captured");
		assertNull(parent.get(S3Api.KEY_PART_CHECKSUM_PREFIX + "1"),
						"MD5 mode should not infer part checksum from ETag-only responses");
	}

	@Test
	void completeMultipartUpload_md5_mode_doesNotEmitChecksumMd5_withoutPartChecksumEntries() throws Exception {
		Config cfg = baseConfig(false, 4, true, "md5", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-complete-md5");
		parent.put("1", "\"etag-1\"");
		parent.put("2", "\"etag-2\"");
		final var req = drv.completeMultipartUploadRequest(parent, "s3.us-east-1.amazonaws.com");
		final String body = req.content().toString(StandardCharsets.UTF_8);
		assertTrue(body.contains("<ETag>\"etag-1\"</ETag>"), "XML should include ETags: " + body);
		assertFalse(body.contains("<ChecksumMD5>"),
						"CompleteMultipartUpload XML should not include ChecksumMD5 when per-part checksum entries are absent: " + body);
	}

	// ---------- objectVersioningRequest ----------
	@Test
	void objectVersioningRequest_setsVersionHeaderAndPath() throws Exception {
		Config cfg = baseConfig(true, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		Item item = new ItemImpl("/bucketV/key~VER123");
		Operation<Item> op = new OperationImpl<>(123, OpType.READ, item, null, "/bucketV", TEST_CRED);
		HttpRequest req = (HttpRequest) drv.httpRequest(op, "127.0.0.1");
		assertEquals(HttpMethod.GET, req.method());
		// version suffix stripped from URI
		assertEquals("/bucketV/key", req.uri());
		assertEquals("VER123", req.headers().get("x-amz-version-id"));
		// Authorization added
		assertTrue(req.headers().contains(HttpHeaderNames.AUTHORIZATION));
	}

	@SuppressWarnings("unchecked")
	private HttpRequest httpRequestFor(
					final ListOperation<PathItemImpl> listOp, final TestS3Driver drv) throws Exception {
		return (HttpRequest) drv.httpRequest((Operation<Item>) (Operation<?>) listOp, "127.0.0.1");
	}

	private ListOperation<PathItemImpl> newListOperation(
					final String bucketPath, final String prefix, final ListOptions options) throws Exception {
		final var builder = new PathOperationsBuilderImpl<PathItemImpl, PathOperation<PathItemImpl>>(0)
						.opType(OpType.LIST)
						.inputPath(bucketPath)
						.credentialInput(new ConstantValueInputImpl<>(TEST_CRED));
		final var listOp = (ListOperation<PathItemImpl>) builder.buildOp(new PathItemImpl(prefix));
		listOp.srcPath(bucketPath);
		listOp.item().name(prefix);
		listOp.options(options);
		return listOp;
	}

	// MPU signing tests (inserted inside class)
	@Test
	void mpu_init_signsWithV4() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		Item item = new ItemImpl("/bucket/obj");
		Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, item, null, "/bucket", TEST_CRED);
		HttpRequest req = drv.initMultipartUploadRequest(op, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.POST, req.method());
		assertTrue(req.uri().contains("?uploads"));
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
		assertEquals(S3Api.AMZ_EMPTY_BODY_SHA256, req.headers().get(S3Api.AMZ_PAYLOAD_HEADER));
	}

	@Test
	void mpu_part_signsWithUnsignedPayload() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u123");
		final var partItem = base.slice(0, 1024);
		final var slice = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		HttpRequest req = drv.partUploadRequest(slice, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.PUT, req.method());
		assertTrue(req.uri().contains("?partNumber=1&uploadId=u123"));
		assertEquals(S3Api.AMZ_UNSIGNED_PAYLOAD, req.headers().get(S3Api.AMZ_PAYLOAD_HEADER));
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	@Test
	void mpu_complete_signsEmptyBody() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put("1", "etag-1");
		parent.put("2", "etag-2");
		final var req = drv.completeMultipartUploadRequest(parent, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.POST, req.method());
		assertTrue(req.uri().contains("?uploadId="));
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	@Test
	void mpu_part_appliesChecksumWhenEnabled() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc32", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-checksum");
		final var partItem = base.slice(0, 1024);
		final var slice = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		HttpRequest req = drv.partUploadRequest(slice, "s3.us-east-1.amazonaws.com");
		String checksumHeader = req.headers().get(S3Api.AMZ_CHECKSUM_PREFIX + "crc32");
		assertNotNull(checksumHeader, "Part upload should include checksum header when checksums enabled");
		assertFalse(checksumHeader.isEmpty(), "Checksum value should not be empty");
	}

	@Test
	void mpu_part_appliesCrc64NvmeChecksumWhenEnabled() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc64-nvme", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-crc64");
		final var partItem = base.slice(0, 1024);
		final var slice = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		HttpRequest req = drv.partUploadRequest(slice, "s3.us-east-1.amazonaws.com");
		String checksumHeader = req.headers().get(S3Api.AMZ_CHECKSUM_PREFIX + "crc64nvme");
		assertNotNull(checksumHeader, "Part upload should include CRC64-NVME checksum header when enabled");
		assertEquals(12, checksumHeader.length(), "CRC64-NVME Base64 checksum should be 12 chars (8 bytes)");
	}

	@Test
	void mpu_part_noChecksumWhenDisabled() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-nochecksum");
		final var partItem = base.slice(0, 1024);
		final var slice = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		HttpRequest req = drv.partUploadRequest(slice, "s3.us-east-1.amazonaws.com");
		// No x-amz-checksum-* header should be present
		boolean hasChecksumHeader = req.headers().names().stream().anyMatch(
						name -> name.toString().toLowerCase().startsWith("x-amz-checksum-"));
		assertFalse(hasChecksumHeader, "Part upload should not include checksum header when checksums disabled");
		assertNull(req.headers().get(HttpHeaderNames.CONTENT_MD5), "No Content-MD5 when checksums disabled");
	}

	@Test
	void mpu_init_includesChecksumAlgorithmHeaderWhenEnabled() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc32c", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		Item item = new ItemImpl("/bucket/obj");
		Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, item, null, "/bucket", TEST_CRED);
		HttpRequest req = drv.initMultipartUploadRequest(op, "s3.us-east-1.amazonaws.com");
		assertEquals("CRC32C", req.headers().get(S3Api.AMZ_CHECKSUM_ALGORITHM_HEADER),
						"InitiateMultipartUpload should include x-amz-checksum-algorithm when checksums enabled");
	}

	@Test
	void mpu_init_includesCrc64NvmeChecksumAlgorithmHeaderWhenEnabled() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc64-nvme", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		Item item = new ItemImpl("/bucket/obj");
		Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, item, null, "/bucket", TEST_CRED);
		HttpRequest req = drv.initMultipartUploadRequest(op, "s3.us-east-1.amazonaws.com");
		assertEquals("CRC64NVME", req.headers().get(S3Api.AMZ_CHECKSUM_ALGORITHM_HEADER),
						"InitiateMultipartUpload should include CRC64-NVME algorithm token when enabled");
	}

	@Test
	void mpu_init_noChecksumAlgorithmHeaderWhenDisabled() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		Item item = new ItemImpl("/bucket/obj");
		Operation<Item> op = new OperationImpl<>(1, OpType.CREATE, item, null, "/bucket", TEST_CRED);
		HttpRequest req = drv.initMultipartUploadRequest(op, "s3.us-east-1.amazonaws.com");
		assertNull(req.headers().get(S3Api.AMZ_CHECKSUM_ALGORITHM_HEADER),
						"InitiateMultipartUpload should not include x-amz-checksum-algorithm when checksums disabled");
	}

	@Test
	void mpu_complete_includesPerPartChecksums() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc32c", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-complete-checksum");
		parent.put("1", "\"etag-1\"");
		parent.put("2", "\"etag-2\"");
		parent.put(S3Api.KEY_PART_CHECKSUM_PREFIX + "1", "AAAAAA==");
		parent.put(S3Api.KEY_PART_CHECKSUM_PREFIX + "2", "BBBBBB==");
		final var req = drv.completeMultipartUploadRequest(parent, "s3.us-east-1.amazonaws.com");
		final String body = req.content().toString(java.nio.charset.StandardCharsets.UTF_8);
		assertTrue(body.contains("<ChecksumCRC32C>AAAAAA==</ChecksumCRC32C>"),
						"CompleteMultipartUpload XML should include part 1 checksum: " + body);
		assertTrue(body.contains("<ChecksumCRC32C>BBBBBB==</ChecksumCRC32C>"),
						"CompleteMultipartUpload XML should include part 2 checksum: " + body);
		assertTrue(body.contains("<ETag>\"etag-1\"</ETag>"), "XML should still include ETags: " + body);
	}

	@Test
	void mpu_complete_includesPerPartCrc64NvmeChecksums() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc64-nvme", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-complete-crc64");
		parent.put("1", "\"etag-1\"");
		parent.put("2", "\"etag-2\"");
		parent.put(S3Api.KEY_PART_CHECKSUM_PREFIX + "1", "AAAAAAAAAAA=");
		parent.put(S3Api.KEY_PART_CHECKSUM_PREFIX + "2", "rosUhgp5mIg=");
		final var req = drv.completeMultipartUploadRequest(parent, "s3.us-east-1.amazonaws.com");
		final String body = req.content().toString(java.nio.charset.StandardCharsets.UTF_8);
		assertTrue(body.contains("<ChecksumCRC64NVME>AAAAAAAAAAA=</ChecksumCRC64NVME>"),
						"CompleteMultipartUpload XML should include part 1 CRC64 checksum: " + body);
		assertTrue(body.contains("<ChecksumCRC64NVME>rosUhgp5mIg=</ChecksumCRC64NVME>"),
						"CompleteMultipartUpload XML should include part 2 CRC64 checksum: " + body);
		assertTrue(body.contains("<ETag>\"etag-1\"</ETag>"), "XML should still include ETags: " + body);
	}

	@Test
	void mpu_complete_noChecksumElementsWhenDisabled() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-complete-nochecksum");
		parent.put("1", "\"etag-1\"");
		parent.put("2", "\"etag-2\"");
		final var req = drv.completeMultipartUploadRequest(parent, "s3.us-east-1.amazonaws.com");
		final String body = req.content().toString(java.nio.charset.StandardCharsets.UTF_8);
		assertFalse(body.contains("Checksum"),
						"CompleteMultipartUpload XML should not include checksum elements when disabled: " + body);
		assertTrue(body.contains("<ETag>\"etag-1\"</ETag>"), "XML should include ETags: " + body);
	}

	@Test
	void responseHandler_capturesPartChecksum() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc32c", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-resp-checksum");
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		final var handler = new S3ResponseHandler<>(
						drv, false, false, S3Api.AMZ_CHECKSUM_PREFIX + "crc32c");
		final var headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.ETAG, "\"etag-part1\"");
		headers.set("x-amz-checksum-crc32c", "XYZABC==");
		Method m = S3ResponseHandler.class.getDeclaredMethod(
						"handleResponseHeaders", Channel.class, Operation.class, HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(handler, null, partOp, headers);
		assertEquals("\"etag-part1\"", parent.get("1"), "ETag should be captured");
		assertEquals("XYZABC==", parent.get(S3Api.KEY_PART_CHECKSUM_PREFIX + "1"),
						"Per-part checksum should be captured from response header");
	}

	@Test
	void responseHandler_capturesCrc64NvmePartChecksum() throws Exception {
		Config cfg = baseConfig(false, 4, true, "crc64-nvme", "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-resp-crc64");
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		final var handler = new S3ResponseHandler<>(
						drv, false, false, S3Api.AMZ_CHECKSUM_PREFIX + "crc64nvme");
		final var headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.ETAG, "\"etag-part1\"");
		headers.set(S3Api.AMZ_CHECKSUM_PREFIX + "crc64nvme", "AQIDBAUGBwg=");
		Method m = S3ResponseHandler.class.getDeclaredMethod(
						"handleResponseHeaders", Channel.class, Operation.class, HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(handler, null, partOp, headers);
		assertEquals("\"etag-part1\"", parent.get("1"), "ETag should be captured");
		assertEquals("AQIDBAUGBwg=", parent.get(S3Api.KEY_PART_CHECKSUM_PREFIX + "1"),
						"Per-part CRC64 checksum should be captured from response header");
	}

	@Test
	void responseHandler_noChecksumCaptureWhenDisabled() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-resp-nochecksum");
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		final var handler = new S3ResponseHandler<>(drv, false, false, null);
		final var headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.ETAG, "\"etag-part1\"");
		headers.set("x-amz-checksum-crc32c", "XYZABC==");
		Method m = S3ResponseHandler.class.getDeclaredMethod(
						"handleResponseHeaders", Channel.class, Operation.class, HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(handler, null, partOp, headers);
		assertEquals("\"etag-part1\"", parent.get("1"), "ETag should still be captured");
		assertNull(parent.get(S3Api.KEY_PART_CHECKSUM_PREFIX + "1"),
						"Per-part checksum should NOT be captured when checksums disabled");
	}

	@Test
	void mpu_abort_sendsDeleteWithUploadId() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "abort-upload-123");
		parent.put(S3Api.KEY_MPU_ABORT, "true");
		HttpRequest req = drv.abortMultipartUploadRequest(parent, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.DELETE, req.method());
		assertTrue(req.uri().contains("?uploadId=abort-upload-123"));
		assertEquals("0", req.headers().get(HttpHeaderNames.CONTENT_LENGTH));
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	// ---------- UploadId regex tests ----------

	/** Read the PATTERN_UPLOAD_ID regex from S3ResponseHandler via reflection. */
	private static Pattern uploadIdPattern() throws Exception {
		Field f = S3ResponseHandler.class.getDeclaredField("PATTERN_UPLOAD_ID");
		f.setAccessible(true);
		return (Pattern) f.get(null);
	}

	private static String extractUploadId(String xml) throws Exception {
		Matcher m = uploadIdPattern().matcher(xml);
		return m.find() ? m.group(1) : null;
	}

	@Test
	void uploadIdRegex_matchesAwsStyle() throws Exception {
		// AWS: base64-like alphanumeric with +, /, =, -
		String xml = "<UploadId>VXBsb2FkIElE/abc+def=</UploadId>";
		assertEquals("VXBsb2FkIElE/abc+def=", extractUploadId(xml));
	}

	@Test
	void uploadIdRegex_matchesCephTilde() throws Exception {
		// Ceph RadosGW: "2~" prefix followed by alphanumeric
		String xml = "<UploadId>2~abcdef1234567890ABCDEF</UploadId>";
		assertEquals("2~abcdef1234567890ABCDEF", extractUploadId(xml));
	}

	@Test
	void uploadIdRegex_matchesDotsAndColons() throws Exception {
		// Hypothetical store using dots or colons
		String xml = "<UploadId>upload.id:with-mixed_chars+2024</UploadId>";
		assertEquals("upload.id:with-mixed_chars+2024", extractUploadId(xml));
	}

	@Test
	void uploadIdRegex_returnsNullForMissingElement() throws Exception {
		String xml = "<InitiateMultipartUploadResult><Bucket>b</Bucket></InitiateMultipartUploadResult>";
		assertNull(extractUploadId(xml));
	}

	// ---------- Range-read tests ----------

	@Test
	void rangeReadRequest_setsCorrectRangeHeader() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		// partNumber=2 → rangeStart = 2 * 1024 = 2048, rangeEnd = 2048 + 1024 - 1 = 3071
		final var partItem = base.slice(2048, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, partItem, "/bucket", null, TEST_CRED, 2, parent);
		HttpRequest req = drv.rangeReadRequest(partOp, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.GET, req.method());
		assertEquals("bytes=2048-3071", req.headers().get(HttpHeaderNames.RANGE));
		assertEquals("0", req.headers().get(HttpHeaderNames.CONTENT_LENGTH));
		assertTrue(req.uri().contains("/bucket/obj"));
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	@Test
	void rangeReadRequest_tailPartHasSmallerRange() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		// 100 bytes, threshold=30 → parts: 30, 30, 30, 10
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/tailtest", 0, 100);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, base, "/bucket", null, TEST_CRED, null, 0, 30);
		// Tail part: partNumber=3, offset=90, size=10
		final var tailItem = base.slice(90, 10);
		final var tailOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, tailItem, "/bucket", null, TEST_CRED, 3, parent);
		HttpRequest req = drv.rangeReadRequest(tailOp, "s3.us-east-1.amazonaws.com");
		assertEquals("bytes=90-99", req.headers().get(HttpHeaderNames.RANGE));
	}

	@Test
	void rangeReadRequest_firstPartStartsAtZero() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 2048);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, partItem, "/bucket", null, TEST_CRED, 0, parent);
		HttpRequest req = drv.rangeReadRequest(partOp, "s3.us-east-1.amazonaws.com");
		assertEquals("bytes=0-1023", req.headers().get(HttpHeaderNames.RANGE));
	}

	@Test
	void httpRequest_routesPartialReadToRangeRead() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, partItem, "/bucket", null, TEST_CRED, 0, parent);
		@SuppressWarnings("unchecked")
		final var req = (HttpRequest) drv.httpRequest(
						(Operation<Item>) (Operation<?>) partOp, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.GET, req.method());
		assertNotNull(req.headers().get(HttpHeaderNames.RANGE), "Range header must be set for partial READ");
		assertEquals("bytes=0-1023", req.headers().get(HttpHeaderNames.RANGE));
	}

	@Test
	void httpRequest_throwsForCompositeRead() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		@SuppressWarnings("unchecked")
		final var compositeOp = (Operation<Item>) (Operation<?>) parent;
		assertThrows(AssertionError.class, () -> drv.httpRequest(compositeOp, "s3.us-east-1.amazonaws.com"));
	}

	// ---------- STAT (HEAD) request tests ----------

	@Test
	void httpRequest_stat_usesHeadMethod() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "127.0.0.1:8080");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var item = new com.dell.spt.base.item.DataItemImpl("/bucket/stat-key", 0, 1024);
		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) new OperationImpl<>(0, OpType.STAT, item, null, "/bucket", TEST_CRED);
		final var req = (HttpRequest) drv.httpRequest(op, "127.0.0.1");
		assertEquals(HttpMethod.HEAD, req.method());
		assertTrue(req.uri().contains("/bucket/stat-key"));
		assertEquals("0", req.headers().get(HttpHeaderNames.CONTENT_LENGTH));
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	@Test
	void httpRequest_stat_withAuth_v2() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1:8080");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var item = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 512);
		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) new OperationImpl<>(0, OpType.STAT, item, null, "/bucket", TEST_CRED);
		final var req = (HttpRequest) drv.httpRequest(op, "127.0.0.1");
		assertEquals(HttpMethod.HEAD, req.method());
		String authHeader = req.headers().get(HttpHeaderNames.AUTHORIZATION);
		assertNotNull(authHeader);
		assertTrue(authHeader.startsWith("AWS "), "v2 auth must start with 'AWS '");
	}

	// ---------- S3ResponseHandler ETag guard tests ----------

	@Test
	void responseHandler_capturesETagForCreateParts() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		parent.put(S3Api.KEY_UPLOAD_ID, "u-etag-test");
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.CREATE, partItem, "/bucket", null, TEST_CRED, 0, parent);
		final var handler = new S3ResponseHandler<>(drv, false, false, null);
		final var headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.ETAG, "\"abc123\"");
		// Use reflection to call protected handleResponseHeaders
		Method m = S3ResponseHandler.class.getDeclaredMethod(
						"handleResponseHeaders", Channel.class, Operation.class, HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(handler, null, partOp, headers);
		// ETag should be stored in parent contextData for CREATE parts
		assertEquals("\"abc123\"", parent.get("1"), "ETag should be captured for CREATE part");
	}

	@Test
	void responseHandler_skipsETagForReadParts() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "127.0.0.1");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, 4096);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, base, "/bucket", null, TEST_CRED, null, 0, 1024);
		final var partItem = base.slice(0, 1024);
		final var partOp = new com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>(
						0, OpType.READ, partItem, "/bucket", null, TEST_CRED, 0, parent);
		final var handler = new S3ResponseHandler<>(drv, false, false, null);
		final var headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.ETAG, "\"def456\"");
		Method m = S3ResponseHandler.class.getDeclaredMethod(
						"handleResponseHeaders", Channel.class, Operation.class, HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(handler, null, partOp, headers);
		// ETag should NOT be stored for READ parts
		assertNull(parent.get("1"), "ETag should not be captured for READ parts");
	}

	// ========== Helpers for submit() / complete() tests ==========

	@SuppressWarnings("unchecked")
	private static void setOpResultOut(TestS3Driver drv) throws Exception {
		Output<Operation<Item>> mockOutput = Mockito.mock(Output.class);
		Mockito.when(mockOutput.put(Mockito.<Operation<Item>> any())).thenReturn(true);
		Field outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(drv, mockOutput);
	}

	@SuppressWarnings("unchecked")
	private static BlockingQueue<Operation<Item>> childQueueOf(TestS3Driver drv) throws Exception {
		Field field = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		field.setAccessible(true);
		return (BlockingQueue<Operation<Item>>) field.get(drv);
	}

	private static com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem> newCompositeOp(OpType opType, long itemSize, long threshold) throws Exception {
		final var base = new com.dell.spt.base.item.DataItemImpl("/bucket/obj", 0, itemSize);
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167",
						new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
		return new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<>(
						0, opType, base, "/bucket", null, TEST_CRED, null, 0, threshold);
	}

	// ========== Fix #1: httpRequest — composite DELETE delegates to super ==========

	@Test
	void httpRequest_delegatesCompositeDeleteToSuper() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		final var deleteOp = newCompositeOp(OpType.DELETE, 4096, 1024);
		@SuppressWarnings("unchecked")
		final var compositeOp = (Operation<Item>) (Operation<?>) deleteOp;
		// Before fix: composite DELETE would throw AssertionError (only CREATE was handled).
		// After fix: composite DELETE/UPDATE delegates to standard path via super.httpRequest().
		final var req = (HttpRequest) drv.httpRequest(compositeOp, "s3.us-east-1.amazonaws.com");
		assertEquals(HttpMethod.DELETE, req.method());
		assertTrue(req.uri().contains("/bucket/obj"), "URI should contain the item path");
		assertNotNull(req.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	// ========== Fix #2: submit() — range-read initial and finalization passes ==========

	@Test
	void submit_compositeRead_initialPass_dispatchesSubOps() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var readOp = newCompositeOp(OpType.READ, 4096, 1024);
		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) readOp;

		boolean result = drv.submit(op);

		assertTrue(result, "submit should succeed");
		assertEquals(Operation.Status.SUCC, op.status(), "Op should be marked SUCC");
		// handleCompleted() should have dispatched sub-ops to the child queue
		var queue = childQueueOf(drv);
		assertEquals(4, queue.size(), "4 range-read sub-ops should be dispatched");
	}

	@Test
	void submit_compositeRead_finalizationPass_marksSuccWithNoSubOpDispatch() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var readOp = newCompositeOp(OpType.READ, 4096, 1024);
		// Generate sub-tasks and mark them all completed
		readOp.subOperations();
		for (int i = 0; i < 4; i++)
			readOp.markSubTaskCompleted();
		assertTrue(readOp.allSubOperationsDone());

		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) readOp;

		boolean result = drv.submit(op);

		assertTrue(result, "submit should succeed for finalization");
		assertEquals(Operation.Status.SUCC, op.status());
		// Finalization should NOT dispatch sub-ops
		var queue = childQueueOf(drv);
		assertTrue(queue.isEmpty(), "No sub-ops should be dispatched during finalization");
	}

	@Test
	void submit_compositeRead_returnsFalseWhenConcurrencyExhausted() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);

		// Drain all permits from the concurrency throttle
		Field semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		Semaphore sem = (Semaphore) semField.get(drv);
		sem.drainPermits();

		var readOp = newCompositeOp(OpType.READ, 4096, 1024);
		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) readOp;

		boolean result = drv.submit(op);

		assertFalse(result, "submit should return false when concurrency is exhausted");
	}

	// ========== Fix #3: complete() — MPU logic scoped to CREATE only ==========

	@Test
	void complete_compositeDelete_doesNotSetUploadId() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var deleteOp = newCompositeOp(OpType.DELETE, 4096, 1024);
		deleteOp.status(Operation.Status.SUCC);
		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) deleteOp;

		// Before Fix #3, composite DELETE with non-null channel would enter MPU logic
		// and try to extract upload ID. After the fix, the CREATE guard prevents this.
		// With null channel, the guard is also short-circuited — this test verifies
		// the complete() path is clean for composite DELETE operations.
		assertDoesNotThrow(() -> drv.complete(null, op));
		assertNull(deleteOp.get(S3Api.KEY_UPLOAD_ID), "DELETE op should not get upload ID");
	}

	// ========== Fix #4: submit() — recycled ops regenerate sub-tasks ==========

	@Test
	void submit_compositeRead_recycledOp_regeneratesSubTasks() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		// Create original op, generate sub-tasks, mark all done
		var original = newCompositeOp(OpType.READ, 4096, 1024);
		original.subOperations();
		for (int i = 0; i < 4; i++)
			original.markSubTaskCompleted();
		assertTrue(original.allSubOperationsDone());

		// Create recycled copy — has empty subTasks and pendingSubTasksCount=0
		var recycled = original.result();
		assertTrue(recycled.allSubOperationsDone(),
						"Recycled copy should initially have pending count = 0");

		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) recycled;

		// Before Fix #4, submit() would see allSubOperationsDone()==true and go to
		// finalization without doing any real work. After the fix, subOperations()
		// is called first to regenerate sub-tasks, resetting pendingSubTasksCount to N.
		boolean result = drv.submit(op);

		assertTrue(result, "submit should succeed for recycled op");
		assertEquals(Operation.Status.SUCC, op.status());
		// Recycled op should regenerate and dispatch sub-ops
		var queue = childQueueOf(drv);
		assertEquals(4, queue.size(), "Recycled op should regenerate and dispatch 4 sub-ops");
	}

	@Test
	void submit_compositeRead_recycledOp_producesCleanTimingFields() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		// Create op with stale timing from a previous cycle
		var original = newCompositeOp(OpType.READ, 4096, 1024);
		original.subOperations();
		for (int i = 0; i < 4; i++)
			original.markSubTaskCompleted();
		var recycled = original.result();

		@SuppressWarnings("unchecked")
		final var op = (Operation<Item>) (Operation<?>) recycled;

		// submit() calls reset() before the timing cycle, clearing stale values
		boolean result = drv.submit(op);
		assertTrue(result);

		// Verify latency is non-negative (reset cleared stale timing, then fresh timing was set)
		assertTrue(op.latency() >= 0,
						"Latency should be non-negative after clean timing cycle");
	}

	// ========== Helpers for per-phase MPU logging tests ==========

	private static class CapturingAppender extends AbstractAppender {
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch latch;

		CapturingAppender(int expectedMessages) {
			super("test-capture", null, null, true, Property.EMPTY_ARRAY);
			this.latch = new CountDownLatch(expectedMessages);
		}

		@Override
		public void append(LogEvent event) {
			messages.add(event.getMessage().getFormattedMessage());
			latch.countDown();
		}

		void awaitMessages(long timeoutMs) throws InterruptedException {
			latch.await(timeoutMs, TimeUnit.MILLISECONDS);
		}
	}

	private static CapturingAppender attachCapture(int expectedMessages) {
		var appender = new CapturingAppender(expectedMessages);
		appender.start();
		var logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.MULTIPART.getName());
		logger.addAppender(appender);
		logger.setLevel(Level.INFO);
		return appender;
	}

	private static void detachCapture(CapturingAppender appender) {
		// Allow async logger thread to finish processing before removing the appender
		try {
			Thread.sleep(50);
		} catch (InterruptedException ignored) {}
		var logger = (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Loggers.MULTIPART.getName());
		logger.removeAppender(appender);
		appender.stop();
	}

	@SuppressWarnings("unchecked")
	private static Channel mockChannelForComplete(String initUploadId) {
		Channel channel = Mockito.mock(Channel.class);
		// For INIT phase: channel.attr(KEY_ATTR_UPLOAD_ID).get()
		Attribute<String> uploadIdAttr = Mockito.mock(Attribute.class);
		Mockito.when(uploadIdAttr.get()).thenReturn(initUploadId);
		Mockito.when(channel.attr(S3Api.KEY_ATTR_UPLOAD_ID)).thenReturn(uploadIdAttr);
		// For super.complete(): report already released to skip connPool interaction
		Attribute<Boolean> releasedAttr = Mockito.mock(Attribute.class);
		Mockito.when(releasedAttr.getAndSet(Boolean.TRUE)).thenReturn(Boolean.TRUE);
		Mockito.when(channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED)).thenReturn((Attribute) releasedAttr);
		return channel;
	}

	// ========== Per-phase MPU logging: INIT ==========

	@Test
	void complete_mpuInit_logsInitPhaseAndSetsUploadId() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var compositeOp = newCompositeOp(OpType.CREATE, 4096, 1024);
		compositeOp.status(Operation.Status.SUCC);
		// Fresh op: pendingSubTasksCount=-1, allSubOperationsDone()=false → enters INIT branch

		Channel channel = mockChannelForComplete("test-upload-id-123");

		var capture = attachCapture(1);
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) compositeOp;
			drv.complete(channel, op);
			capture.awaitMessages(2000);
		} finally {
			detachCapture(capture);
		}

		assertEquals("test-upload-id-123", compositeOp.get(S3Api.KEY_UPLOAD_ID),
						"INIT should store upload ID on the composite op");
		assertEquals(1, capture.messages.size(), "Exactly one INIT log row expected");
		assertTrue(capture.messages.get(0).startsWith("INIT,"), "Log row should start with INIT phase");
		assertTrue(capture.messages.get(0).contains("test-upload-id-123"), "Log row should contain upload ID");
	}

	@Test
	void complete_mpuInit_setsFailNotFoundWhenUploadIdMissing() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var compositeOp = newCompositeOp(OpType.CREATE, 4096, 1024);
		compositeOp.status(Operation.Status.SUCC);

		Channel channel = mockChannelForComplete(null); // null upload ID

		var capture = attachCapture(1); // expect 0, but latch(1) so await times out quickly
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) compositeOp;
			drv.complete(channel, op);
			capture.awaitMessages(200); // short timeout — no messages expected
		} finally {
			detachCapture(capture);
		}

		assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, compositeOp.status(),
						"Missing upload ID should set RESP_FAIL_NOT_FOUND");
		assertTrue(capture.messages.isEmpty(), "No log message should be emitted for missing upload ID");
	}

	// ========== Per-phase MPU logging: ABORT ==========

	@Test
	void complete_mpuAbort_logsAbortPhase() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var compositeOp = newCompositeOp(OpType.CREATE, 4096, 1024);
		compositeOp.put(S3Api.KEY_UPLOAD_ID, "abort-id-456");
		compositeOp.put(S3Api.KEY_MPU_ABORT, "true");
		compositeOp.status(Operation.Status.SUCC);

		Channel channel = mockChannelForComplete(null);

		var capture = attachCapture(1);
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) compositeOp;
			drv.complete(channel, op);
			capture.awaitMessages(2000);
		} finally {
			detachCapture(capture);
		}

		assertEquals(1, capture.messages.size(), "Exactly one ABORT log row expected");
		assertTrue(capture.messages.get(0).startsWith("ABORT,"), "Log row should start with ABORT phase");
		assertTrue(capture.messages.get(0).contains("abort-id-456"), "Log row should contain upload ID");
	}

	// ========== Per-phase MPU logging: PART + COMPLETE ==========

	@Test
	void complete_mpuAllDone_logsPartAndCompletePhases() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var compositeOp = newCompositeOp(OpType.CREATE, 4096, 1024);
		compositeOp.put(S3Api.KEY_UPLOAD_ID, "complete-id-789");
		compositeOp.subOperations(); // generates 4 sub-tasks (4096 / 1024)
		for (int i = 0; i < 4; i++)
			compositeOp.markSubTaskCompleted();
		assertTrue(compositeOp.allSubOperationsDone());
		compositeOp.status(Operation.Status.SUCC);

		Channel channel = mockChannelForComplete(null);

		var capture = attachCapture(5); // 4 PART + 1 COMPLETE
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) compositeOp;
			drv.complete(channel, op);
			capture.awaitMessages(2000);
		} finally {
			detachCapture(capture);
		}

		// 4 PART rows + 1 COMPLETE row
		assertEquals(5, capture.messages.size(), "4 PART + 1 COMPLETE rows expected");
		for (int i = 0; i < 4; i++) {
			String[] cols = capture.messages.get(i).split(",");
			assertEquals(8, cols.length, "PART row should have 8 CSV columns");
			assertEquals("PART", cols[0]);
			assertEquals("/bucket/obj", cols[1], "ItemPath should be the composite item name");
			assertEquals("complete-id-789", cols[2], "UploadId column");
			assertEquals(String.valueOf(i + 1), cols[3], "1-based part number");
			// StartTs, Duration, Latency are timing — just verify they parse as longs
			assertDoesNotThrow(() -> Long.parseLong(cols[4]), "StartTs should be numeric");
			assertDoesNotThrow(() -> Long.parseLong(cols[5]), "Duration should be numeric");
			assertDoesNotThrow(() -> Long.parseLong(cols[6]), "Latency should be numeric");
			assertEquals("1024", cols[7], "Bytes should equal the part size");
		}
		String[] completeCols = capture.messages.get(4).split(",");
		assertEquals(8, completeCols.length, "COMPLETE row should have 8 CSV columns");
		assertEquals("COMPLETE", completeCols[0]);
		assertEquals("/bucket/obj", completeCols[1]);
		assertEquals("complete-id-789", completeCols[2]);
		assertEquals("-1", completeCols[3], "COMPLETE partNumber should be -1");
		assertEquals("0", completeCols[7], "COMPLETE bytes should be 0");
	}

	@Test
	void complete_mpuAbort_logsEmptyUploadIdWhenMissing() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var compositeOp = newCompositeOp(OpType.CREATE, 4096, 1024);
		// No KEY_UPLOAD_ID set — simulates abort before init response
		compositeOp.put(S3Api.KEY_MPU_ABORT, "true");
		compositeOp.status(Operation.Status.SUCC);

		Channel channel = mockChannelForComplete(null);

		var capture = attachCapture(1);
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) compositeOp;
			drv.complete(channel, op);
			capture.awaitMessages(2000);
		} finally {
			detachCapture(capture);
		}

		assertEquals(1, capture.messages.size(), "Exactly one ABORT log row expected");
		String[] cols = capture.messages.get(0).split(",");
		assertEquals(8, cols.length, "ABORT row should have 8 CSV columns");
		assertEquals("ABORT", cols[0]);
		assertEquals("", cols[2], "UploadId should be empty when not set");
	}

	@Test
	void complete_mpuAllDone_tailPartHasSmallerBytes() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		// 5000 / 1024 = 4 full parts + 1 tail part of 904 bytes
		var compositeOp = newCompositeOp(OpType.CREATE, 5000, 1024);
		compositeOp.put(S3Api.KEY_UPLOAD_ID, "tail-id");
		compositeOp.subOperations(); // generates 5 sub-tasks
		for (int i = 0; i < 5; i++)
			compositeOp.markSubTaskCompleted();
		assertTrue(compositeOp.allSubOperationsDone());
		compositeOp.status(Operation.Status.SUCC);

		Channel channel = mockChannelForComplete(null);

		var capture = attachCapture(6); // 5 PART + 1 COMPLETE
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) compositeOp;
			drv.complete(channel, op);
			capture.awaitMessages(2000);
		} finally {
			detachCapture(capture);
		}

		assertEquals(6, capture.messages.size(), "5 PART + 1 COMPLETE rows expected");
		// First 4 parts: 1024 bytes each
		for (int i = 0; i < 4; i++) {
			String[] cols = capture.messages.get(i).split(",");
			assertEquals("1024", cols[7], "Full part should be 1024 bytes");
		}
		// Tail part: 904 bytes
		String[] tailCols = capture.messages.get(4).split(",");
		assertEquals("PART", tailCols[0]);
		assertEquals("5", tailCols[3], "Tail part number should be 5");
		assertEquals("904", tailCols[7], "Tail part should be 904 bytes");
		// COMPLETE row
		assertTrue(capture.messages.get(5).startsWith("COMPLETE,"));
	}

	@Test
	void complete_compositeDeleteWithChannel_skipsMpuLogging() throws Exception {
		Config cfg = baseConfig(false, 4, false, null, "s3.us-east-1.amazonaws.com:443");
		TestS3Driver drv = new TestS3Driver(cfg);
		setOpResultOut(drv);

		var deleteOp = newCompositeOp(OpType.DELETE, 4096, 1024);
		deleteOp.status(Operation.Status.SUCC);

		Channel channel = mockChannelForComplete(null);

		var capture = attachCapture(1); // expect 0 messages
		try {
			@SuppressWarnings("unchecked")
			final var op = (Operation<Item>) (Operation<?>) deleteOp;
			drv.complete(channel, op);
			capture.awaitMessages(200); // short timeout — no messages expected
		} finally {
			detachCapture(capture);
		}

		assertTrue(capture.messages.isEmpty(),
						"Non-CREATE composite op with non-null channel should not produce MPU log rows");
		assertNull(deleteOp.get(S3Api.KEY_UPLOAD_ID), "DELETE op should not get upload ID");
	}
}
