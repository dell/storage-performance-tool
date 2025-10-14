package com.dell.spt.storage.driver.coop.netty.http.s3;

import com.dell.spt.base.data.DataInput;
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
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.PathItemImpl;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.stream.Collectors;
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

	private static class TestS3Driver extends S3StorageDriver<Item, Operation<Item>> {
		private final Queue<FullHttpRequest> requests = new ArrayDeque<>();
		private final ArrayDeque<FullHttpResponse> stubResponses = new ArrayDeque<>();

		TestS3Driver(Config cfg) throws Exception {
			super("test-s3", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false), cfg.configVal("storage"), false, cfg.intVal("load-batch-size"));
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
		item.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false));
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
	void constructor_extractsAwsRegionFromHost() throws Exception {
		Config cfg = baseConfig(false, 2, false, null, "s3.us-west-2.amazonaws.com:80");
		TestS3Driver drv = new TestS3Driver(cfg);
		Field regionF = S3StorageDriver.class.getDeclaredField("awsRegion");
		regionF.setAccessible(true);
		assertEquals("us-west-2", regionF.get(drv));
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
		base.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false));
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
}
