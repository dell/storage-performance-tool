package com.dell.spt.storage.driver.coop.netty.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.InvalidTextValueException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.naming.ItemNameInput;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Tests for HttpStorageDriverBase that do not require any real network. */
class HttpStorageDriverBaseTest {

	@Test
	void timeoutRequestDescriptionExcludesHeaders() {
		final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, "/bucket");
		request.headers().set(HttpHeaderNames.AUTHORIZATION, "sensitive-signature");

		final var description = HttpStorageDriverBase.timeoutRequestDescription(request);

		assertEquals("method=HEAD, uri=/bucket", description);
		assertFalse(description.contains("sensitive-signature"));
	}

	/** A control request ended by an interrupt or a failure releases a response already read. */
	@Test
	void anUnclaimedResponseAlreadyReadIsReleased() {
		final FullHttpResponse response = new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.buffer(4).writeInt(1));
		final var fullResp = CompletableFuture.completedFuture(response);

		HttpStorageDriverBase.releaseUnclaimedResponse(fullResp);

		assertEquals(0, response.refCnt());
	}

	/** One still unread is settled empty, so the handler releases it when it arrives. */
	@Test
	void anUnclaimedResponseNotYetReadIsLeftForTheHandlerToRelease() {
		final var fullResp = new CompletableFuture<FullHttpResponse>();

		HttpStorageDriverBase.releaseUnclaimedResponse(fullResp);

		assertNull(fullResp.getNow(null));
		assertFalse(fullResp.complete(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)),
						"a response read later is the handler's to release");
	}

	private static Config baseConfig() {
		try {
			final java.util.List<java.util.Map<String, Object>> configSchemas = com.dell.spt.base.env.Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(com.dell.spt.base.env.Extension::schemaProvider)
							.filter(java.util.Objects::nonNull)
							.map(sp -> {
								try {
									return sp.schema();
								} catch (Exception e) {
									throw new RuntimeException(e);
								}
							})
							.filter(java.util.Objects::nonNull)
							.collect(java.util.stream.Collectors.toList());
			SchemaProvider
							.resolve("spt", Thread.currentThread().getContextClassLoader())
							.stream().findFirst().ifPresent(configSchemas::add);
			final java.util.Map<String, Object> configSchema = com.github.akurilov.commons.collection.TreeUtil.reduceForest(configSchemas);
			final Config cfg = new BasicConfig("-", configSchema);
			cfg.val("load-batch-size", 1024);
			cfg.val("storage-driver-limit-concurrency", 0);
			cfg.val("storage-driver-threads", 0);
			cfg.val("storage-driver-limit-queue-input", 1024);
			cfg.val("storage-integrity-mode", "none");
			cfg.val("storage-integrity-algorithm", "sha256");
			cfg.val("storage-integrity-input-provenance", "none");
			cfg.val("storage-integrity-input-expectedProducerId", "");
			// Net
			cfg.val("storage-net-transport", "nio");
			cfg.val("storage-net-ssl-enabled", false);
			cfg.val("storage-net-node-addrs", List.of("127.0.0.1"));
			cfg.val("storage-net-node-port", 9024);
			cfg.val("storage-net-node-connAttemptsLimit", 0);
			cfg.val("storage-net-ioRatio", 50);
			cfg.val("storage-net-timeoutMilliSec", 0);
			cfg.val("storage-net-reuseAddr", true);
			cfg.val("storage-net-bindBacklogSize", 0);
			cfg.val("storage-net-keepAlive", true);
			cfg.val("storage-net-rcvBuf", 0);
			cfg.val("storage-net-sndBuf", 0);
			cfg.val("storage-net-ssl-protocols", List.of());
			cfg.val("storage-net-ssl-provider", "OPENSSL");
			cfg.val("storage-net-tcpNoDelay", false);
			cfg.val("storage-net-interestOpQueued", false);
			cfg.val("storage-net-writeSpinCount", 1);
			cfg.val("storage-net-linger", 0);
			cfg.val("storage-net-http-headers", Map.of());
			cfg.val("storage-net-http-uri-args", Map.of());
			cfg.val("storage-net-http-read-metadata-only", false);
			cfg.val("storage-net-http-max-chunk-size", 65536);
			// Auth minimal
			cfg.val("storage-auth-uid", "user1");
			cfg.val("storage-auth-secret", "secret1");
			return cfg;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static class TestHttpDriver extends HttpStorageDriverBase<DataItemImpl, DataOperation<DataItemImpl>> {
		TestHttpDriver(final Config storage) throws Exception {
			super("test-http", DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true), storage.configVal("storage"), false, 1024);
		}

		String exposeDataUriPath(final DataItemImpl item, final String src, final String dst, final OpType t) {
			return dataUriPath(item, src, dst, t);
		}

		void exposeSendRequest(final io.netty.channel.Channel ch, final DataOperation<DataItemImpl> op) {
			sendRequest(ch, op);
		}

		void exposeAppendHandlers(final io.netty.channel.Channel channel) {
			appendHandlers(channel);
		}

		void bindOperation(final io.netty.channel.Channel channel, final Operation<?> operation) {
			channel.attr(ATTR_KEY_OPERATION).set(operation);
		}

		/** Binds {@code operation} to {@code channel} as a lease does, the transport held. */
		void leaseFor(final io.netty.channel.Channel channel, final Operation<?> operation) {
			channel.attr(ATTR_KEY_RELEASED).set(Boolean.FALSE);
			channel.attr(ATTR_KEY_OPERATION).set(operation);
		}

		int releasedTransports;

		/** The test channels are not pooled: count the release instead. */
		@Override
		protected void releaseTransport(final io.netty.channel.Channel channel) {
			releasedTransports++;
		}

		HttpMethod exposeDataHttpMethod(final OpType opType) {
			return dataHttpMethod(opType);
		}

		void exposeApplyRangesHeaders(final HttpHeaders headers, final DataOperation<?> dataOp) {
			applyRangesHeaders(headers, dataOp);
		}

		@Override
		protected HttpMethod tokenHttpMethod(final OpType opType) {
			return HttpMethod.GET;
		}

		@Override
		protected HttpMethod pathHttpMethod(final OpType opType) {
			return HttpMethod.PUT;
		}

		@Override
		protected String tokenUriPath(final DataItemImpl item, final String srcPath, final String dstPath, final OpType opType) {
			return "/token";
		}

		@Override
		protected String pathUriPath(final DataItemImpl item, final String srcPath, final String dstPath, final OpType opType) {
			return exposeDataUriPath(item, srcPath, dstPath, opType);
		}

		@Override
		protected void applyChecksum(final HttpHeaders httpHeaders, final DataOperation<DataItemImpl> op) {}

		@Override
		protected void applyMetaDataHeaders(final HttpHeaders httpHeaders) {}

		private final List<String> signedUris = new java.util.ArrayList<>();

		@Override
		protected void applyAuthHeaders(final HttpHeaders httpHeaders, final HttpMethod httpMethod, final String dstUriPath, final Credential credential) {
			signedUris.add(dstUriPath);
		}

		List<String> signedUris() {
			return signedUris;
		}

		@Override
		protected void applyCopyHeaders(final HttpHeaders httpHeaders, final String srcPath) {
			httpHeaders.set("x-test-copy-source", srcPath);
		}

		@Override
		protected String requestNewAuthToken(final Credential credential) {
			return null;
		}

		@Override
		public String requestNewPath(final String path) {
			return path;
		}

		@Override
		public java.util.List<DataItemImpl> list(
						final ItemFactory<DataItemImpl> itemFactory,
						final String path,
						final String prefix,
						final int idRadix,
						final DataItemImpl lastPrevItem,
						final int count) {
			return java.util.List.of();
		}
	}

	@Test
	void deleteResponseFirstByteIsMarkedBeforeSplitHeadersCanBeDecoded() throws Exception {
		final var driver = new TestHttpDriver(baseConfig());
		final var channel = new EmbeddedChannel();
		driver.exposeAppendHandlers(channel);
		final var operation = new DeleteRequestOperationImpl(
						0,
						new DeleteRequest(
										"bucket",
										Credential.getInstance("uid", "secret"),
										List.of(new DeleteTarget(
														new IntegrityManifestDataItem("bucket", "key", 0, null)))));
		operation.startRequest();
		operation.markRequestFirstByteSent();
		operation.finishRequest();
		driver.bindOperation(channel, operation);

		assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(new byte[]{'H'
		})),
						"one raw byte must not complete HTTP header decoding");
		final long firstByteMarker = operation.responseFirstByteTime();
		assertTrue(firstByteMarker > 0,
						"the raw transport byte must mark latency before an HttpResponse exists");

		assertTrue(channel.writeInbound(Unpooled.copiedBuffer(
						"TTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n",
						java.nio.charset.StandardCharsets.US_ASCII)));
		assertEquals(firstByteMarker, operation.responseFirstByteTime(),
						"decoded headers must not replace the raw first-byte boundary");
		channel.finishAndReleaseAll();
	}

	@Test
	void dataUriPath_prefersDstOverSrc_noDoubleSlash() throws Exception {
		final var cfg = baseConfig();
		final var drv = new TestHttpDriver(cfg);
		final var item = new DataItemImpl("logs/AA/fileA", 0, 128);
		final String uri = drv.exposeDataUriPath(item, "/srcBucket", "dstBucket", OpType.CREATE);
		assertEquals("/dstBucket/logs/AA/fileA", uri);
	}

	@Test
	void dataUriPath_usesSrcWhenDstNull() throws Exception {
		final var cfg = baseConfig();
		final var drv = new TestHttpDriver(cfg);
		final var item = new DataItemImpl("fileB", 0, 64);
		final String uri = drv.exposeDataUriPath(item, "srcBucket", null, OpType.CREATE);
		assertEquals("/srcBucket/fileB", uri);
	}

	@Test
	void shardedCreateResultRoundTripsToIdenticalReadUri() throws Exception {
		final var cfg = baseConfig();
		final var drv = new TestHttpDriver(cfg);
		final String generatedName;
		try (final var names = ItemNameInput.Builder.newInstance()
						.prefix("base/")
						.shardCount(3)
						.radix(10)
						.seed(0)
						.step(1)
						.type(ItemNameInput.ItemNamingType.SERIAL)
						.build()) {
			generatedName = names.get();
		}
		final var createdItem = new DataItemImpl(generatedName, 0, 128);
		final var create = new DataOperationImpl<>(
						0, OpType.CREATE, createdItem, null, "/bucket", null, null, 0);
		final var createResult = create.result();
		assertEquals("/bucket/base/s0000001/1", createResult.item().name());

		final var read = new DataOperationImpl<>(
						0, OpType.READ, createResult.item(), null, null, null, null, 0);
		assertEquals(
						"/bucket/base/s0000001/1",
						drv.exposeDataUriPath(read.item(), read.srcPath(), read.dstPath(), OpType.READ));
	}

	/**
	 * A defaults document can give a list or map URI argument or header, or a header with no value;
	 * the driver refuses them with the setting's name, where it failed with a cast or a null before.
	 */
	@Test
	void aListMapOrMissingHeaderOrUriArgumentIsRefusedWithItsName() {
		final var cases = List.of(
						List.of("storage-net-http-uri-args", List.of(1, 2), "storage-net-http-uri-args-tid: the value is a list, not text"),
						List.of("storage-net-http-uri-args", Map.of("a", 1), "storage-net-http-uri-args-tid: the value is a map, not text"),
						List.of("storage-net-http-headers", List.of("a"), "storage-net-http-headers-tid: the value is a list, not text"),
						List.of("storage-net-http-headers", Map.of("a", "b"), "storage-net-http-headers-tid: the value is a map, not text"));
		for (final var c : cases) {
			final var cfg = baseConfig();
			cfg.val((String) c.get(0), Map.of("tid", c.get(1)));
			final var failure = assertThrows(InvalidTextValueException.class, () -> new TestHttpDriver(cfg));
			assertEquals(c.get(2), failure.getMessage());
		}
		final var cfg = baseConfig();
		final var headers = new java.util.HashMap<String, Object>();
		headers.put("X-Id", null);
		cfg.val("storage-net-http-headers", headers);
		assertEquals("storage-net-http-headers-X-Id: a request header needs a value",
						assertThrows(InvalidTextValueException.class, () -> new TestHttpDriver(cfg)).getMessage());
	}

	/** A URI argument with no value is sent as {@code name=null}, as before. */
	@Test
	void aUriArgumentWithNoValueIsSentAsNull() throws Exception {
		final var cfg = baseConfig();
		final var args = new java.util.HashMap<String, Object>();
		args.put("tid", null);
		cfg.val("storage-net-http-uri-args", args);
		final var req = new TestHttpDriver(cfg).httpRequest(
						new DataOperationImpl<>(0, OpType.READ, new DataItemImpl("obj", 0, 0), null, "/bucket", null, null, 0),
						"127.0.0.1");
		assertEquals("/bucket/obj?tid=null", req.uri());
	}

	@Test
	void defaultRequestSignsThePathWithoutUriArgs() throws Exception {
		final var cfg = baseConfig();
		cfg.val("storage-net-http-uri-args", Map.of("tri", "probe"));
		final var drv = new TestHttpDriver(cfg);
		final var item = new DataItemImpl("obj", 0, 0);
		final var req = drv.httpRequest(
						new DataOperationImpl<>(0, OpType.READ, item, null, "/bucket", null, null, 0), "127.0.0.1");

		assertEquals("/bucket/obj?tri=probe", req.uri());
		assertEquals(List.of("/bucket/obj"), drv.signedUris(),
						"the base default (Atmos, Swift) must keep signing the bare path");
	}

	/** The base keeps the request path as built, so Atmos and Swift send a key as they did. */
	@Test
	void defaultRequestPathIsThePathItself() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var path = "/bucket/a?b#c";
		assertSame(path, drv.requestPath(path));
		final var req = drv.httpRequest(
						new DataOperationImpl<>(0, OpType.READ, new DataItemImpl("a?b#c", 0, 0), null, "/bucket", null, null, 0),
						"127.0.0.1");
		assertEquals(path, req.uri());
		assertEquals(List.of(path), drv.signedUris());
	}

	@Test
	void sendRequest_create_writesPayload_whenNoSrcPath() throws Exception {
		final var cfg = baseConfig();
		final var drv = new TestHttpDriver(cfg);

		// Prepare a data item with in-memory generator and non-zero size
		final var item = new DataItemImpl("payload.bin", 0, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		final var op = new DataOperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null, null, 0);

		final var ch = new EmbeddedChannel();
		// Minimal headers for HOST are injected by httpRequest path (when node address null, none is set)
		// Call the protected sendRequest via the public wrapper
		drv.exposeSendRequest(ch, op);

		// Grab outbound writes: HttpRequest, Data payload region/stream, LastHttpContent
		Object first = ch.readOutbound();
		Object second = ch.readOutbound();
		Object third = ch.readOutbound();

		assertTrue(first instanceof HttpRequest, "First outbound must be HttpRequest");
		final HttpRequest req = (HttpRequest) first;
		assertEquals(HttpMethod.PUT, req.method());
		assertEquals("/bucket/payload.bin", req.uri());
		final String cl = req.headers().get(HttpHeaderNames.CONTENT_LENGTH);
		assertNotNull(cl, "Content-Length must be present for CREATE");
		assertTrue(Integer.parseInt(cl) > 0, "Content-Length should reflect payload size (>0)");
		assertNotNull(second, "Expected a payload write after the headers");
		assertNotNull(third, "Expected terminating LastHttpContent after payload");
		ch.finishAndReleaseAll();
	}

	@Test
	void sendRequest_create_writesPayload_whenNoSrcPath_sslEnabled() throws Exception {
		final var cfg = baseConfig();
		cfg.val("storage-net-ssl-enabled", true);
		cfg.val("storage-net-ssl-provider", "JDK");
		final var drv = new TestHttpDriver(cfg);

		final var item = new DataItemImpl("payloadSSL.bin", 0, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		final var op = new DataOperationImpl<>(0, OpType.CREATE, item, null, "/bucket", null, null, 0);

		final var ch = new EmbeddedChannel();
		drv.exposeSendRequest(ch, op);

		Object first = ch.readOutbound();
		Object second = ch.readOutbound();
		Object third = ch.readOutbound();
		assertTrue(first instanceof HttpRequest);
		assertNotNull(second);
		assertNotNull(third);
		ch.finishAndReleaseAll();
	}

	// --- dataHttpMethod tests ---

	@Test
	void dataHttpMethod_read_returnsGet() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		assertEquals(HttpMethod.GET, drv.exposeDataHttpMethod(OpType.READ));
	}

	@Test
	void dataHttpMethod_readMetadataOnly_returnsHead() throws Exception {
		final var cfg = baseConfig();
		cfg.val("storage-net-http-read-metadata-only", true);
		final var drv = new TestHttpDriver(cfg);
		assertEquals(HttpMethod.HEAD, drv.exposeDataHttpMethod(OpType.READ));
	}

	@Test
	void dataHttpMethod_delete_returnsDelete() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		assertEquals(HttpMethod.DELETE, drv.exposeDataHttpMethod(OpType.DELETE));
	}

	@Test
	void dataHttpMethod_create_returnsPut() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		assertEquals(HttpMethod.PUT, drv.exposeDataHttpMethod(OpType.CREATE));
	}

	// --- dataUriPath edge cases ---

	@Test
	void dataUriPath_bothPathsNull_returnsItemNameOnly() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("object.dat", 0, 64);
		assertEquals("/object.dat", drv.exposeDataUriPath(item, null, null, OpType.CREATE));
	}

	@Test
	void dataUriPath_itemNameStartsWithSlash_noDoubleSlash() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("/already/slashed", 0, 64);
		assertEquals("/bucket/already/slashed", drv.exposeDataUriPath(item, "/bucket", null, OpType.READ));
	}

	@Test
	void dataUriPath_itemNameStartsWithPathPrefix_noDuplication() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		// Item name starts with the bucket path — should not duplicate
		final var item = new DataItemImpl("/bucket/sub/file.dat", 0, 64);
		assertEquals("/bucket/sub/file.dat", drv.exposeDataUriPath(item, "/bucket", null, OpType.READ));
	}

	@Test
	void dataUriPath_dstPathWithLeadingSlash_noDoubleSlash() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("file.dat", 0, 64);
		assertEquals("/dst/file.dat", drv.exposeDataUriPath(item, "/src", "/dst", OpType.CREATE));
	}

	@Test
	void dataUriPath_trailingSlashDstPath_bareItemName_noDoubleSlash() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("ldypc1g3fjmr", 0, 10_485_760);
		assertEquals(
						"/streaming/20MB/109/ldypc1g3fjmr",
						drv.exposeDataUriPath(item, null, "streaming/20MB/109/", OpType.CREATE));
	}

	@Test
	void dataUriPath_generatedNameRepeatingTheBucket_keepsThePrefix() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("abc/s0/k", 0, 64);
		assertEquals("/abc/abc/s0/k", drv.exposeDataUriPath(item, null, "/abc", OpType.CREATE));
	}

	@Test
	void dataUriPath_generatedNameStartingWithTheBucketLetters_staysInTheBucket() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("abcdef/s0/k", 0, 64);
		assertEquals("/abc/abcdef/s0/k", drv.exposeDataUriPath(item, null, "/abc", OpType.CREATE));
	}

	@Test
	void dataUriPath_rootedNameNamingAnotherBucket_staysInTheBucket() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("/abcdef", 0, 64);
		assertEquals("/abc/abcdef", drv.exposeDataUriPath(item, "/abc", null, OpType.READ));
	}

	@Test
	void dataUriPath_shortBucketWithoutShards_keepsTheBucket() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("a1b2c3", 0, 64);
		assertEquals("/a/a1b2c3", drv.exposeDataUriPath(item, null, "/a", OpType.CREATE));
	}

	/** The first request and every later use of the recorded name address the same object. */
	@ParameterizedTest
	@CsvSource({
			"abc/s0/k, /abc, /abc/abc/s0/k",
			"abcdef/s0/k, /abc, /abc/abcdef/s0/k",
			"zzz/s0/k, /abc, /abc/zzz/s0/k",
			"s0/k, /s00, /s00/s0/k",
			"k, /abc/, /abc/k",
			"/k, /abc, /abc/k",
			"/k, /abc/, /abc/k",
			"k, abc, /abc/k",
			"abc/k, abc, /abc/k",
	})
	void dataUriPath_agreesWithTheRecordedName(
					final String name, final String path, final String expected) throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl(name, 0, 64);
		assertEquals(expected, drv.exposeDataUriPath(item, null, path, OpType.CREATE));

		final var create = new DataOperationImpl<>(
						0, OpType.CREATE, item, null, path, null, null, 0);
		final var recorded = create.result().item();
		assertEquals(expected, drv.exposeDataUriPath(recorded, null, path, OpType.CREATE));

		final var read = new DataOperationImpl<>(0, OpType.READ, recorded, null, null, null, null, 0);
		assertEquals(expected, drv.exposeDataUriPath(read.item(), read.srcPath(), read.dstPath(), OpType.READ));
	}

	@Test
	void dataUriPath_rootedNameHoldsAPathWrittenWithoutASlash() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("/abc/k", 0, 64);
		assertEquals("/abc/k", drv.exposeDataUriPath(item, "abc", null, OpType.CREATE));
	}

	@Test
	void dataUriPath_usesOneSeparatorForEveryForm() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		assertEquals("/abc/k", drv.exposeDataUriPath(new DataItemImpl("k", 0, 1), null, "abc", OpType.CREATE));
		assertEquals("/abc/k", drv.exposeDataUriPath(new DataItemImpl("/k", 0, 1), null, "abc/", OpType.CREATE));
		assertEquals("/abc/k", drv.exposeDataUriPath(new DataItemImpl("/k", 0, 1), null, "/abc", OpType.CREATE));
		assertEquals("/k", drv.exposeDataUriPath(new DataItemImpl("k", 0, 1), null, "/", OpType.CREATE));
		assertEquals("/k", drv.exposeDataUriPath(new DataItemImpl("k", 0, 1), null, "", OpType.CREATE));
	}

	@Test
	void dataUriPath_manifestKeyRepeatingTheBucket_addressesTheKey() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final var item = new DataItemImpl("abc/x", 0, 64);
		assertEquals("/abc/abc/x", drv.exposeDataUriPath(item, "/abc", null, OpType.READ));
	}

	// --- applyRangesHeaders + rangeListToStringBuff tests ---

	@Test
	void applyRangesHeaders_emptyBitSets_noRangeHeader() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final HttpHeaders headers = new DefaultHttpHeaders();

		final var item = new DataItemImpl("emptyRange", 0, 4096);
		@SuppressWarnings("unchecked")
		final DataOperation<DataItemImpl> op = mock(DataOperation.class);
		when(op.item()).thenReturn(item);
		when(op.fixedRanges()).thenReturn(null);
		when(op.markedRangesMaskPair()).thenReturn(new BitSet[]{new BitSet(), new BitSet()
		});

		drv.exposeApplyRangesHeaders(headers, op);
		assertNull(headers.get(HttpHeaderNames.RANGE), "Empty BitSets should not produce a Range header");
	}

	@Test
	void applyRangesHeaders_singleBitSetRange_producesCorrectHeader() throws Exception {
		final var drv = new TestHttpDriver(baseConfig());
		final HttpHeaders headers = new DefaultHttpHeaders();

		// Item size 4MB = 4194304 bytes. Range 0 in rangeOffset scheme = bytes 0..1048575 (1MB range)
		final var item = new DataItemImpl("rangeItem", 0, 4_194_304);
		@SuppressWarnings("unchecked")
		final DataOperation<DataItemImpl> op = mock(DataOperation.class);
		when(op.item()).thenReturn(item);
		when(op.fixedRanges()).thenReturn(null);
		final BitSet current = new BitSet();
		current.set(0); // first range
		when(op.markedRangesMaskPair()).thenReturn(new BitSet[]{current, new BitSet()
		});

		drv.exposeApplyRangesHeaders(headers, op);
		final String range = headers.get(HttpHeaderNames.RANGE);
		assertNotNull(range, "Single set bit should produce a Range header");
		assertTrue(range.startsWith("bytes="), "Range header must start with 'bytes='");
	}

	@Test
	void rangeListToStringBuff_singleRangeWithExplicitSize_appendsBaseLength() {
		// When size != -1, the code writes baseLength + "-" (append semantics)
		final var range = new Range(0, 99, 100);
		final StringBuilder sb = new StringBuilder();
		HttpStorageDriverBase.rangeListToStringBuff(List.of(range), 1024, sb);
		assertEquals("1024-", sb.toString());
	}

	@Test
	void rangeListToStringBuff_singleRangeNoSize_usesToString() {
		// When size == -1, the code uses range.toString() for the beg-end format
		final var range = new Range(0, 99, -1);
		final StringBuilder sb = new StringBuilder();
		HttpStorageDriverBase.rangeListToStringBuff(List.of(range), 1024, sb);
		assertEquals(range.toString(), sb.toString());
	}

	@Test
	void rangeListToStringBuff_multipleRanges_commaSeparated() {
		final var r1 = new Range(0, 99, -1);
		final var r2 = new Range(200, 299, -1);
		final StringBuilder sb = new StringBuilder();
		HttpStorageDriverBase.rangeListToStringBuff(List.of(r1, r2), 1024, sb);
		assertTrue(sb.toString().contains(","), "Multiple ranges should be comma-separated");
	}

	// --- fixed-range UPDATE: append offsets, recorded size, byte count and retry ---

	@Test
	void rangeListToStringBuff_appendFollowedByOtherRanges_isClosed() {
		final StringBuilder sb = new StringBuilder();
		HttpStorageDriverBase.rangeListToStringBuff(List.of(new Range("-512-"), new Range("0-99")), 1024, sb);
		assertEquals("1024-1535,0-99", sb.toString());
	}

	@Test
	void rangeListToStringBuff_twoAppends_secondStartsWhereTheFirstEnds() {
		final StringBuilder sb = new StringBuilder();
		HttpStorageDriverBase.rangeListToStringBuff(List.of(new Range("-512-"), new Range("-512-")), 1024, sb);
		assertEquals("1024-1535,1536-", sb.toString());
	}

	private static final long ITEM_SIZE = 1024;

	private static DataItemImpl appendItem() throws Exception {
		final var item = new DataItemImpl("obj", 0, ITEM_SIZE);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return item;
	}

	private static DataOperationImpl<DataItemImpl> fixedRangeUpdate(final DataItemImpl item, final String... ranges) {
		final List<Range> fixedRanges = java.util.Arrays.stream(ranges).map(Range::new).toList();
		final var op = new DataOperationImpl<>(0, OpType.UPDATE, item, null, "/bucket", null, fixedRanges, 0);
		op.reset();
		return op;
	}

	private static TestHttpDriver driver(final boolean ssl) throws Exception {
		final var cfg = baseConfig();
		if (ssl) {
			cfg.val("storage-net-ssl-enabled", true);
			cfg.val("storage-net-ssl-provider", "JDK");
		}
		final var drv = new TestHttpDriver(cfg);
		@SuppressWarnings("unchecked")
		final com.github.akurilov.commons.io.Output<DataOperation<DataItemImpl>> results = mock(com.github.akurilov.commons.io.Output.class);
		when(results.put(any(DataOperation.class))).thenReturn(true);
		drv.operationResultOutput(results);
		return drv;
	}

	/** The request, the bytes of each queued body write, and whether the last content followed. */
	private record SentUpdate(HttpRequest request, List<byte[]> bodies) {}

	private static SentUpdate sendUpdate(
					final TestHttpDriver drv, final EmbeddedChannel ch, final DataOperationImpl<DataItemImpl> op)
					throws Exception {
		drv.leaseFor(ch, op);
		op.startRequest();
		drv.exposeSendRequest(ch, op);
		final HttpRequest request = ch.readOutbound();
		final List<byte[]> bodies = new java.util.ArrayList<>();
		Object next;
		while (!((next = ch.readOutbound()) instanceof io.netty.handler.codec.http.LastHttpContent)) {
			assertNotNull(next, "the request must end with its last content");
			bodies.add(bytesOf(next));
		}
		return new SentUpdate(request, bodies);
	}

	private static byte[] bytesOf(final Object queued) throws Exception {
		final var out = new java.io.ByteArrayOutputStream();
		if (queued instanceof io.netty.channel.FileRegion region) {
			final var target = java.nio.channels.Channels.newChannel(out);
			long done = 0;
			while (done < region.count()) {
				done += region.transferTo(target, done);
			}
		} else {
			final var stream = (com.dell.spt.storage.driver.coop.netty.data.SeekableByteChannelChunkedNioStream) queued;
			ByteBuf chunk;
			while ((chunk = stream.readChunk(io.netty.buffer.ByteBufAllocator.DEFAULT)) != null) {
				final byte[] b = new byte[chunk.readableBytes()];
				chunk.readBytes(b);
				chunk.release();
				out.write(b);
			}
		}
		return out.toByteArray();
	}

	/** The item's bytes at {@code [beg, beg + size)}. */
	private static byte[] itemBytes(final long beg, final long size) throws Exception {
		return bytesOf(new com.dell.spt.storage.driver.coop.netty.data.DataItemFileRegion(appendItem().slice(beg, size)));
	}

	/**
	 * Each append starts where the previous append of the request ended, an append that other
	 * ranges follow is closed, the item grows by the appended bytes only when the UPDATE succeeds,
	 * and the byte count is the Content-Length. The lone append, a list whose only append is last, and overwrite-only lists
	 * are as before, as is a lone past-end overwrite (recorded size unchanged).
	 */
	@ParameterizedTest
	@CsvSource(delimiter = ';', value = {
			"false; -512-; 1024-; 1024:512; 1536",
			"false; 0-99|-512-; 0-99,1024-; 0:100|1024:512; 1536",
			"false; -512-|0-99; 1024-1535,0-99; 1024:512|0:100; 1536",
			"false; -512-|-512-; 1024-1535,1536-; 1024:512|1536:512; 2048",
			"false; -512-|100-; 1024-1535,100-; 1024:512|100:924; 1536",
			"false; 300-|-512-; 300-,1024-; 300:724|1024:512; 1536",
			"false; 0-99|200-299; 0-99,200-299; 0:100|200:100; 1024",
			"false; 0-2047; 0-2047; 0:2048; 1024",
			"true; -512-; 1024-; 1024:512; 1536",
			"true; 0-99|-512-; 0-99,1024-; 0:100|1024:512; 1536",
			"true; -512-|0-99; 1024-1535,0-99; 1024:512|0:100; 1536",
			"true; -512-|-512-; 1024-1535,1536-; 1024:512|1536:512; 2048",
			"true; -512-|100-; 1024-1535,100-; 1024:512|100:924; 1536",
			"true; 300-|-512-; 300-,1024-; 300:724|1024:512; 1536",
	})
	void fixedRangeUpdateSendsEachAppendAtItsOwnOffset(
					final boolean ssl, final String ranges, final String expectedRange,
					final String expectedSlices, final long expectedSize) throws Exception {
		final var drv = driver(ssl);
		final var item = appendItem();
		final var op = fixedRangeUpdate(item, ranges.split("\\|"));
		final var ch = new EmbeddedChannel();

		final var sent = sendUpdate(drv, ch, op);

		assertEquals("bytes=" + expectedRange, sent.request().headers().get(HttpHeaderNames.RANGE));
		long expectedLength = 0;
		final var slices = expectedSlices.split("\\|");
		assertEquals(slices.length, sent.bodies().size());
		for (int i = 0; i < slices.length; i++) {
			final long beg = Long.parseLong(slices[i].split(":")[0]);
			final long size = Long.parseLong(slices[i].split(":")[1]);
			assertArrayEquals(itemBytes(beg, size), sent.bodies().get(i), "slice " + i + " of " + ranges);
			expectedLength += size;
		}
		assertEquals(expectedLength, Long.parseLong(sent.request().headers().get(HttpHeaderNames.CONTENT_LENGTH)));
		assertEquals(expectedLength, op.countBytesDone(), "the count is the bytes queued");
		assertEquals(ITEM_SIZE, item.size(), "queueing the body leaves the size");
		op.status(Operation.Status.SUCC);
		drv.complete(ch, op);
		assertEquals(expectedSize, item.size());
		ch.finishAndReleaseAll();
	}

	/**
	 * A failed UPDATE attempt leaves the size, so the retry sends the same ranges; the success grows
	 * the item by its appends once. An overwrite beside the append tells the append sum from the
	 * bytes queued.
	 */
	@ParameterizedTest
	@CsvSource({"-512-, 1024-, 1536", "-512-|-512-, 1024-1535|1536-, 2048",
			"0-99|-512-, 0-99|1024-, 1536", "-512-|0-99, 1024-1535|0-99, 1536"
	})
	void aFailedAppendLeavesTheSizeAndTheRetryStartsFromIt(
					final String ranges, final String expectedRange, final long expectedSize) throws Exception {
		final var drv = driver(false);
		final var item = appendItem();
		final var op = fixedRangeUpdate(item, ranges.split("\\|"));
		final var ch = new EmbeddedChannel();
		final var first = sendUpdate(drv, ch, op);
		assertEquals("bytes=" + expectedRange.replace('|', ','), first.request().headers().get(HttpHeaderNames.RANGE));

		op.status(Operation.Status.RESP_FAIL_SVC);
		drv.complete(ch, op);
		assertEquals(ITEM_SIZE, item.size(), "a failed attempt does not grow the item");
		drv.complete(ch, op);
		assertEquals(ITEM_SIZE, item.size());
		assertEquals(1, drv.releasedTransports);

		final var retry = op.result();
		retry.reset();
		final var retryCh = new EmbeddedChannel();
		final var second = sendUpdate(drv, retryCh, retry);
		assertEquals(first.request().headers().get(HttpHeaderNames.RANGE), second.request().headers().get(HttpHeaderNames.RANGE));
		retry.status(Operation.Status.SUCC);
		drv.complete(retryCh, retry);
		assertEquals(expectedSize, item.size(), "a successful attempt keeps its appends");
		drv.complete(retryCh, retry);
		assertEquals(expectedSize, item.size(), "a second complete() grows nothing more");
		ch.finishAndReleaseAll();
		retryCh.finishAndReleaseAll();
	}

	/**
	 * A failure the event loop sees while the dispatcher still queues the body: an early response
	 * (a 503) or a failed write flips the status before the body is queued, and complete() runs
	 * after the body ({@code AFTER_BODY}) or at its first write ({@code AT_BODY}). Either way the
	 * attempt leaves the size, also when the append is longer than the item, and the retry starts
	 * from it.
	 */
	@ParameterizedTest
	@CsvSource({"AFTER_BODY, -512-, 1024-, 1536", "AFTER_BODY, 0-99|-512-, 0-99|1024-, 1536",
			"AFTER_BODY, -2048-, 1024-, 3072", "AT_BODY, -512-, 1024-, 1536",
			"AT_BODY, -2048-, 1024-, 3072"
	})
	void anAppendFailedWhileItsBodyIsQueuedLeavesTheSize(
					final String completeAt, final String ranges, final String expectedRange, final long expectedSize)
					throws Exception {
		final var drv = driver(false);
		final var item = appendItem();
		final var op = fixedRangeUpdate(item, ranges.split("\\|"));
		final boolean atBody = "AT_BODY".equals(completeAt);
		final var failing = new io.netty.channel.ChannelOutboundHandlerAdapter() {
			private boolean completed;
			private HttpRequest request;

			@Override
			public void write(
							final io.netty.channel.ChannelHandlerContext ctx, final Object msg,
							final io.netty.channel.ChannelPromise promise) throws Exception {
				if (msg instanceof HttpRequest sentRequest) {
					request = sentRequest;
					op.status(Operation.Status.RESP_FAIL_SVC);
				} else if (atBody && !completed && !(msg instanceof io.netty.handler.codec.http.LastHttpContent)) {
					completed = true;
					drv.complete(ctx.channel(), op);
				}
				super.write(ctx, msg, promise);
			}
		};
		final var ch = new EmbeddedChannel(failing);
		drv.leaseFor(ch, op);
		op.startRequest();
		drv.exposeSendRequest(ch, op);
		final HttpRequest sent = failing.request;
		assertEquals("bytes=" + expectedRange.replace('|', ','), sent.headers().get(HttpHeaderNames.RANGE));
		if (!atBody) {
			drv.complete(ch, op);
		}
		assertEquals(1, drv.releasedTransports);
		assertEquals(ITEM_SIZE, item.size(), "the failed attempt leaves the size");

		final var retry = op.result();
		retry.reset();
		final var retryCh = new EmbeddedChannel();
		final var second = sendUpdate(drv, retryCh, retry);
		assertEquals(sent.headers().get(HttpHeaderNames.RANGE), second.request().headers().get(HttpHeaderNames.RANGE));
		retry.status(Operation.Status.SUCC);
		drv.complete(retryCh, retry);
		assertEquals(expectedSize, item.size());
		ch.finishAndReleaseAll();
		retryCh.finishAndReleaseAll();
	}

	/**
	 * On a closed channel the header write fails at once and complete() runs before the body is
	 * queued: the attempt must not grow the item, so the retry appends at the object's end.
	 */
	@Test
	void anAppendFailedOnAClosedChannelDoesNotGrowTheItem() throws Exception {
		final var drv = driver(false);
		final var item = appendItem();
		final var op = fixedRangeUpdate(item, "-512-");
		final var closed = new EmbeddedChannel();
		final var unusedClose = closed.close();
		drv.leaseFor(closed, op);
		op.startRequest();
		drv.exposeSendRequest(closed, op);
		assertEquals(Operation.Status.FAIL_IO, op.status());
		assertEquals(1, drv.releasedTransports, "complete() ran for the failed write");
		assertEquals(ITEM_SIZE, item.size());

		final var retry = op.result();
		retry.reset();
		final var ch = new EmbeddedChannel();
		final var sent = sendUpdate(drv, ch, retry);
		assertEquals("bytes=1024-", sent.request().headers().get(HttpHeaderNames.RANGE));
		retry.status(Operation.Status.SUCC);
		drv.complete(ch, retry);
		assertEquals(1536, item.size());
		ch.finishAndReleaseAll();
	}

	/** An UPDATE that queues no ranges (a tagging UPDATE sends no body) leaves the size alone. */
	@ParameterizedTest
	@CsvSource({"SUCC", "RESP_FAIL_SVC"
	})
	void anUpdateThatQueuedNoRangesLeavesTheSize(final Operation.Status status) throws Exception {
		final var drv = driver(false);
		final var item = appendItem();
		final var op = fixedRangeUpdate(item, "-512-");
		final var ch = new EmbeddedChannel();
		drv.leaseFor(ch, op);
		op.startRequest();
		op.status(status);
		drv.complete(ch, op);
		assertEquals(ITEM_SIZE, item.size());
		ch.finishAndReleaseAll();
	}

	private static final int DEFAULT_CAP = HttpStorageDriverBase.CONTROL_RESPONSE_MAX_CONTENT_LENGTH;

	@Test
	void theDefaultControlResponseCapIs16MiB() {
		assertEquals(16 * 1024 * 1024, DEFAULT_CAP);
	}

	/**
	 * A HEAD response states the object's size in Content-Length but has no body, so the control
	 * handler returns it whatever the size, also under the default cap.
	 */
	@ParameterizedTest
	@CsvSource({
			"16777217, 16777216",
			"2147483647, 16777216",
			"2147483648, 16777216",
			"3221225472, 16777216",
			"5497558138880, 16777216",
			"2147483648, 2147483647"
	})
	void aHeadResponseOfAnySizeIsReturned(final long contentLength, final int maxContentLength) {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.HEAD, maxContentLength);
		final var ch = controlChannel(HttpMethod.HEAD, control);

		ch.writeInbound(responseHeaders(contentLength));
		final FullHttpResponse response = control.response().getNow(null);
		try {
			assertNotNull(response);
			assertEquals(HttpResponseStatus.OK, response.status());
			assertEquals(0, response.content().readableBytes());
			assertEquals(
							Long.toString(contentLength),
							response.headers().get(HttpHeaderNames.CONTENT_LENGTH));
			assertFalse(control.oversized());
			assertTrue(ch.isOpen());
		} finally {
			if (response != null) {
				response.release();
			}
			ch.finishAndReleaseAll();
		}
	}

	/** A GET answered with a Content-Length over the cap is refused at once: null, one WARN. */
	@ParameterizedTest
	@CsvSource({"16777217", "2147483648"
	})
	void aBodyOverTheCapByContentLengthReturnsNullWithOneWarning(final long contentLength)
					throws Exception {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var ch = controlChannel(HttpMethod.GET, control);

		ch.writeInbound(responseHeaders(contentLength));

		assertFalse(ch.isOpen());
		assertTrue(control.oversized());
		assertOversizeWarnedOnce(control, HttpMethod.GET, "/bucket/key");
		ch.finishAndReleaseAll();
	}

	/** A chunked body that grows past the cap, as an endless one would, is refused when it passes it. */
	@Test
	void aChunkedBodyGrowingOverTheCapReturnsNullWithOneWarning() throws Exception {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, 4096);
		final var ch = controlChannel(HttpMethod.GET, control);

		ch.writeInbound(Unpooled.copiedBuffer(
						"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n", StandardCharsets.US_ASCII));
		final var chunk = "1000\r\n" + "x".repeat(4096) + "\r\n";
		ch.writeInbound(Unpooled.copiedBuffer(chunk, StandardCharsets.US_ASCII));
		assertTrue(ch.isOpen(), "a body at the cap is still taken");
		ch.writeInbound(Unpooled.copiedBuffer(chunk, StandardCharsets.US_ASCII));

		assertFalse(ch.isOpen());
		assertTrue(control.oversized());
		assertOversizeWarnedOnce(control, HttpMethod.GET, "/bucket/key");
		ch.finishAndReleaseAll();
	}

	/** A response ended by the connection close, with no Content-Length and not chunked, is returned. */
	@Test
	void aCloseDelimitedResponseIsReturned() throws Exception {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var ch = controlChannel(HttpMethod.GET, control);

		ch.writeInbound(Unpooled.copiedBuffer(
						"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n<ListBucketResult/>",
						StandardCharsets.US_ASCII));
		assertFalse(control.response().isDone(), "the body ends only at the close");
		ch.close().sync();
		ch.runPendingTasks();

		final FullHttpResponse response = control.response().getNow(null);
		try {
			assertNotNull(response);
			assertEquals("<ListBucketResult/>", response.content().toString(StandardCharsets.US_ASCII));
			assertFalse(control.oversized());
		} finally {
			if (response != null) {
				response.release();
			}
			ch.finishAndReleaseAll();
		}
	}

	/** A response under the cap is returned as sent, and the waiter takes it without a warning. */
	@Test
	void aResponseUnderTheCapIsReturnedUnchanged() throws Exception {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var ch = controlChannel(HttpMethod.GET, control);

		final var wire = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nx-amz-request-id: r1\r\n\r\nhello";
		ch.writeInbound(Unpooled.copiedBuffer(wire, StandardCharsets.US_ASCII));
		final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/bucket/key");
		final FullHttpResponse response = new TestHttpDriver(baseConfig()).awaitResponse(control, request, true);
		try {
			assertNotNull(response);
			assertEquals(HttpResponseStatus.OK, response.status());
			assertEquals("r1", response.headers().get("x-amz-request-id"));
			assertEquals("hello", response.content().toString(StandardCharsets.US_ASCII));
			assertFalse(control.oversized());
			assertTrue(ch.isOpen());
		} finally {
			if (response != null) {
				response.release();
			}
			ch.finishAndReleaseAll();
		}
	}

	/** A caller's larger cap, through the overload, admits a body the default refuses. */
	@Test
	void aLargerCapAdmitsABodyTheDefaultRefuses() throws Exception {
		final int size = DEFAULT_CAP + 1024 * 1024;
		final var body = new byte[size];
		java.util.Arrays.fill(body, (byte) 'm');

		final var refused = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var refusedCh = controlChannel(HttpMethod.GET, refused);
		refusedCh.writeInbound(responseHeaders(size), Unpooled.wrappedBuffer(body));
		assertNull(refused.response().getNow(null));
		assertTrue(refused.oversized());
		refusedCh.finishAndReleaseAll();

		final var admitted = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, 64 * 1024 * 1024);
		final var admittedCh = controlChannel(HttpMethod.GET, admitted);
		admittedCh.writeInbound(responseHeaders(size), Unpooled.wrappedBuffer(body));
		final FullHttpResponse response = admitted.response().getNow(null);
		try {
			assertNotNull(response);
			assertEquals(size, response.content().readableBytes());
			assertFalse(admitted.oversized());
		} finally {
			if (response != null) {
				response.release();
			}
			admittedCh.finishAndReleaseAll();
		}
	}

	/** A plain close before any response is still reported as one, not as an oversize. */
	@Test
	void aCloseBeforeTheResponseIsNotReportedAsOversize() throws Exception {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var ch = controlChannel(HttpMethod.GET, control);

		ch.close().sync();
		ch.runPendingTasks();

		assertNull(control.response().getNow(null));
		assertFalse(control.oversized());
		final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/bucket/key");
		final var lines = captureMessages(() -> assertNull(
						new TestHttpDriver(baseConfig()).awaitResponse(control, request, true)));
		assertEquals(0, lines.stream().filter(line -> line.contains("cap for control requests")).count());
		assertEquals(1, lines.stream().filter(line -> line.contains("Connection closed before the response")).count());
		ch.finishAndReleaseAll();
	}

	/**
	 * After a refusal, the driver's codec ({@code failOnMissingResponse}) reports the response the
	 * close cut short; that is swallowed with the refusal, so nothing reaches the pipeline tail,
	 * where Netty would log a warning with a stack trace.
	 */
	@Test
	void aRefusalLeavesNoExceptionForThePipelineTail() throws Exception {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var ch = driverCodecChannel(HttpMethod.GET, control);

		ch.writeInbound(responseHeaders(DEFAULT_CAP + 1L));
		ch.runPendingTasks();

		assertFalse(ch.isOpen());
		assertTrue(control.oversized());
		ch.checkException();
		ch.finishAndReleaseAll();
	}

	/** A plain close with the response missing still reports it, as before. */
	@Test
	void aPlainCloseStillReportsTheMissingResponse() {
		final var control = new HttpStorageDriverBase.ControlResponseHandler(HttpMethod.GET, DEFAULT_CAP);
		final var ch = driverCodecChannel(HttpMethod.GET, control);

		assertThrows(io.netty.handler.codec.PrematureChannelClosureException.class, () -> {
			ch.close().sync();
			ch.runPendingTasks();
			ch.checkException();
		});
		assertFalse(control.oversized());
		assertNull(control.response().getNow(null));
		ch.finishAndReleaseAll();
	}

	/** The client codec as the driver builds it, which fails a response the close cuts short. */
	private static EmbeddedChannel driverCodecChannel(
					final HttpMethod method, final HttpStorageDriverBase.ControlResponseHandler control) {
		final var ch = new EmbeddedChannel(
						new HttpClientCodec(HttpStorageDriver.REQ_LINE_LEN, HttpStorageDriver.HEADERS_LEN, 65536, true),
						control.aggregator(),
						control);
		assertTrue(ch.writeOutbound(
						new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, "/bucket/key")));
		ch.releaseOutbound();
		return ch;
	}

	/**
	 * The real {@code executeHttpRequest} methods against a loopback target that answers every
	 * request with a body of 16 MiB + 1: the one- and two-argument methods refuse it with the default
	 * cap, and the overload with a larger cap admits it.
	 */
	@Test
	@org.junit.jupiter.api.Timeout(60)
	void executeHttpRequestAppliesTheDefaultCapAndTheOverloadsCap() throws Exception {
		final int size = DEFAULT_CAP + 1;
		try (final var server = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
			final var acceptor = Thread.ofPlatform().daemon().name("control-cap-target").start(() -> {
				while (!server.isClosed()) {
					try {
						final var connection = server.accept();
						Thread.ofVirtual().start(() -> answerWithBody(connection, size));
					} catch (final java.io.IOException closed) {
						// The test closed the server.
					}
				}
			});
			final Config cfg = baseConfig();
			cfg.val("storage-net-node-addrs", List.of(server.getInetAddress().getHostAddress()));
			cfg.val("storage-net-node-port", server.getLocalPort());
			cfg.val("storage-net-timeoutMilliSec", 10_000);
			final var driver = new TestHttpDriver(cfg);
			try {
				assertNull(driver.executeHttpRequest(controlGet()), "one-argument method, default cap");
				assertNull(driver.executeHttpRequest(controlGet(), false), "two-argument method, default cap");
				final FullHttpResponse admitted = driver.executeHttpRequest(controlGet(), true, 64 * 1024 * 1024);
				try {
					assertNotNull(admitted, "the overload's 64 MiB cap admits the body");
					assertEquals(HttpResponseStatus.OK, admitted.status());
					assertEquals(size, admitted.content().readableBytes());
				} finally {
					if (admitted != null) {
						admitted.release();
					}
				}
			} finally {
				driver.close();
				server.close();
				acceptor.join(5_000);
			}
		}
	}

	private static io.netty.handler.codec.http.FullHttpRequest controlGet() {
		final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/bucket?list-type=2");
		request.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
		return request;
	}

	/** Reads one request head and answers 200 with {@code size} bytes of body, or stops when the client goes. */
	private static void answerWithBody(final java.net.Socket connection, final int size) {
		try (connection) {
			final var in = connection.getInputStream();
			final var head = new StringBuilder();
			int value;
			while ((value = in.read()) >= 0) {
				head.append((char) value);
				if (head.length() >= 4 && head.lastIndexOf("\r\n\r\n") == head.length() - 4) {
					break;
				}
			}
			final var out = connection.getOutputStream();
			out.write(("HTTP/1.1 200 OK\r\nContent-Length: " + size + "\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
			final var chunk = new byte[1 << 20];
			java.util.Arrays.fill(chunk, (byte) 'b');
			for (int sent = 0; sent < size;) {
				final int n = Math.min(chunk.length, size - sent);
				out.write(chunk, 0, n);
				sent += n;
			}
			out.flush();
			// Hold the connection until the client closes it, as a keep-alive target does.
			while (in.read() >= 0) {
				// Discard.
			}
		} catch (final java.io.IOException closed) {
			// The client refused the body and closed the connection.
		}
	}

	private static void assertOversizeWarnedOnce(
					final HttpStorageDriverBase.ControlResponseHandler control,
					final HttpMethod method,
					final String uri)
					throws Exception {
		final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, uri);
		final var driver = new TestHttpDriver(baseConfig());
		final var lines = captureMessages(() -> assertNull(driver.awaitResponse(control, request, true)));
		final var expected = "test-http: Response body over the " + control.maxContentLength()
						+ "-byte cap for control requests, refused: method=" + method + ", uri=" + uri;
		assertEquals(List.of(expected), lines.stream().filter(line -> line.contains("cap for control requests")).toList());
		assertEquals(0, lines.stream().filter(line -> line.contains("Connection closed before the response")).count());
	}

	@FunctionalInterface
	private interface Action {
		void run() throws Exception;
	}

	/** The WARN and higher messages of the Messages logger while {@code action} runs. */
	private static List<String> captureMessages(final Action action) throws Exception {
		final var marker = "control-response-capture-done:" + System.nanoTime();
		final var lines = new java.util.concurrent.CopyOnWriteArrayList<String>();
		final var done = new java.util.concurrent.CountDownLatch(1);
		final var appender = new org.apache.logging.log4j.core.appender.AbstractAppender(
						"control-response-capture", null, null, true,
						org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
			@Override
			public void append(final org.apache.logging.log4j.core.LogEvent event) {
				final var message = event.getMessage().getFormattedMessage();
				if (marker.equals(message)) {
					done.countDown();
				} else if (event.getLevel().isMoreSpecificThan(org.apache.logging.log4j.Level.WARN)) {
					lines.add(message);
				}
			}
		};
		appender.start();
		final var logger = org.apache.logging.log4j.core.LoggerContext.getContext(false)
						.getLogger(com.dell.spt.base.logging.Loggers.MSG.getName());
		logger.addAppender(appender);
		try {
			action.run();
			com.dell.spt.base.logging.Loggers.MSG.error(marker);
			assertTrue(done.await(30, java.util.concurrent.TimeUnit.SECONDS), "the Messages logger drained");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
		return List.copyOf(lines);
	}

	/** A client codec that has sent the request, so it knows a HEAD response has no body. */
	private static EmbeddedChannel controlChannel(
					final HttpMethod method, final HttpStorageDriverBase.ControlResponseHandler control) {
		final var ch = new EmbeddedChannel(new HttpClientCodec(), control.aggregator(), control);
		assertTrue(ch.writeOutbound(
						new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, "/bucket/key")));
		ch.releaseOutbound();
		return ch;
	}

	private static ByteBuf responseHeaders(final long contentLength) {
		return Unpooled.copiedBuffer(
						"HTTP/1.1 200 OK\r\nContent-Length: " + contentLength + "\r\n\r\n",
						StandardCharsets.US_ASCII);
	}
}
