package com.dell.spt.storage.driver.coop.netty.http;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

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
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

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

		TestHttpDriver(final Config storage, final boolean readMetaOnly) throws Exception {
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

		@Override
		protected void applyAuthHeaders(final HttpHeaders httpHeaders, final HttpMethod httpMethod, final String dstUriPath, final Credential credential) {}

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
		final var drv = new TestHttpDriver(cfg, true);
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
}
