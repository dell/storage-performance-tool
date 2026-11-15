package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteRequest;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl;
import com.dell.spt.base.item.op.deletion.DeleteTarget;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * With {@code storage.object.versioning} on, a {@code key~versionId} item names one object version.
 * A READ (also with {@code read-metadata-only}, a HEAD) and a STAT select it with the {@code
 * versionId} query, as {@code GetObject} and {@code HeadObject} do; {@code x-amz-version-id} is a
 * response header. DELETE keeps its query and UPDATE its header. The request builder never renames
 * the item, so a request built again for the same operation, as a retry builds it, selects the same
 * version, and a successful response replaces the version suffix. Under V2 the version is signed
 * decoded.
 */
@Timeout(60)
final class S3VersionedRequestTest {

	private static final Credential CREDENTIAL = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final String HOST = "127.0.0.1:9024";
	private static final String VERSION_HEADER = "x-amz-version-id";
	/** Version IDs and their encoding in the query: plain, reserved characters, and UTF-8. */
	private static final Map<String, String> ENCODED_IDS = Map.of(
					"v1", "v1",
					"v+1=:x", "v%2B1%3D%3Ax",
					"v 1&a%b", "v%201%26a%25b",
					"\u00e9\u4e2d", "%C3%A9%E4%B8%AD");

	private final List<AutoCloseable> resources = new ArrayList<>();

	@AfterEach
	void tearDown() throws Exception {
		for (final var resource : resources.reversed()) {
			resource.close();
		}
	}

	// ---------- the request each type sends ----------

	@Test
	void aVersionedReadSelectsTheVersionWithTheQueryAndSendsNoHeader() throws Exception {
		final var driver = driver(true, 4, false);
		final var op = op(OpType.READ, "key~v1");

		final var request = driver.httpRequest(op, HOST);

		assertEquals(HttpMethod.GET, request.method());
		assertEquals("/b/key?versionId=v1", request.uri());
		assertNull(request.headers().get(VERSION_HEADER));
		assertEquals("key~v1", op.item().name(), "the item keeps the version it names");
	}

	@Test
	void aMetadataOnlyVersionedReadIsAHeadWithTheQuery() throws Exception {
		final var driver = driver(true, 4, true);

		final var request = driver.httpRequest(op(OpType.READ, "key~v1"), HOST);

		assertEquals(HttpMethod.HEAD, request.method());
		assertEquals("/b/key?versionId=v1", request.uri());
		assertNull(request.headers().get(VERSION_HEADER));
	}

	@Test
	void aVersionedStatChecksTheVersionWithTheQuery() throws Exception {
		final var driver = driver(true, 4, false);

		final var request = driver.httpRequest(op(OpType.STAT, "key~v1"), HOST);

		assertEquals(HttpMethod.HEAD, request.method());
		assertEquals("/b/key?versionId=v1", request.uri());
		assertNull(request.headers().get(VERSION_HEADER));
	}

	@Test
	void aVersionedDeleteIsUnchanged() throws Exception {
		final var driver = driver(true, 4, false);

		final var request = driver.httpRequest(op(OpType.DELETE, "key~v1"), HOST);

		assertEquals(HttpMethod.DELETE, request.method());
		assertEquals("/b/key?versionId=v1", request.uri());
		assertNull(request.headers().get(VERSION_HEADER));
	}

	@Test
	void aVersionedUpdateKeepsTheHeaderAndSendsNoQuery() throws Exception {
		final var driver = driver(true, 4, false);

		final var request = driver.httpRequest(op(OpType.UPDATE, "key~v1"), HOST);

		assertEquals(HttpMethod.PUT, request.method());
		assertEquals("/b/key", request.uri());
		assertEquals("v1", request.headers().get(VERSION_HEADER));
	}

	@Test
	void aVersionedItemWithoutAVersionSendsNoSelector() throws Exception {
		final var driver = driver(true, 4, false);

		for (final var type : List.of(OpType.READ, OpType.STAT, OpType.DELETE)) {
			final var request = driver.httpRequest(op(type, "key"), HOST);
			assertEquals("/b/key", request.uri(), type::toString);
			assertNull(request.headers().get(VERSION_HEADER), type::toString);
		}
	}

	/** With versioning off, {@code ~} is part of the key and nothing selects a version. */
	@Test
	void withVersioningOffTheRequestsAreThoseOfAPlainKey() throws Exception {
		final var driver = driver(false, 4, false);
		final Map<OpType, HttpMethod> methods = new LinkedHashMap<>();
		methods.put(OpType.READ, HttpMethod.GET);
		methods.put(OpType.STAT, HttpMethod.HEAD);
		methods.put(OpType.DELETE, HttpMethod.DELETE);
		methods.put(OpType.UPDATE, HttpMethod.PUT);
		for (final var entry : methods.entrySet()) {
			final var op = op(entry.getKey(), "key~v1");
			final var request = driver.httpRequest(op, HOST);
			final var type = entry.getKey().toString();
			assertEquals(entry.getValue(), request.method(), type);
			assertEquals("/b/key~v1", request.uri(), type);
			assertNull(request.headers().get(VERSION_HEADER), type);
			final var expected = new ArrayList<>(
							List.of("authorization", "content-length", "host", "x-amz-content-sha256", "x-amz-date"));
			if (OpType.UPDATE.equals(entry.getKey())) {
				expected.add(3, "range");
			}
			assertEquals(expected, headerNames(request.headers()), type);
			assertEquals("key~v1", op.item().name(), type);
		}
	}

	@Test
	void aVersionIdThatNeedsEncodingIsSentAndSignedOnceEncodedUnderV4() throws Exception {
		final var driver = driver(true, 4, false);

		final var request = driver.httpRequest(op(OpType.READ, "key~v+1=:x"), HOST);

		assertEquals("/b/key?versionId=v%2B1%3D%3Ax", request.uri());
		assertEquals(referenceV4Signature(request), signature(request.headers()));
	}

	@Test
	void aV2SignatureIncludesTheVersionIdSubResource() throws Exception {
		final var driver = driver(true, 2, false);

		final var request = driver.httpRequest(op(OpType.READ, "key~v1"), HOST);

		assertEquals("/b/key?versionId=v1", request.uri());
		final var expected = "GET\n\n\n" + request.headers().get(HttpHeaderNames.DATE) + "\n/b/key?versionId=v1";
		assertEquals(
						"AWS " + CREDENTIAL.getUid() + ":" + hmacBase64("HmacSHA1", CREDENTIAL.getSecret(), expected),
						request.headers().get(HttpHeaderNames.AUTHORIZATION));
	}

	/**
	 * V2 signs a sub-resource value decoded, while the URI carries it percent-encoded. The anchors are
	 * botocore 1.37.9 {@code HmacV1Auth} signatures of {@code /b/key?versionId=<encoded>} with only a
	 * Date header; they check the reference signer the request tests below use.
	 */
	@Test
	void theReferenceV2SignerMatchesBotocore() throws Exception {
		final var date = "Thu, 01 Jan 2026 00:00:00 GMT";
		final String[][] anchors = {
				{"GET", "v1", "iFL/wEe0MWhY1+hWveWUF9hGvP4="
				},
				{"HEAD", "v1", "g+3aglVcNhNw0N8xz1qrSOGfEyE="
				},
				{"DELETE", "v1", "pI3HdsY4AgvNU0eyks1kQeoVDg8="
				},
				{"GET", "v%2B1%3D%3Ax", "MMxXsneSXyqem0+XcCP8uuPPAqE="
				},
				{"HEAD", "v%2B1%3D%3Ax", "K4bQ+ph6WcwSLr3g1Nj/Jmtkqk4="
				},
				{"DELETE", "v%2B1%3D%3Ax", "I4qFYpab4+lS+VD02D4v7DoZUVg="
				},
				{"GET", "v%201%26a%25b", "V2Ad17cFCSdSJeV8oTWcDe7trfk="
				},
				{"HEAD", "v%201%26a%25b", "soqt3I4M1HXoT6XzJOs4IjL7p4U="
				},
				{"DELETE", "v%201%26a%25b", "ydo9hetriLHeAVCWuOQtIrXGOKM="
				},
				{"GET", "%C3%A9%E4%B8%AD", "GXg9L09QAvn7Q6Jwsy+5YygGnWQ="
				},
				{"HEAD", "%C3%A9%E4%B8%AD", "OEec+UMa5yw17l4aUzmOmuaJZW8="
				},
				{"DELETE", "%C3%A9%E4%B8%AD", "wjAzaTSnv9gBSalNOiXNruWgyi0="
				},
		};
		for (final var anchor : anchors) {
			final var headers = new DefaultHttpHeaders().set(HttpHeaderNames.DATE, date);
			final HttpRequest request = new io.netty.handler.codec.http.DefaultHttpRequest(
							io.netty.handler.codec.http.HttpVersion.HTTP_1_1, HttpMethod.valueOf(anchor[0]),
							"/b/key?versionId=" + anchor[1], headers);
			assertEquals(anchor[2], referenceV2Signature(request), () -> anchor[0] + " " + anchor[1]);
		}
	}

	@Test
	void aVersionIdThatNeedsEncodingIsSentEncodedAndSignedDecodedUnderV2() throws Exception {
		final Map<String, String> encoded = ENCODED_IDS;
		final var driver = driver(true, 2, false);
		final var headDriver = driver(true, 2, true);
		for (final var entry : encoded.entrySet()) {
			final var versionId = entry.getKey();
			final var wire = "/b/key?versionId=" + entry.getValue();
			final Map<String, HttpRequest> requests = new LinkedHashMap<>();
			requests.put("READ", driver.httpRequest(op(OpType.READ, "key~" + versionId), HOST));
			requests.put("STAT", driver.httpRequest(op(OpType.STAT, "key~" + versionId), HOST));
			requests.put("metadata-only READ", headDriver.httpRequest(op(OpType.READ, "key~" + versionId), HOST));
			requests.put("DELETE", driver.httpRequest(op(OpType.DELETE, "key~" + versionId), HOST));
			for (final var type : List.of(OpType.READ, OpType.DELETE)) {
				final var explicit = op(type, "key");
				explicit.requestedVersionId(versionId);
				requests.put("explicit " + type, driver.httpRequest(explicit, HOST));
			}
			for (final var request : requests.entrySet()) {
				final var what = request.getKey() + " of " + versionId;
				assertEquals(wire, request.getValue().uri(), what);
				assertEquals(
								"AWS " + CREDENTIAL.getUid() + ":" + referenceV2Signature(request.getValue()),
								request.getValue().headers().get(HttpHeaderNames.AUTHORIZATION),
								what);
			}
		}
	}

	/** The standalone (manifest) DELETE and the delete-verification HEAD select a version too. */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aManifestDeleteAndAPresenceHeadSignAnEncodedVersionDecodedUnderV2() throws Exception {
		final var driver = driver(false, 2, false);
		for (final var entry : ENCODED_IDS.entrySet()) {
			final var target = new DeleteTarget(new IntegrityManifestDataItem("b", "key", 0, entry.getKey()));
			final Operation delete = new DeleteRequestOperationImpl(0, new DeleteRequest("b", CREDENTIAL, List.of(target)));
			final HttpRequest deleteRequest = driver.httpRequest(delete, HOST);
			driver.log().clear();
			driver.presence(target);
			final HttpRequest head = driver.log().poll();
			for (final var request : List.of(deleteRequest, head)) {
				final var what = request.method() + " of " + entry.getKey();
				assertEquals("/b/key?versionId=" + entry.getValue(), request.uri(), what);
				assertEquals(
								"AWS " + CREDENTIAL.getUid() + ":" + referenceV2Signature(request),
								request.headers().get(HttpHeaderNames.AUTHORIZATION),
								what);
			}
		}
	}

	/**
	 * Requests that select no version sign their URI exactly as sent, as before: only the requests
	 * that put a version ID in the query sign it decoded. That holds for an unversioned name that
	 * itself holds {@code versionId=}, and for the multipart, tagging and list queries.
	 */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void requestsThatSelectNoVersionSignTheirUriAsSentUnderV2() throws Exception {
		final var unversioned = driver(false, 2, false);
		final List<HttpRequest> requests = new ArrayList<>();
		requests.add(unversioned.httpRequest(op(OpType.READ, "k?x=1&versionId=a%2Bb"), HOST));
		requests.add(unversioned.httpRequest(op(OpType.READ, "key"), HOST));
		final var item = new DataItemImpl("key", 0, 16);
		item.dataInput(dataInput());
		final var upload = new CompositeDataOperationImpl<>(0, OpType.CREATE, item, null, "/b", CREDENTIAL, null, 0, 8);
		requests.add(unversioned.httpRequest((Operation) upload, HOST));
		upload.put(S3Api.KEY_UPLOAD_ID, "2~up+load/id");
		final var part = new DataItemImpl("key", 0, 8);
		part.dataInput(dataInput());
		requests.add(unversioned.httpRequest(
						(Operation) new PartialDataOperationImpl<>(0, OpType.CREATE, part, null, "/b", CREDENTIAL, 1, upload),
						HOST));
		final var list = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("/b/pre fix"), CREDENTIAL);
		list.options(ListOptions.builder().startAfter("a+b c").build());
		requests.add(unversioned.httpRequest((Operation) list, HOST));
		final Config config = S3StorageDriverTest.baseConfig(true, 2, false, null, HOST);
		config.val("storage-object-tagging-enabled", true);
		final var tagging = new S3StorageDriverTest.TestS3Driver(config);
		resources.add(tagging);
		requests.add(tagging.httpRequest(op(OpType.READ, "key~v1"), HOST));

		final List<String> uris = new ArrayList<>();
		for (final var request : requests) {
			uris.add(request.uri());
			assertEquals(
							"AWS " + CREDENTIAL.getUid() + ":" + asSentV2Signature(request),
							request.headers().get(HttpHeaderNames.AUTHORIZATION),
							request::uri);
		}
		assertEquals("/b/k?x=1&versionId=a%2Bb", uris.get(0));
		assertEquals("/b/key", uris.get(1));
		assertTrue(uris.get(2).endsWith("?uploads"), uris.get(2));
		assertTrue(uris.get(3).contains("uploadId=2~up+load/id"), uris.get(3));
		assertTrue(uris.get(4).contains("start-after=a%2Bb%20c"), uris.get(4));
		assertEquals("/b/key~v1?tagging", uris.get(5));
	}

	/** SigV4 of a version ID that needs no encoding, and of a query of several parameters. */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void v4SignsAPlainVersionIdAndAMultiParameterQueryAsTheSpecificationDoes() throws Exception {
		final var driver = driver(true, 4, false);
		for (final var type : List.of(OpType.READ, OpType.STAT, OpType.DELETE)) {
			final var request = driver.httpRequest(op(type, "key~3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"), HOST);
			assertEquals("/b/key?versionId=3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY", request.uri());
			assertEquals(referenceV4Signature(request), signature(request.headers()), type::toString);
		}
		final var list = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("/b/pre"), CREDENTIAL);
		list.options(ListOptions.builder().startAfter("a+b").build());
		final var listRequest = driver.httpRequest((Operation) list, HOST);
		assertTrue(listRequest.uri().contains("&"), listRequest::uri);
		assertEquals(referenceV4Signature(listRequest), signature(listRequest.headers()));
		// A single parameter whose value is sent unencoded: %41 is canonically A.
		final var item = new DataItemImpl("key", 0, 16);
		item.dataInput(dataInput());
		final var upload = new CompositeDataOperationImpl<>(0, OpType.CREATE, item, null, "/b", CREDENTIAL, null, 0, 8);
		upload.put(S3Api.KEY_UPLOAD_ID, "a%41");
		upload.put(S3Api.KEY_MPU_ABORT, "true");
		final var abort = driver.httpRequest((Operation) upload, HOST);
		assertEquals("/b/key?uploadId=a%41", abort.uri());
		assertEquals(referenceV4Signature(abort), signature(abort.headers()));
	}

	// ---------- the item is never renamed while its request is built ----------

	/**
	 * The channel owns the operation while its request is built, so its idle timeout can complete the
	 * operation, and a retry can be built from it, at any point of the build. The probe reads the item
	 * name from inside the build, after the URI is made.
	 */
	@Test
	void theItemKeepsItsVersionedNameWhileTheRequestIsBuilt() throws Exception {
		final List<String> seen = new CopyOnWriteArrayList<>();
		final Config config = S3StorageDriverTest.baseConfig(true, 4, false, null, HOST);
		final var driver = new S3StorageDriverTest.TestS3Driver(config) {
			@Override
			protected void applyMetaDataHeaders(final HttpHeaders headers, final Operation<Item> op) {
				seen.add(op.type() + " " + op.item().name());
				super.applyMetaDataHeaders(headers, op);
			}
		};
		resources.add(driver);
		final List<String> uris = new ArrayList<>();
		for (final var type : List.of(OpType.READ, OpType.STAT, OpType.DELETE, OpType.UPDATE)) {
			uris.add(driver.httpRequest(op(type, "key~v1"), HOST).uri());
		}
		final var copy = op(OpType.CREATE, "key~v1", "/src", "/b");
		final var copyRequest = driver.httpRequest(copy, HOST);

		assertEquals(List.of("READ key~v1", "STAT key~v1", "DELETE key~v1", "UPDATE key~v1", "CREATE key~v1"), seen);
		assertEquals(
						List.of("/b/key?versionId=v1", "/b/key?versionId=v1", "/b/key?versionId=v1", "/b/key"), uris);
		assertEquals("/b/key", copyRequest.uri());
		assertEquals("/src/key", copyRequest.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
		assertEquals("key~v1", copy.item().name());
	}

	/** A subclass's {@code dataUriPath} still makes the paths of the versioned route. */
	@Test
	void aDataUriPathOverrideMakesTheVersionedPaths() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(true, 2, false, null, HOST);
		final var driver = new S3StorageDriverTest.TestS3Driver(config) {
			@Override
			protected String dataUriPath(
							final Item item, final String srcPath, final String dstPath, final OpType opType) {
				return "/custom" + super.dataUriPath(item, srcPath, dstPath, opType);
			}
		};
		resources.add(driver);
		final Map<OpType, String> uris = new LinkedHashMap<>();
		for (final var type : List.of(OpType.READ, OpType.STAT, OpType.DELETE, OpType.UPDATE)) {
			final var op = op(type, "key~v+1");
			final var request = driver.httpRequest(op, HOST);
			uris.put(type, request.uri());
			assertEquals("key~v+1", op.item().name());
			assertEquals(
							"AWS " + CREDENTIAL.getUid() + ":" + referenceV2Signature(request),
							request.headers().get(HttpHeaderNames.AUTHORIZATION),
							type::toString);
		}
		final var copy = op(OpType.CREATE, "key~v1", "/src", "/b");
		final var copyRequest = driver.httpRequest(copy, HOST);
		final var plain = driver.httpRequest(op(OpType.READ, "key"), HOST);

		assertEquals("/custom/b/key?versionId=v%2B1", uris.get(OpType.READ));
		assertEquals("/custom/b/key?versionId=v%2B1", uris.get(OpType.STAT));
		assertEquals("/custom/b/key?versionId=v%2B1", uris.get(OpType.DELETE));
		assertEquals("/custom/b/key", uris.get(OpType.UPDATE));
		assertEquals("/custom/b/key", copyRequest.uri());
		assertEquals("/custom/src/key", copyRequest.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
		assertEquals("/custom/b/key", plain.uri());
	}

	/**
	 * An override that rewrites the name gives a path without the {@code ~versionId} suffix. The key's
	 * path is then unknown, so the request is not built, rather than sent for another object.
	 */
	@Test
	void aDataUriPathOverrideThatRewritesTheNameFailsTheVersionedRequest() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(true, 4, false, null, HOST);
		final var driver = new S3StorageDriverTest.TestS3Driver(config) {
			@Override
			protected String dataUriPath(
							final Item item, final String srcPath, final String dstPath, final OpType opType) {
				return "/b/" + Integer.toHexString(item.name().hashCode());
			}
		};
		resources.add(driver);
		for (final var type : List.of(OpType.READ, OpType.DELETE, OpType.UPDATE)) {
			final var op = op(type, "key~v1");
			final var failure = assertThrows(IllegalStateException.class, () -> driver.httpRequest(op, HOST));
			assertTrue(failure.getMessage().contains("~v1"), failure::getMessage);
			assertEquals("key~v1", op.item().name());
		}
		final var copy = op(OpType.CREATE, "key~v1", "/src", "/b");
		assertThrows(IllegalStateException.class, () -> driver.httpRequest(copy, HOST));
		// Without a version suffix there is nothing to take off: the override's path is sent as it is.
		assertEquals("/b/" + Integer.toHexString("key".hashCode()), driver.httpRequest(op(OpType.READ, "key"), HOST).uri());
	}

	// ---------- the routes that do not split the name append the version ----------

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aCompositeResponseAppendsTheVersion() throws Exception {
		final var driver = driver(true, 4, false);
		final var handler = new S3ResponseHandler(driver, false, true, null);
		final var item = new DataItemImpl("key~v1", 0, 16);
		item.dataInput(dataInput());
		final Operation upload = new CompositeDataOperationImpl<>(0, OpType.CREATE, item, null, "/b", CREDENTIAL, null, 0, 8);

		handler.handleResponseHeaders(null, upload, versionResponse("v2"));

		assertEquals("key~v1~v2", item.name());
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aPartResponseAppendsTheVersion() throws Exception {
		final var driver = driver(true, 4, false);
		final var handler = new S3ResponseHandler(driver, false, true, null);
		final var item = new DataItemImpl("key~v1", 0, 16);
		item.dataInput(dataInput());
		final var upload = new CompositeDataOperationImpl<>(0, OpType.CREATE, item, null, "/b", CREDENTIAL, null, 0, 8);
		final var part = new DataItemImpl("key~v1", 0, 8);
		part.dataInput(dataInput());
		final Operation partOp = new PartialDataOperationImpl<>(0, OpType.CREATE, part, null, "/b", CREDENTIAL, 1, upload);

		handler.handleResponseHeaders(null, partOp, versionResponse("v2"));

		assertEquals("key~v1~v2", part.name());
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aTaggingResponseAppendsTheVersion() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(true, 4, false, null, HOST);
		config.val("storage-object-tagging-enabled", true);
		final var driver = new S3StorageDriverTest.TestS3Driver(config);
		resources.add(driver);
		final var handler = new S3ResponseHandler(driver, false, true, null);
		final Operation op = op(OpType.READ, "key~v1");
		assertEquals("/b/key~v1?tagging", driver.httpRequest(op, HOST).uri(), "the tagging route keeps the whole name");

		handler.handleResponseHeaders(null, op, versionResponse("v2"));

		assertEquals("key~v1~v2", op.item().name());
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aListResponseAppendsTheVersion() throws Exception {
		final var driver = driver(true, 4, false);
		final var handler = new S3ResponseHandler(driver, false, true, null);
		final var item = new PathItemImpl("pre~fix");
		final Operation list = new ListOperationImpl<>(0, OpType.LIST, item, CREDENTIAL);

		handler.handleResponseHeaders(null, list, versionResponse("v2"));

		assertEquals("pre~fix~v2", item.name());
	}

	// ---------- building the request again, as a retry does ----------

	@Test
	void theSameOperationBuiltTwiceSendsTheSameRequest() throws Exception {
		final var driver = driver(true, 2, false);
		for (final var type : List.of(OpType.READ, OpType.STAT, OpType.UPDATE, OpType.DELETE)) {
			final var op = op(type, "key~v1");

			final var first = driver.httpRequest(op, HOST);
			final var second = driver.httpRequest(op, HOST);

			assertEquals(first.method(), second.method(), type::toString);
			assertEquals(first.uri(), second.uri(), type::toString);
			assertEquals(first.headers().get(VERSION_HEADER), second.headers().get(VERSION_HEADER), type::toString);
			assertEquals(headerNames(first.headers()), headerNames(second.headers()), type::toString);
			assertTrue(first.uri().startsWith("/b/key"), first::uri);
			assertFalse(first.uri().contains("~"), first::uri);
		}
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aFailedAttemptLeavesTheItemNamingItsVersion() throws Exception {
		final var driver = driver(true, 4, false);
		final var handler = new S3ResponseHandler(driver, false, true, null);
		for (final var type : List.of(OpType.READ, OpType.DELETE)) {
			// An error response carries no x-amz-version-id.
			final Operation op = op(type, "key~v1");
			driver.httpRequest(op, HOST);
			handler.handleResponseHeaders(null, op, new DefaultHttpHeaders());
			assertEquals("key~v1", op.item().name(), type::toString);
			assertEquals("/b/key?versionId=v1", driver.httpRequest(op, HOST).uri(), type::toString);

			// A timeout or a connection failure has no response at all.
			final Operation timedOut = op(type, "key~v1");
			driver.httpRequest(timedOut, HOST);
			assertEquals("key~v1", timedOut.item().name(), type::toString);
			assertEquals("/b/key?versionId=v1", driver.httpRequest(timedOut, HOST).uri(), type::toString);
		}
	}

	/**
	 * The README §4.3.1 recycle flow: each successful operation ends with the item named {@code
	 * key~<returned version>}, and a name never collects two suffixes.
	 */
	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void eachSuccessfulOperationRenamesTheItemToTheReturnedVersion() throws Exception {
		final var driver = driver(true, 4, false);
		final var handler = new S3ResponseHandler(driver, false, true, null);
		final var item = new DataItemImpl("key", 0, 16);
		item.dataInput(dataInput());
		final List<String> uris = new ArrayList<>();
		final List<String> names = new ArrayList<>();
		var version = 0;
		for (final var type : List.of(OpType.CREATE, OpType.READ, OpType.UPDATE, OpType.STAT, OpType.READ)) {
			final Operation op = new DataOperationImpl<>(0, type, item, "/b", "/b", CREDENTIAL, null, 0);
			uris.add(driver.httpRequest(op, HOST).uri());
			final HttpHeaders response = new DefaultHttpHeaders();
			if (OpType.CREATE.equals(type) || OpType.UPDATE.equals(type)) {
				version++;
			}
			response.set(VERSION_HEADER, "v" + version);
			handler.handleResponseHeaders(null, op, response);
			names.add(item.name());
		}
		assertEquals(List.of("/b/key", "/b/key?versionId=v1", "/b/key", "/b/key?versionId=v2", "/b/key?versionId=v2"), uris);
		assertEquals(List.of("key~v1", "key~v1", "key~v2", "key~v2", "key~v2"), names);
	}

	@Test
	void aSuffixIsReplacedOnlyOnTheRouteThatSplitsIt() throws Exception {
		assertEquals("key~v2", S3StorageDriver.withLegacyVersion("key~v1", "v2"));
		assertEquals("key~v2", S3StorageDriver.withLegacyVersion("key", "v2"));
		assertEquals("a~b~v2", S3StorageDriver.withLegacyVersion("a~b~v1", "v2"));

		final var driver = driver(true, 4, false);
		assertTrue(driver.namesLegacyVersion(op(OpType.READ, "key~v1")));
		final var unversioned = driver(false, 4, false);
		assertFalse(unversioned.namesLegacyVersion(op(OpType.READ, "key~v1")));
		final var explicit = op(OpType.READ, "key");
		explicit.requestedVersionId("v1");
		assertFalse(driver.namesLegacyVersion(explicit));
	}

	// ---------- a retry through the driver, against a loopback target that keeps versions ----------

	@Test
	void aRetriedVersionedDeleteRemovesTheNamedVersion() throws Exception {
		final var target = new VersionedTarget("DELETE");
		final var driver = loopbackDriver(target);

		final var first = run(driver, op(OpType.DELETE, "obj~ver0001"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, first.status());
		assertEquals("/b/obj~ver0001", first.item().name());
		final var retried = run(driver, first);

		assertEquals(Operation.Status.SUCC, retried.status());
		assertEquals(List.of("DELETE /b/obj?versionId=ver0001 503", "DELETE /b/obj?versionId=ver0001 204"), target.objectLog());
		assertEquals(List.of("ver0002"), target.versions(), "the named version is gone and no delete marker is added");
	}

	@Test
	void aRetriedVersionedReadReadsTheNamedVersion() throws Exception {
		final var target = new VersionedTarget("GET");
		final var driver = loopbackDriver(target);

		final var first = run(driver, op(OpType.READ, "obj~ver0001"));
		assertEquals(Operation.Status.RESP_FAIL_SVC, first.status());
		final var retried = run(driver, first);

		assertEquals(Operation.Status.SUCC, retried.status());
		assertEquals(List.of("GET /b/obj?versionId=ver0001 503", "GET /b/obj?versionId=ver0001 200 1000"), target.objectLog());
		assertEquals("/b/obj~ver0001", retried.item().name());
	}

	@Test
	void withoutARetryAFailedVersionedDeleteFailsAndChangesNothing() throws Exception {
		final var target = new VersionedTarget("DELETE");
		final var driver = loopbackDriver(target);

		final var result = run(driver, op(OpType.DELETE, "obj~ver0001"));

		assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
		assertEquals(List.of("DELETE /b/obj?versionId=ver0001 503"), target.objectLog());
		assertEquals(List.of("ver0001", "ver0002"), target.versions());
	}

	/**
	 * A READ or STAT naming a version the target no longer holds fails with 404. Before the {@code
	 * versionId} query it read, or checked, the current version and succeeded.
	 */
	@Test
	void aReadOrStatOfAVersionThatNoLongerExistsFailsNotFound() throws Exception {
		final var target = new VersionedTarget("NONE");
		final var driver = loopbackDriver(target);

		final var read = run(driver, op(OpType.READ, "obj~ver0009"));
		final var stat = run(driver, op(OpType.STAT, "obj~ver0009"));

		assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, read.status());
		assertEquals(Operation.Status.RESP_FAIL_NOT_FOUND, stat.status());
		assertEquals(List.of("GET /b/obj?versionId=ver0009 404", "HEAD /b/obj?versionId=ver0009 404"), target.objectLog());
		assertEquals("/b/obj~ver0009", read.item().name());
	}

	// ---------- helpers ----------

	private S3StorageDriverTest.TestS3Driver driver(
					final boolean versioning, final int authVersion, final boolean readMetadataOnly) throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(versioning, authVersion, false, null, HOST);
		config.val("storage-net-http-read-metadata-only", readMetadataOnly);
		final var driver = new S3StorageDriverTest.TestS3Driver(config);
		resources.add(driver);
		return driver;
	}

	private static DataInput dataInput() throws IOException {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> op(final OpType type, final String name) throws IOException {
		final var item = new DataItemImpl(name, 0, 16);
		item.dataInput(dataInput());
		final var op = new DataOperationImpl<>(0, type, item, "/b", null, CREDENTIAL, null, 0);
		if (OpType.UPDATE.equals(type)) {
			op.markRandomRanges(1);
		}
		return (Operation<Item>) (Operation<?>) op;
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> op(
					final OpType type, final String name, final String srcPath, final String dstPath) throws IOException {
		final var item = new DataItemImpl(name, 0, 16);
		item.dataInput(dataInput());
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<>(0, type, item, srcPath, dstPath, CREDENTIAL, null, 0);
	}

	private static HttpHeaders versionResponse(final String versionId) {
		return new DefaultHttpHeaders().set(VERSION_HEADER, versionId);
	}

	/**
	 * An independent V2 (HMAC-SHA1) signature of {@code request}, after the AWS specification and
	 * botocore: the path as sent, then the sub-resources sorted, each value percent-decoded.
	 */
	private static String referenceV2Signature(final HttpRequest request) throws Exception {
		return v2Signature(request, true);
	}

	/** The V2 signature SPT gives a request that selects no version: its URI signed exactly as sent. */
	private static String asSentV2Signature(final HttpRequest request) throws Exception {
		return v2Signature(request, false);
	}

	private static String v2Signature(final HttpRequest request, final boolean subResourcesDecoded) throws Exception {
		final var headers = request.headers();
		final var stringToSign = new StringBuilder(request.method().name()).append('\n');
		for (final var name : List.of("content-md5", "content-type", "date")) {
			stringToSign.append(headers.get(name, "")).append('\n');
		}
		final var amz = new TreeMap<String, String>();
		for (final var header : headers) {
			final var name = header.getKey().toLowerCase(java.util.Locale.ROOT);
			if (name.startsWith("x-amz-")) {
				amz.merge(name, header.getValue().trim(), (a, b) -> a + "," + b);
			}
		}
		amz.forEach((name, value) -> stringToSign.append(name).append(':').append(value).append('\n'));
		final var uri = request.uri();
		final var queryIndex = uri.indexOf('?');
		if (!subResourcesDecoded) {
			stringToSign.append(uri);
		} else {
			stringToSign.append(queryIndex < 0 ? uri : uri.substring(0, queryIndex));
		}
		if (subResourcesDecoded && queryIndex >= 0) {
			final var subResources = new TreeMap<String, String>();
			for (final var pair : uri.substring(queryIndex + 1).split("&")) {
				final var eq = pair.indexOf('=');
				subResources.put(
								eq < 0 ? pair : pair.substring(0, eq), eq < 0 ? null : percentDecode(pair.substring(eq + 1)));
			}
			stringToSign.append('?').append(String.join("&", subResources.entrySet().stream()
							.map(e -> e.getValue() == null ? e.getKey() : e.getKey() + "=" + e.getValue())
							.toList()));
		}
		return hmacBase64("HmacSHA1", CREDENTIAL.getSecret(), stringToSign.toString());
	}

	/** RFC 3986 percent-decoding to UTF-8; {@code +} is a plus sign. */
	private static String percentDecode(final String value) {
		final var bytes = new java.io.ByteArrayOutputStream();
		for (int i = 0; i < value.length(); i++) {
			final char c = value.charAt(i);
			if (c == '%') {
				bytes.write(Integer.parseInt(value.substring(i + 1, i + 3), 16));
				i += 2;
			} else {
				bytes.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
			}
		}
		return bytes.toString(StandardCharsets.UTF_8);
	}

	private static List<String> headerNames(final HttpHeaders headers) {
		return headers.names().stream().map(String::toLowerCase).sorted().distinct().toList();
	}

	private static String signature(final HttpHeaders headers) {
		final var auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		return auth.substring(auth.indexOf("Signature=") + "Signature=".length());
	}

	/** An independent SigV4 signature of {@code request} from the AWS specification. */
	private static String referenceV4Signature(final HttpRequest request) throws Exception {
		final var headers = request.headers();
		final var auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		final var scope = auth.substring(auth.indexOf("Credential=") + "Credential=".length(), auth.indexOf(','));
		final var scopeParts = scope.split("/");
		final var signedHeaders = auth.substring(
						auth.indexOf("SignedHeaders=") + "SignedHeaders=".length(), auth.indexOf(", Signature="));
		final var uri = request.uri();
		final var queryIndex = uri.indexOf('?');
		final var query = new TreeMap<String, String>();
		for (final var pair : uri.substring(queryIndex + 1).split("&")) {
			final var eq = pair.indexOf('=');
			query.put(
							awsEncode(URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8)),
							awsEncode(URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8)));
		}
		final var canonical = new StringBuilder()
						.append(request.method().name()).append('\n')
						.append(uri, 0, queryIndex).append('\n');
		canonical.append(String.join("&", query.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).toList()))
						.append('\n');
		for (final var name : signedHeaders.split(";")) {
			canonical.append(name).append(':').append(headers.get(name).trim()).append('\n');
		}
		canonical.append('\n').append(signedHeaders).append('\n').append(headers.get("x-amz-content-sha256"));
		final var amzDate = headers.get("x-amz-date");
		final var stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + "\n"
						+ scope.substring(scope.indexOf('/') + 1) + "\n"
						+ HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
										.digest(canonical.toString().getBytes(StandardCharsets.UTF_8)));
		byte[] key = ("AWS4" + CREDENTIAL.getSecret()).getBytes(StandardCharsets.UTF_8);
		for (final var part : List.of(scopeParts[1], scopeParts[2], scopeParts[3], scopeParts[4])) {
			key = hmac("HmacSHA256", key, part);
		}
		return HexFormat.of().formatHex(hmac("HmacSHA256", key, stringToSign));
	}

	private static String awsEncode(final String value) {
		final var out = new StringBuilder();
		for (final byte b : value.getBytes(StandardCharsets.UTF_8)) {
			final char c = (char) (b & 0xFF);
			if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
							|| c == '-' || c == '_' || c == '.' || c == '~') {
				out.append(c);
			} else {
				out.append('%').append(String.format("%02X", b & 0xFF));
			}
		}
		return out.toString();
	}

	private static byte[] hmac(final String algorithm, final byte[] key, final String data) throws Exception {
		final var mac = Mac.getInstance(algorithm);
		mac.init(new SecretKeySpec(key, algorithm));
		return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
	}

	private static String hmacBase64(final String algorithm, final String secret, final String data) throws Exception {
		return java.util.Base64.getEncoder().encodeToString(
						hmac(algorithm, secret.getBytes(StandardCharsets.UTF_8), data));
	}

	@SuppressWarnings("rawtypes")
	private S3StorageDriver loopbackDriver(final VersionedTarget target) throws Exception {
		resources.add(target);
		final Config config = S3StorageDriverTest.baseConfig(true, 4, false, null, "127.0.0.1");
		config.val("storage-driver-limit-concurrency", 1);
		config.val("storage-net-timeoutMilliSec", 5_000);
		config.val("storage-net-node-port", target.port());
		final var driver = (S3StorageDriver) new S3StorageDriverExtension().create(
						"versioned-retry", new SeedDataInput(1, 1000, 1, true), config.configVal("storage"), false, 16);
		resources.add(driver);
		driver.start();
		return driver;
	}

	/** One attempt through the driver; a retry puts the previous attempt's result back, as the engine does. */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static Operation<?> run(final S3StorageDriver driver, final Operation<?> op) throws Exception {
		final var results = new EndpointSelectionTestSupport.Results();
		driver.operationResultOutput((Output) results);
		op.reset();
		assertTrue(driver.put(op));
		final var result = results.await();
		assertNotNull(result);
		return result;
	}

	/**
	 * A loopback target with S3 version semantics for one key, {@code /b/obj}, holding {@code
	 * ver0001} (1000 bytes) and {@code ver0002} (2000 bytes). The first object request of {@code
	 * failMethod} gets a 503 without a version header.
	 */
	private static final class VersionedTarget implements AutoCloseable {

		private final HttpServer server;
		private final ExecutorService threads = Executors.newCachedThreadPool();
		private final String failMethod;
		private final AtomicInteger failures = new AtomicInteger(1);
		private final AtomicInteger sequence = new AtomicInteger(2);
		private final Map<String, Integer> versions = new LinkedHashMap<>();
		private final List<String> objectLog = new CopyOnWriteArrayList<>();

		VersionedTarget(final String failMethod) throws IOException {
			this.failMethod = failMethod;
			versions.put("ver0001", 1000);
			versions.put("ver0002", 2000);
			server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
			server.createContext("/", this::handle);
			server.setExecutor(threads);
			server.start();
		}

		int port() {
			return server.getAddress().getPort();
		}

		List<String> objectLog() {
			return List.copyOf(objectLog);
		}

		synchronized List<String> versions() {
			return List.copyOf(versions.keySet());
		}

		private synchronized void handle(final HttpExchange exchange) throws IOException {
			try (exchange) {
				exchange.getRequestBody().readAllBytes();
				final var method = exchange.getRequestMethod();
				final var path = exchange.getRequestURI().getRawPath();
				final var query = exchange.getRequestURI().getRawQuery();
				if (!"/b/obj".equals(path)) {
					// The bucket checks on first use: it exists, with versioning on.
					final byte[] body = "GET".equals(method)
									? "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"
													.getBytes(StandardCharsets.UTF_8)
									: new byte[0];
					exchange.sendResponseHeaders(200, body.length == 0 ? -1 : body.length);
					if (body.length > 0) {
						exchange.getResponseBody().write(body);
					}
					return;
				}
				final var line = method + " " + path + (query == null ? "" : "?" + query);
				if (method.equals(failMethod) && failures.getAndDecrement() > 0) {
					objectLog.add(line + " 503");
					exchange.sendResponseHeaders(503, -1);
					return;
				}
				final var versionId = query != null && query.startsWith("versionId=")
								? query.substring("versionId=".length())
								: null;
				switch (method) {
				case "GET", "HEAD" -> {
					final var selected = versionId != null ? versionId : last();
					final int size = versions.getOrDefault(selected, -1);
					if (size < 0) {
						objectLog.add(line + " 404");
						exchange.sendResponseHeaders(404, -1);
						return;
					}
					objectLog.add(line + " 200 " + size);
					exchange.getResponseHeaders().set(VERSION_HEADER, selected);
					if ("HEAD".equals(method)) {
						exchange.getResponseHeaders().set("Content-Length", Integer.toString(size));
						exchange.sendResponseHeaders(200, -1);
					} else {
						exchange.sendResponseHeaders(200, size);
						exchange.getResponseBody().write(new byte[size]);
					}
				}
				case "DELETE" -> {
					final var deleted = versionId != null ? versionId : "mark" + sequence.incrementAndGet();
					if (versionId != null) {
						versions.remove(versionId);
					} else {
						versions.put(deleted, 0);
					}
					objectLog.add(line + " 204");
					exchange.getResponseHeaders().set(VERSION_HEADER, deleted);
					exchange.sendResponseHeaders(204, -1);
				}
				default -> {
					objectLog.add(line + " 400");
					exchange.sendResponseHeaders(400, -1);
				}
				}
			}
		}

		private String last() {
			String last = null;
			for (final var key : versions.keySet()) {
				last = key;
			}
			return last;
		}

		@Override
		public void close() {
			server.stop(0);
			threads.shutdownNow();
		}
	}
}
