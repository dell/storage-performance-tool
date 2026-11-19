package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.logging.OperationTraceRecord;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestEncoder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * An object key with {@code ?}, {@code #} or a space is sent with that character percent-encoded
 * ({@code %3F}, {@code %23}, {@code %20}) in every request path the {@code s3} driver builds, and the
 * path is signed as sent under V4 and V2. Sent raw, a {@code ?} made the target read the rest of the
 * key as a query and act on another object. Every other character is sent as before, and the item
 * name, the operation trace and the multipart record keep the literal key.
 */
final class S3KeyReservedCharsTest {

	private static final Credential CREDENTIAL = Credential.getInstance(
					SigV2TestSupport.UID, SigV2TestSupport.SECRET);
	private static final String HOST = "127.0.0.1:9024";

	/** Keys that hold a character the path must not carry raw, and the key as the path carries it. */
	private static final Map<String, String> ENCODED_KEYS = orderedMap(
					"a?b", "a%3Fb",
					"weird?x=1", "weird%3Fx=1",
					"k?versionId=abc", "k%3FversionId=abc",
					"hash#frag", "hash%23frag",
					"my photo.jpg", "my%20photo.jpg",
					"dir/a b?c#d", "dir/a%20b%3Fc%23d");

	/** Keys sent exactly as before: pre-encoded ones, other reserved characters, a generated name. */
	private static final List<String> UNCHANGED_KEYS = List.of(
					"a%2Bb", "dt%3D1/x%20y", "plus+p", "bang!", "ldypc1g3fjmr", "run~1/k", "é");

	private final List<AutoCloseable> resources = new ArrayList<>();

	@AfterEach
	void tearDown() throws Exception {
		for (final var resource : resources.reversed()) {
			resource.close();
		}
	}

	// ---------- the encoder ----------

	@Test
	void onlyTheQueryFragmentAndSpaceCharactersAreEncoded() {
		ENCODED_KEYS.forEach((key, encoded) -> assertEquals(
						"/b/" + encoded, S3StorageDriver.encodeKeyQueryChars("/b/" + key), key));
		assertEquals("%3F%23%20", S3StorageDriver.encodeKeyQueryChars("?# "));
		// whichever of the three comes first starts the encoding, and each later one is still encoded
		assertEquals("x%23y%3Fz%20", S3StorageDriver.encodeKeyQueryChars("x#y?z "));
		assertEquals("x%20y%23z%3F", S3StorageDriver.encodeKeyQueryChars("x y#z?"));
		assertEquals("%20", S3StorageDriver.encodeKeyQueryChars(" "));
		assertEquals("a%23", S3StorageDriver.encodeKeyQueryChars("a#"));
		assertEquals("", S3StorageDriver.encodeKeyQueryChars(""));
	}

	@Test
	void aPathWithNothingToEncodeIsReturnedItself() {
		for (final var key : UNCHANGED_KEYS) {
			final String path = "/b/" + key;
			assertSame(path, S3StorageDriver.encodeKeyQueryChars(path), key);
		}
	}

	// ---------- ordinary object requests ----------

	@Test
	void everyObjectRequestCarriesTheEncodedKeyAndSignsItUnderV4() throws Exception {
		final var driver = driver(4, false, false);
		final var headDriver = driver(4, false, true);
		for (final var entry : ENCODED_KEYS.entrySet()) {
			for (final var request : objectRequests(driver, headDriver, entry.getKey()).entrySet()) {
				final var what = request.getKey() + " of " + entry.getKey();
				assertEquals("/b/" + entry.getValue(), request.getValue().uri(), what);
				assertEquals("", S3UriArgsSigningTest.verifySigV4(request.getValue()),
								what + ": no query, so no versionId or other sub-resource is signed");
			}
		}
	}

	@Test
	void everyObjectRequestCarriesTheEncodedKeyAndSignsItUnderV2() throws Exception {
		final var driver = driver(2, false, false);
		final var headDriver = driver(2, false, true);
		for (final var entry : ENCODED_KEYS.entrySet()) {
			for (final var request : objectRequests(driver, headDriver, entry.getKey()).entrySet()) {
				final var wire = "/b/" + entry.getValue();
				assertEquals(wire, request.getValue().uri(), request.getKey() + " of " + entry.getKey());
				// The canonical resource is the path as sent: with no ?, no sub-resource is signed.
				SigV2TestSupport.assertSignedOver(request.getValue(), wire);
			}
		}
	}

	@Test
	void keysWithNothingToEncodeSendTheSameRequestAsBefore() throws Exception {
		for (final int authVersion : List.of(2, 4)) {
			final var driver = driver(authVersion, false, false);
			final var headDriver = driver(authVersion, false, true);
			for (final var key : UNCHANGED_KEYS) {
				for (final var request : objectRequests(driver, headDriver, key).entrySet()) {
					final var what = "V" + authVersion + " " + request.getKey() + " of " + key;
					assertEquals("/b/" + key, request.getValue().uri(), what);
					if (authVersion == 2) {
						SigV2TestSupport.assertSignedOver(request.getValue(), "/b/" + key);
					}
				}
				final var op = op(OpType.READ, key);
				final String itemPath = driver.dataUriPathOf(op);
				assertSame(itemPath, driver.requestPath(itemPath), key);
			}
		}
	}

	@Test
	void aSpaceKeyIsWrittenToTheWireEncoded() throws Exception {
		final var driver = driver(4, false, false);
		for (final var type : List.of(OpType.CREATE, OpType.READ, OpType.DELETE)) {
			final var request = driver.httpRequest(op(type, "my photo.jpg"), HOST);
			final var channel = new EmbeddedChannel(new HttpRequestEncoder());
			try {
				assertTrue(channel.writeOutbound(request));
				final ByteBuf wire = channel.readOutbound();
				try {
					final String head = wire.toString(StandardCharsets.US_ASCII);
					assertTrue(head.startsWith(request.method() + " /b/my%20photo.jpg HTTP/1.1\r\n"), head);
				} finally {
					wire.release();
				}
			} finally {
				channel.finishAndReleaseAll();
			}
		}
	}

	// ---------- requests with SPT's own query ----------

	@Test
	void multipartRequestsCarryTheEncodedKeyBeforeTheirQuery() throws Exception {
		for (final int authVersion : List.of(2, 4)) {
			final var driver = driver(authVersion, false, false);
			final var item = new DataItemImpl("a?b", 0, 2048);
			item.dataInput(dataInput());
			final var upload = new CompositeDataOperationImpl<DataItem>(
							0, OpType.CREATE, item, null, "/b", CREDENTIAL, null, 0, 1024);
			@SuppressWarnings("unchecked")
			final var init = driver.initMultipartUploadRequest((Operation<Item>) (Operation<?>) upload, HOST);
			upload.put(S3Api.KEY_UPLOAD_ID, "u1");
			upload.put("1", "\"e1\"");
			upload.put("2", "\"e2\"");
			final var part = driver.partUploadRequest(
							new PartialDataOperationImpl<>(0, OpType.CREATE, item.slice(0, 1024), null, "/b", CREDENTIAL, 0, upload),
							HOST);
			final var complete = driver.completeMultipartUploadRequest(upload, HOST);
			final var abort = driver.abortMultipartUploadRequest(upload, HOST);
			try {
				final Map<HttpRequest, String> expected = new LinkedHashMap<>();
				expected.put(init, "/b/a%3Fb?uploads");
				expected.put(part, "/b/a%3Fb?partNumber=1&uploadId=u1");
				expected.put(complete, "/b/a%3Fb?uploadId=u1");
				expected.put(abort, "/b/a%3Fb?uploadId=u1");
				for (final var request : expected.entrySet()) {
					assertEquals(request.getValue(), request.getKey().uri(), "V" + authVersion + " " + request.getKey().method());
					assertSignedAsSent(authVersion, request.getKey());
				}
			} finally {
				complete.release();
			}
			assertEquals("a?b", S3StorageDriver.multipartKey(driver.dataUriPathOf(upload), "b"),
							"the multipart record keeps the literal key");
		}
	}

	@Test
	void aRangePartReadCarriesTheEncodedKey() throws Exception {
		final var driver = driver(4, false, false);
		final var item = new DataItemImpl("a?b c", 0, 2048);
		item.dataInput(dataInput());
		final var read = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, "/b", null, CREDENTIAL, null, 0, 1024);
		final var partOp = new PartialDataOperationImpl<>(0, OpType.READ, item.slice(0, 1024), "/b", null, CREDENTIAL, 0, read);

		final var request = driver.rangeReadRequest(partOp, HOST);

		assertEquals("/b/a%3Fb%20c", request.uri());
		assertEquals("bytes=0-1023", request.headers().get("Range"));
		assertEquals("", S3UriArgsSigningTest.verifySigV4(request));
	}

	@Test
	void aTaggingRequestCarriesTheEncodedKey() throws Exception {
		final var driver = driver(4, false, false);
		@SuppressWarnings("unchecked")
		final var request = driver.objectTaggingRequest(
						(Operation<Item>) (Operation<?>) dataOp(OpType.READ, "hash#frag", "/b", null), HOST);
		try {
			assertEquals(HttpMethod.GET, request.method());
			assertEquals("/b/hash%23frag?tagging", request.uri());
			assertEquals("tagging=", S3UriArgsSigningTest.verifySigV4(request));
		} finally {
			request.release();
		}
	}

	@Test
	void aCopyEncodesTheSourceKeyAndKeepsTheLiteralSource() throws Exception {
		for (final int authVersion : List.of(2, 4)) {
			final var driver = driver(authVersion, false, false);
			final var op = dataOp(OpType.CREATE, "a?b", "/src", "/dst");
			@SuppressWarnings("unchecked")
			final var request = driver.httpRequest((Operation<Item>) (Operation<?>) op, HOST);

			assertEquals("/dst/a%3Fb", request.uri());
			assertEquals("/src/a%3Fb", request.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
			assertEquals("/src/a?b", op.copySource(), "the kept source is the literal path");
			assertSignedAsSent(authVersion, request);
		}
	}

	// ---------- versioned requests ----------

	@Test
	void versionedRequestsEncodeTheKeyAndKeepTheVersionQuery() throws Exception {
		for (final int authVersion : List.of(2, 4)) {
			final var driver = driver(authVersion, true, false);
			final Map<String, HttpRequest> requests = new LinkedHashMap<>();
			final Map<String, String> expected = new LinkedHashMap<>();
			for (final var type : List.of(OpType.READ, OpType.STAT, OpType.DELETE)) {
				final var legacy = op(type, "a?b~v1");
				requests.put("legacy " + type, driver.httpRequest(legacy, HOST));
				expected.put("legacy " + type, "/b/a%3Fb?versionId=v1");
				assertEquals("a?b~v1", legacy.item().name(), "the item keeps its name");
				final var explicit = op(type, "a?b");
				explicit.requestedVersionId("v1");
				requests.put("explicit " + type, driver.httpRequest(explicit, HOST));
				expected.put("explicit " + type, "/b/a%3Fb?versionId=v1");
			}
			requests.put("legacy CREATE", driver.httpRequest(op(OpType.CREATE, "a b#c~v1"), HOST));
			expected.put("legacy CREATE", "/b/a%20b%23c");
			requests.put("unversioned READ", driver.httpRequest(op(OpType.READ, "k?versionId=abc"), HOST));
			expected.put("unversioned READ", "/b/k%3FversionId=abc");
			requests.put("encoded version", driver.httpRequest(op(OpType.READ, "a b~v+1"), HOST));
			expected.put("encoded version", "/b/a%20b?versionId=v%2B1");
			for (final var request : requests.entrySet()) {
				final var what = "V" + authVersion + " " + request.getKey();
				assertEquals(expected.get(request.getKey()), request.getValue().uri(), what);
				if (authVersion == 4) {
					S3UriArgsSigningTest.verifySigV4(request.getValue());
				} else {
					// V2 signs a versionId value decoded.
					SigV2TestSupport.assertSignedOver(
									request.getValue(), request.getValue().uri().replace("v%2B1", "v+1"));
				}
			}
		}
	}

	@Test
	void aVersionedKeyWithNothingToEncodeIsSentAsBefore() throws Exception {
		final var driver = driver(4, true, false);
		assertEquals("/b/key?versionId=v1", driver.httpRequest(op(OpType.READ, "key~v1"), HOST).uri());
		assertEquals("/b/key", driver.httpRequest(op(OpType.CREATE, "key~v1"), HOST).uri());
		assertEquals("/b/key", driver.httpRequest(op(OpType.READ, "key"), HOST).uri());
	}

	// ---------- records keep the literal key ----------

	@Test
	void theTraceRowNamesTheLiteralKey() throws Exception {
		final var driver = driver(4, false, false);
		final var item = new IntegrityManifestDataItem("bucket-a", "dir/a?b #c", 16, "");
		item.dataInput(dataInput());
		final var op = new DataOperationImpl<>(0, OpType.READ, item, null, null, CREDENTIAL, null, 0);
		@SuppressWarnings("unchecked")
		final var request = driver.httpRequest((Operation<Item>) (Operation<?>) op, HOST);

		assertEquals("/bucket-a/dir/a%3Fb%20%23c", request.uri());
		final var row = new StringBuilder();
		new OperationTraceRecord<>(op.result()).format(row);
		assertEquals("/bucket-a/dir/a?b #c", row.toString().split(",", -1)[1]);
	}

	// ---------- helpers ----------

	/** PUT, GET, HEAD (metadata-only READ and STAT) and DELETE of {@code key} under {@code /b}. */
	private static Map<String, HttpRequest> objectRequests(
					final KeyDriver driver, final KeyDriver headDriver, final String key) throws Exception {
		final Map<String, HttpRequest> requests = new LinkedHashMap<>();
		requests.put("PUT", driver.httpRequest(op(OpType.CREATE, key), HOST));
		requests.put("GET", driver.httpRequest(op(OpType.READ, key), HOST));
		requests.put("HEAD", headDriver.httpRequest(op(OpType.READ, key), HOST));
		requests.put("STAT", driver.httpRequest(op(OpType.STAT, key), HOST));
		requests.put("DELETE", driver.httpRequest(op(OpType.DELETE, key), HOST));
		assertEquals(HttpMethod.PUT, requests.get("PUT").method());
		assertEquals(HttpMethod.GET, requests.get("GET").method());
		assertEquals(HttpMethod.HEAD, requests.get("HEAD").method());
		assertEquals(HttpMethod.HEAD, requests.get("STAT").method());
		assertEquals(HttpMethod.DELETE, requests.get("DELETE").method());
		return requests;
	}

	private static void assertSignedAsSent(final int authVersion, final HttpRequest request) throws Exception {
		if (authVersion == 4) {
			S3UriArgsSigningTest.verifySigV4(request);
		} else {
			SigV2TestSupport.assertSignedOver(request, request.uri());
		}
	}

	private KeyDriver driver(final int authVersion, final boolean versioning, final boolean readMetadataOnly)
					throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(versioning, authVersion, false, null, HOST);
		config.val("storage-net-http-read-metadata-only", readMetadataOnly);
		final var driver = new KeyDriver(config);
		resources.add(driver);
		return driver;
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> op(final OpType type, final String key) throws IOException {
		// A CREATE writes under its output path; any other type takes the path from the item's name, as
		// an items file gives it, and splits the name at its last /.
		final var op = OpType.CREATE.equals(type) ? dataOp(type, key, null, "/b") : dataOp(type, "/b/" + key, null, null);
		if (OpType.UPDATE.equals(type)) {
			op.markRandomRanges(1);
		}
		return (Operation<Item>) (Operation<?>) op;
	}

	private static DataOperationImpl<DataItem> dataOp(
					final OpType type, final String key, final String srcPath, final String dstPath) throws IOException {
		final var item = new DataItemImpl(key, 0, 16);
		item.dataInput(dataInput());
		return new DataOperationImpl<>(0, type, item, srcPath, dstPath, CREDENTIAL, null, 0);
	}

	private static DataInput dataInput() throws IOException {
		return DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
	}

	private static Map<String, String> orderedMap(final String... keyValues) {
		final Map<String, String> map = new LinkedHashMap<>();
		for (int i = 0; i < keyValues.length; i += 2) {
			map.put(keyValues[i], keyValues[i + 1]);
		}
		return map;
	}

	/** The test driver, with the data URI path its requests are built from. */
	private static final class KeyDriver extends S3StorageDriverTest.TestS3Driver {

		KeyDriver(final Config config) throws Exception {
			super(config);
		}

		String dataUriPathOf(final Operation<?> op) {
			return dataUriPath((Item) op.item(), op.srcPath(), op.dstPath(), op.type());
		}
	}
}
