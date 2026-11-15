package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactoryImpl;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.base.storage.driver.ListOptions;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

/**
 * The SigV2 canonical resource: the path plus only the query's sub-resources, sorted, with an empty
 * value signed as the bare name. Expected canonical strings are written out by hand, and expected
 * signatures are computed here with {@link Mac} over a hand-built string to sign, never with the
 * driver's own {@code getCanonical}.
 */
class S3SigV2CanonicalResourceTest {

	private static final String UID = "user1";
	// The secret S3StorageDriverTest.baseConfig configures.
	private static final String SECRET = "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb";

	private static String resource(final String uri) {
		final var buff = new StringBuilder("prefix\n");
		S3StorageDriver.appendV2CanonicalResource(buff, uri);
		final var text = buff.toString();
		assertTrue(text.startsWith("prefix\n"), "the method only appends: " + text);
		return text.substring("prefix\n".length());
	}

	// ---------- the canonical resource ----------

	@Test
	void listingParametersAreNotSigned() {
		assertEquals("/b", resource("/b?list-type=2&prefix=p&max-keys=5&start-after=x"));
		assertEquals("/b", resource("/b?max-keys=10"));
		assertEquals("/b", resource("/b?list-type=2&prefix=p&delimiter=%2F&continuation-token=abc%3D&max-keys=1000"));
		assertEquals("/b", resource("/b?"));
	}

	@Test
	void anEmptyValueSignsAsTheBareName() {
		// SPT's real versions-listing URI: appendInitialQuery sends "versions=".
		assertEquals("/b?versions", resource("/b?versions=&prefix=p&key-marker=k&max-keys=5"));
		assertEquals("/b?versions", resource("/b?versions&max-keys=5"));
		assertEquals("/b?versions", resource("/b?max-keys=5&versions="));
	}

	@Test
	void subResourcesAreSortedByName() {
		assertEquals("/b/k?partNumber=2&uploadId=U", resource("/b/k?partNumber=2&uploadId=U"));
		assertEquals("/b/k?partNumber=2&uploadId=U", resource("/b/k?uploadId=U&partNumber=2"));
		// String.compareTo order: versionId < versioning < versions.
		assertEquals("/b?versionId=V&versioning&versions", resource("/b?versions=&versioning&versionId=V"));
		assertEquals("/b/k?acl&tagging&versionId=V", resource("/b/k?versionId=V&max-keys=1&tagging&acl="));
		// Equal names keep their order in the query.
		assertEquals("/b?acl=2&acl=1&tagging", resource("/b?tagging&acl=2&acl=1"));
	}

	@Test
	void requestsThatAlreadySignCorrectlyAreUnchanged() {
		for (final var uri : List.of(
						"/b/k?versionId=V",
						"/b?versioning",
						"/b?delete",
						"/b/k?tagging",
						"/b/k?uploads",
						"/b/k?uploadId=U",
						"/b/k?partNumber=1&uploadId=2~abc.DEF_ghi-123",
						"/b/k?response-content-type=text%2Fplain")) {
			assertEquals(uri, resource(uri), uri);
		}
	}

	@Test
	void aUriWithNoQueryIsAppendedUnchanged() {
		assertEquals("/bucket/objectkey", resource("/bucket/objectkey"));
		assertEquals("/b/", resource("/b/"));
		assertEquals("/", resource("/"));
		assertEquals("", resource(""));
	}

	@Test
	void namesAreMatchedWholeAndCaseSensitively() {
		assertEquals("/b", resource("/b?versionsx=1&xversions&Versions&upload=1&uploadIds=2&=v&&"));
		assertEquals("/b?uploads", resource("/b?&uploads&"));
	}

	// ---------- the whole string to sign ----------

	@Test
	void theStringToSignOfAListingEndsInTheBarePath() throws Exception {
		final var drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
		try {
			final var headers = new DefaultHttpHeaders();
			headers.set(HttpHeaderNames.DATE, "Tue, 27 Mar 2007 19:42:41 +0000");
			headers.set("x-amz-meta-b", "2");
			headers.set("x-amz-meta-a", "1");
			assertEquals(
							"GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\nx-amz-meta-a:1\nx-amz-meta-b:2\n/b",
							drv.getCanonical(headers, HttpMethod.GET, "/b?list-type=2&max-keys=1000"));
			assertEquals(
							"PUT\n\n\nTue, 27 Mar 2007 19:42:41 +0000\nx-amz-meta-a:1\nx-amz-meta-b:2\n/b/k?partNumber=2&uploadId=U",
							drv.getCanonical(headers, HttpMethod.PUT, "/b/k?uploadId=U&partNumber=2"));
		} finally {
			drv.close();
		}
	}

	/**
	 * A cross-check, not the primary proof (the canonical-string tests above are). The vector is the
	 * ListObjects example of the AWS SigV2 REST authentication documentation ("Signing and
	 * authenticating REST requests", with AWS's published example key), as quoted in the SPT ticket
	 * {@code .scratch/s3-v2-list-signature} 01; it was recomputed there but not checked against the
	 * AWS page. The expected signature here is computed with {@link Mac} over a hand-built string to
	 * sign and compared with the published value, and the driver must produce the same bytes.
	 */
	@Test
	void theAwsListObjectsExampleSignsTheBucketOnly() throws Exception {
		final var awsSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
		final var date = "Tue, 27 Mar 2007 19:42:41 +0000";
		final var stringToSign = "GET\n\n\n" + date + "\n/johnsmith/";
		assertEquals("htDYFYduRNen8P9ZfE/s9SuKy0U=", sign(awsSecret, stringToSign));

		final var drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
		try {
			final var headers = new DefaultHttpHeaders();
			headers.set(HttpHeaderNames.DATE, date);
			assertEquals(stringToSign,
							drv.getCanonical(headers, HttpMethod.GET, "/johnsmith/?prefix=photos&max-keys=50&marker=puppy"));
		} finally {
			drv.close();
		}
	}

	// ---------- the signed requests ----------

	@Test
	void theItemInputListingSignsTheBucketOnEveryPage() throws Exception {
		final var drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
		try {
			drv.enqueueResponse(listPage(true, "k0000", "k0001"));
			drv.enqueueResponse(listPage(false, "k0002"));
			final var factory = new ItemFactoryImpl<Item>();
			final var firstPage = drv.list(factory, "/tb", null, 36, null, 100, ListOptions.DEFAULT);
			drv.list(factory, "/tb", null, 36, firstPage.get(firstPage.size() - 1), 100, ListOptions.DEFAULT);

			final var first = drv.log().poll();
			assertEquals("/tb?list-type=2&max-keys=100", first.uri());
			assertSignedOver(first, "/tb");
			final var second = drv.log().poll();
			assertEquals("/tb?list-type=2&start-after=k0001&max-keys=100", second.uri());
			assertSignedOver(second, "/tb");
		} finally {
			drv.close();
		}
	}

	@Test
	void theVersionsListingSignsTheVersionsSubResourceBare() throws Exception {
		final var drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
		try {
			drv.enqueueResponse(listPage(false, "p0000"));
			drv.list(new ItemFactoryImpl<Item>(), "/tb", "p", 36, null, 5,
							ListOptions.builder().includeVersions(true).keyMarker("k").build());

			final var request = drv.log().poll();
			assertEquals("/tb?versions=&prefix=p&key-marker=k&max-keys=5", request.uri());
			assertSignedOver(request, "/tb?versions");
		} finally {
			drv.close();
		}
	}

	@Test
	void theDelimiterProbeSignsTheBucket() throws Exception {
		final var drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
		try {
			drv.enqueueResponse(ok("<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>"));
			drv.probeCommonPrefixes("/tb", "p/", "/", 10);

			final var request = drv.log().poll();
			assertTrue(request.uri().startsWith("/tb?list-type=2&"), request.uri());
			assertSignedOver(request, "/tb");
		} finally {
			drv.close();
		}
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void theListStepSignsTheBucketAndTheVersionsSubResource() throws Exception {
		final var drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
		try {
			final var credential = Credential.getInstance(UID, SECRET);
			final var objects = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("/tb/p"), credential);
			objects.options(ListOptions.builder().maxKeys(5).startAfter("x").build());
			final var objectsRequest = (FullHttpRequest) drv.httpRequest((Operation) objects, "127.0.0.1");
			assertTrue(objectsRequest.uri().startsWith("/tb?list-type=2&"), objectsRequest.uri());
			assertSignedOver(objectsRequest, "/tb");

			final var versions = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("/tb/p"), credential);
			versions.options(ListOptions.builder().includeVersions(true).maxKeys(5).build());
			final var versionsRequest = (FullHttpRequest) drv.httpRequest((Operation) versions, "127.0.0.1");
			assertTrue(versionsRequest.uri().startsWith("/tb?versions=&"), versionsRequest.uri());
			assertSignedOver(versionsRequest, "/tb?versions");
		} finally {
			drv.close();
		}
	}

	@Test
	void bucketHeadAndCreateWithUriArgumentsSignTheBarePath() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1");
		config.val("storage-net-http-uri-args", Map.of("tenant", "t1"));
		final var drv = new S3StorageDriverTest.TestS3Driver(config);
		try {
			drv.checkTargetBucket("bench", null);
			drv.createTargetBucket("bench", null);
			drv.requestNewPath("/bench2/p");

			for (final var expectedPath : List.of("/bench", "/bench", "/bench2")) {
				final var request = drv.log().poll();
				assertNotNull(request);
				assertEquals(expectedPath + "?tenant=t1", request.uri());
				assertSignedOver(request, expectedPath);
			}
		} finally {
			drv.close();
		}
	}

	// ---------- helpers ----------

	/** Asserts the request's V2 signature covers its headers and exactly {@code resource}. */
	private static void assertSignedOver(final FullHttpRequest request, final String resource) throws Exception {
		final var headers = request.headers();
		for (final var header : headers) {
			assertTrue(!header.getKey().toLowerCase(Locale.ROOT).startsWith("x-amz-"),
							"this check builds the string to sign with no x-amz- headers: " + header.getKey());
		}
		final var date = headers.get(HttpHeaderNames.DATE);
		assertNotNull(date, "a V2 request carries a Date");
		final var stringToSign = request.method().name()
						+ '\n' + nullToEmpty(headers.get(HttpHeaderNames.CONTENT_MD5))
						+ '\n' + nullToEmpty(headers.get(HttpHeaderNames.CONTENT_TYPE))
						+ '\n' + date
						+ '\n' + resource;
		assertEquals("AWS " + UID + ':' + sign(SECRET, stringToSign), headers.get(HttpHeaderNames.AUTHORIZATION),
						request.method() + " " + request.uri() + " signs over " + resource);
	}

	private static String nullToEmpty(final String value) {
		return value == null ? "" : value;
	}

	private static String sign(final String secret, final String stringToSign) throws Exception {
		final var mac = Mac.getInstance("HmacSHA1");
		mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
		return Base64.getEncoder().encodeToString(mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8)));
	}

	private static DefaultFullHttpResponse listPage(final boolean truncated, final String... keys) {
		final var xml = new StringBuilder("<ListBucketResult><IsTruncated>").append(truncated).append("</IsTruncated>");
		for (final var key : keys) {
			xml.append("<Contents><Key>").append(key).append("</Key><Size>1</Size></Contents>");
		}
		return ok(xml.append("</ListBucketResult>").toString());
	}

	private static DefaultFullHttpResponse ok(final String body) {
		return new DefaultFullHttpResponse(
						HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
	}
}
