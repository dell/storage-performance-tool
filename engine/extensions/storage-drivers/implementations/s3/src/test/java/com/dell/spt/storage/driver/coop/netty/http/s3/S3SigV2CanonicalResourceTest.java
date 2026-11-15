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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.crypto.Mac;
import org.junit.jupiter.api.Test;

/**
 * The SigV2 canonical resource: the path plus only the query's sub-resources, sorted, with an empty
 * value signed as the bare name and a {@code versionId} value decoded. Expected canonical strings are written out by hand, and expected
 * signatures are computed here with {@link Mac} over a hand-built string to sign, never with the
 * driver's own {@code getCanonical}.
 */
class S3SigV2CanonicalResourceTest {

	private static final String UID = SigV2TestSupport.UID;
	private static final String SECRET = SigV2TestSupport.SECRET;

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

	/** As #319 signs a version ID that needs encoding: decoded, while the URI carries it encoded. */
	@Test
	void aVersionIdValueWithAPercentIsSignedDecoded() {
		assertEquals("/b/k?versionId=v+1=:x", resource("/b/k?versionId=v%2B1%3D%3Ax"));
		assertEquals("/b/k?versionId=v 1&a%b", resource("/b/k?versionId=v%201%26a%25b"));
		assertEquals("/b/k?versionId=\u00e9\u4e2d", resource("/b/k?versionId=%C3%A9%E4%B8%AD"));
		// A '+' is a plus sign, as the URI encodes a space as %20.
		assertEquals("/b/k?versionId=a+b", resource("/b/k?versionId=a+b"));
		// An unversioned name that holds a query: only its sub-resources, decoded.
		assertEquals("/b/k?versionId=a+b", resource("/b/k?x=1&versionId=a%2Bb"));
		assertEquals("/b/k?tagging&versionId=a+b", resource("/b/k?versionId=a%2Bb&tagging"));
		// A malformed escape can't be decoded: signed as sent.
		assertEquals("/b/k?versionId=50%", resource("/b/k?versionId=50%"));
		assertEquals("/b/k?versionId=a%zz", resource("/b/k?versionId=a%zz"));
		// Only versionId is decoded.
		assertEquals("/b/k?uploadId=a%41", resource("/b/k?uploadId=a%41"));
		assertEquals("/b/k?response-content-type=text%2Fplain", resource("/b/k?response-content-type=text%2Fplain"));
		assertTrue(!S3StorageDriver.isCanonicalV2Query("/b/k?versionId=a%2Bb", "/b/k".length()));
	}

	@Test
	void namesAreMatchedWholeAndCaseSensitively() {
		assertEquals("/b", resource("/b?versionsx=1&xversions&Versions&upload=1&uploadIds=2&=v&&"));
		assertEquals("/b?uploads", resource("/b?&uploads&"));
	}

	@Test
	void selectAndSelectTypeAreBothWholeNames() {
		assertEquals("/b/k?select&select-type=2", resource("/b/k?select-type=2&select"));
		assertEquals("/b/k", resource("/b/k?select-typex=2&selectx&select-"));
	}

	@Test
	void moreSubResourcesThanTheSortArrayFirstHoldsAreSortedInReverseInput() {
		// The sorting path starts with room for four; eight in reverse order make it grow.
		final var names = new ArrayList<>(List.of(
						"acl", "cors", "delete", "lifecycle", "partNumber=3", "tagging", "uploadId=U", "website"));
		final var expected = "/b/k?" + String.join("&", names);
		Collections.reverse(names);
		final var uri = "/b/k?max-keys=1&" + String.join("&", names) + "&prefix=p";
		assertEquals(expected, resource(uri));
	}

	/** Pins the whole set, so any later edit to it shows up here. */
	@Test
	void theSubResourceSetIsTheTicketListPlusBotocoreQsaOfInterest() {
		assertEquals(Set.of(
						"accelerate", "acl", "analytics", "cors", "defaultObjectAcl", "delete", "inventory",
						"lifecycle", "location", "logging", "metrics", "notification", "object-lock", "partNumber",
						"policy", "replication", "requestPayment", "response-cache-control",
						"response-content-disposition", "response-content-encoding", "response-content-language",
						"response-content-type", "response-expires", "restore", "select", "select-type",
						"storageClass", "tagging", "torrent", "uploadId", "uploads", "versionId", "versioning",
						"versions", "website"),
						S3Api.SIGV2_SUB_RESOURCES);
		for (final var name : S3Api.SIGV2_SUB_RESOURCES) {
			assertEquals("/b?" + name, resource("/b?" + name + "=&max-keys=1"), name);
		}
	}

	// ---------- the fast path against the filtering route ----------

	/** Every URI with a query that the tests in this class use, plus edge shapes. */
	private static final List<String> QUERY_URIS = List.of(
					"/b?list-type=2&prefix=p&max-keys=5&start-after=x",
					"/b?max-keys=10",
					"/b?list-type=2&prefix=p&delimiter=%2F&continuation-token=abc%3D&max-keys=1000",
					"/b?",
					"/b?versions=&prefix=p&key-marker=k&max-keys=5",
					"/b?versions&max-keys=5",
					"/b?max-keys=5&versions=",
					"/b/k?partNumber=2&uploadId=U",
					"/b/k?uploadId=U&partNumber=2",
					"/b?versions=&versioning&versionId=V",
					"/b/k?versionId=V&max-keys=1&tagging&acl=",
					"/b?tagging&acl=2&acl=1",
					"/b/k?versionId=V",
					"/b?versioning",
					"/b?delete",
					"/b/k?tagging",
					"/b/k?uploads",
					"/b/k?uploadId=U",
					"/b/k?partNumber=1&uploadId=2~abc.DEF_ghi-123",
					"/b/k?response-content-type=text%2Fplain",
					"/b?versionsx=1&xversions&Versions&upload=1&uploadIds=2&=v&&",
					"/b?&uploads&",
					"/b/k?select-type=2&select",
					"/b/k?select-typex=2&selectx&select-",
					"/b?acl=1&acl=2",
					"/b?acl=2&acl=1",
					"/b?acl&acl",
					"/b?uploads&",
					"/b?&uploads",
					"/b?uploads=",
					"/b?=",
					"/b?&",
					"/b??uploads",
					"/b?uploads?x=1",
					"/b/k?partNumber=1&uploadId=U&tenant=t1",
					"/bench?tenant=t1",
					"/bench?object-lock=",
					"/johnsmith/?prefix=photos&max-keys=50&marker=puppy",
					"/b/k?versionId=v%2B1%3D%3Ax",
					"/b/k?versionId=v%201%26a%25b",
					"/b/k?x=1&versionId=a%2Bb",
					"/b/k?versionId=a%2Bb&tagging",
					"/b/k?versionId=50%",
					"/b/k?versionId=a%zz",
					"/b/k?versionId=%FF",
					"/b/k?uploadId=a%41");

	/**
	 * An independent reference for the canonical resource, written from the SigV2 specification
	 * without the driver's scanning: split the query on {@code &}, keep the sub-resources, decode a
	 * {@code versionId} value (as sent when malformed), sort stably by name, and write an empty value
	 * as the bare name.
	 */
	private static String reference(final String uri) {
		final int q = uri.indexOf('?');
		if (q < 0) {
			return uri;
		}
		final List<String[]> subResources = new ArrayList<>();
		for (final var param : uri.substring(q + 1).split("&", -1)) {
			final int eq = param.indexOf('=');
			final var name = eq < 0 ? param : param.substring(0, eq);
			if (S3Api.SIGV2_SUB_RESOURCES.contains(name)) {
				final var value = eq < 0 ? "" : param.substring(eq + 1);
				subResources.add(new String[]{name, name.equals("versionId") ? referenceDecode(value) : value
				});
			}
		}
		subResources.sort(java.util.Comparator.comparing(pair -> pair[0]));
		final var out = new StringBuilder(uri.substring(0, q));
		for (int i = 0; i < subResources.size(); i++) {
			final var pair = subResources.get(i);
			out.append(i == 0 ? '?' : '&').append(pair[0]);
			if (!pair[1].isEmpty()) {
				out.append('=').append(pair[1]);
			}
		}
		return out.toString();
	}

	/** RFC 3986 percent-decoding to UTF-8, {@code +} kept; the value as sent if an escape is malformed. */
	private static String referenceDecode(final String value) {
		final var bytes = new java.io.ByteArrayOutputStream();
		for (int i = 0; i < value.length(); i++) {
			final char c = value.charAt(i);
			if (c != '%') {
				bytes.writeBytes(String.valueOf(c).getBytes(StandardCharsets.UTF_8));
			} else if (i + 2 < value.length() && Character.digit(value.charAt(i + 1), 16) >= 0
							&& Character.digit(value.charAt(i + 2), 16) >= 0) {
				bytes.write(Integer.parseInt(value.substring(i + 1, i + 3), 16));
				i += 2;
			} else {
				return value;
			}
		}
		return bytes.toString(StandardCharsets.UTF_8);
	}

	@Test
	void theFilteringRouteSignsWhatTheReferenceSigns() {
		for (final var uri : QUERY_URIS) {
			assertEquals(reference(uri), filtered(uri), uri);
			assertEquals(reference(uri), resource(uri), uri);
		}
	}

	private static String filtered(final String uri) {
		final var buff = new StringBuilder("prefix\n");
		S3StorageDriver.appendFilteredV2Resource(buff, uri, uri.indexOf('?'));
		return buff.substring("prefix\n".length());
	}

	@Test
	void theFastPathSignsWhatTheFilteringRouteSigns() {
		for (final var uri : QUERY_URIS) {
			assertEquals(filtered(uri), resource(uri), uri);
		}
	}

	/**
	 * Random queries from sub-resources, other names, empty, repeated and percent-encoded values, and
	 * stray separators: the fast path, the filtering route and the reference agree on each.
	 */
	@Test
	void theFastPathSignsWhatTheFilteringRouteSignsForRandomQueries() {
		final var names = List.of("acl", "delete", "partNumber", "tagging", "uploadId", "uploads", "versionId",
						"versioning", "versions", "select", "select-type", "response-expires", "list-type", "max-keys",
						"prefix", "uploadIds", "Versions", "");
		final var values = List.of(
						"", "=", "=1", "=U", "=2~abc.DEF", "=a%2Fb", "==", "=x=y", "=a%2Bb%3D", "=a+b", "=50%", "=%zz", "=%C3%A9");
		final var random = new java.util.Random(20261115L);
		int fast = 0;
		for (int n = 0; n < 50_000; n++) {
			final var uri = new StringBuilder(random.nextBoolean() ? "/b" : "/b/k");
			uri.append('?');
			final int params = random.nextInt(6);
			for (int i = 0; i < params; i++) {
				if (i > 0) {
					uri.append('&');
				}
				uri.append(names.get(random.nextInt(names.size()))).append(values.get(random.nextInt(values.size())));
			}
			if (random.nextInt(10) == 0) {
				uri.append('&');
			}
			final var text = uri.toString();
			final var expected = reference(text);
			assertEquals(expected, filtered(text), text);
			assertEquals(expected, resource(text), text);
			if (S3StorageDriver.isCanonicalV2Query(text, text.indexOf('?'))) {
				fast++;
			}
		}
		assertTrue(fast > 1_000, "the random queries reach the fast path: " + fast);
	}

	@Test
	void sptsOwnSubResourceQueriesTakeTheFastPathAndListingsDoNot() {
		for (final var uri : List.of(
						"/b/k?uploads", "/b/k?partNumber=2&uploadId=U", "/b/k?uploadId=U", "/b/k?versionId=V",
						"/b/k?tagging", "/b?delete", "/b?versioning")) {
			assertTrue(S3StorageDriver.isCanonicalV2Query(uri, uri.indexOf('?')), uri);
		}
		for (final var uri : List.of(
						"/b?list-type=2&max-keys=1000", "/b?versions=&max-keys=5", "/b/k?uploadId=U&partNumber=2",
						"/bench?tenant=t1", "/b?", "/b?uploads&")) {
			assertTrue(!S3StorageDriver.isCanonicalV2Query(uri, uri.indexOf('?')), uri);
		}
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
		assertEquals("htDYFYduRNen8P9ZfE/s9SuKy0U=", SigV2TestSupport.sign(awsSecret, stringToSign));

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
			SigV2TestSupport.assertSignedOver(first, "/tb");
			final var second = drv.log().poll();
			assertEquals("/tb?list-type=2&start-after=k0001&max-keys=100", second.uri());
			SigV2TestSupport.assertSignedOver(second, "/tb");
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
			SigV2TestSupport.assertSignedOver(request, "/tb?versions");
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
			SigV2TestSupport.assertSignedOver(request, "/tb");
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
			SigV2TestSupport.assertSignedOver(objectsRequest, "/tb");

			final var versions = new ListOperationImpl<PathItemImpl>(0, OpType.LIST, new PathItemImpl("/tb/p"), credential);
			versions.options(ListOptions.builder().includeVersions(true).maxKeys(5).build());
			final var versionsRequest = (FullHttpRequest) drv.httpRequest((Operation) versions, "127.0.0.1");
			assertTrue(versionsRequest.uri().startsWith("/tb?versions=&"), versionsRequest.uri());
			SigV2TestSupport.assertSignedOver(versionsRequest, "/tb?versions");
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
				SigV2TestSupport.assertSignedOver(request, expectedPath);
			}
		} finally {
			drv.close();
		}
	}

	@Test
	void aBucketHeadSignsABotocoreSubResourceFromUriArguments() throws Exception {
		final Config config = S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1");
		config.val("storage-net-http-uri-args", Map.of("object-lock", ""));
		final var drv = new S3StorageDriverTest.TestS3Driver(config);
		try {
			drv.checkTargetBucket("bench", null);

			final var request = drv.log().poll();
			assertEquals("/bench?object-lock=", request.uri());
			SigV2TestSupport.assertSignedOver(request, "/bench?object-lock");
		} finally {
			drv.close();
		}
	}

	// ---------- helpers ----------

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
