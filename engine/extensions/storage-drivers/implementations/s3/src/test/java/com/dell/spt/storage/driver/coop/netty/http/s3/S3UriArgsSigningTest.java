package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

/**
 * Data requests with {@code storage-net-http-uri-args} sign the query they send under SigV4, sorted by
 * name like botocore, and keep the bare path under V2.
 */
class S3UriArgsSigningTest {

	private static final Credential CRED = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final String NODE = "127.0.0.1:9024";
	private static final Pattern AUTH_V4 = Pattern.compile(
					"AWS4-HMAC-SHA256 Credential=([^/]+)/(\\d{8})/([^/]*)/([^/]+)/aws4_request, "
									+ "SignedHeaders=([^,]+), Signature=([0-9a-f]{64})");

	// ---------- V4: the sent query is signed ----------

	@Test
	void v4PutAndGetWithOneUriArgSignTheQueryTheySend() throws Exception {
		final var drv = driver(4, args("tri", "probe"));
		for (final var opType : List.of(OpType.CREATE, OpType.READ)) {
			final var req = drv.httpRequest(dataOp(opType), NODE);
			assertEquals("/bucket/obj1?tri=probe", req.uri());
			assertEquals("tri=probe", verifySigV4(req));
		}
	}

	@Test
	void v4TwoUriArgsInUnsortedOrderAreSignedSorted() throws Exception {
		final var drv = driver(4, args("zeta", "1", "alpha", "2"));
		final var req = drv.httpRequest(dataOp(OpType.CREATE), NODE);
		assertEquals("/bucket/obj1?zeta=1&alpha=2", req.uri());
		assertEquals("alpha=2&zeta=1", verifySigV4(req));
	}

	@Test
	void v4PrefixRelatedNamesSortByName() throws Exception {
		final var tenant = driver(4, args("tenant", "t1", "tenant-id", "7")).httpRequest(dataOp(OpType.READ), NODE);
		assertEquals("tenant=t1&tenant-id=7", verifySigV4(tenant));
		final var reversed = driver(4, args("tenant-id", "7", "tenant", "t1")).httpRequest(dataOp(OpType.READ), NODE);
		assertEquals("tenant=t1&tenant-id=7", verifySigV4(reversed));
		final var digit = driver(4, args("a1", "2", "a", "1")).httpRequest(dataOp(OpType.CREATE), NODE);
		assertEquals("a=1&a1=2", verifySigV4(digit));
	}

	@Test
	void canonicalQuerySortsByNameThenValue() throws Exception {
		assertEquals("tenant=t1&tenant-id=7", canonicalQuery("tenant-id=7&tenant=t1"));
		assertEquals("a=1&a1=2", canonicalQuery("a1=2&a=1"));
		assertEquals("a=1&a%2Fc=3&a.b=2", canonicalQuery("a.b=2&a%2Fc=3&a=1"));
		assertEquals("k=1&k=2&k2=0", canonicalQuery("k2=0&k=2&k=1"));
	}

	@Test
	void canonicalPairOrderMatchesSortByEncodedNameThenValue() {
		final Comparator<String> order = S3StorageDriver.CANONICAL_PAIR_ORDER;
		assertTrue(order.compare("a=1", "a1=2") < 0);
		assertTrue(order.compare("tenant=t1", "tenant-id=7") < 0);
		assertTrue(order.compare("k=1", "k=12") < 0);
		assertEquals(0, order.compare("k=1", "k=1"));
		// Encoded atoms on both sides of '=' (0x3D): '%' 0x25, '-' '.' '0'-'9' below it; 'A' '_' 'a' '~' above.
		final String[] atoms = {"-", ".", "0", "5", "9", "A", "_", "a", "~", "%2F", "%3D", "%20"
		};
		final Random random = new Random(0x5167_4a43L);
		for (int c = 0; c < 10_000; c++) {
			final String stem = randomAtoms(random, atoms, 1 + random.nextInt(2));
			final int count = 2 + random.nextInt(5);
			final List<String[]> pairs = new ArrayList<>();
			final List<String> joined = new ArrayList<>();
			for (int i = 0; i < count; i++) {
				// Names extend one stem, so many are prefixes of one another.
				final String name = stem + randomAtoms(random, atoms, random.nextInt(3));
				final String value = randomAtoms(random, atoms, random.nextInt(4));
				pairs.add(new String[]{name, value
				});
				joined.add(name + '=' + value);
			}
			pairs.sort(Comparator.<String[], String> comparing(pair -> pair[0]).thenComparing(pair -> pair[1]));
			final List<String> expected = new ArrayList<>();
			for (final var pair : pairs) {
				expected.add(pair[0] + '=' + pair[1]);
			}
			joined.sort(order);
			assertEquals(expected, joined, "case " + c);
		}
	}

	private static String randomAtoms(final Random random, final String[] atoms, final int count) {
		final var out = new StringBuilder();
		for (int i = 0; i < count; i++) {
			out.append(atoms[random.nextInt(atoms.length)]);
		}
		return out.toString();
	}

	@Test
	void v4ExpressionValuedUriArgIsSignedWithTheValueSent() throws Exception {
		final var drv = driver(4, args("seq", "${rnd.nextInt(1000000)}"));
		final Set<String> sent = new HashSet<>();
		for (int i = 0; i < 8; i++) {
			final var req = drv.httpRequest(dataOp(OpType.CREATE), NODE);
			final var query = req.uri().substring(req.uri().indexOf('?') + 1);
			assertTrue(query.matches("seq=\\d+"), query);
			assertEquals(query, verifySigV4(req));
			sent.add(query);
		}
		assertTrue(sent.size() > 1, "the expression should yield different values: " + sent);
	}

	@Test
	void v4UriArgValueThatNeedsEncodingIsSignedEncoded() throws Exception {
		final var req = driver(4, args("path", "x/y")).httpRequest(dataOp(OpType.READ), NODE);
		assertEquals("/bucket/obj1?path=x/y", req.uri());
		assertEquals("path=x%2Fy", verifySigV4(req));
	}

	@Test
	void v4WithoutUriArgsSignsThePathItSends() throws Exception {
		final var drv = driver(4, Map.of());
		assertSame("/bucket/obj1", drv.authUri("/bucket/obj1", "/bucket/obj1"));
		for (final var opType : List.of(OpType.CREATE, OpType.READ)) {
			final var req = drv.httpRequest(dataOp(opType), NODE);
			assertEquals("/bucket/obj1", req.uri());
			assertEquals("", verifySigV4(req));
		}
	}

	// ---------- V2: byte-identical, the bare path is signed ----------

	@Test
	void v2SignsTheBarePathWithAndWithoutUriArgs() throws Exception {
		final var plain = driver(2, Map.of());
		final var withArg = driver(2, args("tri", "probe"));
		assertEquals("/bucket/obj1", withArg.authUri("/bucket/obj1", "/bucket/obj1?tri=probe"));
		for (final var opType : List.of(OpType.CREATE, OpType.READ)) {
			for (final var drv : List.of(plain, withArg)) {
				final var req = drv.httpRequest(dataOp(opType), NODE);
				final var headers = new DefaultHttpHeaders().add(req.headers());
				headers.remove(HttpHeaderNames.AUTHORIZATION);
				drv.applyAuthHeaders(headers, req.method(), "/bucket/obj1", CRED);
				assertEquals(headers.get(HttpHeaderNames.AUTHORIZATION), req.headers().get(HttpHeaderNames.AUTHORIZATION));
			}
		}
	}

	// ---------- SPT's own parameter sets keep their canonical bytes ----------

	@Test
	void sptOwnQueriesCanonicalizeAsBefore() throws Exception {
		final var queries = List.of(
						"list-type=2&prefix=dir%2Fsub&delimiter=%2F&max-keys=1000",
						"list-type=2&prefix=p&delimiter=%2F&continuation-token=1%2Fabc%3D%3D&max-keys=1000",
						"list-type=2&prefix=p&start-after=p%2Fk0&max-keys=1",
						"versions&prefix=p&delimiter=%2F&key-marker=k&version-id-marker=v1&max-keys=1000",
						"versions&prefix=p&max-keys=1000&key-marker=k&version-id-marker=v1",
						"list-type=2&prefix=p&delimiter=%2F&max-keys=1000&start-after=s&continuation-token=t",
						"uploads",
						"partNumber=1&uploadId=u123",
						"partNumber=10000&uploadId=2~abc_def-ghi",
						"uploadId=u123",
						"versionId=VER%2B1%3D",
						"tagging",
						"versioning",
						"delete",
						// s3-tables GetTable: name and namespace are the only prefix-related names SPT builds.
						"tableBucketARN=arn:aws:s3tables:us-east-1:111122223333:bucket/tb&namespace=ns1&name=tbl1",
						"tableBucketARN=arn%3Aaws%3As3tables%3Aus-east-1%3A111122223333%3Abucket%2Ftb&namespace=ns1&name=tbl1");
		for (final var query : queries) {
			assertEquals(legacyCanonicalQuery(query), canonicalQuery(query), query);
		}
		assertNotEquals(legacyCanonicalQuery("tenant=t1&tenant-id=7"), canonicalQuery("tenant=t1&tenant-id=7"));
	}

	// ---------- helpers ----------

	private static Map<String, String> args(final String... nameValues) {
		final Map<String, String> args = new LinkedHashMap<>();
		for (int i = 0; i < nameValues.length; i += 2) {
			args.put(nameValues[i], nameValues[i + 1]);
		}
		return args;
	}

	private static S3StorageDriverTest.TestS3Driver driver(final int authVersion, final Map<String, String> uriArgs)
					throws Exception {
		final Config cfg = S3StorageDriverTest.baseConfig(false, authVersion, false, null, "127.0.0.1");
		cfg.val("storage-net-http-uri-args", uriArgs);
		return new S3StorageDriverTest.TestS3Driver(cfg);
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> dataOp(final OpType opType) throws Exception {
		final var item = new DataItemImpl("obj1", 0, 1024);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<DataItem>(
						0, opType, item, null, "/bucket", CRED, null, 0);
	}

	private static String canonicalQuery(final String rawQuery) throws Exception {
		final Method m = S3StorageDriver.class.getDeclaredMethod("canonicalizeQueryParameters", String.class);
		m.setAccessible(true);
		return (String) m.invoke(null, rawQuery);
	}

	/** The canonicaliser before this fix: it sorted the joined {@code name=value} strings. */
	private static String legacyCanonicalQuery(final String rawQuery) {
		final List<String> pairs = new ArrayList<>();
		for (final var part : rawQuery.split("&")) {
			if (part.isEmpty()) {
				continue;
			}
			final int eq = part.indexOf('=');
			final var name = eq >= 0 ? part.substring(0, eq) : part;
			final var value = eq >= 0 ? part.substring(eq + 1) : "";
			pairs.add(uriEncode(percentDecode(name)) + '=' + uriEncode(percentDecode(value)));
		}
		Collections.sort(pairs);
		return String.join("&", pairs);
	}

	/**
	 * Recomputes the SigV4 signature of {@code req} the way a verifying target does, independently of the
	 * driver, and checks it against the request's {@code Authorization}.
	 *
	 * @return the canonical query string the target computed from the sent URI
	 */
	static String verifySigV4(final HttpRequest req) throws Exception {
		final var auth = req.headers().get(HttpHeaderNames.AUTHORIZATION);
		assertNotNull(auth, "no Authorization header");
		final Matcher m = AUTH_V4.matcher(auth);
		assertTrue(m.matches(), auth);
		assertEquals(CRED.getUid(), m.group(1));
		final var date = m.group(2);
		final var region = m.group(3);
		final var service = m.group(4);
		final var signedHeaders = m.group(5).split(";");

		final var uri = req.uri();
		final int q = uri.indexOf('?');
		final var path = q < 0 ? uri : uri.substring(0, q);
		final var canonicalQuery = q < 0 ? "" : botocoreCanonicalQuery(uri.substring(q + 1));

		final var headers = req.headers();
		final var canonicalHeaders = new StringBuilder();
		for (final var name : signedHeaders) {
			final List<String> values = new ArrayList<>();
			for (final var value : headers.getAll(name)) {
				values.add(value.trim().replaceAll("\\s+", " "));
			}
			assertTrue(!values.isEmpty(), "signed header missing from the request: " + name);
			canonicalHeaders.append(name.toLowerCase(Locale.ROOT)).append(':').append(String.join(",", values)).append('\n');
		}
		final var payloadHash = headers.get("x-amz-content-sha256");
		assertNotNull(payloadHash);
		final var canonicalRequest = req.method().name() + '\n' + path + '\n' + canonicalQuery + '\n'
						+ canonicalHeaders + '\n' + String.join(";", signedHeaders) + '\n' + payloadHash;

		final var amzDate = headers.get("x-amz-date");
		assertTrue(amzDate.startsWith(date), amzDate);
		final var scope = date + '/' + region + '/' + service + "/aws4_request";
		final var stringToSign = "AWS4-HMAC-SHA256\n" + amzDate + '\n' + scope + '\n'
						+ HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
										.digest(canonicalRequest.getBytes(StandardCharsets.UTF_8)));
		byte[] key = ("AWS4" + CRED.getSecret()).getBytes(StandardCharsets.UTF_8);
		for (final var part : List.of(date, region, service, "aws4_request")) {
			key = hmac(key, part);
		}
		final var expected = HexFormat.of().formatHex(hmac(key, stringToSign));
		assertEquals(expected, m.group(6), "SigV4 signature mismatch; canonical request:\n" + canonicalRequest);
		return canonicalQuery;
	}

	/** botocore: decode each pair, encode it RFC 3986 style, sort by name then value. */
	private static String botocoreCanonicalQuery(final String rawQuery) {
		final List<String[]> params = new ArrayList<>();
		for (final var part : rawQuery.split("&")) {
			if (part.isEmpty()) {
				continue;
			}
			final int eq = part.indexOf('=');
			params.add(new String[]{
					uriEncode(percentDecode(eq >= 0 ? part.substring(0, eq) : part)),
					uriEncode(percentDecode(eq >= 0 ? part.substring(eq + 1) : ""))
			});
		}
		params.sort((a, b) -> a[0].equals(b[0]) ? a[1].compareTo(b[1]) : a[0].compareTo(b[0]));
		final List<String> joined = new ArrayList<>();
		for (final var p : params) {
			joined.add(p[0] + '=' + p[1]);
		}
		return String.join("&", joined);
	}

	private static String percentDecode(final String s) {
		final var out = new ByteArrayOutputStream();
		for (int i = 0; i < s.length(); i++) {
			final char c = s.charAt(i);
			if (c == '%' && i + 2 < s.length()) {
				out.write(Integer.parseInt(s.substring(i + 1, i + 3), 16));
				i += 2;
			} else {
				final var bytes = String.valueOf(c).getBytes(StandardCharsets.UTF_8);
				out.write(bytes, 0, bytes.length);
			}
		}
		return out.toString(StandardCharsets.UTF_8);
	}

	private static String uriEncode(final String s) {
		final var sb = new StringBuilder();
		for (final byte b : s.getBytes(StandardCharsets.UTF_8)) {
			final int c = b & 0xFF;
			if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
							|| c == '-' || c == '_' || c == '.' || c == '~') {
				sb.append((char) c);
			} else {
				sb.append('%').append(String.format("%02X", c));
			}
		}
		return sb.toString();
	}

	private static byte[] hmac(final byte[] key, final String data) throws Exception {
		final var mac = Mac.getInstance("HmacSHA256");
		mac.init(new SecretKeySpec(key, "HmacSHA256"));
		return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
	}
}
