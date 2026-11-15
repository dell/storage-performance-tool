package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Checks a SigV2 signature independently of the driver: the string to sign is built here from the
 * request's headers and an expected canonical resource, and signed with {@link Mac}. Uses no driver
 * API beyond the request, so it compiles against the base revision too.
 */
final class SigV2TestSupport {

	static final String UID = "user1";
	// The secret S3StorageDriverTest.baseConfig configures.
	static final String SECRET = "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb";

	private SigV2TestSupport() {}

	/**
	 * The SigV2 string to sign of a request with the given canonical resource: the method,
	 * Content-MD5, Content-Type and Date, then the {@code x-amz-} headers sorted by lower-case name.
	 */
	static String stringToSign(final HttpRequest request, final String resource) {
		final var headers = request.headers();
		final var date = headers.get(HttpHeaderNames.DATE);
		assertNotNull(date, "a V2 request carries a Date");
		final var text = new StringBuilder(request.method().name())
						.append('\n').append(nullToEmpty(headers.get(HttpHeaderNames.CONTENT_MD5)))
						.append('\n').append(nullToEmpty(headers.get(HttpHeaderNames.CONTENT_TYPE)))
						.append('\n').append(date);
		final Map<String, String> amz = new TreeMap<>();
		for (final var header : headers) {
			final var name = header.getKey().toLowerCase(Locale.ROOT);
			if (name.startsWith("x-amz-")) {
				amz.put(name, header.getValue());
			}
		}
		amz.forEach((name, value) -> text.append('\n').append(name).append(':').append(value));
		return text.append('\n').append(resource).toString();
	}

	/** Asserts the request's V2 signature covers its headers and exactly {@code resource}. */
	static void assertSignedOver(final HttpRequest request, final String resource) throws Exception {
		assertEquals("AWS " + UID + ':' + sign(SECRET, stringToSign(request, resource)),
						request.headers().get(HttpHeaderNames.AUTHORIZATION),
						request.method() + " " + request.uri() + " signs over " + resource);
	}

	static String sign(final String secret, final String stringToSign) throws Exception {
		final var mac = Mac.getInstance("HmacSHA1");
		mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA1"));
		return Base64.getEncoder().encodeToString(mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8)));
	}

	private static String nullToEmpty(final String value) {
		return value == null ? "" : value;
	}
}
