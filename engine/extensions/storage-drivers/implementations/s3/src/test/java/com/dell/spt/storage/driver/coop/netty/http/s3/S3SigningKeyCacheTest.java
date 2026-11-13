package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

/** The SigV4 signing key depends on the secret and the date, and is derived once per date. */
class S3SigningKeyCacheTest {

	/** A made-up second credential; no real secret. */
	private static final Credential OTHER = Credential.getInstance("user2", "notARealSecretJustATestValue0123456789ab");
	/** AWS's published example of a SigV4 signing-key derivation; not a real secret. */
	private static final String AWS_EXAMPLE_SECRET = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
	private static final String AWS_EXAMPLE_KEY = "f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d";
	private static final int REQUESTS = 100;
	private static final int TIMESTAMP_ATTEMPTS = 20;

	@Test
	void twoSecretsOnOneThreadEachMatchAFreshDriversSignature() throws Exception {
		try (final var shared = driver(); final var fresh = driver(); final var freshOther = driver()) {
			sign(shared, CREDENTIAL);
			final var second = atOneTimestamp(shared, freshOther, OTHER);
			assertEquals(OTHER.getUid(), uid(second.actual()));
			assertEquals(signature(second.expected()), signature(second.actual()), "the second secret is signed with its own key");
			final var firstAgain = atOneTimestamp(shared, fresh, CREDENTIAL);
			assertEquals(CREDENTIAL.getUid(), uid(firstAgain.actual()));
			assertEquals(signature(firstAgain.expected()), signature(firstAgain.actual()), "the first secret still has its own key");
		}
	}

	@Test
	void oneCredentialKeepsItsSignaturesAndDerivesItsKeyOncePerDate() throws Exception {
		try (final var driver = driver(); final var fresh = driver()) {
			final var counting = new CountingFunction(CREDENTIAL.getSecret(), driver.awsRegion, driver.sigV4ServiceName);
			driver.signingKeyCreateFunctionCache.get().put(CREDENTIAL.getSecret(), counting);
			final Set<String> dates = new TreeSet<>();
			var compared = 0;
			for (var i = 0; i < REQUESTS; i++) {
				final var signed = sign(driver, CREDENTIAL);
				final var reference = sign(fresh, CREDENTIAL);
				dates.add(date(signed));
				if (amzDate(signed).equals(amzDate(reference))) {
					assertEquals(signature(reference), signature(signed), "request " + i);
					compared++;
				}
			}
			assertTrue(compared > 0, "no request was signed in the same second as its reference");
			assertEquals(dates.size(), counting.derivations, "one derivation per date over " + REQUESTS + " requests");
			assertEquals(1, driver.signingKeyCreateFunctionCache.get().size(), "one function for the one secret");
			for (final var date : dates) {
				assertArrayEquals(
								independentKey(CREDENTIAL.getSecret(), date, driver.awsRegion, driver.sigV4ServiceName),
								counting.apply(date));
			}
		}
	}

	@Test
	void alternatingSecretsDeriveEachKeyOncePerDate() throws Exception {
		try (final var driver = driver()) {
			final var first = new CountingFunction(CREDENTIAL.getSecret(), driver.awsRegion, driver.sigV4ServiceName);
			final var second = new CountingFunction(OTHER.getSecret(), driver.awsRegion, driver.sigV4ServiceName);
			driver.signingKeyCreateFunctionCache.get().put(CREDENTIAL.getSecret(), first);
			driver.signingKeyCreateFunctionCache.get().put(OTHER.getSecret(), second);
			final Set<String> dates = new TreeSet<>();
			for (var i = 0; i < REQUESTS; i++) {
				dates.add(date(sign(driver, CREDENTIAL)));
				dates.add(date(sign(driver, OTHER)));
			}
			assertEquals(dates.size(), first.derivations, "the first secret's key, once per date");
			assertEquals(dates.size(), second.derivations, "the second secret's key, once per date");
		}
	}

	@Test
	void aDateChangeDerivesANewKey() throws Exception {
		final var function = new CountingFunction(AWS_EXAMPLE_SECRET, "us-east-1", "iam");

		final var day1 = function.apply("20120215");
		assertEquals(AWS_EXAMPLE_KEY, HexFormat.of().formatHex(day1), "AWS's published example key");
		assertSame(day1, function.apply("20120215"), "the same date reuses its key");
		assertEquals(1, function.derivations);

		final var day2 = function.apply("20120216");
		assertEquals(2, function.derivations, "a new date derives a new key");
		assertArrayEquals(independentKey(AWS_EXAMPLE_SECRET, "20120216", "us-east-1", "iam"), day2);
		assertFalse(HexFormat.of().formatHex(day2).equals(AWS_EXAMPLE_KEY));
		assertSame(day2, function.apply("20120216"));
		assertEquals(2, function.derivations);
	}

	@Test
	void eachThreadHasItsOwnSigningFunction() throws Exception {
		try (final var driver = driver()) {
			sign(driver, CREDENTIAL);
			final var here = driver.signingKeyCreateFunctionCache.get().get(CREDENTIAL.getSecret());
			final AtomicReference<Object> there = new AtomicReference<>();
			final var other = new Thread(() -> {
				sign(driver, CREDENTIAL);
				there.set(driver.signingKeyCreateFunctionCache.get().get(CREDENTIAL.getSecret()));
				driver.signingKeyCreateFunctionCache.remove();
			}, "signing-key-other-thread");
			other.start();
			other.join(10_000);
			assertFalse(other.isAlive());
			assertNotNull(here);
			assertNotNull(there.get());
			assertNotSame(here, there.get(), "the date and key a function keeps are confined to its thread");
		}
	}

	/** Counts the derivations behind the real date check. */
	private static final class CountingFunction extends S3StorageDriver.SigningKeyCreateFunctionImpl {

		int derivations;

		CountingFunction(final String secret, final String region, final String service) {
			super(secret, region, service);
		}

		@Override
		byte[] derive(final String datestamp) {
			derivations++;
			return super.derive(datestamp);
		}
	}

	private static S3StorageDriver<Item, Operation<Item>> driver() throws Exception {
		return new S3StorageDriver<>("signing-key", null,
						S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1:9024").configVal("storage"), false, 1);
	}

	private static HttpHeaders sign(final S3StorageDriver<?, ?> driver, final Credential credential) {
		final HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, "127.0.0.1:9024");
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		driver.applyAuthHeaders(headers, HttpMethod.HEAD, "/bucket", credential);
		assertNotNull(headers.get(HttpHeaderNames.AUTHORIZATION));
		return headers;
	}

	/** One request's headers as {@code driver} signed it, and as the reference driver signed it in the same second. */
	private record Signed(HttpHeaders actual, HttpHeaders expected) {}

	/** Signs on {@code driver} and on {@code reference} until both signatures carry the same second. */
	private static Signed atOneTimestamp(
					final S3StorageDriver<?, ?> driver, final S3StorageDriver<?, ?> reference, final Credential credential) {
		for (var attempt = 0; attempt < TIMESTAMP_ATTEMPTS; attempt++) {
			final var signed = sign(driver, credential);
			final var expected = sign(reference, credential);
			if (amzDate(signed).equals(amzDate(expected))) {
				return new Signed(signed, expected);
			}
		}
		return fail("no two signatures in the same second after " + TIMESTAMP_ATTEMPTS + " attempts");
	}

	private static String amzDate(final HttpHeaders headers) {
		return headers.get(S3Api.AMZ_DATE_HEADER);
	}

	private static String date(final HttpHeaders headers) {
		return amzDate(headers).substring(0, 8);
	}

	private static String signature(final HttpHeaders headers) {
		final var auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		return auth.substring(auth.indexOf("Signature=") + "Signature=".length());
	}

	private static String uid(final HttpHeaders headers) {
		final var auth = headers.get(HttpHeaderNames.AUTHORIZATION);
		final var start = auth.indexOf("Credential=") + "Credential=".length();
		return auth.substring(start, auth.indexOf('/', start));
	}

	private static byte[] independentKey(final String secret, final String date, final String region, final String service)
					throws Exception {
		return hmac("aws4_request", hmac(service, hmac(region, hmac(date, ("AWS4" + secret).getBytes(StandardCharsets.UTF_8)))));
	}

	private static byte[] hmac(final String data, final byte[] key) throws Exception {
		final var mac = Mac.getInstance("HmacSHA256");
		mac.init(new SecretKeySpec(key, "HmacSHA256"));
		return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
	}
}
