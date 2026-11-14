package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.CREDENTIAL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import java.util.Collections;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;

/**
 * The SigV4 signing key depends on the secret and the date, and is derived once per date. SPT's own
 * function keeps its key itself; a function an extension installs is memoized by date, as before.
 *
 * <p>A derivation makes a new key array, and a key is reused by identity, so the derivations of
 * SPT's own function are counted as the distinct key arrays it returns.
 */
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
			final Set<byte[]> keys = identitySet();
			final Set<String> dates = new TreeSet<>();
			var compared = 0;
			for (var i = 0; i < REQUESTS; i++) {
				final var signed = sign(driver, CREDENTIAL);
				final var reference = sign(fresh, CREDENTIAL);
				dates.add(date(signed));
				keys.add(lastKey(driver, CREDENTIAL, signed));
				if (amzDate(signed).equals(amzDate(reference))) {
					assertEquals(signature(reference), signature(signed), "request " + i);
					compared++;
				}
			}
			assertTrue(compared > 0, "no request was signed in the same second as its reference");
			assertEquals(dates.size(), keys.size(), "one derivation per date over " + REQUESTS + " requests");
			assertEquals(1, driver.signingKeyCreateFunctionCache.get().size(), "one function for the one secret");
			assertTrue(driver.signingKeyCache.get().isEmpty(), "SPT's own keys are not in the date-keyed cache");
			final var function = builtIn(driver, CREDENTIAL);
			for (final var date : dates) {
				assertArrayEquals(
								independentKey(CREDENTIAL.getSecret(), date, driver.awsRegion, driver.sigV4ServiceName),
								function.apply(date));
			}
		}
	}

	@Test
	void alternatingSecretsDeriveEachKeyOncePerDate() throws Exception {
		try (final var driver = driver()) {
			final Set<byte[]> firstKeys = identitySet();
			final Set<byte[]> secondKeys = identitySet();
			final Set<String> dates = new TreeSet<>();
			for (var i = 0; i < REQUESTS; i++) {
				final var first = sign(driver, CREDENTIAL);
				firstKeys.add(lastKey(driver, CREDENTIAL, first));
				final var second = sign(driver, OTHER);
				secondKeys.add(lastKey(driver, OTHER, second));
				dates.add(date(first));
				dates.add(date(second));
			}
			assertEquals(dates.size(), firstKeys.size(), "the first secret's key, once per date");
			assertEquals(dates.size(), secondKeys.size(), "the second secret's key, once per date");
		}
	}

	@Test
	void aDateChangeDerivesANewKey() throws Exception {
		final var function = new S3StorageDriver.SigningKeyCreateFunctionImpl(AWS_EXAMPLE_SECRET, "us-east-1", "iam");

		final var day1 = function.apply("20120215");
		assertEquals(AWS_EXAMPLE_KEY, HexFormat.of().formatHex(day1), "AWS's published example key");
		assertSame(day1, function.apply("20120215"), "the same date reuses its key");

		final var day2 = function.apply("20120216");
		assertNotSame(day1, day2, "a new date derives a new key");
		assertArrayEquals(independentKey(AWS_EXAMPLE_SECRET, "20120216", "us-east-1", "iam"), day2);
		assertFalse(HexFormat.of().formatHex(day2).equals(AWS_EXAMPLE_KEY));
		assertSame(day2, function.apply("20120216"));
		assertEquals(AWS_EXAMPLE_KEY, HexFormat.of().formatHex(day1), "a returned key is never changed");
	}

	/**
	 * An extension that installs its own function, which derives on every call, still has it called
	 * once per date: the driver memoizes it in the date-keyed cache, as on 11f30c45.
	 */
	@Test
	void anExtensionsFunctionIsMemoizedByDate() throws Exception {
		try (final var driver = driver(); final var fresh = driver()) {
			final var extension = new ExtensionFunction(CREDENTIAL.getSecret(), driver.awsRegion, driver.sigV4ServiceName);
			driver.signingKeyCreateFunctionCache.get().put(CREDENTIAL.getSecret(), extension);
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
			// One date, so one derivation, unless the requests crossed midnight UTC.
			assertEquals(dates.size(), extension.derivations, "the extension's function, once per date over " + REQUESTS + " requests");
			assertSame(extension, driver.signingKeyCreateFunctionCache.get().get(CREDENTIAL.getSecret()));
			assertEquals(dates, driver.signingKeyCache.get().keySet(), "the extension's keys are cached by date");
		}
	}

	/** SPT's own function for another secret does not take an extension's key from the date-keyed cache. */
	@Test
	void sptsOwnFunctionIgnoresAnExtensionsKeys() throws Exception {
		try (final var driver = driver(); final var freshOther = driver()) {
			final var extension = new ExtensionFunction(CREDENTIAL.getSecret(), driver.awsRegion, driver.sigV4ServiceName);
			driver.signingKeyCreateFunctionCache.get().put(CREDENTIAL.getSecret(), extension);
			final var first = sign(driver, CREDENTIAL);
			final var extensionKey = driver.signingKeyCache.get().get(date(first));
			assertNotNull(extensionKey, "the extension's key is cached by date");

			final var other = atOneTimestamp(driver, freshOther, OTHER);
			assertEquals(signature(other.expected()), signature(other.actual()), "the other secret is signed with its own key");
			assertInstanceOf(S3StorageDriver.SigningKeyCreateFunctionImpl.class,
							driver.signingKeyCreateFunctionCache.get().get(OTHER.getSecret()));
			assertSame(extensionKey, driver.signingKeyCache.get().get(date(first)), "SPT's own key is not cached by date");
			assertEquals(1, driver.signingKeyCache.get().size());
		}
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

	/**
	 * An extension's own function, a plain implementation of the public interface: it derives the
	 * key on every call and counts its calls.
	 */
	private static final class ExtensionFunction implements SigningKeyCreateFunction {

		private final String secret;
		private final String region;
		private final String service;
		int derivations;

		ExtensionFunction(final String secret, final String region, final String service) {
			this.secret = secret;
			this.region = region;
			this.service = service;
		}

		@Override
		public byte[] apply(final String datestamp) {
			derivations++;
			try {
				return independentKey(secret, datestamp, region, service);
			} catch (final Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private static Set<byte[]> identitySet() {
		return Collections.newSetFromMap(new IdentityHashMap<>());
	}

	private static S3StorageDriver.SigningKeyCreateFunctionImpl builtIn(
					final S3StorageDriver<?, ?> driver, final Credential credential) {
		return assertInstanceOf(S3StorageDriver.SigningKeyCreateFunctionImpl.class,
						driver.signingKeyCreateFunctionCache.get().get(credential.getSecret()));
	}

	/**
	 * The key SPT's own function holds after signing {@code signed}. Asked for the date it was just
	 * asked for, the function returns its key without a derivation.
	 */
	private static byte[] lastKey(final S3StorageDriver<?, ?> driver, final Credential credential, final HttpHeaders signed) {
		final var function = builtIn(driver, credential);
		final var key = function.apply(date(signed));
		assertSame(key, function.apply(date(signed)));
		return key;
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
