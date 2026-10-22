package com.dell.spt.base.storage.driver.bucket;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Region selection and CreateBucket bodies shared by the S3 drivers.
 *
 * <p>Only the standard AWS S3 service endpoints {@code s3.amazonaws.com} and
 * {@code s3.<region>.amazonaws.com} are recognized. Selecting an S3 driver does not prove the
 * service is AWS, so every other endpoint is treated as a generic S3-compatible service.
 */
public final class AwsS3Region {

	/** The region of the global endpoint, and the fallback when nothing else selects one. */
	public static final String DEFAULT = "us-east-1";

	static final String GLOBAL_ENDPOINT = "s3.amazonaws.com";

	private static final Pattern REGIONAL_ENDPOINT = Pattern.compile("s3\\.([a-z0-9-]+)\\.amazonaws\\.com");

	private AwsS3Region() {}

	/**
	 * Returns the region a standard AWS S3 endpoint names, {@link #DEFAULT} for the global
	 * endpoint, or null for any other endpoint.
	 *
	 * @param endpoint a host, {@code host:port}, or URI
	 */
	public static String ofEndpoint(final String endpoint) {
		final var host = host(endpoint);
		if (GLOBAL_ENDPOINT.equals(host)) {
			return DEFAULT;
		}
		final var matcher = REGIONAL_ENDPOINT.matcher(host);
		return matcher.matches() ? matcher.group(1) : null;
	}

	/**
	 * Returns the effective region: a configured one, else the one a recognized AWS endpoint names,
	 * else {@link #DEFAULT}. A configured region that conflicts with the endpoint's wins, and the
	 * conflict is reported once through {@code warning}.
	 */
	public static String effective(final String configured, final String endpoint, final Consumer<String> warning) {
		final var inferred = endpoint == null ? null : ofEndpoint(endpoint);
		if (configured != null && !configured.isBlank()) {
			final var region = configured.trim();
			if (inferred != null && !inferred.equals(region) && warning != null) {
				warning.accept("storage.region " + region + " differs from the region " + inferred
								+ " of the AWS endpoint; requests are signed for " + region);
			}
			return region;
		}
		return inferred != null ? inferred : DEFAULT;
	}

	/**
	 * Returns the CreateBucket request body: a location constraint for a recognized AWS endpoint
	 * outside {@link #DEFAULT}, otherwise empty, as generic S3-compatible services expect.
	 */
	public static byte[] createBucketBody(final String region, final String endpoint) {
		if (endpoint == null || ofEndpoint(endpoint) == null || region == null || DEFAULT.equals(region)) {
			return new byte[0];
		}
		return ("<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
						+ "<LocationConstraint>" + region + "</LocationConstraint>"
						+ "</CreateBucketConfiguration>").getBytes(StandardCharsets.UTF_8);
	}

	/** Normalizes a host, {@code host:port} or URI to a lowercase host without port or trailing dot. */
	static String host(final String endpoint) {
		if (endpoint == null) {
			return "";
		}
		var host = endpoint.trim().toLowerCase(Locale.ROOT);
		final int scheme = host.indexOf("://");
		if (scheme >= 0) {
			host = host.substring(scheme + 3);
		}
		final int path = host.indexOf('/');
		if (path >= 0) {
			host = host.substring(0, path);
		}
		final int userInfo = host.lastIndexOf('@');
		if (userInfo >= 0) {
			host = host.substring(userInfo + 1);
		}
		if (host.startsWith("[")) {
			return host;
		}
		final int port = host.indexOf(':');
		if (port >= 0) {
			host = host.substring(0, port);
		}
		return host.endsWith(".") ? host.substring(0, host.length() - 1) : host;
	}
}
