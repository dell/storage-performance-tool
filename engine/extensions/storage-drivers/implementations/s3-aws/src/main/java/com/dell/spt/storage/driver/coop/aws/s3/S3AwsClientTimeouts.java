package com.dell.spt.storage.driver.coop.aws.s3;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.ConnectionHealthConfiguration;
import software.amazon.awssdk.services.s3.crt.S3CrtHttpConfiguration;

/**
 * The bounds the s3-aws driver puts on the network behavior of every S3 client it builds, read
 * from {@code storage.crt.connectionTimeoutMs} and {@code storage.crt.socketTimeoutMs}.
 *
 * <p>The connection timeout bounds establishing a connection. The stall timeout bounds a request
 * that makes no progress: the native connection monitor closes a connection that moves no data
 * for that long while a request is in flight, and the request then fails. The monitor counts
 * whole seconds. The bound is per attempt. A client may retry a failed attempt on a new connection, so a request that stalls on
 * every attempt fails after all of them. Neither bound is a request deadline: a transfer that
 * keeps moving data is never cut off, and no timer is armed per request.
 *
 * @param connectionTimeout the time allowed to establish a connection
 * @param stallTimeout the time a connection with a request in flight may move no data, or
 *     {@link Duration#ZERO} when stalled requests are not detected
 */
record S3AwsClientTimeouts(Duration connectionTimeout, Duration stallTimeout) {

	static final String CONNECTION_TIMEOUT_KEY = "connectionTimeoutMs";
	static final String SOCKET_TIMEOUT_KEY = "socketTimeoutMs";

	/** The monitor counts a connection that moves at least this much as making progress. */
	static final long MINIMUM_PROGRESS_BYTES_PER_SECOND = 1L;

	/**
	 * The native monitor takes its failure interval in whole seconds and refuses an interval below
	 * two.
	 */
	static final int MINIMUM_STALL_TIMEOUT_MS = 2000;

	/** Part of the message of the failure a client reports when the monitor closes a connection. */
	static final String STALL_FAILURE_MESSAGE = "failure to meet throughput minimum";

	S3AwsClientTimeouts {
		Objects.requireNonNull(connectionTimeout);
		Objects.requireNonNull(stallTimeout);
	}

	/**
	 * Reads the bounds from the storage configuration. Both keys ship with defaults, so a value
	 * that is missing, not an integer or out of range is a configuration error.
	 *
	 * @throws IllegalConfigurationException when a value cannot be used
	 */
	static S3AwsClientTimeouts from(final Config storageConfig) throws IllegalConfigurationException {
		final Config crt = storageConfig.configVal("crt");
		if (crt == null) {
			throw new IllegalConfigurationException("Missing required config: storage.crt");
		}
		final long connectionMs = readMillis(crt, CONNECTION_TIMEOUT_KEY);
		final long socketMs = readMillis(crt, SOCKET_TIMEOUT_KEY);
		if (connectionMs <= 0) {
			throw new IllegalConfigurationException(
							"storage.crt." + CONNECTION_TIMEOUT_KEY + " must be a positive number of milliseconds, got "
											+ connectionMs);
		}
		if (socketMs < 0 || (socketMs > 0 && socketMs < MINIMUM_STALL_TIMEOUT_MS)) {
			throw new IllegalConfigurationException(
							"storage.crt." + SOCKET_TIMEOUT_KEY + " must be 0 (stalled requests are not detected) or at least "
											+ MINIMUM_STALL_TIMEOUT_MS + " milliseconds, got " + socketMs);
		}
		return new S3AwsClientTimeouts(Duration.ofMillis(connectionMs), Duration.ofMillis(socketMs));
	}

	private static long readMillis(final Config crt, final String key) throws IllegalConfigurationException {
		try {
			return crt.intVal(key);
		} catch (final RuntimeException failure) {
			throw new IllegalConfigurationException(
							"storage.crt." + key + " is missing or is not an integer number of milliseconds", failure);
		}
	}

	boolean stallDetectionEnabled() {
		return !stallTimeout.isZero();
	}

	/** Returns the connection settings for the CRT transfer client that serves most operations. */
	S3CrtHttpConfiguration crtHttpConfiguration() {
		final var http = S3CrtHttpConfiguration.builder().connectionTimeout(connectionTimeout);
		if (stallDetectionEnabled()) {
			http.connectionHealthConfiguration(health -> health
							.minimumThroughputInBps(MINIMUM_PROGRESS_BYTES_PER_SECOND)
							.minimumThroughputTimeout(stallTimeout));
		}
		return http.build();
	}

	/** Applies the bounds to the CRT HTTP client of an ordinary SDK client. */
	AwsCrtAsyncHttpClient.Builder applyTo(final AwsCrtAsyncHttpClient.Builder builder) {
		builder.connectionTimeout(connectionTimeout);
		healthConfiguration().ifPresent(builder::connectionHealthConfiguration);
		return builder;
	}

	/** Applies the bounds to the CRT HTTP client of the standalone DELETE path. */
	CrtDeleteTimingAsyncHttpClient.Builder applyTo(final CrtDeleteTimingAsyncHttpClient.Builder builder) {
		builder.connectionTimeout(connectionTimeout);
		healthConfiguration().ifPresent(builder::connectionHealthConfiguration);
		return builder;
	}

	private Optional<ConnectionHealthConfiguration> healthConfiguration() {
		if (!stallDetectionEnabled()) {
			return Optional.empty();
		}
		return Optional.of(ConnectionHealthConfiguration.builder()
						.minimumThroughputInBps(MINIMUM_PROGRESS_BYTES_PER_SECOND)
						.minimumThroughputTimeout(stallTimeout)
						.build());
	}
}
