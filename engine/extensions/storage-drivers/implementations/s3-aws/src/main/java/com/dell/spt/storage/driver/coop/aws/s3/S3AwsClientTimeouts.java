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
 * The bounds the s3-aws driver puts on its requests: on the network behavior of every S3 client
 * it builds, read from {@code storage.crt.connectionTimeoutMs} and {@code
 * storage.crt.socketTimeoutMs}, and on its own wait for a request, read from {@code
 * storage.crt.requestTimeoutMs} and {@code storage.crt.stopTimeoutMs}.
 *
 * <p>The connection timeout bounds establishing a connection. The stall timeout bounds a request
 * that makes no progress: the native connection monitor closes a connection that moves no data
 * for that long while a request is in flight, and the request then fails. The monitor counts
 * whole seconds. The bound is per attempt. A client may retry a failed attempt on a new connection, so a request that stalls on
 * every attempt fails after all of them. Neither bound is a request deadline: a transfer that
 * keeps moving data is never cut off, and no timer is armed per request.
 *
 * <p>The request timeout is the deadline the driver itself puts on each request, from the moment
 * it is made until its response has been read in full, a GET's body included: it covers a request
 * the clients have lost with no connection the monitor could watch, and a response that keeps
 * coming but never ends. The stop timeout bounds the requests in flight when the load step stops:
 * once admission has closed, they get that long to finish before the driver cancels them. It is
 * half of the engine's ten-second budget for stopping a run, so that the stop itself fits in the
 * rest.
 *
 * @param connectionTimeout the time allowed to establish a connection
 * @param stallTimeout the time a connection with a request in flight may move no data, or
 *     {@link Duration#ZERO} when stalled requests are not detected
 * @param requestTimeout the time a request may take in all before the driver ends it, or
 *     {@link Duration#ZERO} for no deadline
 * @param stopTimeout the time requests in flight get once the step stops, or {@link Duration#ZERO}
 *     when the driver does not cancel them on its own
 */
record S3AwsClientTimeouts(
				Duration connectionTimeout, Duration stallTimeout, Duration requestTimeout, Duration stopTimeout) {

	static final String CONNECTION_TIMEOUT_KEY = "connectionTimeoutMs";
	static final String SOCKET_TIMEOUT_KEY = "socketTimeoutMs";
	static final String REQUEST_TIMEOUT_KEY = "requestTimeoutMs";
	static final String STOP_TIMEOUT_KEY = "stopTimeoutMs";

	/** The monitor counts a connection that moves at least this much as making progress. */
	static final long MINIMUM_PROGRESS_BYTES_PER_SECOND = 1L;

	/**
	 * The native monitor takes its failure interval in whole seconds and refuses an interval below
	 * two.
	 */
	static final int MINIMUM_STALL_TIMEOUT_MS = 2000;

	/** Part of the message of the failure a client reports when the monitor closes a connection. */
	static final String STALL_FAILURE_MESSAGE = "failure to meet throughput minimum";

	/**
	 * Part of the message of the native socket timeout, which the clients report when a connection
	 * cannot be established within the connection timeout.
	 */
	static final String SOCKET_TIMEOUT_MESSAGE = "socket operation timed out";

	S3AwsClientTimeouts {
		Objects.requireNonNull(connectionTimeout);
		Objects.requireNonNull(stallTimeout);
		Objects.requireNonNull(requestTimeout);
		Objects.requireNonNull(stopTimeout);
	}

	/** The network bounds alone, with the driver's own waits unbounded. */
	S3AwsClientTimeouts(final Duration connectionTimeout, final Duration stallTimeout) {
		this(connectionTimeout, stallTimeout, Duration.ZERO, Duration.ZERO);
	}

	/** The values the driver ships with, as in its defaults file. */
	static final S3AwsClientTimeouts SHIPPED = new S3AwsClientTimeouts(
					Duration.ofMillis(5000), Duration.ofMillis(30000), Duration.ofMillis(300000), Duration.ofMillis(5000));

	/**
	 * Reads the bounds for a driver. The factory reads them strictly, with {@link #from}, before it
	 * builds any client. A driver built around prebuilt clients, through its public constructors,
	 * may be given a configuration without the {@code storage.crt} branch at all; it then takes the
	 * shipped values. A branch that is present is read strictly.
	 */
	static S3AwsClientTimeouts forDriver(final Config storageConfig) throws IllegalConfigurationException {
		Config crt;
		try {
			crt = storageConfig.configVal("crt");
		} catch (final RuntimeException absent) {
			crt = null;
		}
		return crt == null ? SHIPPED : from(storageConfig);
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
		final long requestMs = readMillis(crt, REQUEST_TIMEOUT_KEY);
		final long stopMs = readMillis(crt, STOP_TIMEOUT_KEY);
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
		if (requestMs < 0) {
			throw new IllegalConfigurationException(
							"storage.crt." + REQUEST_TIMEOUT_KEY + " must be 0 (requests are waited for without bound) or a"
											+ " positive number of milliseconds, got " + requestMs);
		}
		if (stopMs < 0) {
			throw new IllegalConfigurationException(
							"storage.crt." + STOP_TIMEOUT_KEY + " must be 0 (requests in flight are not cancelled when the step"
											+ " stops) or a positive number of milliseconds, got " + stopMs);
		}
		return new S3AwsClientTimeouts(
						Duration.ofMillis(connectionMs), Duration.ofMillis(socketMs), Duration.ofMillis(requestMs),
						Duration.ofMillis(stopMs));
	}

	private static long readMillis(final Config crt, final String key) throws IllegalConfigurationException {
		try {
			return crt.intVal(key);
		} catch (final RuntimeException failure) {
			throw new IllegalConfigurationException(
							"storage.crt." + key + " is missing or is not an integer number of milliseconds", failure);
		}
	}

	/**
	 * Returns whether the native connection monitor closed the connection of a failed request
	 * because it moved no data. The clients report it as a client or I/O failure that carries only
	 * the message of the native error.
	 */
	static boolean isStall(final Throwable failure) {
		return hasMessage(failure, STALL_FAILURE_MESSAGE);
	}

	/** Returns whether a failed request ended with the native socket timeout, such as a connect deadline. */
	static boolean isSocketTimeout(final Throwable failure) {
		return hasMessage(failure, SOCKET_TIMEOUT_MESSAGE);
	}

	private static boolean hasMessage(final Throwable failure, final String text) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			final String message = cause.getMessage();
			if (message != null && message.contains(text)) {
				return true;
			}
		}
		return false;
	}

	boolean stallDetectionEnabled() {
		return !stallTimeout.isZero();
	}

	/** Whether the driver bounds its wait for each request. */
	boolean requestBoundEnabled() {
		return !requestTimeout.isZero();
	}

	/** Whether the driver cancels the requests in flight once the step has stopped. */
	boolean cancelAtStopEnabled() {
		return !stopTimeout.isZero();
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
