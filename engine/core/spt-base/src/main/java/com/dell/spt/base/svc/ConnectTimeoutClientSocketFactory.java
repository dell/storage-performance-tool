package com.dell.spt.base.svc;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.rmi.server.RMIClientSocketFactory;

/**
 * An RMI client socket factory whose connect gives up after a timeout. A timed-out connect is
 * reported as a {@link ConnectException}, which RMI turns into a {@link java.rmi.ConnectException},
 * as it does a refused one, so callers that retry a refused connect retry a timed-out one too.
 *
 * <p>RMI keys its connections by endpoint and client socket factory, so two factories with the same
 * timeout are equal.
 */
final class ConnectTimeoutClientSocketFactory implements RMIClientSocketFactory {

	private final int connectTimeoutMillis;

	ConnectTimeoutClientSocketFactory(final int connectTimeoutMillis) {
		if (connectTimeoutMillis <= 0) {
			throw new IllegalArgumentException("Connect timeout must be positive: " + connectTimeoutMillis);
		}
		this.connectTimeoutMillis = connectTimeoutMillis;
	}

	@Override
	public Socket createSocket(final String host, final int port) throws IOException {
		final Socket socket = new Socket();
		try {
			socket.connect(new InetSocketAddress(host, port), connectTimeoutMillis);
			return socket;
		} catch (final SocketTimeoutException e) {
			closeQuietly(socket, e);
			final ConnectException timedOut = new ConnectException(
							"Connect to " + host + ":" + port + " timed out after " + connectTimeoutMillis + " ms");
			timedOut.initCause(e);
			throw timedOut;
		} catch (final IOException | RuntimeException e) {
			closeQuietly(socket, e);
			throw e;
		}
	}

	private static void closeQuietly(final Socket socket, final Exception cause) {
		try {
			socket.close();
		} catch (final IOException e) {
			cause.addSuppressed(e);
		}
	}

	@Override
	public boolean equals(final Object other) {
		return other instanceof ConnectTimeoutClientSocketFactory that
						&& that.connectTimeoutMillis == connectTimeoutMillis;
	}

	@Override
	public int hashCode() {
		return Integer.hashCode(connectTimeoutMillis);
	}

	@Override
	public String toString() {
		return "ConnectTimeoutClientSocketFactory[" + connectTimeoutMillis + " ms]";
	}
}
