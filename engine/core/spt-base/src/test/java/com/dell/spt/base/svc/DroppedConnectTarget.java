package com.dell.spt.base.svc;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;

/**
 * A loopback port whose new connects time out, as when a firewall drops SYNs: a listener that never
 * accepts, with its accept queue filled until a probe connect times out. Kernels that refuse a full
 * queue's connects instead of dropping them are not supported.
 */
public final class DroppedConnectTarget implements AutoCloseable {

	private static final int PROBE_TIMEOUT_MILLIS = 200;
	private static final int MAX_QUEUED = 64;

	private final ServerSocket listener;
	private final List<Socket> queued = new ArrayList<>();

	public DroppedConnectTarget() throws IOException {
		final InetAddress loopback = InetAddress.getLoopbackAddress();
		listener = new ServerSocket(0, 1, loopback);
		try {
			while (true) {
				if (queued.size() >= MAX_QUEUED) {
					throw new IllegalStateException("The accept queue of port " + port() + " never filled");
				}
				final Socket probe = new Socket();
				try {
					probe.connect(new InetSocketAddress(loopback, port()), PROBE_TIMEOUT_MILLIS);
					queued.add(probe);
				} catch (final SocketTimeoutException e) {
					probe.close();
					return;
				}
			}
		} catch (final IOException | RuntimeException e) {
			close();
			throw e;
		}
	}

	public String host() {
		return listener.getInetAddress().getHostAddress();
	}

	public int port() {
		return listener.getLocalPort();
	}

	@Override
	public void close() throws IOException {
		for (final Socket socket : queued) {
			socket.close();
		}
		listener.close();
	}
}
