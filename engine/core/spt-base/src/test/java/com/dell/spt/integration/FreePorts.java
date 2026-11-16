package com.dell.spt.integration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

/**
 * Free ephemeral ports for integration tests that must pass a port number on, such as an API port or an RMI
 * port given to a node, rather than binding port 0 themselves. Never use the default API port 9999 or the
 * default RMI port 1099: other engines on the same host may hold them.
 */
final class FreePorts {

	private FreePorts() {}

	/** Returns one free ephemeral port. */
	static int freePort() {
		return freePorts(1)[0];
	}

	/**
	 * Returns {@code count} distinct free ephemeral ports. All sockets stay open until every port is taken, so the
	 * ports differ from each other; they are closed before return, so the caller can bind them.
	 */
	static int[] freePorts(final int count) {
		final List<ServerSocket> sockets = new ArrayList<>(count);
		try {
			final int[] ports = new int[count];
			for (int i = 0; i < count; i++) {
				final var socket = new ServerSocket(0);
				sockets.add(socket);
				ports[i] = socket.getLocalPort();
			}
			return ports;
		} catch (final IOException e) {
			throw new UncheckedIOException("Failed to take a free ephemeral port", e);
		} finally {
			for (final var socket : sockets) {
				try {
					socket.close();
				} catch (final IOException ignored) {
					// the port is still usable once the OS releases it
				}
			}
		}
	}
}
