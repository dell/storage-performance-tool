package com.dell.spt.storage.driver.coop.aws.s3;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A loopback server that accepts connections and reads everything a client sends, but never
 * answers a request: the stalled server that a request with no timeout waits for forever. The
 * one exception is the check that the bucket exists, which the driver makes before its first
 * request and which the server answers so that the request under test is the one that stalls.
 */
final class NeverRespondingServer implements AutoCloseable {

	private static final int READ_BUFFER_BYTES = 8192;
	private static final String HEADER_END = "\r\n\r\n";
	private static final String BUCKET_CHECK = "HEAD /bucket HTTP/1.1";
	private static final String EMPTY_OK = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

	private final ServerSocket server;
	private final AtomicInteger connections = new AtomicInteger();
	private final AtomicInteger stalledClosedByClient = new AtomicInteger();
	private final List<String> requestLines = new CopyOnWriteArrayList<>();
	private final List<Socket> sockets = new CopyOnWriteArrayList<>();

	NeverRespondingServer() throws IOException {
		server = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
		Thread.ofVirtual().name("never-responding-server-accept").start(this::acceptConnections);
	}

	int port() {
		return server.getLocalPort();
	}

	/** Returns the connections accepted so far. */
	int connections() {
		return connections.get();
	}

	/** Returns the connections with a request left unanswered that the client has closed. */
	int stalledClosedByClient() {
		return stalledClosedByClient.get();
	}

	/** Returns the first line of what each connection sent, such as {@code PUT /bucket/key HTTP/1.1}. */
	List<String> requestLines() {
		return List.copyOf(requestLines);
	}

	private void acceptConnections() {
		try {
			while (true) {
				final Socket socket = server.accept();
				sockets.add(socket);
				connections.incrementAndGet();
				Thread.ofVirtual().name("never-responding-server-read").start(() -> drain(socket));
			}
		} catch (final IOException closed) {
			// The server was closed.
		}
	}

	private void drain(final Socket socket) {
		final var received = new StringBuilder();
		boolean stalled = false;
		try {
			final var in = socket.getInputStream();
			final var out = socket.getOutputStream();
			final byte[] buffer = new byte[READ_BUFFER_BYTES];
			int read;
			while ((read = in.read(buffer)) >= 0) {
				if (stalled) {
					continue;
				}
				received.append(new String(buffer, 0, read, StandardCharsets.ISO_8859_1));
				int headerEnd;
				while (!stalled && (headerEnd = received.indexOf(HEADER_END)) >= 0) {
					final String head = received.substring(0, headerEnd);
					received.delete(0, headerEnd + HEADER_END.length());
					final int lineEnd = head.indexOf('\r');
					final String requestLine = lineEnd < 0 ? head : head.substring(0, lineEnd);
					requestLines.add(requestLine);
					if (BUCKET_CHECK.equals(requestLine)) {
						out.write(EMPTY_OK.getBytes(StandardCharsets.ISO_8859_1));
						out.flush();
					} else {
						stalled = true;
					}
				}
			}
			if (stalled) {
				stalledClosedByClient.incrementAndGet();
			}
		} catch (final IOException failure) {
			if (stalled && !server.isClosed()) {
				stalledClosedByClient.incrementAndGet();
			}
		}
	}

	@Override
	public void close() throws IOException {
		server.close();
		for (final Socket socket : sockets) {
			socket.close();
		}
	}
}
