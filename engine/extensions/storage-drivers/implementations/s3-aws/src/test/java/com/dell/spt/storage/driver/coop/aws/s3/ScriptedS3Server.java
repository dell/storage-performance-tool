package com.dell.spt.storage.driver.coop.aws.s3;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A loopback server whose behavior depends on the object a request names. It answers the check
 * that the bucket exists, which the driver makes before its first request, so that the request
 * under test is the one that misbehaves:
 *
 * <ul>
 *   <li>{@code progress}: a GET whose body arrives in chunks, one a second, for longer than the
 *       stall timeouts the tests use;
 *   <li>{@code body-stall}: a GET that sends its headers and one chunk of the body, then stops;
 *       a HEAD of either object, which the CRT transfer client makes first, is answered;
 *   <li>{@code upload-stall}: a PUT whose body the server reads up to a limit and then stops reading;
 *   <li>{@code stall-once}: the first request is read and never answered, and the same request
 *       made again, as a client does after it gives up the connection, succeeds;
 *   <li>any other object: the request is read and never answered.
 * </ul>
 *
 * <p>A key may continue the name of a scripted object, so that a test can tell its requests apart.
 */
final class ScriptedS3Server implements AutoCloseable {

	static final String PROGRESS_KEY = "progress";
	static final String BODY_STALL_KEY = "body-stall";
	static final String UPLOAD_STALL_KEY = "upload-stall";
	static final String STALL_ONCE_KEY = "stall-once";
	static final int CHUNK_BYTES = 1024;
	static final int CHUNKS = 6;
	static final long CHUNK_INTERVAL_MS = 1000;

	/** How much of an upload the server reads before it stops reading. */
	static final long UPLOAD_READ_LIMIT_BYTES = 1024 * 1024;

	private static final int READ_BUFFER_BYTES = 8192;
	private static final int ACCEPT_POLL_MS = 50;
	private static final int FILLER_CONNECT_TIMEOUT_MS = 200;
	private static final int MAX_FILLERS = 32;
	private static final String HEADER_END = "\r\n\r\n";
	private static final String BUCKET_CHECK = "HEAD /bucket HTTP/1.1";
	private static final String EMPTY_OK = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";

	private final ServerSocket server;
	private final AtomicInteger connections = new AtomicInteger();
	private final AtomicInteger stalledClosedByClient = new AtomicInteger();
	private final AtomicLong uploadBytesRead = new AtomicLong();
	private final List<String> requestLines = new CopyOnWriteArrayList<>();
	private final List<String> requestHeads = new CopyOnWriteArrayList<>();
	private final List<String> stalledRequestsGivenUp = new CopyOnWriteArrayList<>();
	private final Set<String> stalledOnce = ConcurrentHashMap.newKeySet();
	private final List<Socket> sockets = new CopyOnWriteArrayList<>();
	private final CountDownLatch closed = new CountDownLatch(1);
	private volatile boolean accepting = true;

	ScriptedS3Server() throws IOException {
		this(50);
	}

	/** @param backlog the length of the queue of connections waiting to be accepted */
	ScriptedS3Server(final int backlog) throws IOException {
		server = new ServerSocket(0, backlog, InetAddress.getLoopbackAddress());
		server.setSoTimeout(ACCEPT_POLL_MS);
		Thread.ofVirtual().name("scripted-s3-server-accept").start(this::acceptConnections);
	}

	int port() {
		return server.getLocalPort();
	}

	/** Returns the connections accepted so far. */
	int connections() {
		return connections.get();
	}

	/** Returns the connections left with a request unanswered that the client has closed. */
	int stalledClosedByClient() {
		return stalledClosedByClient.get();
	}

	/** Returns the first line of each request left unanswered whose connection the client closed. */
	List<String> stalledRequestsGivenUp() {
		return List.copyOf(stalledRequestsGivenUp);
	}

	/** Returns the first line of each request received, such as {@code PUT /bucket/key HTTP/1.1}. */
	List<String> requestLines() {
		return List.copyOf(requestLines);
	}

	/** Returns how much of the stalled upload the server read before it stopped reading. */
	long uploadBytesRead() {
		return uploadBytesRead.get();
	}

	/** Returns the request line and headers of each request received. */
	List<String> requestHeads() {
		return List.copyOf(requestHeads);
	}

	/**
	 * Stops accepting connections and fills the queue of connections waiting to be accepted, so
	 * that a client which connects now waits for a connection that never comes. The connections
	 * accepted before stay served.
	 */
	void refuseNewConnections() throws Exception {
		accepting = false;
		Thread.sleep(2L * ACCEPT_POLL_MS);
		for (int filler = 0; filler < MAX_FILLERS; filler++) {
			final Socket socket = new Socket();
			sockets.add(socket);
			try {
				socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port()), FILLER_CONNECT_TIMEOUT_MS);
			} catch (final SocketTimeoutException full) {
				return;
			}
		}
		throw new IllegalStateException("the queue of connections to accept did not fill");
	}

	private void acceptConnections() {
		while (!server.isClosed()) {
			if (!accepting) {
				try {
					Thread.sleep(ACCEPT_POLL_MS);
				} catch (final InterruptedException interrupted) {
					return;
				}
				continue;
			}
			try {
				final Socket socket = server.accept();
				sockets.add(socket);
				connections.incrementAndGet();
				Thread.ofVirtual().name("scripted-s3-server-read").start(() -> serve(socket));
			} catch (final SocketTimeoutException idle) {
				// Poll again, so that a server told to stop accepting is not left inside accept().
			} catch (final IOException failure) {
				return;
			}
		}
	}

	private void serve(final Socket socket) {
		final var received = new StringBuilder();
		boolean stalled = false;
		String stalledRequest = null;
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
					requestHeads.add(head);
					if (requestLine.contains("/bucket/" + STALL_ONCE_KEY) && !stalledOnce.add(requestLine)) {
						answerRetry(requestLine, head, received, in, out, buffer);
						continue;
					}
					stalled = respond(requestLine, head, out);
					stalledRequest = stalled ? requestLine : null;
					if (requestLine.startsWith("PUT /bucket/" + UPLOAD_STALL_KEY)) {
						long bodyBytes = received.length();
						while (bodyBytes < UPLOAD_READ_LIMIT_BYTES && (read = in.read(buffer)) >= 0) {
							bodyBytes += read;
						}
						uploadBytesRead.set(bodyBytes);
						// Stop reading: the rest of the upload backs up in the buffers of the connection.
						closed.await();
						return;
					}
				}
			}
			if (stalled) {
				givenUp(stalledRequest);
			}
		} catch (final IOException failure) {
			if (stalled && !server.isClosed()) {
				givenUp(stalledRequest);
			}
		} catch (final InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private void givenUp(final String stalledRequest) {
		stalledClosedByClient.incrementAndGet();
		stalledRequestsGivenUp.add(stalledRequest);
	}

	/** Reads the body of a request made again after a stall and answers it as S3 answers success. */
	private static void answerRetry(
					final String requestLine,
					final String head,
					final StringBuilder received,
					final InputStream in,
					final OutputStream out,
					final byte[] buffer) throws IOException {
		if (head.toLowerCase(Locale.ROOT).contains("\r\nexpect: 100-continue")) {
			write(out, "HTTP/1.1 100 Continue\r\n\r\n");
		}
		long remaining = contentLength(head);
		final int buffered = (int) Math.min(remaining, received.length());
		received.delete(0, buffered);
		remaining -= buffered;
		while (remaining > 0) {
			final int read = in.read(buffer, 0, (int) Math.min(buffer.length, remaining));
			if (read < 0) {
				throw new EOFException("the client closed the connection within a request body");
			}
			remaining -= read;
		}
		// A 204 without any header is read by the CRT clients as a response without a status.
		final String date = "Date: " + DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC)) + "\r\n";
		write(out, requestLine.startsWith("DELETE ")
						? "HTTP/1.1 204 No Content\r\n" + date + "\r\n"
						: "HTTP/1.1 200 OK\r\n" + date + "ETag: \"scripted\"\r\nContent-Length: 0\r\n\r\n");
	}

	private static long contentLength(final String head) {
		for (final String line : head.split("\r\n")) {
			if (line.regionMatches(true, 0, "Content-Length:", 0, "Content-Length:".length())) {
				return Long.parseLong(line.substring("Content-Length:".length()).trim());
			}
		}
		return 0;
	}

	/** Answers a request as the scripted object says, and returns whether it left the connection stalled. */
	private boolean respond(final String requestLine, final String head, final OutputStream out)
					throws IOException, InterruptedException {
		if (BUCKET_CHECK.equals(requestLine)) {
			write(out, EMPTY_OK);
			return false;
		}
		if (requestLine.startsWith("HEAD /bucket/" + PROGRESS_KEY)
						|| requestLine.startsWith("HEAD /bucket/" + BODY_STALL_KEY)) {
			write(out, "HTTP/1.1 200 OK\r\nContent-Length: " + CHUNKS * CHUNK_BYTES
							+ "\r\nContent-Type: application/octet-stream\r\nAccept-Ranges: bytes\r\nETag: \"scripted\"\r\n\r\n");
			return false;
		}
		if (requestLine.startsWith("GET /bucket/" + PROGRESS_KEY)) {
			writeBody(out, head, CHUNKS);
			return false;
		}
		if (requestLine.startsWith("GET /bucket/" + BODY_STALL_KEY)) {
			writeBody(out, head, 1);
			return true;
		}
		return true;
	}

	/** Advertises the whole body and sends the given number of its chunks, one every interval. */
	private void writeBody(final OutputStream out, final String head, final int chunks)
					throws IOException, InterruptedException {
		final int total = CHUNKS * CHUNK_BYTES;
		final boolean ranged = head.toLowerCase(Locale.ROOT).contains("\r\nrange:");
		write(out, (ranged ? "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-" + (total - 1) + "/" + total + "\r\n"
						: "HTTP/1.1 200 OK\r\n")
						+ "Content-Length: " + total + "\r\nContent-Type: application/octet-stream\r\n\r\n");
		final byte[] chunk = new byte[CHUNK_BYTES];
		for (int sent = 0; sent < chunks; sent++) {
			out.write(chunk);
			out.flush();
			if (sent + 1 < chunks) {
				Thread.sleep(CHUNK_INTERVAL_MS);
			}
		}
	}

	private static void write(final OutputStream out, final String text) throws IOException {
		out.write(text.getBytes(StandardCharsets.ISO_8859_1));
		out.flush();
	}

	@Override
	public void close() throws IOException {
		closed.countDown();
		server.close();
		for (final Socket socket : sockets) {
			socket.close();
		}
	}
}
