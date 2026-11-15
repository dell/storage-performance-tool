package com.dell.spt.storage.driver.coop.netty.http.s3;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/**
 * A loopback HTTP/1.1 stub whose pace is chosen by the object name: it reads a PUT body or sends
 * a GET body at a fixed rate, stops reading, or never answers. Small socket buffers keep the
 * client's writes in step with the stub's reads. Optional TLS uses a key pair made by the JDK's
 * keytool, so no certificate library is needed.
 *
 * <ul>
 *   <li>{@code paced-*}: a PUT body is read, or a GET body sent, at {@link #rateBytesPerSec};
 *   <li>{@code quick-*}: answered at once;
 *   <li>{@code delay-*}: answered {@link #delayMillis} after the request body is read;
 *   <li>{@code silent-*}: the request is read, the response never comes;
 *   <li>{@code stall-*}: the first {@link #STALL_AFTER_BYTES} of the body are read, then nothing.
 * </ul>
 */
final class PacedS3Stub implements AutoCloseable {

	static final int STALL_AFTER_BYTES = 256 * 1024;
	private static final int SOCKET_BUFFER_BYTES = 64 * 1024;

	private final ServerSocket server;
	private final ExecutorService threads = Executors.newCachedThreadPool();
	private final CountDownLatch closed = new CountDownLatch(1);
	private final List<Socket> sockets = new CopyOnWriteArrayList<>();
	private final AtomicInteger connections = new AtomicInteger();
	private final Map<String, Integer> connectionByName = new ConcurrentHashMap<>();
	private final long rateBytesPerSec;
	private final long delayMillis;
	private final long getBodyBytes;

	PacedS3Stub(final SSLContext tls, final long rateBytesPerSec, final long delayMillis, final long getBodyBytes)
					throws IOException {
		this.rateBytesPerSec = rateBytesPerSec;
		this.delayMillis = delayMillis;
		this.getBodyBytes = getBodyBytes;
		server = tls == null ? new ServerSocket() : tls.getServerSocketFactory().createServerSocket();
		// Set before bind so that accepted sockets inherit it.
		server.setReceiveBufferSize(SOCKET_BUFFER_BYTES);
		server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
		threads.execute(this::acceptLoop);
	}

	int port() {
		return server.getLocalPort();
	}

	/** The connection, numbered from 1 in accept order, that carried the named object's request. */
	Integer connectionOf(final String name) {
		return connectionByName.get(name);
	}

	private void acceptLoop() {
		while (!server.isClosed()) {
			try {
				final var socket = server.accept();
				socket.setSendBufferSize(SOCKET_BUFFER_BYTES);
				socket.setTcpNoDelay(true);
				final int connection = connections.incrementAndGet();
				sockets.add(socket);
				threads.execute(() -> serve(socket, connection));
			} catch (final IOException e) {
				return;
			}
		}
	}

	private void serve(final Socket socket, final int connection) {
		try (socket) {
			final var in = new BufferedInputStream(socket.getInputStream(), 16 * 1024);
			final var out = socket.getOutputStream();
			while (true) {
				final var head = readHead(in);
				if (head == null) {
					return;
				}
				final var requestLine = head.get(0);
				final int methodEnd = requestLine.indexOf(' ');
				final var method = requestLine.substring(0, methodEnd);
				final var path = requestLine.substring(methodEnd + 1, requestLine.indexOf(' ', methodEnd + 1));
				final var name = path.substring(path.lastIndexOf('/') + 1);
				final var length = contentLength(head);
				connectionByName.put(name, connection);
				if (name.startsWith("stall-")) {
					skip(in, Math.min(length, STALL_AFTER_BYTES), 0);
					closed.await();
					return;
				}
				skip(in, length, name.startsWith("paced-") && "PUT".equals(method) ? rateBytesPerSec : 0);
				if (name.startsWith("silent-")) {
					closed.await();
					return;
				}
				if (name.startsWith("delay-") && closed.await(delayMillis, TimeUnit.MILLISECONDS)) {
					return;
				}
				if ("GET".equals(method)) {
					answer(out, getBodyBytes);
					send(out, getBodyBytes, name.startsWith("paced-") ? rateBytesPerSec : 0);
				} else {
					answer(out, 0);
				}
				out.flush();
			}
		} catch (final IOException e) {
			// The client closed the connection: nothing more to serve.
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	private static void answer(final OutputStream out, final long bodyBytes) throws IOException {
		out.write(("HTTP/1.1 200 OK\r\nETag: \"stub\"\r\nContent-Length: " + bodyBytes + "\r\n\r\n")
						.getBytes(StandardCharsets.US_ASCII));
		out.flush();
	}

	private static List<String> readHead(final InputStream in) throws IOException {
		final var lines = new java.util.ArrayList<String>();
		final var line = new ByteArrayOutputStream();
		int b;
		while ((b = in.read()) >= 0) {
			if (b == '\n') {
				final var text = line.toString(StandardCharsets.US_ASCII).strip();
				line.reset();
				if (text.isEmpty()) {
					return lines.isEmpty() ? readHead(in) : lines;
				}
				lines.add(text);
			} else {
				line.write(b);
			}
		}
		return null;
	}

	private static long contentLength(final List<String> head) {
		for (final var line : head) {
			if (line.toLowerCase(Locale.ROOT).startsWith("content-length:")) {
				return Long.parseLong(line.substring(line.indexOf(':') + 1).strip());
			}
		}
		return 0;
	}

	/** Reads {@code n} bytes, at {@code rate} bytes per second when it is positive. */
	private static void skip(final InputStream in, final long n, final long rate)
					throws IOException, InterruptedException {
		final var buf = new byte[16 * 1024];
		final long start = System.nanoTime();
		long got = 0;
		while (got < n) {
			final int r = in.read(buf, 0, (int) Math.min(buf.length, n - got));
			if (r < 0) {
				throw new IOException("EOF after " + got + " of " + n + " bytes");
			}
			got += r;
			pace(start, got, rate);
		}
	}

	private static void send(final OutputStream out, final long n, final long rate)
					throws IOException, InterruptedException {
		final var buf = new byte[16 * 1024];
		final long start = System.nanoTime();
		long sent = 0;
		while (sent < n) {
			final int w = (int) Math.min(buf.length, n - sent);
			out.write(buf, 0, w);
			out.flush();
			sent += w;
			pace(start, sent, rate);
		}
	}

	private static void pace(final long start, final long done, final long rate) throws InterruptedException {
		if (rate > 0) {
			final long dueNanos = done * 1_000_000_000L / rate - (System.nanoTime() - start);
			if (dueNanos > 0) {
				TimeUnit.NANOSECONDS.sleep(dueNanos);
			}
		}
	}

	@Override
	public void close() throws Exception {
		closed.countDown();
		server.close();
		for (final var socket : sockets) {
			socket.close();
		}
		threads.shutdownNow();
		threads.awaitTermination(5, TimeUnit.SECONDS);
	}

	/** A server TLS context over a fresh key pair from the running JDK's keytool. */
	static SSLContext selfSignedTls(final Path dir) throws Exception {
		final var keystore = dir.resolve("stub.p12");
		final var password = "stub-pass";
		final var keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString();
		final var process = new ProcessBuilder(keytool, "-genkeypair", "-alias", "stub", "-keyalg", "EC",
						"-groupname", "secp256r1", "-dname", "CN=localhost", "-validity", "2", "-storetype", "PKCS12",
						"-keystore", keystore.toString(), "-storepass", password, "-keypass", password)
						.redirectErrorStream(true)
						.start();
		final var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
			throw new IllegalStateException("keytool failed: " + output);
		}
		final var store = KeyStore.getInstance("PKCS12");
		try (final var in = Files.newInputStream(keystore)) {
			store.load(in, password.toCharArray());
		}
		final var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		keys.init(store, password.toCharArray());
		final var context = SSLContext.getInstance("TLS");
		context.init(keys.getKeyManagers(), null, null);
		return context;
	}
}
