package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.net.ServerSocket;
import java.net.InetAddress;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** Shared fixtures for endpoint-selection driver tests. */
final class EndpointSelectionTestSupport {

	static final Credential CREDENTIAL = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	static final int ITEM_SIZE = 1024;
	static final long RESULT_TIMEOUT_SECONDS = 10;
	static final String HOSTNAME = "s3.example.test";

	private EndpointSelectionTestSupport() {}

	static DataOperation<DataItem> dataOp(final OpType type, final String name) {
		return new DataOperationImpl<>(0, type, new DataItemImpl(name, name.hashCode() & 0xFFFF, ITEM_SIZE), null,
						"/bucket", CREDENTIAL, null, 0);
	}

	/** Collects published operation results. */
	static final class Results implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<DataItem>> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			for (var i = from; i < to; i++) {
				results.offer(values.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}

		Operation<?> poll() throws InterruptedException {
			return poll(10);
		}

		Operation<?> poll(final long millis) throws InterruptedException {
			return results.poll(millis, TimeUnit.MILLISECONDS);
		}

		Operation<?> await() throws InterruptedException {
			final var result = results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			assertNotNull(result, "no operation result before the timeout");
			return result;
		}
	}

	/** Accepts one TLS connection, records the SNI host name from its ClientHello and closes it. */
	static final class ClientHelloListener implements AutoCloseable {

		private static final int HANDSHAKE = 22;
		private static final int CLIENT_HELLO = 1;
		private static final int SERVER_NAME_EXTENSION = 0;

		private final ServerSocket socket = new ServerSocket(0, 8, InetAddress.getLoopbackAddress());
		private final Thread acceptor;
		private volatile boolean helloReceived;
		private volatile String serverName;

		ClientHelloListener() throws IOException {
			acceptor = new Thread(this::acceptOne, "client-hello-listener");
			acceptor.setDaemon(true);
			acceptor.start();
		}

		int port() {
			return socket.getLocalPort();
		}

		boolean helloReceived() throws InterruptedException {
			acceptor.join(TimeUnit.SECONDS.toMillis(RESULT_TIMEOUT_SECONDS));
			return helloReceived;
		}

		String serverName() throws InterruptedException {
			acceptor.join(TimeUnit.SECONDS.toMillis(RESULT_TIMEOUT_SECONDS));
			return serverName;
		}

		private void acceptOne() {
			try (final var connection = socket.accept()) {
				final var in = connection.getInputStream();
				final var header = in.readNBytes(5);
				if (header.length < 5 || header[0] != HANDSHAKE) {
					return;
				}
				final var record = in.readNBytes(((header[3] & 0xFF) << 8) | (header[4] & 0xFF));
				if (record.length > 0 && record[0] == CLIENT_HELLO) {
					helloReceived = true;
					serverName = parseServerName(record);
				}
			} catch (final IOException ignored) {
				// The test asserts on what was captured before the connection ended.
			}
		}

		private static String parseServerName(final byte[] hello) {
			var at = 4 + 2 + 32; // handshake header, client version, random
			at += 1 + (hello[at] & 0xFF); // session id
			at += 2 + u16(hello, at); // cipher suites
			at += 1 + (hello[at] & 0xFF); // compression methods
			final var end = at + 2 + u16(hello, at);
			at += 2;
			while (at + 4 <= end) {
				final var type = u16(hello, at);
				final var length = u16(hello, at + 2);
				if (type == SERVER_NAME_EXTENSION) {
					final var nameLength = u16(hello, at + 4 + 3);
					return new String(hello, at + 4 + 5, nameLength, StandardCharsets.US_ASCII);
				}
				at += 4 + length;
			}
			return null;
		}

		private static int u16(final byte[] data, final int at) {
			return ((data[at] & 0xFF) << 8) | (data[at + 1] & 0xFF);
		}

		@Override
		public void close() throws IOException {
			socket.close();
		}
	}
}
