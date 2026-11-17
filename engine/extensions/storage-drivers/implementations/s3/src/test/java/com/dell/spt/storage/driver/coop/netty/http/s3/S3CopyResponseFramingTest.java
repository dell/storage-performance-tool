package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A copy answered {@code 200 OK} with an S3 {@code <Error>} document fails, whatever frames the body:
 * chunked, {@code Content-Length}, or ended by the connection's close (HTTP/1.1 and 1.0). The
 * handler runs behind the driver's {@link HttpClientCodec} on a loopback socket, on NIO and, where
 * available, epoll. A close-delimited body's last content comes from the codec when the channel goes
 * inactive, after its close future completes, so nothing may release the copy's buffer on close.
 */
@Timeout(120)
final class S3CopyResponseFramingTest {

	private static final String TRANSIENT = "<Error><Code>InternalError</Code><Message>m</Message></Error>";
	private static final String PERMANENT = "<Error><Code>AccessDenied</Code><Message>m</Message></Error>";

	@Test
	void aCopyAnswered200WithAnErrorBodyFailsInEveryFraming() throws Exception {
		final List<String> wrong = new ArrayList<>();
		for (final boolean epoll : Epoll.isAvailable() ? new boolean[]{false, true
		} : new boolean[]{false
		}) {
			for (final var framing : Framing.values()) {
				check(wrong, epoll, framing, TRANSIENT, Operation.Status.RESP_FAIL_SVC);
				check(wrong, epoll, framing, PERMANENT, Operation.Status.RESP_FAIL_CLIENT);
			}
		}
		assertTrue(wrong.isEmpty(), () -> String.join("\n", wrong));
	}

	@Test
	void aCopyResultEndedByTheCloseStaysSuccessful() throws Exception {
		final String result = "<CopyObjectResult><ETag>\"e\"</ETag></CopyObjectResult>";
		assertEquals(Operation.Status.SUCC.toString(), statusSeenByComplete(Framing.CLOSE_DELIMITED_1_1.response(result), false));
	}

	private static void check(final List<String> wrong, final boolean epoll, final Framing framing, final String body,
					final Operation.Status expected) throws Exception {
		final String seen = statusSeenByComplete(framing.response(body), epoll);
		if (!expected.toString().equals(seen)) {
			wrong.add((epoll ? "epoll " : "nio ") + framing + " " + body + ": expected " + expected + ", complete() saw " + seen);
		}
	}

	private enum Framing {
		CHUNKED, CONTENT_LENGTH, CLOSE_DELIMITED_1_1, CLOSE_DELIMITED_1_0;

		String response(final String body) {
			return switch (this) {
			case CHUNKED -> "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
							+ Integer.toHexString(body.length()) + "\r\n" + body + "\r\n0\r\n\r\n";
			case CONTENT_LENGTH -> "HTTP/1.1 200 OK\r\nContent-Length: " + body.length() + "\r\nConnection: close\r\n\r\n" + body;
			case CLOSE_DELIMITED_1_1 -> "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n" + body;
			case CLOSE_DELIMITED_1_0 -> "HTTP/1.0 200 OK\r\n\r\n" + body;
			};
		}
	}

	/** Sends one copy, answers it with {@code response} and closes; returns the status the driver's complete() saw. */
	@SuppressWarnings("unchecked")
	private static String statusSeenByComplete(final String response, final boolean epoll) throws Exception {
		final S3StorageDriver<DataItemImpl, Operation<DataItemImpl>> driver = mock(S3StorageDriver.class);
		when(driver.copyErrorCodeLog()).thenReturn(S3ResponseHandler.CompletionErrorCodeLog.forCopies());
		when(driver.isStarted()).thenReturn(true);
		final var seen = new AtomicReference<String>();
		final var done = new CountDownLatch(1);
		doAnswer(invocation -> {
			seen.compareAndSet(null, String.valueOf(invocation.<Operation<DataItemImpl>> getArgument(1).status()));
			done.countDown();
			return null;
		}).when(driver).complete(any(), any());
		final Operation<DataItemImpl> op = new DataOperationImpl<DataItemImpl>(
						0, OpType.CREATE, new DataItemImpl("k0", 0L, 2048), "/src", "/dst/", Credential.NONE, null, 0);
		final EventLoopGroup group = epoll ? new EpollEventLoopGroup(1) : new NioEventLoopGroup(1);
		try (final var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			final var serverThread = new Thread(() -> {
				try (final var socket = server.accept()) {
					final var in = socket.getInputStream();
					final var request = new ByteArrayOutputStream();
					while (!request.toString(StandardCharsets.ISO_8859_1).contains("\r\n\r\n")) {
						final int b = in.read();
						if (b < 0) {
							break;
						}
						request.write(b);
					}
					socket.getOutputStream().write(response.getBytes(StandardCharsets.UTF_8));
					socket.getOutputStream().flush();
				} catch (final Exception e) {
					throw new IllegalStateException(e);
				}
			}, "copy-framing-stub");
			serverThread.start();
			final Channel channel = new Bootstrap().group(group)
							.channel(epoll ? EpollSocketChannel.class : NioSocketChannel.class)
							.handler(new ChannelInitializer<Channel>() {
								@Override
								protected void initChannel(final Channel ch) {
									// the driver's codec arguments
									ch.pipeline().addLast(new HttpClientCodec(4096, 8192, 8192, true));
									ch.pipeline().addLast(new S3ResponseHandler<>(driver, false, false, null));
								}
							})
							.connect(InetAddress.getLoopbackAddress(), server.getLocalPort()).sync().channel();
			channel.attr(NettyStorageDriver.ATTR_KEY_OPERATION).set(op);
			final var request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, "/dst/k0", Unpooled.EMPTY_BUFFER);
			request.headers().set("Host", "127.0.0.1").set("Content-Length", 0).set("x-amz-copy-source", "/src/k0");
			channel.writeAndFlush(request).sync();
			final boolean completed = done.await(10, TimeUnit.SECONDS);
			channel.closeFuture().await(5, TimeUnit.SECONDS);
			serverThread.join(5_000);
			return completed ? seen.get() : "NOT-COMPLETED";
		} finally {
			group.shutdownGracefully(0, 1, TimeUnit.SECONDS).sync();
		}
	}
}
