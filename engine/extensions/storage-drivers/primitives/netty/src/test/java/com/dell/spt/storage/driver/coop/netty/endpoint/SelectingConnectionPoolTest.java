package com.dell.spt.storage.driver.coop.netty.endpoint;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Real loopback sockets; the listeners accept and hold connections until the test closes them. */
@Timeout(30)
class SelectingConnectionPoolTest {

	private final NioEventLoopGroup group = new NioEventLoopGroup(1);
	private final Bootstrap bootstrap = new Bootstrap().group(group).channel(NioSocketChannel.class);
	private final List<Holder> holders = new CopyOnWriteArrayList<>();

	@AfterEach
	void closeAll() throws Exception {
		for (final var holder : holders) {
			holder.close();
		}
		group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
	}

	@Test
	void pooledReleaseReusesTheConnectionToTheSameDestination() throws Exception {
		final var a = holder();
		final var pool = pool(List.of(a.address()), true, 4);

		final var first = pool.lease();
		pool.release(first);
		final var second = pool.lease();

		assertSame(first, second);
		assertEquals(1, a.accepted());
		pool.close();
	}

	@Test
	void pooledReleaseClosesConnectionsBeyondTheIdleLimit() throws Exception {
		final var a = holder();
		final var b = holder();
		final var pool = pool(List.of(a.address(), b.address()), true, 1);

		final var toA = pool.lease();
		final var toB = pool.lease();
		pool.release(toA);
		pool.release(toB);

		assertEquals(1, pool.idleChannelCount());
		assertTrue(toB.closeFuture().await(5, TimeUnit.SECONDS));
		assertTrue(toA.isActive());
		pool.close();
	}

	@Test
	void perRequestReleaseWaitsForTheServerThenClosesAfterTheGracePeriod() throws Exception {
		final var holding = holder();
		final var pool = pool(List.of(holding.address()), false, 0);
		final var channel = pool.lease();

		final var released = System.nanoTime();
		pool.release(channel);

		assertTrue(channel.isActive(), "the client must not close before the server's grace period");
		assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));
		final var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - released);
		assertTrue(elapsed >= EndpointSelectionConstants.SERVER_CLOSE_GRACE_MILLIS - 50, "closed after " + elapsed);
		pool.close();
	}

	@Test
	void perRequestReleaseCompletesAtOnceWhenTheServerClosesFirst() throws Exception {
		final var closing = holder();
		final var pool = pool(List.of(closing.address()), false, 0);
		final var channel = pool.lease();
		closing.closeAccepted();
		assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));

		pool.release(channel);

		assertEquals(0, pool.openChannelCount());
		pool.close();
	}

	@Test
	void closeClosesEveryConnectionAndRejectsNewAcquisitions() throws Exception {
		final var a = holder();
		final var pool = pool(List.of(a.address()), true, 4);
		final var leased = pool.lease();
		final var idle = pool.lease();
		pool.release(idle);

		pool.close();

		assertTrue(leased.closeFuture().await(5, TimeUnit.SECONDS));
		assertTrue(idle.closeFuture().await(5, TimeUnit.SECONDS));
		assertEquals(0, pool.idleChannelCount());
		assertThrows(ConnectException.class, pool::lease);
	}

	@Test
	void refusedDestinationFailsTheLeaseWithoutTryingAnother() throws Exception {
		final var live = holder();
		final int refusedPort;
		try (final var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			refusedPort = socket.getLocalPort();
		}
		final var pool = pool(List.of(new InetSocketAddress("127.0.0.1", refusedPort), live.address()), true, 4);

		assertThrows(ConnectException.class, pool::lease);
		assertEquals(0, live.accepted());
		assertTrue(pool.lease().isActive());
		pool.close();
	}

	@Test
	void bindingIsRequiredOnceBeforeUse() {
		final var pool = new SelectingConnectionPool(bootstrap, new NoopHandler());
		assertThrows(IllegalStateException.class, pool::acquire);

		final var destinations = new RoundRobinDestinations(List.of(new InetSocketAddress("127.0.0.1", 9)));
		pool.bind(destinations, true, 1, 1_000, 1_000);
		assertThrows(IllegalStateException.class, () -> pool.bind(destinations, true, 1, 1_000, 1_000));
		pool.close();
	}

	private SelectingConnectionPool pool(final List<InetSocketAddress> destinations, final boolean pooled,
					final int idleLimit) {
		final var pool = new SelectingConnectionPool(bootstrap, new NoopHandler());
		pool.bind(new RoundRobinDestinations(destinations), pooled, idleLimit, 2_000, 2_000);
		return pool;
	}

	private Holder holder() throws Exception {
		final var holder = new Holder();
		holders.add(holder);
		return holder;
	}

	private static final class NoopHandler extends AbstractChannelPoolHandler {
		@Override
		public void channelCreated(final Channel channel) {}
	}

	/** Accepts connections and keeps them open until told otherwise. */
	private static final class Holder implements AutoCloseable {

		private final ServerSocket server = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
		private final List<Socket> accepted = new CopyOnWriteArrayList<>();
		private final Thread acceptor = new Thread(this::accept, "connection-holder");

		Holder() throws Exception {
			acceptor.setDaemon(true);
			acceptor.start();
		}

		InetSocketAddress address() {
			return new InetSocketAddress("127.0.0.1", server.getLocalPort());
		}

		int accepted() throws InterruptedException {
			Thread.sleep(50);
			return accepted.size();
		}

		void closeAccepted() throws Exception {
			final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (accepted.isEmpty() && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			for (final var socket : accepted) {
				socket.close();
			}
		}

		private void accept() {
			try {
				while (!server.isClosed()) {
					accepted.add(server.accept());
				}
			} catch (final Exception ignored) {
				// Closed by the test.
			}
		}

		@Override
		public void close() throws Exception {
			server.close();
			for (final var socket : accepted) {
				socket.close();
			}
		}
	}
}
