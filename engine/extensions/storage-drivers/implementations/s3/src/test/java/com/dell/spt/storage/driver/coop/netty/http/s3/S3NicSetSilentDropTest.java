package com.dell.spt.storage.driver.coop.netty.http.s3;

import static com.dell.spt.storage.driver.coop.netty.http.s3.EndpointSelectionTestSupport.dataOp;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.nics;
import static com.dell.spt.storage.driver.coop.netty.http.s3.NicSetLoopback.port;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import io.netty.channel.epoll.Epoll;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A NIC whose endpoint goes silent, as under an nft drop, and comes back (nic-set-retry-dead-path 06). The
 * silent endpoint is a user-space proxy: it moves no byte on its connections, and its one-slot accept queue
 * is filled, so new SYNs go unanswered.
 */
@Timeout(90)
final class S3NicSetSilentDropTest {

	// storage-net-timeoutMilliSec in NicSetLoopback.root.
	private static final long TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(2_000);
	private static final long DEAD_AFTER_NANOS = TimeUnit.SECONDS.toNanos(1);
	private static final long REVIVE_AFTER_NANOS = TimeUnit.SECONDS.toNanos(13);
	private static final long END_AFTER_NANOS = TimeUnit.SECONDS.toNanos(19);

	@Test
	void aSilentNicTakesNoOperationAfterItsFirstFailuresAndComesBack() throws Exception {
		assumeTrue(Epoll.isAvailable());
		final var run = runThroughAFault(false);
		final var firstDeadFailure = run.firstFailureOnDeadNic();
		assertTrue(firstDeadFailure < run.reviveAt, "the dead NIC's attempts failed before the revive");
		final var held = run.onDeadNic(firstDeadFailure + TimeUnit.MILLISECONDS.toNanos(50), run.reviveAt);
		assertEquals(List.of(), held, "no operation ends on the silent NIC between its first failures and the revive");
		assertTrue(run.liveSuccesses(run.deadAt + TIMEOUT_NANOS + DEAD_AFTER_NANOS, run.reviveAt) > 0,
						"the live NIC keeps serving");
		assertTrue(run.firstSuccessOnDeadNicAfterRevive() - run.reviveAt < TimeUnit.SECONDS.toNanos(5),
						"the NIC takes operations again soon after it comes back");
	}

	@Test
	void aNicThatAcceptsButNeverAnswersHoldsOneTrialAtATime() throws Exception {
		assumeTrue(Epoll.isAvailable());
		final var run = runThroughAFault(true);
		final var firstDeadFailure = run.firstFailureOnDeadNic();
		final var trials = run.onDeadNic(firstDeadFailure + TimeUnit.MILLISECONDS.toNanos(50), run.reviveAt);
		final long window = run.reviveAt - firstDeadFailure;
		assertTrue(trials.size() <= window / TIMEOUT_NANOS + 1,
						"at most one trial of a timeout at a time: " + trials.size() + " in " + window / 1e9 + " s");
		assertTrue(run.liveSuccesses(run.deadAt + TIMEOUT_NANOS + DEAD_AFTER_NANOS, run.reviveAt) > 0,
						"the live NIC keeps serving");
		assertTrue(run.firstSuccessOnDeadNicAfterRevive() - run.reviveAt < TimeUnit.SECONDS.toNanos(5),
						"the NIC takes operations again soon after it answers");
	}

	private record Result(long endNanos, String node, Operation.Status status) {}

	private record Run(List<Result> results, String deadNode, long deadAt, long reviveAt) {

		long firstFailureOnDeadNic() {
			return results.stream()
							.filter(r -> r.endNanos > deadAt && deadNode.equals(r.node) && r.status != Operation.Status.SUCC)
							.mapToLong(Result::endNanos)
							.min()
							.orElse(Long.MAX_VALUE);
		}

		/** Results on the dead NIC, or with no node (a failed setup), ending in [from, to). */
		List<Result> onDeadNic(final long from, final long to) {
			return results.stream()
							.filter(r -> r.endNanos >= from && r.endNanos < to && (r.node == null || deadNode.equals(r.node)))
							.toList();
		}

		long liveSuccesses(final long from, final long to) {
			return results.stream()
							.filter(r -> r.endNanos >= from && r.endNanos < to && !deadNode.equals(r.node)
											&& r.status == Operation.Status.SUCC)
							.count();
		}

		long firstSuccessOnDeadNicAfterRevive() {
			return results.stream()
							.filter(r -> r.endNanos > reviveAt && deadNode.equals(r.node) && r.status == Operation.Status.SUCC)
							.mapToLong(Result::endNanos)
							.min()
							.orElse(Long.MAX_VALUE);
		}
	}

	private static Run runThroughAFault(final boolean acceptOnly) throws Exception {
		try (var stub = new NicSetLoopback()) {
			final var endpoints = stub.listeners(2);
			try (var proxy = new SilentProxy(port(endpoints.get(1)))) {
				final var deadNode = "127.0.0.1:" + proxy.port();
				final var root = NicSetLoopback.root(List.of(endpoints.get(0), deadNode), nics(2), 4);
				root.val("storage-net-endpoint-connect-timeoutMilliSec", 3_000);
				try (var driver = NicSetLoopback.driver(root.configVal("storage"), false)) {
					final var run = NicSetLoopback.started(driver);
					final List<Result> results = new ArrayList<>();
					final long t0 = System.nanoTime();
					var submitted = 0;
					for (; submitted < 4; submitted++) {
						run.driver().put(dataOp(OpType.CREATE, "w" + submitted));
					}
					long deadAt = 0;
					long reviveAt = 0;
					while (true) {
						final var result = run.results().await();
						final long now = System.nanoTime() - t0;
						results.add(new Result(now, result.nodeAddr(), result.status()));
						if (deadAt == 0 && now > DEAD_AFTER_NANOS) {
							proxy.die(acceptOnly);
							deadAt = System.nanoTime() - t0;
						}
						if (reviveAt == 0 && now > REVIVE_AFTER_NANOS) {
							proxy.revive();
							reviveAt = System.nanoTime() - t0;
						}
						if (now > END_AFTER_NANOS) {
							break;
						}
						run.driver().put(dataOp(OpType.CREATE, "w" + submitted++));
					}
					for (var i = 0; i < 3; i++) {
						final var result = run.results().await();
						results.add(new Result(System.nanoTime() - t0, result.nodeAddr(), result.status()));
					}
					return new Run(results, deadNode, deadAt, reviveAt);
				}
			}
		}
	}

	/** Forwards to a target until it dies: then it moves no byte, and, unless it accepts only, answers no SYN. */
	private static final class SilentProxy implements AutoCloseable {

		private final ServerSocketChannel listener;
		private final int targetPort;
		private final List<SocketChannel> fillers = new CopyOnWriteArrayList<>();
		private final List<SocketChannel> all = new CopyOnWriteArrayList<>();
		private volatile boolean dead;
		private volatile boolean acceptOnly;

		SilentProxy(final int targetPort) throws IOException {
			this.targetPort = targetPort;
			listener = ServerSocketChannel.open();
			listener.setOption(StandardSocketOptions.SO_REUSEADDR, true);
			listener.bind(new InetSocketAddress("127.0.0.1", 0), 1);
			final var acceptor = new Thread(this::acceptLoop, "silent-proxy-accept");
			acceptor.setDaemon(true);
			acceptor.start();
		}

		int port() {
			return listener.socket().getLocalPort();
		}

		private void acceptLoop() {
			while (listener.isOpen()) {
				try {
					if (dead && !acceptOnly) {
						Thread.sleep(5);
						continue;
					}
					final var client = listener.accept();
					all.add(client);
					if (dead) {
						continue;
					}
					final var server = SocketChannel.open(new InetSocketAddress("127.0.0.1", targetPort));
					all.add(server);
					pump(client, server);
					pump(server, client);
				} catch (final Exception e) {
					if (!listener.isOpen()) {
						return;
					}
				}
			}
		}

		private void pump(final SocketChannel from, final SocketChannel to) {
			final var pump = new Thread(() -> {
				final var buffer = ByteBuffer.allocate(64 * 1024);
				try {
					while (true) {
						while (dead) {
							Thread.sleep(5);
						}
						buffer.clear();
						if (from.read(buffer) < 0) {
							to.shutdownOutput();
							return;
						}
						if (dead) {
							continue;
						}
						buffer.flip();
						while (buffer.hasRemaining()) {
							to.write(buffer);
						}
					}
				} catch (final Exception e) {
					// Closed by the test.
				}
			}, "silent-proxy-pump");
			pump.setDaemon(true);
			pump.start();
		}

		void die(final boolean acceptOnly) throws Exception {
			this.acceptOnly = acceptOnly;
			dead = true;
			Thread.sleep(20);
			if (acceptOnly) {
				return;
			}
			// With nobody accepting, connects that complete fill the queue; the next SYN gets no answer.
			final var address = new InetSocketAddress("127.0.0.1", port());
			while (fillers.size() < 8) {
				final var filler = SocketChannel.open();
				filler.configureBlocking(false);
				fillers.add(filler);
				if (!filler.connect(address)) {
					final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200);
					while (System.nanoTime() < deadline) {
						if (filler.finishConnect()) {
							break;
						}
						Thread.sleep(5);
					}
					if (!filler.isConnected()) {
						return;
					}
				}
			}
			throw new IOException("the accept queue never filled");
		}

		void revive() throws IOException {
			for (final var filler : fillers) {
				filler.close();
			}
			fillers.clear();
			acceptOnly = false;
			dead = false;
		}

		@Override
		public void close() throws IOException {
			listener.close();
			for (final var filler : fillers) {
				filler.close();
			}
			for (final var channel : all) {
				channel.close();
			}
		}
	}
}
