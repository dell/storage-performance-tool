package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriverBase.NamedIoGroup;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** The base group's size with and without a NIC set, and the groups a NIC-set driver adds. */
@SuppressWarnings("unchecked")
class NettyStorageDriverIoGroupsTest {

	private static final String TEST_SEED = "7a42d9c483244167";

	private static final class GroupsDriver extends NettyStorageDriverBase<Item, Operation<Item>> {
		final AtomicBoolean baseRunningAtHook = new AtomicBoolean();
		volatile boolean hookRan;

		GroupsDriver(final Config storageConfig) throws Exception {
			super("netty-io-groups", DataInput.instance(null, TEST_SEED, new SizeInBytes("64KB"), 4, false, 0.0, true),
							storageConfig, false, 3);
		}

		@Override
		protected NonBlockingConnPool createConnectionPool() {
			return mock(NonBlockingConnPool.class);
		}

		@Override
		protected void beforeIoShutdown() {
			hookRan = true;
			baseRunningAtHook.set(!baseIoGroup().isShuttingDown());
		}

		@Override
		protected void sendRequest(final Channel channel, final Operation<Item> op) {}

		@Override
		protected String requestNewPath(final String path) {
			return path;
		}

		@Override
		protected String requestNewAuthToken(final Credential credential) {
			return "token";
		}

		@Override
		public List<Item> list(
						final ItemFactory<Item> itemFactory, final String path, final String prefix,
						final int idRadix, final Item lastPrevItem, final int count) throws IOException {
			return List.of();
		}
	}

	private static Config storageConfig(final int threads, final List<String> interfaces) {
		final var config = NettyStorageDriverBaseTest.storageConfig(1);
		when(config.intVal("driver-threads")).thenReturn(threads);
		when(config.listVal("net-interfaces")).thenReturn((List) interfaces);
		return config;
	}

	private static int loops(final EventLoopGroup group) {
		return ((MultithreadEventLoopGroup) group).executorCount();
	}

	@Test
	void defaultRouteKeepsTheConfiguredGroupSize() throws Exception {
		for (final var threads : List.of(1, 3)) {
			try (final var driver = new GroupsDriver(storageConfig(threads, List.of()))) {
				assertEquals(threads, loops(driver.baseIoGroup()));
				assertTrue(driver.extraIoGroups().isEmpty());
			}
		}
	}

	@Test
	void nicSetBaseGroupHasTwoLoopsWhateverTheThreadSetting() throws Exception {
		for (final var threads : List.of(0, 1, 7)) {
			try (final var driver = new GroupsDriver(storageConfig(threads, List.of("ens2np0")))) {
				assertEquals(NettyStorageDriverBase.NIC_SET_BASE_IO_WORKERS, loops(driver.baseIoGroup()));
			}
		}
	}

	@Test
	void createdGroupsShareTheTransportAndStopWithTheBaseGroup() throws Exception {
		final var driver = new GroupsDriver(storageConfig(1, List.of("ens2np0", "ens10np0")));
		try {
			driver.start();
			final var first = driver.createIoGroup(3, "ens2np0");
			final var second = driver.createIoGroup(3, "ens10np0");
			assertSame(driver.baseIoGroup().getClass(), first.getClass());
			assertEquals(3, loops(first));
			assertTrue(first.submit(() -> Thread.currentThread().getName()).get(5, TimeUnit.SECONDS)
							.startsWith("ioWorker-ens2np0"));
			assertEquals(List.of("ens2np0", "ens10np0"),
							driver.extraIoGroups().stream().map(NamedIoGroup::name).toList());

			driver.stop();

			assertTrue(driver.hookRan);
			assertTrue(driver.baseRunningAtHook.get(), "the hook runs before any group shuts down");
			assertTrue(driver.baseIoGroup().isTerminated());
			assertTrue(first.isTerminated());
			assertTrue(second.isTerminated());
		} finally {
			driver.close();
		}
	}

	@Test
	void aFailedStartReleasesItsGroupsAndKeepsTheBaseGroup() throws Exception {
		try (final var driver = new GroupsDriver(storageConfig(1, List.of("ens2np0")))) {
			final var group = driver.createIoGroup(1, "ens2np0");

			driver.shutdownExtraIoGroups();

			assertTrue(group.isTerminated());
			assertTrue(driver.extraIoGroups().isEmpty());
			assertFalse(driver.baseIoGroup().isShuttingDown());
		}
	}

	@Test
	void groupsShutDownTogetherWithinOneBudgetAndLateOnesAreNamed() {
		final var fast = mock(EventLoopGroup.class);
		final var slow = mock(EventLoopGroup.class);
		final Future<?> fastDone = mock(Future.class);
		final Future<?> slowDone = mock(Future.class);
		doReturn(fastDone).when(fast).shutdownGracefully(0, 0, TimeUnit.NANOSECONDS);
		doReturn(slowDone).when(slow).shutdownGracefully(0, 0, TimeUnit.NANOSECONDS);
		when(fastDone.awaitUninterruptibly(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
						.thenReturn(true);
		when(slowDone.awaitUninterruptibly(org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS)))
						.thenReturn(false);

		final var late = NettyStorageDriverBase.shutdownIoGroups(List.of(
						new NamedIoGroup("base", fast), new NamedIoGroup("ens2np0", slow)));

		assertEquals(List.of("ens2np0"), late);
		// Every shutdown starts before the first wait.
		final var order = inOrder(fast, slow, fastDone);
		order.verify(fast).shutdownGracefully(0, 0, TimeUnit.NANOSECONDS);
		order.verify(slow).shutdownGracefully(0, 0, TimeUnit.NANOSECONDS);
		order.verify(fastDone).awaitUninterruptibly(org.mockito.ArgumentMatchers.longThat(
						nanos -> nanos <= TimeUnit.SECONDS.toNanos(NettyStorageDriverBase.IO_WORKER_SHUTDOWN_TIMEOUT_SECONDS)),
						org.mockito.ArgumentMatchers.eq(TimeUnit.NANOSECONDS));
	}
}
