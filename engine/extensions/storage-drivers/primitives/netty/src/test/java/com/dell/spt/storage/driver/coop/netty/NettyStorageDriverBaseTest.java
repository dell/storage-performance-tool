package com.dell.spt.storage.driver.coop.netty;

import static com.github.akurilov.netty.connection.pool.NonBlockingConnPool.ATTR_KEY_NODE;
import static com.dell.spt.storage.driver.coop.netty.NettyStorageDriver.ATTR_KEY_RELEASED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.netty.connection.pool.NonBlockingConnPool;
import io.netty.channel.Channel;
import io.netty.util.Attribute;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class NettyStorageDriverBaseTest {

	private NettyStorageDriverBase<Item, Operation<Item>> driver;
	private Semaphore concurrencyThrottle;
	private NonBlockingConnPool connPool;

	@BeforeEach
	void setUp() throws Exception {
		driver = mock(NettyStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

		// Set up concurrency throttle with 1 available permit
		concurrencyThrottle = new Semaphore(1, true);
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		semField.set(driver, concurrencyThrottle);

		// Set up concurrencyLimit (needed by isStarted/submit checks)
		final var limitField = StorageDriverBase.class.getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 1);

		final var isStartedField = com.dell.spt.base.concurrent.AsyncRunnableBase.class.getDeclaredField("state");
		isStartedField.setAccessible(true);
		isStartedField.set(driver, com.dell.spt.base.concurrent.AsyncRunnable.State.STARTED);

		// Set up connection pool
		connPool = mock(NonBlockingConnPool.class);
		final var poolField = NettyStorageDriverBase.class.getDeclaredField("connPool");
		poolField.setAccessible(true);
		poolField.set(driver, connPool);

		// We do not want to execute real requests over the mocked channel
		doNothing().when(driver).sendRequest(any(Channel.class), any(Operation.class));
		doNothing().when(driver).complete(any(), any(Operation.class));
	}

	@Test
	void submit_retriesInactiveConnectionsFromPool() throws Exception {
		final Operation<Item> op = mock(Operation.class);
		when(op.type()).thenReturn(OpType.READ);

		// 1. The first connection the pool yields is INACTIVE (simulating a silent drop)
		final Channel deadConn = mock(Channel.class);
		when(deadConn.isActive()).thenReturn(false);

		// 2. The second connection the pool yields is ACTIVE
		final Channel liveConn = mock(Channel.class);
		when(liveConn.isActive()).thenReturn(true);

		final Attribute<Boolean> releasedAttr = mock(Attribute.class);
		when(liveConn.attr(ATTR_KEY_RELEASED)).thenReturn(releasedAttr);

		final Attribute opAttr = mock(Attribute.class);
		when(liveConn.attr(NettyStorageDriverBase.ATTR_KEY_OPERATION)).thenReturn(opAttr);

		final Attribute<String> nodeAttr = mock(Attribute.class);
		when(nodeAttr.get()).thenReturn("node1");
		when(liveConn.attr(ATTR_KEY_NODE)).thenReturn(nodeAttr);

		// Configure pool to return dead first, then live
		when(connPool.lease()).thenReturn(deadConn, liveConn);

		// Act: submit the operation
		driver.submit(op);

		// Assert: The driver should have requested a lease twice
		verify(connPool, times(2)).lease();

		// Assert: The dead connection should have been closed and released back to the pool
		verify(deadConn).close();
		verify(connPool).release(deadConn);

		// Assert: The live connection was actually used to send the request
		verify(liveConn.attr(ATTR_KEY_RELEASED)).set(Boolean.FALSE);
		verify(driver).sendRequest(liveConn, op);
	}
}
