package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;
import com.github.akurilov.commons.io.Output;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * A pooled buffer goes back to the pool only after its request's response arrived. Requests that
 * end any other way may still be accessed by the server, so their buffers are deregistered.
 */
class RdmaBufferLifecycleTest {

	private static final Credential CREDENTIAL = Credential.getInstance("user1", "test-secret");
	private static final int SIZE = 4096;

	@AfterEach
	void closeDrivers() throws Exception {
		S3RdmaStorageDriverTestSupport.closeCreatedDrivers();
	}

	@Test
	void reapedRequestDiscardsItsPooledBuffer() throws Exception {
		final var transport = transport();
		final var driver = driver(new RdmaConfig(true, 0, false, "auto", "", "WARN", 1L), transport);
		final var op = op();
		final var pooled = track(driver, op);
		final var channel = new EmbeddedChannel();
		channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.TRUE);
		invoke(driver, "bindRequestChannel", new Class<?>[]{io.netty.channel.Channel.class, Operation.class}, channel, op);
		invoke(driver, "onRequestDispatched", new Class<?>[]{io.netty.channel.Channel.class, Operation.class}, channel, op);
		Thread.sleep(5);

		invoke(driver, "reapTimedOutOps", new Class<?>[0]);
		channel.finishAndReleaseAll();

		assertTrue(transport.wasDeregistered(pooled.mrHandle()));
		assertEquals(0, driver.bufferPool().idleBuffers());
		assertEquals(1, driver.bufferPool().discarded.sum());
	}

	@Test
	void closeDiscardsBuffersOfRequestsStillInFlight() throws Exception {
		final var transport = transport();
		final var driver = driver(new RdmaConfig(true, 0, false, "auto", "", "WARN", 60_000L), transport);
		final var pooled = track(driver, op());
		final RdmaBufferPool pool = driver.bufferPool();

		driver.close();

		assertTrue(transport.wasDeregistered(pooled.mrHandle()));
		assertEquals(0, pool.liveBuffers());
		assertEquals(1, pool.discarded.sum());
	}

	@Test
	void failedResultPublicationAfterTokenFailureKeepsThePoolConsistent() throws Exception {
		// The released buffer belongs to the pool again; the exception cleanup must not return it twice.
		final var transport = transport();
		transport.setFailTokenGeneration(true);
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(
						new RdmaConfig(true, 0, false, "auto", "", "WARN", 60_000L), transport);
		@SuppressWarnings("unchecked")
		final Output<Operation<Item>> results = Mockito.mock(Output.class);
		Mockito.when(results.put(Mockito.<Operation<Item>> any())).thenThrow(new IllegalStateException("output closed"));
		driver.operationResultOutput(results);
		driver.start();

		try {
			invoke(driver, "submitRdma", new Class<?>[]{Operation.class}, op(OpType.READ));
		} catch (final InvocationTargetException expected) {
			// publication failure may propagate
		}

		final RdmaBufferPool pool = driver.bufferPool();
		assertEquals(0, pool.invalidReturns.sum(), pool.summary());
		assertEquals(1, pool.idleBuffers());
		assertEquals(1, pool.liveBuffers());
		assertEquals(1, transport.getActiveRegistrationCount(), "the idle buffer must stay registered");
	}

	@Test
	void shippedDefaultsEnableThePool() throws Exception {
		assertTrue(new RdmaConfig(null).isBufferPoolEnabled());
		assertNotNull(driver(new RdmaConfig(null), transport()).bufferPool());
	}

	private static FakeRdmaTransport transport() {
		final var transport = new FakeRdmaTransport(new RdmaConfig(true, 0, false, "auto", "", "WARN"));
		transport.init("http://10.0.0.1:9020", "key", "secret");
		return transport;
	}

	@SuppressWarnings("unchecked")
	private static S3RdmaStorageDriver<Item, Operation<Item>> driver(
					final RdmaConfig config, final FakeRdmaTransport transport) throws Exception {
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(config, transport);
		final Output<Operation<Item>> results = Mockito.mock(Output.class);
		Mockito.when(results.put(Mockito.<Operation<Item>> any())).thenReturn(true);
		driver.operationResultOutput(results);
		return driver;
	}

	private static Operation<Item> op() {
		return op(OpType.CREATE);
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> op(final OpType type) {
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<>(
						0, type, new DataItemImpl("obj", 0, SIZE), null, "/bucket", CREDENTIAL, null, 0);
	}

	/** Registers {@code op} as an in-flight RDMA request holding a pool buffer. */
	private static RdmaBufferPool.PooledBuffer track(
					final S3RdmaStorageDriver<?, ?> driver, final Operation<?> op) throws Exception {
		final var pooled = driver.bufferPool().acquire(SIZE);
		assertNotNull(pooled);
		final var ctor = Class.forName(S3RdmaStorageDriver.class.getName() + "$RdmaContext")
						.getDeclaredConstructors()[0];
		ctor.setAccessible(true);
		final Object ctx = ctor.newInstance("token", pooled.buffer(), pooled.mrHandle(), OpType.CREATE, SIZE);
		final Field pooledField = ctx.getClass().getDeclaredField("pooled");
		pooledField.setAccessible(true);
		pooledField.set(ctx, pooled);
		rdmaOps(driver).put(op, ctx);
		return pooled;
	}

	@SuppressWarnings("unchecked")
	private static ConcurrentMap<Operation<?>, Object> rdmaOps(final S3RdmaStorageDriver<?, ?> driver) throws Exception {
		final Field field = S3RdmaStorageDriver.class.getDeclaredField("rdmaOps");
		field.setAccessible(true);
		return (ConcurrentMap<Operation<?>, Object>) field.get(driver);
	}

	private static void invoke(final Object target, final String name, final Class<?>[] types, final Object... args)
					throws Exception {
		final Method method = S3RdmaStorageDriver.class.getDeclaredMethod(name, types);
		method.setAccessible(true);
		method.invoke(target, args);
	}
}
