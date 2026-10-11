package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import io.netty.channel.embedded.EmbeddedChannel;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Multipart accounting when an RDMA part completes outside the normal response path: every
 * completion must settle the part on its parent exactly once, or retries and finalization lose
 * track of the upload.
 */
class RdmaPartSettlementTest {

	private static final Credential CREDENTIAL = Credential.getInstance("user1", "test-secret");
	private static final int PART_SIZE = 1024;
	private static final int MAX_PART_RETRIES = 3;

	@AfterEach
	void closeDrivers() throws Exception {
		S3RdmaStorageDriverTestSupport.closeCreatedDrivers();
	}

	@Test
	void markPartSettledDecrementsOnlyPartsParent() throws Exception {
		final var parent = upload(2);
		final var part = parent.subOperations().get(0);
		assertEquals(2, pending(parent));

		S3RdmaStorageDriver.markPartSettled(part);
		assertEquals(1, pending(parent));

		S3RdmaStorageDriver.markPartSettled(new DataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("plain", 0, PART_SIZE), null, "/bucket", CREDENTIAL, null, 0));
		assertEquals(1, pending(parent), "a non-part operation has no parent to settle");
	}

	@Test
	void reapedPartWithoutResponseIsSettledExactlyOnce() throws Exception {
		final var parent = upload(2);
		final var part = parent.subOperations().get(0);
		exhaustRetries(part);

		reap(part);

		assertEquals(1, pending(parent));
	}

	@Test
	void reapedPartWithStartedResponseIsSettledExactlyOnce() throws Exception {
		final var parent = upload(2);
		final var part = parent.subOperations().get(0);
		exhaustRetries(part);
		part.startRequest();
		part.startResponse();

		reap(part);

		assertEquals(1, pending(parent), "finishResponse() settles it; the reaper must not settle it again");
	}

	private static CompositeDataOperationImpl<DataItem> upload(final int parts) throws Exception {
		final var item = new DataItemImpl("obj", 0, (long) PART_SIZE * parts);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false));
		return new CompositeDataOperationImpl<>(0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0, PART_SIZE);
	}

	/** Exhausted retries keep handleCompleted() from re-enqueueing the part on an unstarted driver. */
	private static void exhaustRetries(final PartialDataOperation<?> part) {
		for (int i = 0; i < MAX_PART_RETRIES; i++) {
			part.incrementRetryCount();
		}
	}

	@SuppressWarnings("unchecked")
	private static void reap(final PartialDataOperation<?> part) throws Exception {
		final var config = new RdmaConfig(true, 0, false, "auto", "", "WARN", 1L);
		final var transport = new FakeRdmaTransport(config);
		transport.init("http://10.0.0.1:9020", "key", "secret");
		final var driver = S3RdmaStorageDriverTestSupport.newDriver(config, transport);
		final Output<Operation<Item>> results = Mockito.mock(Output.class);
		Mockito.when(results.put(Mockito.<Operation<Item>> any())).thenReturn(true);
		driver.operationResultOutput(results);

		final ByteBuffer buffer = ByteBuffer.allocateDirect(PART_SIZE);
		final long handle = transport.registerBuffer(buffer, PART_SIZE);
		final Operation<?> op = (Operation<?>) part;
		rdmaOps(driver).put(op, newRdmaContext(buffer, handle));
		final var channel = new EmbeddedChannel();
		channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.TRUE);
		invoke(driver, "bindRequestChannel", new Class<?>[]{io.netty.channel.Channel.class, Operation.class
		}, channel, op);
		invoke(driver, "onRequestDispatched", new Class<?>[]{io.netty.channel.Channel.class, Operation.class
		}, channel, op);
		Thread.sleep(5);
		invoke(driver, "reapTimedOutOps", new Class<?>[0]);
		channel.finishAndReleaseAll();
		assertEquals(Operation.Status.FAIL_IO, op.status());
	}

	private static int pending(final CompositeDataOperationImpl<?> parent) throws Exception {
		final Field field = CompositeDataOperationImpl.class.getDeclaredField("pendingSubTasksCount");
		field.setAccessible(true);
		return ((AtomicInteger) field.get(parent)).get();
	}

	@SuppressWarnings("unchecked")
	private static ConcurrentMap<Operation<?>, Object> rdmaOps(final S3RdmaStorageDriver<?, ?> driver) throws Exception {
		final Field field = S3RdmaStorageDriver.class.getDeclaredField("rdmaOps");
		field.setAccessible(true);
		return (ConcurrentMap<Operation<?>, Object>) field.get(driver);
	}

	private static Object newRdmaContext(final ByteBuffer buffer, final long handle) throws Exception {
		final var ctor = Class.forName(S3RdmaStorageDriver.class.getName() + "$RdmaContext")
						.getDeclaredConstructors()[0];
		ctor.setAccessible(true);
		return ctor.newInstance("part-token", buffer, handle, OpType.CREATE, PART_SIZE);
	}

	private static void invoke(final Object target, final String name, final Class<?>[] types, final Object... args)
					throws Exception {
		final Method method = S3RdmaStorageDriver.class.getDeclaredMethod(name, types);
		method.setAccessible(true);
		method.invoke(target, args);
	}
}
