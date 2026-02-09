package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

/**
 * Tests for the S3RdmaStorageDriver override methods:
 * applyMetaDataHeaders(), complete(), and the timeout reaper.
 */
@SuppressWarnings("unchecked")
public class S3RdmaStorageDriverOverrideTest {

	private static final Credential TEST_CRED = Credential.getInstance("user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final long THRESHOLD = 1_048_576L;

	// ---------- applyMetaDataHeaders ----------

	@Test
	void testApplyMetaDataHeaders_injectsTokenAndZeroContentLength() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		final var tokenField = S3RdmaStorageDriver.class.getDeclaredField("CURRENT_RDMA_TOKEN");
		tokenField.setAccessible(true);
		final var threadLocal = (ThreadLocal<String>) tokenField.get(null);

		// Set a token on the current thread
		threadLocal.set("00007fffc3200000:00100000:00004d16:0000:00133f:1:fe80000000000000000000000012ab34");

		try {
			final HttpHeaders headers = new DefaultHttpHeaders();
			headers.set(HttpHeaderNames.CONTENT_LENGTH, 1048576);

			invokeApplyMetaDataHeaders(driver, headers);

			assertEquals("00007fffc3200000:00100000:00004d16:0000:00133f:1:fe80000000000000000000000012ab34",
							headers.get("x-amz-rdma-token"));
			assertEquals("0", headers.get(HttpHeaderNames.CONTENT_LENGTH));
		} finally {
			threadLocal.remove();
		}
	}

	@Test
	void testApplyMetaDataHeaders_noTokenNoChange() throws Exception {
		final var driver = newMockDriver(enabledConfig(), availableTransport());

		final HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 1048576);

		invokeApplyMetaDataHeaders(driver, headers);

		assertFalse(headers.contains("x-amz-rdma-token"));
		assertEquals("1048576", headers.get(HttpHeaderNames.CONTENT_LENGTH));
	}

	// ---------- complete (cleanup logic) ----------
	// These tests verify the RDMA cleanup behavior in complete() by testing
	// the cleanupRdmaContext() helper directly, since complete() chains to
	// the Netty parent which requires a fully initialized channel pipeline.

	@Test
	void testCleanup_deregistersAndFreesNonPooledBuffer() throws Exception {
		final var transport = availableTransport();
		final var driver = newMockDriver(enabledConfig(), transport);
		final var rdmaOps = getRdmaOps(driver);

		final var op = dataOp(OpType.CREATE, THRESHOLD);
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long mrHandle = 42L;

		final var ctx = newRdmaContext("token123", buf, mrHandle, OpType.CREATE, (int) THRESHOLD, false);
		rdmaOps.put(op, ctx);

		final boolean found = invokeCleanupRdmaContext(driver, op);

		assertTrue(found);
		assertFalse(rdmaOps.containsKey(op));
		Mockito.verify(transport).deregisterBuffer(buf, mrHandle);
		Mockito.verify(transport).freeBuffer(buf);
	}

	@Test
	void testCleanup_skipsDeregisterForPooledBuffer() throws Exception {
		final var transport = availableTransport();
		final var driver = newMockDriver(enabledConfig(), transport);
		final var rdmaOps = getRdmaOps(driver);

		final var op = dataOp(OpType.CREATE, THRESHOLD);
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long mrHandle = 55L;

		final var ctx = newRdmaContext("tokenPooled", buf, mrHandle, OpType.CREATE, (int) THRESHOLD, true);
		rdmaOps.put(op, ctx);

		final boolean found = invokeCleanupRdmaContext(driver, op);

		assertTrue(found);
		assertFalse(rdmaOps.containsKey(op));
		Mockito.verify(transport, Mockito.never()).deregisterBuffer(buf, mrHandle);
		Mockito.verify(transport).freeBuffer(buf);
	}

	@Test
	void testCleanup_returnsFalseWhenNoContext() throws Exception {
		final var transport = availableTransport();
		final var driver = newMockDriver(enabledConfig(), transport);

		final var op = dataOp(OpType.CREATE, THRESHOLD);
		final boolean found = invokeCleanupRdmaContext(driver, op);

		assertFalse(found);
		Mockito.verify(transport, Mockito.never()).deregisterBuffer(Mockito.any(), Mockito.anyLong());
		Mockito.verify(transport, Mockito.never()).freeBuffer(Mockito.any());
	}

	@Test
	void testRdmaOps_removeOnComplete() throws Exception {
		// Verify that rdmaOps.remove(op) returns the correct context
		final var driver = newMockDriver(enabledConfig(), availableTransport());
		final var rdmaOps = getRdmaOps(driver);

		final var op = dataOp(OpType.READ, THRESHOLD);
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final var ctx = newRdmaContext("tokenRead", buf, 99L, OpType.READ, (int) THRESHOLD, false);
		rdmaOps.put(op, ctx);

		assertEquals(1, rdmaOps.size());
		final var removed = rdmaOps.remove(op);
		assertNotNull(removed);
		assertTrue(rdmaOps.isEmpty());
	}

	// ---------- reapTimedOutOps ----------

	@Test
	void testReaper_cleansUpTimedOutOps() throws Exception {
		final var transport = availableTransport();
		// Use a very short timeout so the entry is already expired
		final var config = new RdmaConfig(true, THRESHOLD, true, 0, 0, "auto", "", "WARN", 1L);
		final var driver = newMockDriver(config, transport);
		final var rdmaOps = getRdmaOps(driver);

		final var op = dataOp(OpType.CREATE, THRESHOLD);
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long mrHandle = 77L;

		final var ctx = newRdmaContext("tokenExpired", buf, mrHandle, OpType.CREATE, (int) THRESHOLD, false);
		rdmaOps.put(op, ctx);

		// Sleep briefly to ensure the 1ms timeout elapses
		Thread.sleep(10);

		// Invoke the reaper
		invokeReapTimedOutOps(driver);

		// Context should have been reaped
		assertTrue(rdmaOps.isEmpty());
		Mockito.verify(transport).deregisterBuffer(buf, mrHandle);
		Mockito.verify(transport).freeBuffer(buf);
	}

	@Test
	void testReaper_doesNotReapFreshOps() throws Exception {
		final var transport = availableTransport();
		// Use a long timeout
		final var config = new RdmaConfig(true, THRESHOLD, true, 0, 0, "auto", "", "WARN", 60_000L);
		final var driver = newMockDriver(config, transport);
		final var rdmaOps = getRdmaOps(driver);

		final var op = dataOp(OpType.CREATE, THRESHOLD);
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long mrHandle = 88L;

		final var ctx = newRdmaContext("tokenFresh", buf, mrHandle, OpType.CREATE, (int) THRESHOLD, false);
		rdmaOps.put(op, ctx);

		invokeReapTimedOutOps(driver);

		// Should still be present — not timed out
		assertEquals(1, rdmaOps.size());
		Mockito.verify(transport, Mockito.never()).deregisterBuffer(Mockito.any(), Mockito.anyLong());
	}

	// ---------- RdmaConfig timeoutMs ----------

	@Test
	void testConfigDefaultTimeout() {
		final var config = new RdmaConfig(true, THRESHOLD, true, 0, 0, "auto", "", "WARN");
		assertEquals(30_000L, config.getTimeoutMs());
	}

	@Test
	void testConfigCustomTimeout() {
		final var config = new RdmaConfig(true, THRESHOLD, true, 0, 0, "auto", "", "WARN", 5_000L);
		assertEquals(5_000L, config.getTimeoutMs());
	}

	@Test
	void testConfigNullTimeout() {
		final var config = new RdmaConfig(null);
		assertEquals(30_000L, config.getTimeoutMs());
	}

	// ==================== Helpers ====================

	private static RdmaConfig enabledConfig() {
		return new RdmaConfig(true, THRESHOLD, true, 0, 0, "auto", "", "WARN");
	}

	private static RdmaTransport availableTransport() {
		final var transport = Mockito.mock(RdmaTransport.class);
		Mockito.when(transport.isAvailable()).thenReturn(true);
		return transport;
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> newMockDriver(
					final RdmaConfig config, final RdmaTransport transport) throws Exception {
		final var driver = (S3RdmaStorageDriver<Item, Operation<Item>>) Mockito.mock(
						S3RdmaStorageDriver.class,
						Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));
		setField(S3RdmaStorageDriver.class, driver, "rdmaConfig", config);
		setField(S3RdmaStorageDriver.class, driver, "rdmaTransport", transport);
		// Mockito CALLS_REAL_METHODS skips field initializers — manually set rdmaOps
		setField(S3RdmaStorageDriver.class, driver, "rdmaOps", new ConcurrentHashMap<>());
		return driver;
	}

	private static Operation<Item> dataOp(final OpType opType, final long size) {
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<>(
						0, opType, new DataItemImpl("testItem", 0, size), null, "/bucket",
						TEST_CRED, null, 0);
	}

	@SuppressWarnings("unchecked")
	private static ConcurrentMap<Operation<?>, Object> getRdmaOps(
					final S3RdmaStorageDriver<?, ?> driver) throws Exception {
		final Field f = S3RdmaStorageDriver.class.getDeclaredField("rdmaOps");
		f.setAccessible(true);
		return (ConcurrentMap<Operation<?>, Object>) f.get(driver);
	}

	private static Object newRdmaContext(final String token, final ByteBuffer buffer,
					final long mrHandle, final OpType opType, final int size,
					final boolean pooled) throws Exception {
		final var ctxClass = Class.forName(
						S3RdmaStorageDriver.class.getName() + "$RdmaContext");
		final var ctor = ctxClass.getDeclaredConstructors()[0];
		ctor.setAccessible(true);
		return ctor.newInstance(token, buffer, mrHandle, opType, size, pooled);
	}

	private static void invokeApplyMetaDataHeaders(
					final S3RdmaStorageDriver<Item, Operation<Item>> driver,
					final HttpHeaders headers) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod(
						"applyMetaDataHeaders", HttpHeaders.class);
		m.setAccessible(true);
		m.invoke(driver, headers);
	}

	private static boolean invokeCleanupRdmaContext(
					final S3RdmaStorageDriver<?, ?> driver,
					final Operation<?> op) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod(
						"cleanupRdmaContext", Operation.class);
		m.setAccessible(true);
		return (boolean) m.invoke(driver, op);
	}

	private static void invokeReapTimedOutOps(
					final S3RdmaStorageDriver<?, ?> driver) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod("reapTimedOutOps");
		m.setAccessible(true);
		m.invoke(driver);
	}

	private static void setField(final Class<?> clazz, final Object obj,
					final String name, final Object value) throws Exception {
		final Field f = clazz.getDeclaredField(name);
		f.setAccessible(true);
		f.set(obj, value);
	}
}
