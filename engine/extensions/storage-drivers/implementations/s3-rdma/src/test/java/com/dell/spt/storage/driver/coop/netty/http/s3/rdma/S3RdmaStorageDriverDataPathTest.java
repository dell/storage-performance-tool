package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.dell.spt.storage.driver.coop.netty.NettyStorageDriver;
import com.github.akurilov.commons.io.Output;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

/**
 * Tests for the S3RdmaStorageDriver data path that do NOT require RDMA hardware.
 *
 * <p>Uses {@link FakeRdmaTransport} to simulate RDMA operations and verify:
 * <ul>
 *   <li>Buffer data copy ({@code transferDataItemToBuffer})</li>
 *   <li>Resource lifecycle tracking (register → deregister)</li>
 *   <li>Failure injection and fallback behavior</li>
 * </ul>
 */
@SuppressWarnings("unchecked")
public class S3RdmaStorageDriverDataPathTest {

	private static final Credential TEST_CRED = Credential.getInstance(
					"user1", "u5QtPuQx+W5nrrQQEg7nArBqSgC8qLiDt2RhQthb");
	private static final long THRESHOLD = 1_048_576L;
	private static final long SEED = 0x12345678ABCDEF00L;
	private static final int LAYER_SIZE = 4096;

	// ---------- FakeRdmaTransport basic contract ----------

	@Test
	void testFakeTransport_initAndAvailability() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		assertFalse(fake.isAvailable(), "Should not be available before init");

		assertTrue(fake.init("http://10.0.0.1:9020", "key", "secret"));
		assertTrue(fake.isAvailable(), "Should be available after init");
	}

	@Test
	void testFakeTransport_initRejectsNullEndpoint() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		assertFalse(fake.init(null, "key", "secret"));
		assertFalse(fake.isAvailable());
	}

	@Test
	void testFakeTransport_registerAndDeregisterTracking() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final ByteBuffer buf1 = ByteBuffer.allocateDirect(1024);
		final ByteBuffer buf2 = ByteBuffer.allocateDirect(2048);

		final long h1 = fake.registerBuffer(buf1, 1024);
		final long h2 = fake.registerBuffer(buf2, 2048);

		assertTrue(h1 > 0);
		assertTrue(h2 > 0);
		assertNotEquals(h1, h2, "Handles must be unique");
		assertEquals(2, fake.getRegisterCount());
		assertEquals(2, fake.getActiveRegistrationCount());
		assertFalse(fake.areAllDeregistered());

		fake.deregisterBuffer(buf1, h1);
		assertEquals(1, fake.getActiveRegistrationCount());

		fake.deregisterBuffer(buf2, h2);
		assertTrue(fake.areAllDeregistered());
		assertEquals(2, fake.getDeregisterCount());
	}

	@Test
	void testFakeTransport_tokenGeneration() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long handle = fake.registerBuffer(buf, 1024);

		final String token = fake.generateToken(handle, 1024);
		assertNotNull(token);
		// Token format: addr:size:rkey:lid:dctn:g:gid (7 colon-separated fields)
		final String[] parts = token.split(":");
		assertEquals(7, parts.length, "Token must have 7 colon-separated fields");
	}

	@Test
	void testFakeTransport_failureInjection_registration() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");
		fake.setFailAfterNRegistrations(2);

		final long h1 = fake.registerBuffer(ByteBuffer.allocateDirect(64), 64);
		final long h2 = fake.registerBuffer(ByteBuffer.allocateDirect(64), 64);
		final long h3 = fake.registerBuffer(ByteBuffer.allocateDirect(64), 64);

		assertTrue(h1 > 0, "First registration should succeed");
		assertTrue(h2 > 0, "Second registration should succeed");
		assertEquals(0, h3, "Third registration should fail");
	}

	@Test
	void testFakeTransport_failureInjection_tokenGeneration() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final long handle = fake.registerBuffer(ByteBuffer.allocateDirect(64), 64);
		assertNotNull(fake.generateToken(handle, 64), "Token should succeed normally");

		fake.setFailTokenGeneration(true);
		assertNull(fake.generateToken(handle, 64), "Token should fail after injection");
	}

	@Test
	void testFakeTransport_close() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		fake.registerBuffer(ByteBuffer.allocateDirect(64), 64);
		assertEquals(1, fake.getActiveRegistrationCount());

		fake.close();
		assertFalse(fake.isAvailable(), "Should not be available after close");
		assertEquals(0, fake.getActiveRegistrationCount(), "Close should clear registrations");
	}

	// ---------- transferDataItemToBuffer ----------

	@Test
	void testTransferDataItemToBuffer_copiesCorrectData() throws Exception {
		final DataInput dataInput = new SeedDataInput(SEED, LAYER_SIZE, 1, false);
		final int size = 256;

		// Create a DataItem with known content
		final DataItemImpl item = new DataItemImpl("testItem", 0, size, 0);
		item.dataInput(dataInput);

		// Read the expected content directly from the data input
		final ByteBuffer expected = ByteBuffer.allocateDirect(size);
		final ByteBuffer ringBuf = dataInput.getLayer(0).asReadOnlyBuffer();
		ringBuf.position(0);
		ringBuf.limit(size);
		expected.put(ringBuf);
		expected.flip();

		// Now invoke transferDataItemToBuffer via the driver
		final var driver = newMockDriver(enabledConfig(), new FakeRdmaTransport(enabledConfig()));
		final ByteBuffer actual = ByteBuffer.allocateDirect(size);

		invokeTransferDataItemToBuffer(driver, item, actual, size);

		// Compare contents byte by byte
		expected.rewind();
		actual.rewind();
		for (int i = 0; i < size; i++) {
			assertEquals(expected.get(i), actual.get(i),
							"Byte mismatch at position " + i);
		}
	}

	@Test
	void testTransferDataItemToBuffer_fillsEntireBuffer() throws Exception {
		final DataInput dataInput = new SeedDataInput(SEED, LAYER_SIZE, 1, false);
		final int size = 1024;

		final DataItemImpl item = new DataItemImpl("fillTest", 0, size, 0);
		item.dataInput(dataInput);

		final var driver = newMockDriver(enabledConfig(), new FakeRdmaTransport(enabledConfig()));
		final ByteBuffer buf = ByteBuffer.allocateDirect(size);

		invokeTransferDataItemToBuffer(driver, item, buf, size);

		// After the method, buffer should be flipped: position=0, limit=size
		assertEquals(0, buf.position(), "Buffer should be flipped (position=0)");
		assertEquals(size, buf.limit(), "Buffer should be flipped (limit=size)");
		assertEquals(size, buf.remaining(), "All bytes should be readable");
	}

	@Test
	void testTransferDataItemToBuffer_smallSize() throws Exception {
		final DataInput dataInput = new SeedDataInput(SEED, LAYER_SIZE, 1, false);
		final int size = 1;

		final DataItemImpl item = new DataItemImpl("tinyItem", 0, size, 0);
		item.dataInput(dataInput);

		final var driver = newMockDriver(enabledConfig(), new FakeRdmaTransport(enabledConfig()));
		final ByteBuffer buf = ByteBuffer.allocateDirect(size);

		invokeTransferDataItemToBuffer(driver, item, buf, size);

		assertEquals(1, buf.remaining());
		// Verify the single byte matches the data input
		final ByteBuffer ring = dataInput.getLayer(0).asReadOnlyBuffer();
		ring.position(0);
		assertEquals(ring.get(), buf.get());
	}

	@Test
	void testTransferDataItemToBuffer_nonZeroOffset() throws Exception {
		final DataInput dataInput = new SeedDataInput(SEED, LAYER_SIZE, 1, false);
		final int size = 128;
		final long offset = 512;

		final DataItemImpl item = new DataItemImpl("offsetItem", offset, size, 0);
		item.dataInput(dataInput);

		// Expected: read from ring buffer at position=offset
		final ByteBuffer expected = ByteBuffer.allocateDirect(size);
		final ByteBuffer ring = dataInput.getLayer(0).asReadOnlyBuffer();
		ring.position((int) offset);
		ring.limit((int) offset + size);
		expected.put(ring);
		expected.flip();

		final var driver = newMockDriver(enabledConfig(), new FakeRdmaTransport(enabledConfig()));
		final ByteBuffer actual = ByteBuffer.allocateDirect(size);

		invokeTransferDataItemToBuffer(driver, item, actual, size);

		expected.rewind();
		actual.rewind();
		for (int i = 0; i < size; i++) {
			assertEquals(expected.get(i), actual.get(i),
							"Byte mismatch at position " + i + " (offset=" + offset + ")");
		}
	}

	// ---------- Resource lifecycle with FakeRdmaTransport ----------

	@Test
	void testCleanup_deregistersOnFakeTransport() throws Exception {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final var driver = newMockDriver(enabledConfig(), fake);
		final var rdmaOps = getRdmaOps(driver);

		// Simulate what submitRdma does: register, generate token, store context
		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long mrHandle = fake.registerBuffer(buf, 1024);
		assertTrue(mrHandle > 0);
		assertEquals(1, fake.getActiveRegistrationCount());

		// Store context in rdmaOps (simulating submitRdma)
		final var op = dataOp(OpType.CREATE, THRESHOLD);
		final var ctx = newRdmaContext(
						fake.generateToken(mrHandle, 1024), buf, mrHandle, OpType.CREATE, 1024);
		rdmaOps.put(op, ctx);

		// Simulate complete() cleanup
		invokeCleanupRdmaContext(driver, op);

		// Verify the buffer was deregistered
		assertTrue(fake.areAllDeregistered(),
						"Cleanup should deregister the buffer via transport");
		assertEquals(1, fake.getDeregisterCount());
		assertTrue(rdmaOps.isEmpty());
	}

	@Test
	void testReaper_deregistersAllTimedOutOpsOnFakeTransport() throws Exception {
		// Use 1ms timeout so entries are immediately expired.
		// Set opResultOut on the mock driver so completion can publish each timeout result,
		// allowing the reaper to iterate through all entries.
		final var config = new RdmaConfig(true, THRESHOLD, true, "auto", "", "WARN", 1L);
		final var fake = new FakeRdmaTransport(config);
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final var driver = newMockDriver(config, fake);
		setFieldInHierarchy(driver, "opResultOut", Mockito.mock(Output.class));
		final var rdmaOps = getRdmaOps(driver);
		final List<EmbeddedChannel> channels = new ArrayList<>();

		// Register 5 buffers and store dispatched, channel-bound contexts
		for (int i = 0; i < 5; i++) {
			final ByteBuffer buf = ByteBuffer.allocateDirect(64);
			final long mrHandle = fake.registerBuffer(buf, 64);
			final var op = dataOp(OpType.CREATE, THRESHOLD);
			final var ctx = newRdmaContext("tok" + i, buf, mrHandle, OpType.CREATE, 64);
			rdmaOps.put(op, ctx);
			final var channel = new EmbeddedChannel();
			channel.attr(NettyStorageDriver.ATTR_KEY_RELEASED).set(Boolean.TRUE);
			invokeBindRequestChannel(driver, channel, op);
			invokeOnRequestDispatched(driver, channel, op);
			channels.add(channel);
		}

		assertEquals(5, fake.getActiveRegistrationCount());

		// Let entries expire
		Thread.sleep(10);

		// Run reaper — should iterate all entries without NPE
		invokeReapTimedOutOps(driver);

		// All should be deregistered
		assertTrue(fake.areAllDeregistered(),
						"Reaper should deregister all timed-out buffers");
		assertEquals(5, fake.getDeregisterCount());
		assertTrue(rdmaOps.isEmpty(), "All entries should be removed from rdmaOps");
		channels.forEach(EmbeddedChannel::finishAndReleaseAll);
	}

	@Test
	void testMultipleOps_allResourcesFreed() throws Exception {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final var driver = newMockDriver(enabledConfig(), fake);
		final var rdmaOps = getRdmaOps(driver);

		// Simulate 10 operations registering and being cleaned up
		for (int i = 0; i < 10; i++) {
			final ByteBuffer buf = ByteBuffer.allocateDirect(128);
			final long mrHandle = fake.registerBuffer(buf, 128);
			final var op = dataOp(OpType.CREATE, THRESHOLD);
			final var ctx = newRdmaContext("tok" + i, buf, mrHandle, OpType.CREATE, 128);
			rdmaOps.put(op, ctx);
			invokeCleanupRdmaContext(driver, op);
		}

		assertEquals(10, fake.getRegisterCount());
		assertEquals(10, fake.getDeregisterCount());
		assertTrue(fake.areAllDeregistered(),
						"Every registered buffer must be deregistered");
	}

	@Test
	void testFakeTransport_registrationFailure_noLeak() throws Exception {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");
		fake.setFailAfterNRegistrations(0); // all registrations fail

		final var driver = newMockDriver(enabledConfig(), fake);

		// Attempt registration — should fail
		final ByteBuffer buf = ByteBuffer.allocateDirect(64);
		final long handle = fake.registerBuffer(buf, 64);
		assertEquals(0, handle, "Registration should fail");
		assertEquals(0, fake.getActiveRegistrationCount(),
						"Failed registration should not track the buffer");
	}

	// ---------- Double-deregister detection ----------

	@Test
	void testFakeTransport_detectsDoubleDeregister() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final ByteBuffer buf = ByteBuffer.allocateDirect(64);
		final long handle = fake.registerBuffer(buf, 64);

		// First deregister succeeds
		fake.deregisterBuffer(buf, handle);
		assertTrue(fake.wasDeregistered(handle));

		// Second deregister of the same handle should throw
		assertThrows(IllegalStateException.class, () -> fake.deregisterBuffer(buf, handle),
						"Double deregister should be detected");
	}

	@Test
	void testFakeTransport_doubleDeregisterSuppressedInNonStrictMode() {
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");
		fake.setStrictDeregister(false);

		final ByteBuffer buf = ByteBuffer.allocateDirect(64);
		final long handle = fake.registerBuffer(buf, 64);

		fake.deregisterBuffer(buf, handle);
		// In non-strict mode, double deregister should not throw
		assertDoesNotThrow(() -> fake.deregisterBuffer(buf, handle));
		assertEquals(2, fake.getDeregisterCount());
	}

	// ---------- Token failure → buffer must be deregistered ----------

	@Test
	void testTokenFailure_bufferMustBeDeregistered() {
		// Simulates the submitRdma pattern: when generateToken() fails,
		// the buffer MUST be deregistered to prevent memory leaks.
		final var fake = new FakeRdmaTransport(enabledConfig());
		fake.init("http://10.0.0.1:9020", "key", "secret");

		final ByteBuffer buf = ByteBuffer.allocateDirect(1024);
		final long mrHandle = fake.registerBuffer(buf, 1024);
		assertTrue(mrHandle > 0);
		assertEquals(1, fake.getActiveRegistrationCount());

		// Simulate token generation failure
		fake.setFailTokenGeneration(true);
		final String token = fake.generateToken(mrHandle, 1024);
		assertNull(token, "Token generation should fail");

		// The driver's submitRdma deregisters the buffer when token is null:
		//   rdmaTransport.deregisterBuffer(buf, mrHandle);
		fake.deregisterBuffer(buf, mrHandle);

		assertTrue(fake.areAllDeregistered(),
						"Buffer must be deregistered after token failure");
	}

	// ---------- RdmaTransport.IdentityKey: identity vs content-based hashing ----------

	@Test
	void testIdentityKey_differentBuffersSameContentAreNotEqual() throws Exception {
		// Two ByteBuffers with identical content must produce different IdentityKeys.
		// This prevents the wrong buffer from being deregistered if content-based
		// equals/hashCode were used instead of identity-based.
		final ByteBuffer buf1 = ByteBuffer.allocateDirect(8);
		final ByteBuffer buf2 = ByteBuffer.allocateDirect(8);
		buf1.putLong(0, 0xDEADBEEFCAFEBABEL);
		buf2.putLong(0, 0xDEADBEEFCAFEBABEL);

		final Object key1 = newIdentityKey(buf1);
		final Object key2 = newIdentityKey(buf2);

		assertNotEquals(key1, key2,
						"Different buffer instances with same content must NOT be equal");
		assertNotEquals(key1.hashCode(), key2.hashCode(),
						"Different buffer instances should typically have different identity hash codes");
	}

	@Test
	void testIdentityKey_sameBufferInstanceIsEqual() throws Exception {
		final ByteBuffer buf = ByteBuffer.allocateDirect(8);
		buf.putLong(0, 12345L);

		final Object key1 = newIdentityKey(buf);
		final Object key2 = newIdentityKey(buf);

		assertEquals(key1, key2,
						"Same buffer instance must produce equal IdentityKeys");
		assertEquals(key1.hashCode(), key2.hashCode(),
						"Same buffer instance must produce equal hash codes");
	}

	@Test
	void testIdentityKey_usedCorrectlyInConcurrentMap() throws Exception {
		// Verify that ConcurrentHashMap with IdentityKey correctly
		// distinguishes two buffers with identical content
		final ByteBuffer buf1 = ByteBuffer.allocateDirect(8);
		final ByteBuffer buf2 = ByteBuffer.allocateDirect(8);
		buf1.putLong(0, 42L);
		buf2.putLong(0, 42L); // same content

		final var map = new ConcurrentHashMap<>();
		map.put(newIdentityKey(buf1), 100L);
		map.put(newIdentityKey(buf2), 200L);

		// Both entries should exist independently
		assertEquals(2, map.size(),
						"Two buffers with same content should be tracked independently");
		assertEquals(100L, map.get(newIdentityKey(buf1)));
		assertEquals(200L, map.get(newIdentityKey(buf2)));
	}

	// ==================== Helpers ====================

	private static RdmaConfig enabledConfig() {
		return new RdmaConfig(true, THRESHOLD, true, "auto", "", "WARN");
	}

	private static S3RdmaStorageDriver<Item, Operation<Item>> newMockDriver(
					final RdmaConfig config, final RdmaTransport transport) throws Exception {
		final var driver = (S3RdmaStorageDriver<Item, Operation<Item>>) Mockito.mock(
						S3RdmaStorageDriver.class,
						Mockito.withSettings().lenient().defaultAnswer(CALLS_REAL_METHODS));
		setField(S3RdmaStorageDriver.class, driver, "rdmaConfig", config);
		setField(S3RdmaStorageDriver.class, driver, "rdmaTransport", transport);
		setField(S3RdmaStorageDriver.class, driver, "rdmaOps", new ConcurrentHashMap<>());
		return driver;
	}

	private static Operation<Item> dataOp(final OpType opType, final long size) {
		return (Operation<Item>) (Operation<?>) new DataOperationImpl<>(
						0, opType, new DataItemImpl("testItem", 0, size), null, "/bucket",
						TEST_CRED, null, 0);
	}

	@SuppressWarnings("unchecked")
	private static java.util.concurrent.ConcurrentMap<Operation<?>, Object> getRdmaOps(
					final S3RdmaStorageDriver<?, ?> driver) throws Exception {
		final Field f = S3RdmaStorageDriver.class.getDeclaredField("rdmaOps");
		f.setAccessible(true);
		return (java.util.concurrent.ConcurrentMap<Operation<?>, Object>) f.get(driver);
	}

	private static Object newRdmaContext(final String token, final ByteBuffer buffer,
					final long mrHandle, final OpType opType, final int size) throws Exception {
		final var ctxClass = Class.forName(
						S3RdmaStorageDriver.class.getName() + "$RdmaContext");
		final var ctor = ctxClass.getDeclaredConstructors()[0];
		ctor.setAccessible(true);
		return ctor.newInstance(token, buffer, mrHandle, opType, size);
	}

	private static void invokeTransferDataItemToBuffer(
					final S3RdmaStorageDriver<?, ?> driver,
					final DataItem item, final ByteBuffer dst, final int size) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod(
						"transferDataItemToBuffer", DataItem.class, ByteBuffer.class, int.class);
		m.setAccessible(true);
		m.invoke(driver, item, dst, size);
	}

	private static boolean invokeCleanupRdmaContext(
					final S3RdmaStorageDriver<?, ?> driver,
					final Operation<?> op) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod(
						"cleanupRdmaContext", Operation.class);
		m.setAccessible(true);
		return (boolean) m.invoke(driver, op);
	}

	private static void invokeBindRequestChannel(
					final S3RdmaStorageDriver<?, ?> driver,
					final EmbeddedChannel channel,
					final Operation<?> op) throws Exception {
		final Method method = S3RdmaStorageDriver.class.getDeclaredMethod(
						"bindRequestChannel", io.netty.channel.Channel.class, Operation.class);
		method.setAccessible(true);
		method.invoke(driver, channel, op);
	}

	private static void invokeOnRequestDispatched(
					final S3RdmaStorageDriver<?, ?> driver,
					final EmbeddedChannel channel,
					final Operation<?> op) throws Exception {
		final Method method = S3RdmaStorageDriver.class.getDeclaredMethod(
						"onRequestDispatched", io.netty.channel.Channel.class, Operation.class);
		method.setAccessible(true);
		method.invoke(driver, channel, op);
	}

	private static void invokeReapTimedOutOps(
					final S3RdmaStorageDriver<?, ?> driver) throws Exception {
		final Method m = S3RdmaStorageDriver.class.getDeclaredMethod("reapTimedOutOps");
		m.setAccessible(true);
		m.invoke(driver);
	}

	private static Object newIdentityKey(final ByteBuffer buffer) throws Exception {
		final Class<?> keyClass = Class.forName(RdmaTransport.class.getName() + "$IdentityKey");
		final Constructor<?> ctor = keyClass.getDeclaredConstructors()[0];
		ctor.setAccessible(true);
		return ctor.newInstance(buffer);
	}

	private static void setField(final Class<?> clazz, final Object obj,
					final String name, final Object value) throws Exception {
		final Field f = clazz.getDeclaredField(name);
		f.setAccessible(true);
		f.set(obj, value);
	}

	private static void setFieldInHierarchy(final Object obj,
					final String name, final Object value) throws Exception {
		Class<?> clazz = obj.getClass();
		while (clazz != null) {
			try {
				final Field f = clazz.getDeclaredField(name);
				f.setAccessible(true);
				f.set(obj, value);
				return;
			} catch (final NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			}
		}
		throw new NoSuchFieldException(
						name + " not found in hierarchy of " + obj.getClass().getName());
	}
}
